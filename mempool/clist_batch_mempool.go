package mempool

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	abci "github.com/tendermint/tendermint/abci/types"
	cfg "github.com/tendermint/tendermint/config"
	auto "github.com/tendermint/tendermint/libs/autofile"
	"github.com/tendermint/tendermint/libs/clist"
	"github.com/tendermint/tendermint/libs/log"
	tmmath "github.com/tendermint/tendermint/libs/math"
	tmos "github.com/tendermint/tendermint/libs/os"
	tmsync "github.com/tendermint/tendermint/libs/sync"
	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/proxy"
	"github.com/tendermint/tendermint/types"
)

const batchInterval int = 2

//--------------------------------------------------------------------------------

// CListBatchMempool is an ordered in-memory pool for transactions before they are
// proposed in a consensus round. Transaction validity is checked using the
// CheckTx abci message before the transaction is added to the pool. The
// mempool uses a concurrent list structure for storing transactions that can
// be efficiently accessed by multiple concurrent readers.
type CListBatchMempool struct {
	// Atomic integers
	height   int64 // the last block Update()'d to
	txsBytes int64 // total size of mempool, in bytes

	// notify listeners (ie. consensus) when txs are available
	notifiedTxsAvailable bool
	txsAvailable         chan struct{} // fires once for each height, when the mempool is not empty

	config *cfg.MempoolConfig

	// Exclusive mutex for Update method to prevent concurrent execution of
	// CheckTx or ReapMaxBytesMaxGas(ReapMaxTxs) methods.
	updateMtx      tmsync.RWMutex
	batchUpdateMtx tmsync.RWMutex
	preCheck       PreCheckFunc
	postCheck      PostCheckFunc

	batchPreCheck  BatchPreCheckFunc
	batchPostCheck BatchPostCheckFunc

	wal     *auto.AutoFile  // a log of mempool txs
	txs     chan *mempoolTx // concurrent linked-list of good txs
	txBatch *clist.CList    // 交易批量链表

	proxyAppConn proxy.AppConnMempool

	// Track whether we're rechecking txs.
	// These are not protected by a mutex and are expected to be mutated in
	// serial (ie. by abci responses which are called in serial).
	recheckCursor *clist.CElement // next expected response
	recheckEnd    *clist.CElement // re-checking stops here

	// Map for quick access to txs to record sender in CheckTx.
	// txsMap: txKey -> CElement
	txsBatchMap sync.Map

	// Keep a cache of already-seen txs.
	// This reduces the pressure on the proxyApp.
	cache txCache

	logger log.Logger

	metrics *Metrics
}

var _ Mempool = &CListBatchMempool{}

// CListBatchMempoolOption sets an optional parameter on the mempool.
type CListBatchMempoolOption func(*CListBatchMempool)

// NewCListBatchMempool returns a new mempool with the given configuration and connection to an application.
func NewCListBatchMempool(
	config *cfg.MempoolConfig,
	proxyAppConn proxy.AppConnMempool,
	height int64,
	options ...CListBatchMempoolOption,
) *CListBatchMempool {
	mempool := &CListBatchMempool{
		config:        config,
		proxyAppConn:  proxyAppConn,
		txs:           make(chan *mempoolTx, config.Size),
		txBatch:       clist.New(),
		height:        height,
		recheckCursor: nil,
		recheckEnd:    nil,
		logger:        log.NewNopLogger(),
		metrics:       NopMetrics(),
	}
	if config.CacheSize > 0 {
		mempool.cache = newMapTxCache(config.CacheSize)
	} else {
		mempool.cache = nopTxCache{}
	}
	proxyAppConn.SetResponseCallback(mempool.globalCb)
	for _, option := range options {
		option(mempool)
	}

	go mempool.addTxToTxBatch()
	return mempool
}

// NOTE: not thread safe - should only be called once, on startup
func (mem *CListBatchMempool) EnableTxsAvailable() {
	mem.txsAvailable = make(chan struct{}, 1)
}

// SetLogger sets the Logger.
func (mem *CListBatchMempool) SetLogger(l log.Logger) {
	mem.logger = l
}

// WithBatchPreCheck sets a filter for the mempool to reject a tx if f(tx) returns
// false. This is ran before CheckTx. Only applies to the first created block.
// After that, Update overwrites the existing value.
func WithBatchPreCheck(f PreCheckFunc) CListBatchMempoolOption {
	return func(mem *CListBatchMempool) { mem.preCheck = f }
}

func WithBatchPreCheck2(f BatchPreCheckFunc) CListBatchMempoolOption {
	return func(mem *CListBatchMempool) { mem.batchPreCheck = f }
}

// WithBatchPostCheck sets a filter for the mempool to reject a tx if f(tx) returns
// false. This is ran after CheckTx. Only applies to the first created block.
// After that, Update overwrites the existing value.
func WithBatchPostCheck(f PostCheckFunc) CListBatchMempoolOption {
	return func(mem *CListBatchMempool) { mem.postCheck = f }
}

// WithBatchPostCheck sets a filter for the mempool to reject a tx if f(tx) returns
// false. This is ran after CheckTx. Only applies to the first created block.
// After that, Update overwrites the existing value.
func WithBatchPostCheck2(f BatchPostCheckFunc) CListBatchMempoolOption {
	return func(mem *CListBatchMempool) { mem.batchPostCheck = f }
}

// WithBatchMetrics sets the metrics.
func WithBatchMetrics(metrics *Metrics) CListBatchMempoolOption {
	return func(mem *CListBatchMempool) { mem.metrics = metrics }
}

func (mem *CListBatchMempool) InitWAL() error {
	var (
		walDir  = mem.config.WalDir()
		walFile = walDir + "/wal"
	)

	const perm = 0700
	if err := tmos.EnsureDir(walDir, perm); err != nil {
		return err
	}

	af, err := auto.OpenAutoFile(walFile)
	if err != nil {
		return fmt.Errorf("can't open autofile %s: %w", walFile, err)
	}

	mem.wal = af
	return nil
}

func (mem *CListBatchMempool) CloseWAL() {
	if err := mem.wal.Close(); err != nil {
		mem.logger.Error("Error closing WAL", "err", err)
	}
	mem.wal = nil
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListBatchMempool) Lock() {
	mem.updateMtx.Lock()
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListBatchMempool) Unlock() {
	mem.updateMtx.Unlock()
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListBatchMempool) Size() int {
	return mem.txBatch.Len()
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListBatchMempool) TxsBytes() int64 {
	return atomic.LoadInt64(&mem.txsBytes)
}

// Lock() must be help by the caller during execution.
func (mem *CListBatchMempool) FlushAppConn() error {
	return mem.proxyAppConn.FlushSync()
}

// XXX: Unsafe! Calling Flush may leave mempool in inconsistent state.
func (mem *CListBatchMempool) Flush() {
	mem.updateMtx.RLock()
	defer mem.updateMtx.RUnlock()

	_ = atomic.SwapInt64(&mem.txsBytes, 0)
	mem.cache.Reset()
	mem.txs = make(chan *mempoolTx, mem.config.Size)
}

// TxsBatchFront returns the first transaction in the ordered list for peer
// goroutines to call .NextWait() on.
// FIXME: leaking implementation details!
//
// Safe for concurrent use by multiple goroutines.
func (mem *CListBatchMempool) TxsBatchFront() *clist.CElement {
	return mem.txBatch.Front()
}

// TxsWaitChan returns a channel to wait on transactions. It will be closed
// once the mempool is not empty (ie. the internal `mem.txs` has at least one
// element)
//
// Safe for concurrent use by multiple goroutines.
func (mem *CListBatchMempool) TxsWaitChan() <-chan struct{} {
	return mem.txBatch.WaitChan()
}

// It blocks if we're waiting on Update() or Reap().
// cb: A callback from the CheckTx command.
//     It gets called from another goroutine.
// CONTRACT: Either cb will get called, or err returned.
//
// Safe for concurrent use by multiple goroutines.
func (mem *CListBatchMempool) CheckTx(tx types.Tx, cb func(*abci.Response), txInfo TxInfo) error {
	txSize := len(tx)

	if err := mem.isFull(txSize); err != nil {
		return err
	}

	if txSize > mem.config.MaxTxBytes {
		return ErrTxTooLarge{mem.config.MaxTxBytes, txSize}
	}

	// NOTE: writing to the WAL and calling proxy must be done before adding tx
	// to the cache. otherwise, if either of them fails, next time CheckTx is
	// called with tx, ErrTxInCache will be returned without tx being checked at
	// all even once.
	if mem.wal != nil {
		// TODO: Notify administrators when WAL fails
		_, err := mem.wal.Write(append([]byte(tx), newline...))
		if err != nil {
			return fmt.Errorf("wal.Write: %w", err)
		}
	}

	// NOTE: proxyAppConn may error if tx buffer is full
	if err := mem.proxyAppConn.Error(); err != nil {
		return err
	}

	if !mem.cache.Push(tx) {
		return ErrTxInCache
	}
	mem.updateMtx.RLock()
	// use defer to unlock mutex because application (*local client*) might panic
	defer mem.updateMtx.RUnlock()

	if mem.preCheck != nil {
		if err := mem.preCheck(tx); err != nil {
			return ErrPreCheck{err}
		}
	}

	reqRes := mem.proxyAppConn.CheckTxAsync(abci.RequestCheckTx{Tx: tx})
	reqRes.SetCallback(mem.reqResCb(tx, txInfo.SenderID, txInfo.SenderP2PID, cb))

	return nil
}

func (mem *CListBatchMempool) CheckBatchTx(txs types.Txs, txInfo TxInfo) error {
	txBatchSize := txs.ByteSize()

	if err := mem.isFull(txBatchSize); err != nil {
		return err
	}

	if txBatchSize > mem.config.MaxTxBytes*maxTxBatchSize {
		return ErrTxTooLarge{mem.config.MaxTxBytes, txBatchSize}
	}

	// NOTE: writing to the WAL and calling proxy must be done before adding tx
	// to the cache. otherwise, if either of them fails, next time CheckTx is
	// called with tx, ErrTxInCache will be returned without tx being checked at
	// all even once.
	if mem.wal != nil {
		// TODO: Notify administrators when WAL fails
		var toWrite []byte
		for _, tx := range txs {
			toWrite = append(toWrite, append(tx, newline...)...)
		}
		_, err := mem.wal.Write(toWrite)
		if err != nil {
			return fmt.Errorf("wal.Write: %w", err)
		}
	}

	// NOTE: proxyAppConn may error if tx buffer is full
	if err := mem.proxyAppConn.Error(); err != nil {
		return err
	}

	txsId := txs.TxsId()
	if !mem.cache.PushKey(txsId[:]) {
		// Record a new sender for a tx we've already seen.
		// Note it's possible a tx is still in the cache but no longer in the mempool
		// (eg. after committing a block, txs are removed from mempool but not cache),
		// so we only record the sender for txs still in the mempool.
		if e, ok := mem.txsBatchMap.Load(txsId); ok {
			memTx := e.(*clist.CElement).Value.(*mempoolTxBatch)
			memTx.senders.LoadOrStore(txInfo.SenderID, true)
			// TODO: consider punishing peer for dups,
			// its non-trivial since invalid txs can become valid,
			// but they can spam the same tx with little cost to them atm.
		}

		return ErrTxInCache
	}
	mem.batchUpdateMtx.RLock()
	// use defer to unlock mutex because application (*local client*) might panic
	defer mem.batchUpdateMtx.RUnlock()

	if mem.batchPreCheck != nil {
		if err := mem.batchPreCheck(txs); err != nil {
			return ErrPreCheck{err}
		}
	}
	txBatch := &mempoolTxBatch{
		size:     len(txs),
		txs:      make([]types.Tx, len(txs)),
		txHeader: make([]byte, len(txs)),
	}
	wg := &sync.WaitGroup{}
	wg.Add(txBatch.size)
	for i, tx := range txs {
		reqRes := mem.proxyAppConn.CheckTxAsync(abci.RequestCheckTx{Tx: tx})
		reqRes.SetCallback(mem.batchReqResCb(tx, txInfo.SenderP2PID, txBatch, i, wg))
	}
	wg.Wait()
	mem.addTxBatch(txBatch)
	return nil
}

// Global callback that will be called after every ABCI response.
// Having a single global callback avoids needing to set a callback for each request.
// However, processing the checkTx response requires the peerID (so we can track which txs we heard from who),
// and peerID is not included in the ABCI request, so we have to set request-specific callbacks that
// include this information. If we're not in the midst of a recheck, this function will just return,
// so the request specific callback can do the work.
//
// When rechecking, we don't need the peerID, so the recheck callback happens
// here.
func (mem *CListBatchMempool) globalCb(req *abci.Request, res *abci.Response) {
	if mem.recheckCursor == nil {
		return
	}

	mem.metrics.RecheckTimes.Add(1)
	mem.resCbRecheck(req, res)

	// update metrics
	mem.metrics.Size.Set(float64(mem.Size()))
}

// Request specific callback that should be set on individual reqRes objects
// to incorporate local information when processing the response.
// This allows us to track the peer that sent us this tx, so we can avoid sending it back to them.
// NOTE: alternatively, we could include this information in the ABCI request itself.
//
// External callers of CheckTx, like the RPC, can also pass an externalCb through here that is called
// when all other response processing is complete.
//
// Used in CheckTx to record PeerID who sent us the tx.
func (mem *CListBatchMempool) reqResCb(
	tx []byte,
	peerID uint16,
	peerP2PID p2p.ID,
	externalCb func(*abci.Response),
) func(res *abci.Response) {
	return func(res *abci.Response) {
		if mem.recheckCursor != nil {
			// this should never happen
			panic("recheck cursor is not nil in reqResCb")
		}

		mem.resCbFirstTime(tx, peerID, peerP2PID, res)

		// update metrics
		mem.metrics.Size.Set(float64(mem.Size()))

		// passed in by the caller of CheckTx, eg. the RPC
		if externalCb != nil {
			externalCb(res)
		}
	}
}

func (mem *CListBatchMempool) batchReqResCb(tx []byte, peerP2PID p2p.ID, txBatchID *mempoolTxBatch, txBatchIndex int, wg *sync.WaitGroup) func(res *abci.Response) {
	return func(res *abci.Response) {
		if mem.recheckCursor != nil {
			// this should never happen
			panic("recheck cursor is not nil in reqResCb")
		}

		mem.batchResCbFirstTime(tx, peerP2PID, txBatchID, txBatchIndex, res, wg)

		// update metrics
		mem.metrics.Size.Set(float64(mem.Size()))

	}
}

// Called from:
//  - resCbFirstTime (lock not held) if tx is valid
func (mem *CListBatchMempool) addTx(memTx *mempoolTx) {
	mem.txs <- memTx
}

func (mem *CListBatchMempool) addTxBatch(memTx *mempoolTxBatch) {
	e := mem.txBatch.PushBack(memTx)
	mem.txsBatchMap.Store(memTx.hash, e)
	mem.txsBatchMap.Store(TxKey(memTx.txs[0]), e)
	atomic.AddInt64(&mem.txsBytes, int64(memTx.txByteSize))
	mem.metrics.TxSizeBytes.Observe(float64(memTx.txByteSize))
}

func (mem *CListBatchMempool) addTxToTxBatch() {
	var txBatch = &mempoolTxBatch{}
	var timeTicker = time.NewTicker(10 * time.Second)
	for {
		select {
		case tx := <-mem.txs:
			txBatch.AddTx(tx)
			if !txBatch.isFull() {
				continue
			}
		case <-time.After(time.Duration(batchInterval) * time.Second):
		case <-timeTicker.C:

		}
		if txBatch.size > 0 {
			txBatch.hash = sha256.Sum256(txBatch.txHeader)
			mem.addTxBatch(txBatch)
			txBatch = &mempoolTxBatch{}
			timeTicker.Reset(10 * time.Second)
		}

	}
}

// Called from:
//  - Update (lock held) if tx was committed
func (mem *CListBatchMempool) removeTxBatch(tx types.Tx, elem *clist.CElement, removeFromCache bool) {
	txBatch := elem.Value.(*mempoolTxBatch)
	mem.txBatch.Remove(elem)
	elem.DetachPrev()
	mem.txsBatchMap.Delete(txBatch.hash)
	mem.txsBatchMap.Delete(TxKey(tx))
	atomic.AddInt64(&mem.txsBytes, int64(-txBatch.txByteSize))

	if removeFromCache {
		mem.cache.Remove(tx)
		txsId := txBatch.txs.TxsId()
		mem.cache.RemoveKey(txsId[:])
	}
}

// Called from:
//  - Update (lock held) if tx was committed
// 	- resCbRecheck (lock not held) if tx was invalidated
func (mem *CListBatchMempool) removeTx(tx types.Tx, elem *clist.CElement, removeFromCache bool) {

	mem.txBatch.Remove(elem)
	elem.DetachPrev()
	atomic.AddInt64(&mem.txsBytes, int64(-len(tx)))

	if removeFromCache {
		mem.cache.Remove(tx)
	}
}

func (mem *CListBatchMempool) isFull(txSize int) error {
	var (
		memSize  = mem.Size()
		txsBytes = mem.TxsBytes()
	)

	if memSize >= mem.config.Size || int64(txSize)+txsBytes > mem.config.MaxTxsBytes {
		return ErrMempoolIsFull{
			memSize, mem.config.Size,
			txsBytes, mem.config.MaxTxsBytes,
		}
	}

	return nil
}

// callback, which is called after the app checked the tx for the first time.
//
// The case where the app checks the tx for the second and subsequent times is
// handled by the resCbRecheck callback.
func (mem *CListBatchMempool) resCbFirstTime(
	tx []byte,
	peerID uint16,
	peerP2PID p2p.ID,
	res *abci.Response,
) {
	switch r := res.Value.(type) {
	case *abci.Response_CheckTx:
		var postCheckErr error
		if mem.postCheck != nil {
			postCheckErr = mem.postCheck(tx, r.CheckTx)
		}
		if (r.CheckTx.Code == abci.CodeTypeOK) && postCheckErr == nil {
			// Check mempool isn't full again to reduce the chance of exceeding the
			// limits.
			if err := mem.isFull(len(tx)); err != nil {
				// remove from cache (mempool might have a space later)
				mem.cache.Remove(tx)
				mem.logger.Error(err.Error())
				return
			}

			// 把交易放到channel
			//wrapBatchCh <- tx
			mem.logger.Debug("added good transaction",
				"tx", txID(tx),
				"res", r,
				"height", mem.height,
				"total", mem.Size(),
			)

			memTx := &mempoolTx{
				height:    mem.height,
				gasWanted: r.CheckTx.GasWanted,
				tx:        tx,
			}
			mem.addTx(memTx)
			//mem.logger.Debug("added good transaction",
			//	"tx", txID(tx),
			//	"res", r,
			//	"height", memTx.height,
			//	"total", mem.Size(),
			//)
			//mem.notifyTxsAvailable()
		} else {
			// ignore bad transaction
			mem.logger.Debug("rejected bad transaction",
				"tx", txID(tx), "peerID", peerP2PID, "res", r, "err", postCheckErr)
			mem.metrics.FailedTxs.Add(1)
			if !mem.config.KeepInvalidTxsInCache {
				// remove from cache (it might be good later)
				mem.cache.Remove(tx)
			}
		}
	default:
		// ignore other messages
	}
}

func (mem *CListBatchMempool) batchResCbFirstTime(tx []byte, peerP2PID p2p.ID, txBatch *mempoolTxBatch, index int, res *abci.Response, wg *sync.WaitGroup) {
	defer wg.Done()
	switch r := res.Value.(type) {
	case *abci.Response_CheckTx:
		var postCheckErr error
		if mem.postCheck != nil {
			postCheckErr = mem.postCheck(tx, r.CheckTx)
		}
		if (r.CheckTx.Code == abci.CodeTypeOK) && postCheckErr == nil {
			// Check mempool isn't full again to reduce the chance of exceeding the
			// limits.
			if err := mem.isFull(len(tx)); err != nil {
				// remove from cache (mempool might have a space later)
				mem.cache.RemoveKey(tx)
				mem.logger.Error(err.Error())
				return
			}

			// 把交易放到channel
			//wrapBatchCh <- tx
			mem.logger.Debug("added good transaction",
				"tx", txID(tx),
				"res", r,
				"height", mem.height,
				"total", mem.Size(),
			)

			memTx := &mempoolTx{
				height:    mem.height,
				gasWanted: r.CheckTx.GasWanted,
				tx:        tx,
			}
			txBatch.AddTxIndex(memTx, index)
		} else {
			// ignore bad transaction
			mem.logger.Debug("rejected bad transaction",
				"tx", txID(tx), "peerID", peerP2PID, "res", r, "err", postCheckErr)
			mem.metrics.FailedTxs.Add(1)
			if !mem.config.KeepInvalidTxsInCache {
				// remove from cache (it might be good later)
				mem.cache.Remove(tx)
			}
		}
	default:
		// ignore other messages
	}
}

// callback, which is called after the app rechecked the tx.
//
// The case where the app checks the tx for the first time is handled by the
// resCbFirstTime callback.
func (mem *CListBatchMempool) resCbRecheck(req *abci.Request, res *abci.Response) {
	switch r := res.Value.(type) {
	case *abci.Response_CheckTx:
		tx := req.GetCheckTx().Tx
		memTx := mem.recheckCursor.Value.(*mempoolTx)
		if !bytes.Equal(tx, memTx.tx) {
			panic(fmt.Sprintf(
				"Unexpected tx response from proxy during recheck\nExpected %X, got %X",
				memTx.tx,
				tx))
		}
		var postCheckErr error
		if mem.postCheck != nil {
			postCheckErr = mem.postCheck(tx, r.CheckTx)
		}
		if (r.CheckTx.Code == abci.CodeTypeOK) && postCheckErr == nil {
			// Good, nothing to do.
		} else {
			// Tx became invalidated due to newly committed block.
			mem.logger.Debug("tx is no longer valid", "tx", txID(tx), "res", r, "err", postCheckErr)
			// NOTE: we remove tx from the cache because it might be good later
			mem.removeTx(tx, mem.recheckCursor, !mem.config.KeepInvalidTxsInCache)
		}
		if mem.recheckCursor == mem.recheckEnd {
			mem.recheckCursor = nil
		} else {
			mem.recheckCursor = mem.recheckCursor.Next()
		}
		if mem.recheckCursor == nil {
			// Done!
			mem.logger.Debug("done rechecking txs")

			// incase the recheck removed all txs
			if mem.Size() > 0 {
				mem.notifyTxsAvailable()
			}
		}
	default:
		// ignore other messages
	}
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListBatchMempool) TxsAvailable() <-chan struct{} {
	return mem.txsAvailable
}

func (mem *CListBatchMempool) notifyTxsAvailable() {
	if mem.Size() == 0 {
		panic("notified txs available but mempool is empty!")
	}
	if mem.txsAvailable != nil && !mem.notifiedTxsAvailable {
		// channel cap is 1, so this will send once
		mem.notifiedTxsAvailable = true
		select {
		case mem.txsAvailable <- struct{}{}:
		default:
		}
	}
}

func (mem *CListBatchMempool) ReapMaxBytesMaxGas(maxBytes, maxGas int64) types.Txs {
	mem.updateMtx.RLock()
	defer mem.updateMtx.RUnlock()

	// TODO: we will get a performance boost if we have a good estimate of avg
	// size per tx, and set the initial capacity based off of that.
	// txs := make([]types.Tx, 0, tmmath.MinInt(mem.txs.Len(), max/mem.avgTxSize))
	txs := types.Txs{}
	var totalGas int64
	var dataSize int64
	for e := mem.txBatch.Front(); e != nil; e = e.Next() {
		memTx := e.Value.(*mempoolTxBatch)
		dataSize += int64(memTx.txByteSize)

		// Check total size requirement
		if maxBytes > -1 && dataSize > maxBytes {
			return txs
		}
		// Check total gas requirement.
		// If maxGas is negative, skip this check.
		// Since newTotalGas < masGas, which
		// must be non-n、egative, it follows that this won't overflow.
		newTotalGas := totalGas + memTx.gasWanted
		if maxGas > -1 && newTotalGas > maxGas {
			return txs
		}
		totalGas = newTotalGas
		txs = append(txs, memTx.txs...)
	}
	return txs
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListBatchMempool) ReapMaxTxs(max int) types.Txs {
	mem.updateMtx.RLock()
	defer mem.updateMtx.RUnlock()

	if max < 0 {
		max = mem.txBatch.Len()
	}

	txs := make([]types.Tx, 0, tmmath.MinInt(mem.txBatch.Len(), max))
	for e := mem.txBatch.Front(); e != nil && len(txs) <= max; e = e.Next() {
		memTx := e.Value.(*mempoolTxBatch)
		txs = append(txs, memTx.txs...)
	}
	return txs
}

// Lock() must be help by the caller during execution.
func (mem *CListBatchMempool) Update(
	height int64,
	txs types.Txs,
	deliverTxResponses []*abci.ResponseDeliverTx,
	preCheck PreCheckFunc,
	postCheck PostCheckFunc,
) error {
	// Set height
	mem.height = height
	mem.notifiedTxsAvailable = false

	if preCheck != nil {
		mem.preCheck = preCheck
	}
	if postCheck != nil {
		mem.postCheck = postCheck
	}

	for i, tx := range txs {
		if deliverTxResponses[i].Code == abci.CodeTypeOK {
			// Add valid committed tx to the cache (if missing).
			_ = mem.cache.Push(tx)
		} else if !mem.config.KeepInvalidTxsInCache {
			// Allow invalid transactions to be resubmitted.
			mem.cache.Remove(tx)
		}

		// Remove committed tx from the mempool.
		//
		// Note an evil proposer can drop valid txs!
		// Mempool before:
		//   100 -> 101 -> 102
		// Block, proposed by an evil proposer:
		//   101 -> 102
		// Mempool after:
		//   100
		// https://github.com/tendermint/tendermint/issues/3322.
		if e, ok := mem.txsBatchMap.Load(TxKey(tx)); ok {
			mem.removeTxBatch(tx, e.(*clist.CElement), false)
		}
	}

	// Either recheck non-committed txs to see if they became invalid
	// or just notify there're some txs left.
	if mem.Size() > 0 {
		if mem.config.Recheck {
			mem.logger.Debug("recheck txs", "numtxs", mem.Size(), "height", height)
			mem.recheckTxs()
			// At this point, mem.txs are being rechecked.
			// mem.recheckCursor re-scans mem.txs and possibly removes some txs.
			// Before mem.Reap(), we should wait for mem.recheckCursor to be nil.
		} else {
			mem.notifyTxsAvailable()
		}
	}

	// Update metrics
	mem.metrics.Size.Set(float64(mem.Size()))

	return nil
}

// 理论上不会有这段逻辑
func (mem *CListBatchMempool) recheckTxs() {
	if mem.Size() == 0 {
		panic("recheckTxs is called, but the mempool is empty")
	}

	mem.recheckCursor = mem.txBatch.Front()
	mem.recheckEnd = mem.txBatch.Back()

	// Push txs to proxyAppConn
	// NOTE: globalCb may be called concurrently.
	for e := mem.txBatch.Front(); e != nil; e = e.Next() {
		memTx := e.Value.(*mempoolTx)
		mem.proxyAppConn.CheckTxAsync(abci.RequestCheckTx{
			Tx:   memTx.tx,
			Type: abci.CheckTxType_Recheck,
		})
	}

	mem.proxyAppConn.FlushAsync()
}

//--------------------------------------------------------------------------------

//--------------------------------------------------------------------------------
