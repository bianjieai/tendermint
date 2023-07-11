package global

import (
	"context"
	"fmt"
	"github.com/tendermint/tendermint/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otrace "go.opentelemetry.io/otel/trace"
)

var (
	HeightCtx         *JaegerCtx
	RoundCtx          *JaegerCtx
	ProposeCtx        context.Context
	MakeBlockCtx      context.Context
	FinalizeCommitCtx context.Context
	ApplyBlockCtx     context.Context
	BeginBlockCtx     context.Context
	DeliverTxAsyncCtx context.Context
	DeliverTxCtx      context.Context
	EndBlockCtx       context.Context
	CommitCtx         context.Context
	CheckTcCtx        context.Context
	RunTxCtx          context.Context
)

type JaegerCtx struct {
	Ctx      context.Context
	Span     otrace.Span
	TimeSpan otrace.Span
	Height   int64
	Round    int32
}

func (JaegerCtx) WithHeightSpan(span otrace.Span) {
	HeightCtx.Span = span
}

func (JaegerCtx) EndSpan(span otrace.Span) {
	if span != nil {
		span.End()
	}
}

func WithHeightCtx(ctx context.Context) {
	HeightCtx.Ctx = ctx
}

func getHeightCtx(height int64) *JaegerCtx {
	if HeightCtx != nil && HeightCtx.Height == height {
		return HeightCtx
	}
	if HeightCtx != nil && HeightCtx.Span != nil {
		HeightCtx.Span.End()
	}
	CleanCtx()
	HeightCtx := &JaegerCtx{
		Ctx:    context.Background(),
		Height: height,
	}
	return HeightCtx
}

func WitRoundCtx(ctx context.Context) {
	RoundCtx.Ctx = ctx
}

func getRoundCtx(height int64, round int32) *JaegerCtx {
	if RoundCtx != nil && RoundCtx.Height == height && RoundCtx.Round == round {
		return RoundCtx
	}
	if RoundCtx != nil && RoundCtx.Span != nil {
		RoundCtx.Span.End()
	}
	RoundCtx := &JaegerCtx{
		Ctx:    context.Background(),
		Height: height,
	}
	return RoundCtx
}

func FinishRoundSpan(height int64, round int32) {
	if RoundCtx != nil && RoundCtx.Height == height && RoundCtx.Round == round {
		if RoundCtx.Span != nil {
			RoundCtx.Span.End()
			TraceHandleTimeout(height)
		}
		RoundCtx = nil
	}
}

func withFinalizeCommitCtx(ctx context.Context) {
	FinalizeCommitCtx = ctx
}

func getFinalizeCommitCtx() context.Context {
	return FinalizeCommitCtx
}

func withApplyBlockCtx(ctx context.Context) {
	ApplyBlockCtx = ctx
}

func getApplyBlockCtx() context.Context {
	return ApplyBlockCtx
}

func WithBeginBlockCtx(ctx context.Context) {
	BeginBlockCtx = ctx
}

func GetBeginBlockCtx() context.Context {
	return BeginBlockCtx
}

func withDeliverTxAsyncCtx(ctx context.Context) {
	DeliverTxAsyncCtx = ctx
}

func getDeliverTxAsyncCtx() context.Context {
	return DeliverTxAsyncCtx
}

func withDeliverTxCtx(ctx context.Context) {
	DeliverTxCtx = ctx
}

func getDeliverTxCtx() context.Context {
	return DeliverTxCtx
}

func withRunTxCtx(ctx context.Context) {
	RunTxCtx = ctx
}

func getRunTxCtx() context.Context {
	return RunTxCtx
}

func WithEndBlockCtx(ctx context.Context) {
	EndBlockCtx = ctx
}

func GetEndBlockCtx() context.Context {
	return EndBlockCtx
}

func WithCommitCtx(ctx context.Context) {
	CommitCtx = ctx
}

func GetCommitCtx() context.Context {
	return CommitCtx
}

func WithProposeCtx(ctx context.Context) {
	ProposeCtx = ctx
}

func getProposeCtx() context.Context {
	return ProposeCtx
}

func withMakeBlockCtx(ctx context.Context) {
	MakeBlockCtx = ctx
}

func getMakeBlockCtx() context.Context {
	return MakeBlockCtx
}

func WithCheckTcCtx(ctx context.Context) {
	CheckTcCtx = ctx
}

func GetCheckTxCtx() context.Context {
	return CheckTcCtx
}

func WithLogInfo(span otrace.Span, info string) {
	if span != nil {
		span.SetAttributes(attribute.String("msg", info))
	}
}

func WithLogInfoKV(span otrace.Span, key string, value string) {
	if span != nil {
		span.SetAttributes(attribute.String("msg", value))
	}
}
func WithErrInfo(span otrace.Span, err error) {
	if span != nil {
		span.RecordError(err)
	}
}

func TraceHeight(height int64) otrace.Span {
	trace := GetNewHeightTracer()
	if trace != nil {
		HeightCtx = getHeightCtx(height)
		HeightCtx.Ctx, HeightCtx.Span = trace.Start(HeightCtx.Ctx, "Tendermint.Height")
		HeightCtx.Span.SetAttributes(attribute.Int64("height", height))
		HeightCtx.Span.SetAttributes(attribute.Int("round", 0))
	}
	return nil
}

func TraceNewRound(height int64, round int32) otrace.Span {
	trace := GetHeightTrace()
	if trace != nil {
		heightCtx := getHeightCtx(height)
		RoundCtx = getRoundCtx(height, round)
		RoundCtx.Ctx, RoundCtx.Span = trace.Start(heightCtx.Ctx, "Tendermint.Round")
		RoundCtx.Span.SetAttributes(attribute.Int64("height", height))
		RoundCtx.Span.SetAttributes(attribute.Int("round", int(round)))
		return RoundCtx.Span
	}
	return nil
}

func TraceHandleTimeout(height int64) {
	trace := GetHeightTrace()
	if trace != nil {
		ctx := getHeightCtx(height)
		_, ctx.TimeSpan = trace.Start(ctx.Ctx, "Tendermint.Time.wait")
		ctx.TimeSpan.SetAttributes(attribute.Int64("height", height))
	}
}

func TracePropose(height int64, round int32) otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getRoundCtx(height, round)
		enterProposeTrace := GetHeightTrace()
		proposeCtx, span := enterProposeTrace.Start(ctx.Ctx, "Tendermint.Propose")
		WithProposeCtx(proposeCtx)
		span.SetAttributes(attribute.Int64("height", height))
		span.SetAttributes(attribute.Int("round", int(round)))
		return span

	}
	return nil
}

func TracePrevote(height int64, round int32) otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getRoundCtx(height, round)
		enterProposeTrace := GetHeightTrace()
		_, span := enterProposeTrace.Start(ctx.Ctx, "Tendermint.Prevote")
		span.SetAttributes(attribute.Int64("height", height))
		span.SetAttributes(attribute.Int("round", int(round)))
		return span

	}
	return nil
}
func TraceCreateProposalBlock(height int64, appHash string) otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getProposeCtx()
		roposeTrace := GetHeightTrace()
		makeBlockCtx, span := roposeTrace.Start(ctx, "Tendermint.CreateProposalBlock")
		withMakeBlockCtx(makeBlockCtx)
		span.SetAttributes(attribute.Int64("height", height))
		span.SetAttributes(attribute.String("appHash", fmt.Sprintf(`state.AppHash:%X`, appHash)))
		return span
	}
	return nil
}

func TraceReapTx() otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getMakeBlockCtx()
		roposeTrace := GetHeightTrace()
		_, span := roposeTrace.Start(ctx, "Tendermint.CreateProposalBlock.ReapTx")
		return span
	}
	return nil
}

func TracePrecommit(height int64, round int32) otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getRoundCtx(height, round)
		enterProposeTrace := GetHeightTrace()
		_, span := enterProposeTrace.Start(ctx.Ctx, "Tendermint.Precommit")
		span.SetAttributes(attribute.Int64("height", height))
		span.SetAttributes(attribute.Int("round", int(round)))
		return span

	}
	return nil
}

func TraceFinalizeCommit(height int64, round int32) otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getRoundCtx(height, round)
		finalizeCommitTrace := GetHeightTrace()
		finalizeCommitCtx, span := finalizeCommitTrace.Start(ctx.Ctx, "Tendermint.FinalizeCommit")
		withFinalizeCommitCtx(finalizeCommitCtx)
		span.SetAttributes(attribute.Int64("height", height))
		span.SetAttributes(attribute.Int("round", int(round)))
		return span
	}
	return nil
}

func TraceFinalizeCommitSpan(name string) otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getFinalizeCommitCtx()
		if ctx != nil {
			applyBlockTrace := GetHeightTrace()
			_, span := applyBlockTrace.Start(ctx, "Tendermint.FinalizeCommit."+name)
			return span
		}
	}
	return nil
}

func TraceApplyBlock() otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getFinalizeCommitCtx()
		applyBlockTrace := GetHeightTrace()
		if ctx == nil {
			ctx = context.Background()
			applyBlockTrace = GetNewTrace("ReplayBkock")
			applyBlockCtx, span := applyBlockTrace.Start(ctx, "Tendermint.ReplayBkock.ApplyBlock")
			withApplyBlockCtx(applyBlockCtx)
			return span
		} else {
			applyBlockCtx, span := applyBlockTrace.Start(ctx, "Tendermint.ApplyBlock")
			withApplyBlockCtx(applyBlockCtx)
			return span
		}
	}
	return nil
}

func TraceBeginBlock() otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getApplyBlockCtx()
		if ctx != nil {
			trace := GetHeightTrace()
			beginBlockCtx, beginBlockSpan := trace.Start(ctx, "Tendermint.BeginBlock")
			WithBeginBlockCtx(beginBlockCtx)
			return beginBlockSpan
		}
	}
	return nil
}

func TracDeliverTxAsync() otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getApplyBlockCtx()
		if ctx != nil {
			trace := GetHeightTrace()
			deliverTxAsyncCtx, deliverTxAsyncSpan := trace.Start(ctx, "Tendermint.Txs-DeliverTxs")
			withDeliverTxAsyncCtx(deliverTxAsyncCtx)
			return deliverTxAsyncSpan
		}
	}
	return nil
}

func TracDeliverTx() otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getDeliverTxAsyncCtx()
		if ctx != nil {
			trace := GetHeightTrace()
			deliverTxCtx, deliverTxSpan := trace.Start(ctx, "CosmosSdk.DeliverTx")
			withDeliverTxCtx(deliverTxCtx)
			return deliverTxSpan
		}
	}
	return nil
}

func TraceRunTx() otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getDeliverTxCtx()
		if ctx != nil {
			trace := GetHeightTrace()
			runTxCtx, runTxSpan := trace.Start(ctx, "CosmosSdk.RunTx")
			withRunTxCtx(runTxCtx)
			return runTxSpan
		}
	}
	return nil
}

func TraceRunHandle() otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getRunTxCtx()
		if ctx != nil {
			trace := GetHeightTrace()
			_, runHandleSpan := trace.Start(ctx, "CosmosSdk.RunHandle")
			return runHandleSpan
		}
	}
	return nil
}

func TraceMsg() otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getRunTxCtx()
		if ctx != nil {
			trace := GetHeightTrace()
			_, runMsgSpan := trace.Start(ctx, "CosmosSdk.RunMsg")
			return runMsgSpan
		}
	}
	return nil
}

func TraceEndBlock() otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getApplyBlockCtx()
		if ctx != nil {
			trace := GetHeightTrace()
			endBlockCtx, endBlockSpan := trace.Start(ctx, "Tendermint.EndBlock")
			WithEndBlockCtx(endBlockCtx)
			return endBlockSpan
		}
	}
	return nil
}

func TraceCommit() otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := getApplyBlockCtx()
		if ctx != nil {
			commitTrace := GetHeightTrace()
			commitCtx, span := commitTrace.Start(ctx, "Tendermint.Commit")
			WithCommitCtx(commitCtx)
			return span
		}
	}
	return nil

}

func TraceMempoolUpdate() otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := GetCommitCtx()
		if ctx != nil {
			mempoolUpdate := GetHeightTrace()
			_, span := mempoolUpdate.Start(ctx, "Tendermint.Commit.MempoolUpdate")
			return span
		}
	}
	return nil
}

func TraceCheckTx(tx types.Tx) otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		checkTxTrace := GetHeightTrace()
		checkTxCtx, span := checkTxTrace.Start(context.Background(), "Tendermint.CheckTx")
		WithCheckTcCtx(checkTxCtx)
		span.SetAttributes(attribute.String("tx_hash", fmt.Sprintf(`tx_hash:%X`, tx.Hash())))
		return span
	}
	return nil
}

func TraceCheckTxAsSync() otrace.Span {
	tr := otel.GetTracerProvider()
	if tr != nil {
		ctx := GetCheckTxCtx()
		if ctx != nil {
			checkTxTrace := GetHeightTrace()
			_, span := checkTxTrace.Start(ctx, "Tendermint.CheckTxAsSync")
			return span
		}
	}
	return nil
}

func CleanCtx() {
	if HeightCtx != nil && HeightCtx.TimeSpan != nil {
		HeightCtx.TimeSpan.End()
	}
	if HeightCtx != nil && HeightCtx.Span != nil {
		HeightCtx.Span.End()
	}
	if RoundCtx != nil && RoundCtx.Span != nil {
		RoundCtx.Span.End()
	}
	HeightCtx = nil
	RoundCtx = nil
	FinalizeCommitCtx = nil
	ApplyBlockCtx = nil
	BeginBlockCtx = nil
	DeliverTxCtx = nil
	EndBlockCtx = nil
	CommitCtx = nil
	ProposeCtx = nil
	MakeBlockCtx = nil
}
