package algo

import (
	"github.com/tendermint/tendermint/crypto"
	"github.com/tendermint/tendermint/crypto/ed25519"
	"github.com/tendermint/tendermint/crypto/sm2"
)

const (
	ED25519 = "ed25519"
	SM2     = "sm2"
)

var Algo = "ed25519"

func GetPubKeyType() string {
	switch Algo {
	case ED25519:
		return ed25519.KeyType

	case SM2:
		return sm2.KeyType

	default:
		return ed25519.KeyType
	}
}

func GetPrivKeyBytes(privKey crypto.PrivKey) []byte {
	switch Algo {
	case ED25519:
		key := privKey.(ed25519.PrivKey)
		return key[:]

	case SM2:
		key := privKey.(sm2.PrivKeySm2)
		return key[:]

	default:
		key := privKey.(ed25519.PrivKey)
		return key[:]
	}
}

func GetPubKeyBytes(pubKey crypto.PubKey) []byte {
	switch Algo {
	case ED25519:
		key := pubKey.(ed25519.PubKey)
		return key[:]

	case SM2:
		key := pubKey.(sm2.PubKeySm2)
		return key[:]

	default:
		key := pubKey.(ed25519.PubKey)
		return key[:]
	}
}

func GetPubKeyFromData(keyType string, keyData []byte) crypto.PubKey {
	switch Algo {
	case ED25519:
		pubkey := ed25519.PubKey{}
		copy(pubkey[:], keyData)
		return pubkey

	case SM2:
		pubkey := sm2.PubKeySm2{}
		copy(pubkey[:], keyData)
		return pubkey

	default:
		pubkey := ed25519.PubKey{}
		copy(pubkey[:], keyData)
		return pubkey
	}
}

func GenPrivKey() crypto.PrivKey {
	switch Algo {
	case ED25519:
		return ed25519.GenPrivKey()

	case SM2:
		return sm2.GenPrivKey()

	default:
		return ed25519.GenPrivKey()
	}
}

func GenPrivKeyFromSecret(secret []byte) crypto.PrivKey {
	switch Algo {
	case ED25519:
		return ed25519.GenPrivKeyFromSecret(secret)

	case SM2:
		return sm2.GenPrivKeySm2FromSecret(secret)

	default:
		return ed25519.GenPrivKeyFromSecret(secret)
	}
}

func GetPrivKeySize() int {
	switch Algo {
	case ED25519:
		return 64

	case SM2:
		return sm2.PrivKeySize

	default:
		return 64
	}
}

func GetPubKeySize() int {
	switch Algo {
	case ED25519:
		return ed25519.PubKeySize

	case SM2:
		return sm2.PubKeySize

	default:
		return ed25519.PubKeySize
	}
}

func GetSignatureSize() int {
	switch Algo {
	case ED25519:
		return ed25519.SignatureSize

	case SM2:
		return sm2.SignatureSize

	default:
		return ed25519.SignatureSize
	}
}

func VerifyPubKeyType(pubKey crypto.PubKey) bool {
	switch Algo {
	case ED25519:
		if _, ok := pubKey.(ed25519.PubKey); ok {
			return true
		}

	case SM2:
		if _, ok := pubKey.(sm2.PubKeySm2); ok {
			return true
		}
	}

	return false
}

func VerifyPrivKeyType(privKey crypto.PrivKey) bool {
	switch Algo {
	case ED25519:
		if _, ok := privKey.(ed25519.PrivKey); ok {
			return true
		}

	case SM2:
		if _, ok := privKey.(sm2.PrivKeySm2); ok {
			return true
		}
	}

	return false
}
