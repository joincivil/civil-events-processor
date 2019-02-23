package model

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// TokenPurchaseParams are the params to initialize a new TokenPurchase
type TokenPurchaseParams struct {
	PurchaserAddress common.Address
	SourceAddress    common.Address
	Amount           *big.Int
	PurchaseDate     int64
	BlockNumber      uint64
	TxHash           common.Hash
	TxIndex          uint
	BlockHash        common.Hash
	Index            uint
}

// NewTokenPurchase is a convenience method to init a TokenPurchase struct
func NewTokenPurchase(params *TokenPurchaseParams) *TokenPurchase {
	return &TokenPurchase{
		purchaserAddress: params.PurchaserAddress,
		sourceAddress:    params.SourceAddress,
		amount:           params.Amount,
		purchaseDate:     params.PurchaseDate,
		blockData: BlockData{
			blockNumber: params.BlockNumber,
			txHash:      params.TxHash.Hex(),
			txIndex:     params.TxIndex,
			blockHash:   params.BlockHash.Hex(),
			index:       params.Index,
		},
	}
}

// TokenPurchase represents a single token purchase made by an individual
type TokenPurchase struct {
	// The address of the purchaser (purchaser wallet addr)
	purchaserAddress common.Address

	// wallet from which the tokens were transferred from (civil wallet)
	sourceAddress common.Address

	// amount in gwei, not tokens
	amount *big.Int

	purchaseDate int64

	blockData BlockData
}

// PurchaserAddress is the address of the purchaser (purchaser wallet)
func (t *TokenPurchase) PurchaserAddress() common.Address {
	return t.purchaserAddress
}

// SourceAddress is the address of the token source (civil wallet)
func (t *TokenPurchase) SourceAddress() common.Address {
	return t.sourceAddress
}

// Amount is the amount of token purchased
// Is in number of gwei, not in token
func (t *TokenPurchase) Amount() *big.Int {
	return t.amount
}

// AmountInToken is the amount in tokens
func (t *TokenPurchase) AmountInToken() *big.Int {
	return t.amount.Quo(t.amount, big.NewInt(1e18))
}

// PurchaseDate is the purchase date
// Should be based on the block timestamp
func (t *TokenPurchase) PurchaseDate() int64 {
	return t.purchaseDate
}

// BlockData has all the block data from the block associated with the event
// NOTE: This is not secured by consensus
func (t *TokenPurchase) BlockData() BlockData {
	return t.blockData
}

// Equals compares this token purchase structs with another for equality
func (t *TokenPurchase) Equals(purchase *TokenPurchase) bool {
	if t.purchaserAddress.Hex() == purchase.PurchaserAddress().Hex() {
		return false
	}
	if t.sourceAddress.Hex() == purchase.SourceAddress().Hex() {
		return false
	}
	if t.amount.Int64() != purchase.Amount().Int64() {
		return false
	}
	if t.purchaseDate != purchase.PurchaseDate() {
		return false
	}
	return true
}
