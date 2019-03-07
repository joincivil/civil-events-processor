package model

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// TokenTransferParams are the params to initialize a new TokenTransfer
type TokenTransferParams struct {
	ToAddress    common.Address
	FromAddress  common.Address
	Amount       *big.Int
	CvlPrice     *big.Float
	EthPrice     *big.Float
	EventHash    string
	TransferDate int64
	BlockNumber  uint64
	TxHash       common.Hash
	TxIndex      uint
	BlockHash    common.Hash
	Index        uint
}

// NewTokenTransfer is a convenience method to init a TokenTransfer struct
func NewTokenTransfer(params *TokenTransferParams) *TokenTransfer {
	return &TokenTransfer{
		toAddress:    params.ToAddress,
		fromAddress:  params.FromAddress,
		amount:       params.Amount,
		cvlPrice:     params.CvlPrice,
		ethPrice:     params.EthPrice,
		transferDate: params.TransferDate,
		eventHash:    params.EventHash,
		blockData: BlockData{
			blockNumber: params.BlockNumber,
			txHash:      params.TxHash.Hex(),
			txIndex:     params.TxIndex,
			blockHash:   params.BlockHash.Hex(),
			index:       params.Index,
		},
	}
}

// TokenTransfer represents a single token transfer made by an individual
type TokenTransfer struct {
	// The address of the purchaser (purchaser wallet addr)
	toAddress common.Address

	// wallet from which the tokens were transferred from (civil wallet)
	fromAddress common.Address

	// amount in gwei, not tokens
	amount *big.Int

	// best estimation price of CVL around time of transfer
	// if 0, likely unable to give good estimation
	cvlPrice *big.Float

	// best estimation price of ETH around time of transfer
	// if 0, likely unable to give good estimation
	ethPrice *big.Float

	transferDate int64

	eventHash string

	blockData BlockData
}

// ToAddress is the address of the purchaser (purchaser wallet)
func (t *TokenTransfer) ToAddress() common.Address {
	return t.toAddress
}

// FromAddress is the address of the token source (civil wallet)
func (t *TokenTransfer) FromAddress() common.Address {
	return t.fromAddress
}

// Amount is the amount of token transferred
// Is in number of gwei, not in token
func (t *TokenTransfer) Amount() *big.Int {
	return t.amount
}

// AmountInToken is the amount in tokens
func (t *TokenTransfer) AmountInToken() *big.Int {
	return t.amount.Quo(t.amount, big.NewInt(1e18))
}

// CvlPrice is the best estimation price of CVL at time of transfer
// if 0, likely unable to give good estimation
func (t *TokenTransfer) CvlPrice() *big.Float {
	return t.cvlPrice
}

// EthPrice is the best estimation price of ETH at time of transfer
// if 0, likely unable to give good estimation
func (t *TokenTransfer) EthPrice() *big.Float {
	return t.ethPrice
}

// TransferDate is the purchase date
// Should be based on the block timestamp
func (t *TokenTransfer) TransferDate() int64 {
	return t.transferDate
}

// EventHash returns the hash of the event for this transfer
func (t *TokenTransfer) EventHash() string {
	return t.eventHash
}

// BlockData has all the block data from the block associated with the event
// NOTE: This is not secured by consensus
func (t *TokenTransfer) BlockData() BlockData {
	return t.blockData
}

// Equals compares this token transfer structs with another for equality
func (t *TokenTransfer) Equals(purchase *TokenTransfer) bool {
	if t.toAddress.Hex() == purchase.ToAddress().Hex() {
		return false
	}
	if t.fromAddress.Hex() == purchase.FromAddress().Hex() {
		return false
	}
	if t.amount.Int64() != purchase.Amount().Int64() {
		return false
	}
	if t.transferDate != purchase.TransferDate() {
		return false
	}
	if t.blockData.TxHash() == purchase.blockData.TxHash() {
		return false
	}
	return true
}
