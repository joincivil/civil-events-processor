// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

import (
	"github.com/ethereum/go-ethereum/common"
)

// NewMultiSigOwnerParams represents all the necessary data to create a new multiSig owner
// using NewMultiSig
type NewMultiSigOwnerParams struct {
	OwnerAddress    common.Address
	MultiSigAddress common.Address
}

// NewMultiSigOwner is a convenience function to initialize a new Listing struct
func NewMultiSigOwner(params *NewMultiSigOwnerParams) *MultiSigOwner {
	return &MultiSigOwner{
		ownerAddress:    params.OwnerAddress,
		multiSigAddress: params.MultiSigAddress,
	}
}

// MultiSigOwner represents a gnosis multisig owneer
type MultiSigOwner struct {
	ownerAddress    common.Address
	multiSigAddress common.Address
}

// OwnerAddress returns the owner address
func (m *MultiSigOwner) OwnerAddress() common.Address {
	return m.ownerAddress
}

// MultiSigAddress returns the owner address
func (m *MultiSigOwner) MultiSigAddress() common.Address {
	return m.multiSigAddress
}
