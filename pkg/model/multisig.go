// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

import (
	"github.com/ethereum/go-ethereum/common"
)

// NewMultiSigParams represents all the necessary data to create a new multiSig
// using NewMultiSig
type NewMultiSigParams struct {
	ContractAddress common.Address
	OwnerAddresses  []common.Address
}

// NewMultiSig is a convenience function to initialize a new Listing struct
func NewMultiSig(params *NewMultiSigParams) *MultiSig {
	return &MultiSig{
		contractAddress: params.ContractAddress,
		ownerAddresses:  params.OwnerAddresses,
	}
}

// MultiSig represents a gnosis multisig
type MultiSig struct {
	contractAddress common.Address
	ownerAddresses  []common.Address
}

// ContractAddress returns the newsroom contract address
func (m *MultiSig) ContractAddress() common.Address {
	return m.contractAddress
}

// OwnerAddresses is the addresses of the owners of the newsroom - all members of multisig
func (m *MultiSig) OwnerAddresses() []common.Address {
	return m.ownerAddresses
}

// AddOwnerAddress adds another address to the list of owner addresses
func (m *MultiSig) AddOwnerAddress(addr common.Address) {
	m.ownerAddresses = append(m.ownerAddresses, addr)
}

// RemoveOwnerAddress removes an address from the list of owner addresses
func (m *MultiSig) RemoveOwnerAddress(addr common.Address) {
	numAddrs := len(m.ownerAddresses)
	if numAddrs <= 1 {
		m.ownerAddresses = []common.Address{}
		return
	}
	addrs := make([]common.Address, numAddrs-1)
	addrsIndex := 0
	for _, existingAddr := range m.ownerAddresses {
		if existingAddr != addr {
			addrs[addrsIndex] = existingAddr
			addrsIndex++
		}
	}
	m.ownerAddresses = addrs
}
