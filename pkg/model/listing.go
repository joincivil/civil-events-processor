// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

import (
	"github.com/ethereum/go-ethereum/common"
)

// GovernanceState specifies the current state of a listing
type GovernanceState int

const (
	// GovernanceStateNone is an invalid/empty governance state
	GovernanceStateNone GovernanceState = iota
	// GovernanceStateApplied is when a listing has just applied
	GovernanceStateApplied
	// GovernanceStateChallenged is when a listing has been challenged
	GovernanceStateChallenged
	// GovernanceStateAppWhitelisted is when a listing has been whitelisted
	GovernanceStateAppWhitelisted
	// GovernanceStateAppRemoved is when a listing's application has been removed
	GovernanceStateAppRemoved
	// GovernanceStateRemoved is when a listing has been removed
	GovernanceStateRemoved
	// GovernanceStateWithdrawn is when a listing has been withdrawn before it
	// has been whitelisted
	GovernanceStateWithdrawn
)

// NewListing is a convenience function to initialize a new Listing struct
func NewListing(name string, contractAddress common.Address, whitelisted bool,
	lastState GovernanceState, url string, charterURI string, ownerAddresses []common.Address,
	contributorAddresses []common.Address, createdDateTs uint64, applicationDateTs uint64,
	approvalDateTs uint64, lastUpdatedDateTs uint64) *Listing {
	return &Listing{
		name:                 name,
		contractAddress:      contractAddress,
		whitelisted:          whitelisted,
		lastGovernanceState:  lastState,
		url:                  url,
		charterURI:           charterURI,
		ownerAddresses:       ownerAddresses,
		contributorAddresses: contributorAddresses,
		createdDateTs:        createdDateTs,
		applicationDateTs:    applicationDateTs,
		approvalDateTs:       approvalDateTs,
		lastUpdatedDateTs:    lastUpdatedDateTs,
	}
}

// Listing represents a newsroom listing in the Civil TCR
type Listing struct {
	name string

	contractAddress common.Address

	whitelisted bool

	lastGovernanceState GovernanceState

	url string

	charterURI string // Updated to reflect how we are storing the charter

	ownerAddresses []common.Address

	contributorAddresses []common.Address

	createdDateTs uint64

	applicationDateTs uint64

	approvalDateTs uint64

	lastUpdatedDateTs uint64
}

// Name returns the newsroom name
func (l *Listing) Name() string {
	return l.name
}

// SetName sets the value for name
func (l *Listing) SetName(name string) {
	l.name = name
}

// Whitelisted returns a bool to indicate if the newsroom is whitelisted
// and on the registry
func (l *Listing) Whitelisted() bool {
	return l.whitelisted
}

// SetWhitelisted sets the value for whitelisted field
func (l *Listing) SetWhitelisted(whitelisted bool) {
	l.whitelisted = whitelisted
}

// LastGovernanceState returns the last governance event on this newsroom
func (l *Listing) LastGovernanceState() GovernanceState {
	return l.lastGovernanceState
}

// SetLastGovernanceState sets the value of the last governance state
func (l *Listing) SetLastGovernanceState(lastState GovernanceState) {
	l.lastGovernanceState = lastState
}

// ContractAddress returns the newsroom contract address
func (l *Listing) ContractAddress() common.Address {
	return l.contractAddress
}

// URL is the homepage of the newsroom
func (l *Listing) URL() string {
	return l.url
}

// CharterURI returns the URI to the charter post for the newsroom
func (l *Listing) CharterURI() string {
	return l.charterURI
}

// OwnerAddresses is the addresses of the owners of the newsroom
func (l *Listing) OwnerAddresses() []common.Address {
	return l.ownerAddresses
}

// AddOwnerAddress adds another address to the list of owner addresses
func (l *Listing) AddOwnerAddress(addr common.Address) {
	l.ownerAddresses = append(l.ownerAddresses, addr)
}

// RemoveOwnerAddress removes an address from the list of owner addresses
func (l *Listing) RemoveOwnerAddress(addr common.Address) {
	numAddrs := len(l.ownerAddresses)
	if numAddrs <= 1 {
		l.ownerAddresses = []common.Address{}
		return
	}
	addrs := make([]common.Address, numAddrs-1)
	addrsIndex := 0
	for _, existingAddr := range l.ownerAddresses {
		if existingAddr != addr {
			addrs[addrsIndex] = existingAddr
			addrsIndex++
		}
	}
	l.ownerAddresses = addrs
}

// ContributorAddresses returns a list of contributor data to a newsroom
func (l *Listing) ContributorAddresses() []common.Address {
	return l.contributorAddresses
}

// AddContributorAddress adds another address to the list of contributor addresses
func (l *Listing) AddContributorAddress(addr common.Address) {
	l.contributorAddresses = append(l.contributorAddresses, addr)
}

// RemoveContributorAddress removes an address from the list of owner addresses
func (l *Listing) RemoveContributorAddress(addr common.Address) {
	addrs := make([]common.Address, len(l.contributorAddresses)-1)
	addrsIndex := 0
	for _, existingAddr := range l.contributorAddresses {
		if existingAddr != addr {
			addrs[addrsIndex] = existingAddr
			addrsIndex++
		}
	}
	l.contributorAddresses = addrs
}

// ApplicationDateTs returns the timestamp of the listing's initial application
func (l *Listing) ApplicationDateTs() uint64 {
	return l.applicationDateTs
}

// ApprovalDateTs returns the timestamp of the listing's whitelisted/approved
func (l *Listing) ApprovalDateTs() uint64 {
	return l.approvalDateTs
}

// SetApprovalDateTs sets the date of the last time this listing was whitelisted/approval
func (l *Listing) SetApprovalDateTs(date uint64) {
	l.approvalDateTs = date
}

// LastUpdatedDateTs returns the timestamp of the last update to the listing
func (l *Listing) LastUpdatedDateTs() uint64 {
	return l.lastUpdatedDateTs
}

// SetLastUpdatedDateTs sets the value of the last time this listing was updated
func (l *Listing) SetLastUpdatedDateTs(date uint64) {
	l.lastUpdatedDateTs = date
}
