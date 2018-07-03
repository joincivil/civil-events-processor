// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

import (
	"github.com/ethereum/go-ethereum/common"
)

// GovernanceState specifies the current state of a listing
type GovernanceState int

const (
	governanceStateNone GovernanceState = iota
	governanceStateApplied
	governanceStateChallenged
	governanceStateAppWhitelisted
	governanceStateAppRemoved
	governanceStateRemoved
	governanceStateWithdrawn
)

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

	applicationDateTs uint64

	approvalDateTs uint64

	lastUpdatedTs uint64
}

// Name returns the newsroom name
func (l *Listing) Name() string {
	return l.name
}

// ContractAddress returns the newsroom contract address
func (l *Listing) ContractAddress() common.Address {
	return l.contractAddress
}

// Whitelisted returns a bool to indicate if the newsroom is whitelisted
// and on the registry
func (l *Listing) Whitelisted() bool {
	return l.whitelisted
}

// LastGovernanceState returns the last governance event on this newsroom
func (l *Listing) LastGovernanceState() GovernanceState {
	return l.lastGovernanceState
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

// ContributorAddresses returns a list of contributor data to a newsroom
func (l *Listing) ContributorAddresses() []common.Address {
	return l.contributorAddresses
}

// ApplicationDateTs returns the timestamp of the listing's initial application
func (l *Listing) ApplicationDateTs() uint64 {
	return l.applicationDateTs
}

// ApprovalDateTs returns the timestamp of the listing's whitelisted/approved
func (l *Listing) ApprovalDateTs() uint64 {
	return l.approvalDateTs
}

// LastUpdatedTs returns the timestamp of the last update to the listing
func (l *Listing) LastUpdatedTs() uint64 {
	return l.lastUpdatedTs
}
