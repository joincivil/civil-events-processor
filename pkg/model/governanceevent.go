// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

import (
	"github.com/ethereum/go-ethereum/common"
)

// Metadata represents any metadata associated with a governance event
type Metadata map[string]interface{}

// GovernanceEvent represents a single governance event made to a listing.  Meant
// to be a central log of these events for audit.
type GovernanceEvent struct {
	listingAddress common.Address

	senderAddress common.Address

	metadata Metadata

	governanceEventType string

	creationDateTs uint64

	lastUpdatedTs uint64
}

// ListingAddress returns the listing address associated with this event
func (g *GovernanceEvent) ListingAddress() common.Address {
	return g.listingAddress
}

// SenderAddress returns the address of the sender of this event. The sender
// is the address that initiated this event
func (g *GovernanceEvent) SenderAddress() common.Address {
	return g.senderAddress
}

// Metadata returns the Metadata associated with the event. It might anything
// returned in the raw log
func (g *GovernanceEvent) Metadata() Metadata {
	return g.metadata
}

// GovernanceEventType returns the type of this event
func (g *GovernanceEvent) GovernanceEventType() string {
	return g.governanceEventType
}

// CreationDateTs is the timestamp of creation for this event
func (g *GovernanceEvent) CreationDateTs() uint64 {
	return g.creationDateTs
}

// LastUpdatedTs is the timestamp of the last update of this event
func (g *GovernanceEvent) LastUpdatedTs() uint64 {
	return g.lastUpdatedTs
}
