// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

import (
	"github.com/ethereum/go-ethereum/common"
	"math/big"
)

// errors must not be returned in valid conditions, such as when there is no
// record for a query.  In this case, return the empty value for the return
// type. errors must be reserved for actual internal errors.

// ListingPersister is the interface to store the listings data related to the processor
// and the aggregated data from the events.  Potentially to be used to service
// the APIs to pull data.
type ListingPersister interface {
	// ListingsByAddress returns a slice of Listings based on addresses
	ListingsByAddresses(addresses []common.Address) ([]*Listing, error)
	// ListingByAddress retrieves listings based on addresses
	ListingByAddress(address common.Address) (*Listing, error)
	// CreateListing creates a new listing
	CreateListing(listing *Listing) error
	// UpdateListing updates fields on an existing listing
	UpdateListing(listing *Listing, updatedFields []string) error
	// DeleteListing removes a listing
	DeleteListing(listing *Listing) error
}

// ContentRevisionPersister is the interface to store the content data related to the processor
// and the aggregated data from the events.  Potentially to be used to service
// the APIs to pull data.
type ContentRevisionPersister interface {
	// ContentRevisions retrieves the revisions for content on a listing
	ContentRevisions(address common.Address, contentID *big.Int) ([]*ContentRevision, error)
	// ContentRevision retrieves a specific content revision for newsroom content
	ContentRevision(address common.Address, contentID *big.Int, revisionID *big.Int) (*ContentRevision, error)
	// CreateContentRevision creates a new content revision
	CreateContentRevision(revision *ContentRevision) error
	// UpdateContentRevision updates fields on an existing content revision
	UpdateContentRevision(revision *ContentRevision, updatedFields []string) error
	// DeleteContentRevision removes a content revision
	DeleteContentRevision(revision *ContentRevision) error
}

// GovernanceEventPersister is the interface to store the governance event data related to the processor
// and the aggregated data from the events.  Potentially to be used to service
// the APIs to pull data.
type GovernanceEventPersister interface {
	// GovernanceEventsbyListingAddress retrieves governance events based on criteria
	GovernanceEventsByListingAddress(address common.Address) ([]*GovernanceEvent, error)
	// CreateGovernanceEvent creates a new governance event
	CreateGovernanceEvent(govEvent *GovernanceEvent) error
	// UpdateGovernanceEvent updates fields on an existing governance event
	UpdateGovernanceEvent(govEvent *GovernanceEvent, updatedFields []string) error
	// DeleteGovenanceEvent removes a governance event
	DeleteGovenanceEvent(govEvent *GovernanceEvent) error
}
