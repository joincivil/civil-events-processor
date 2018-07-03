// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

import (
	"github.com/ethereum/go-ethereum/common"
)

// AggregateDataPersister is the interface to store data related to the processor
// and the aggregated data from the events.  Potentially to be used to service
// the APIs to pull data.
type AggregateDataPersister interface {
	// GetListingsByAddress returns a slice of Listings based on addresses
	GetListingsByAddress(addresses []common.Address) ([]*Listing, error)
	// GetListingByAddress retrieves listings based on addresses
	GetListingByAddress(address common.Address) (*Listing, error)
	// CreateListing creates a new listing
	CreateListing(listing *Listing) error
	// UpdateListing updates fields on an existing listing
	UpdateListing(listing *Listing) error
	// DeleteListing removes a listing
	DeleteListing(listing *Listing) error

	// GetContent retrieves content items based on criteria
	GetContents() ([]*Content, error)
	// GetContent retrieves content items based on criteria
	GetContent() (*Content, error)
	// CreateContent creates a new content item
	CreateContent(content *Content) error
	// UpdateContent updates fields on an existing content item
	UpdateContent(content *Content) error
	// DeleteContent removes a content item
	DeleteContent(content *Content) error

	// GetGovernanceEvent retrieves governance events based on criteria
	GetGovernanceEvent() (*GovernanceEvent, error)
	// CreateGovernanceEvent creates a new governance event
	CreateGovernanceEvent(govEvent *GovernanceEvent) error
	// UpdateGovernanceEvent updates fields on an existing governance event
	UpdateGovernanceEvent(govEvent *GovernanceEvent) error
	// DeleteGovenanceEvent removes a governance event
	DeleteGovenanceEvent(govEvent *GovernanceEvent) error
}
