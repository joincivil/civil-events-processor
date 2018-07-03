// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

import (
	"github.com/ethereum/go-ethereum/common"
)

// ArticlePayload is the metadata and content data for an article
type ArticlePayload map[string]interface{}

// Content represents a content item
type Content struct {
	listingAddress common.Address

	payload ArticlePayload

	payloadHash string

	contractContentID uint64

	originalPublishDateTs uint64

	lastUpdatedTs uint64
}

// ListingAddress returns the associated listing address
func (c *Content) ListingAddress() common.Address {
	return c.listingAddress
}

// Payload returns the ArticlePayload
func (c *Content) Payload() ArticlePayload {
	return c.payload
}

// ContractContentID returns the contract content ID
func (c *Content) ContractContentID() uint64 {
	return c.contractContentID
}

// OriginalPublishDateTs returns the timestamp of the original publish date
// of the content
func (c *Content) OriginalPublishDateTs() uint64 {
	return c.originalPublishDateTs
}

// LastUpdatedTs returns the timestamp of the last update to this content
func (c *Content) LastUpdatedTs() uint64 {
	return c.lastUpdatedTs
}
