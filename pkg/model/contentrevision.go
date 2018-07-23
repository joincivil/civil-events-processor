// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

import (
	"github.com/ethereum/go-ethereum/common"
)

// ArticlePayload is the metadata and content data for an article
type ArticlePayload map[string]interface{}

// ContentRevision represents a revision to a content item
type ContentRevision struct {
	listingAddress common.Address

	payload ArticlePayload

	payloadHash string

	editorAddress common.Address

	contractContentID uint64

	contractRevisionID uint64

	revisionURI string

	revisionDateTs int64
}

// NewContentRevision is a convenience function to init a ContentRevision
// struct
func NewContentRevision(listingAddr common.Address, payload ArticlePayload,
	editorAddress common.Address, contractContentID uint64, contractRevisionID uint64,
	revisionURI string, revisionDateTs int64) (*ContentRevision, error) {
	revision := &ContentRevision{
		listingAddress:     listingAddr,
		payload:            payload,
		editorAddress:      editorAddress,
		contractContentID:  contractContentID,
		contractRevisionID: contractRevisionID,
		revisionURI:        revisionURI,
		revisionDateTs:     revisionDateTs,
	}
	err := revision.hashPayload()
	if err != nil {
		return nil, err
	}
	return revision, nil
}

// hashPayload creates the hash of the payload and sets the payloadHash field.
// What will this hash be?
func (c *ContentRevision) hashPayload() error {
	c.payloadHash = ""
	return nil
}

// ListingAddress returns the associated listing address
func (c *ContentRevision) ListingAddress() common.Address {
	return c.listingAddress
}

// EditorAddress returns the address of editor who made revision
func (c *ContentRevision) EditorAddress() common.Address {
	return c.editorAddress
}

// Payload returns the ArticlePayload
func (c *ContentRevision) Payload() ArticlePayload {
	return c.payload
}

// PayloadHash returns the hash of the payload
func (c *ContentRevision) PayloadHash() string {
	return c.payloadHash
}

// RevisionURI returns the revision URI
func (c *ContentRevision) RevisionURI() string {
	return c.revisionURI
}

// ContractContentID returns the contract content ID
func (c *ContentRevision) ContractContentID() uint64 {
	return c.contractContentID
}

// ContractRevisionID returns the contract content revision ID
func (c *ContentRevision) ContractRevisionID() uint64 {
	return c.contractRevisionID
}

// RevisionDateTs returns the timestamp of the revision
func (c *ContentRevision) RevisionDateTs() int64 {
	return c.revisionDateTs
}
