// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

import (
	"fmt"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
)

// ArticlePayload is the metadata and content data for an article
type ArticlePayload map[string]interface{}

// Hash returns the hash of the article payload.  Hashes
// all the values from the map together as slice of keyvalue pairs.
// Returns a keccak256 hash hex string.
func (a ArticlePayload) Hash() string {
	toEncode := make([]string, len(a))
	index := 0
	for key, val := range a {
		hashPart := fmt.Sprintf("%v%v", key, val)
		toEncode[index] = hashPart
		index++
	}
	sort.Strings(toEncode)
	eventBytes, _ := rlp.EncodeToBytes(toEncode)
	h := crypto.Keccak256Hash(eventBytes)
	return h.Hex()
}

// NewContentRevision is a convenience function to init a ContentRevision
// struct
func NewContentRevision(listingAddr common.Address, payload ArticlePayload,
	editorAddress common.Address, contractContentID *big.Int, contractRevisionID *big.Int,
	revisionURI string, revisionDateTs uint64) *ContentRevision {
	revision := &ContentRevision{
		listingAddress:     listingAddr,
		payload:            payload,
		payloadHash:        payload.Hash(),
		editorAddress:      editorAddress,
		contractContentID:  contractContentID,
		contractRevisionID: contractRevisionID,
		revisionURI:        revisionURI,
		revisionDateTs:     revisionDateTs,
	}
	return revision
}

// ContentRevision represents a revision to a content item
type ContentRevision struct {
	listingAddress common.Address

	payload ArticlePayload

	payloadHash string

	editorAddress common.Address

	contractContentID *big.Int

	contractRevisionID *big.Int

	revisionURI string

	revisionDateTs uint64
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
func (c *ContentRevision) ContractContentID() *big.Int {
	return c.contractContentID
}

// ContractRevisionID returns the contract content revision ID
func (c *ContentRevision) ContractRevisionID() *big.Int {
	return c.contractRevisionID
}

// RevisionDateTs returns the timestamp of the revision
func (c *ContentRevision) RevisionDateTs() uint64 {
	return c.revisionDateTs
}
