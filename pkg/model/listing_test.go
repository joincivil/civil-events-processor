package model_test

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/joincivil/civil-events-processor/pkg/model"

	cstrings "github.com/joincivil/go-common/pkg/strings"
)

func setupSampleListing() (*model.Listing, common.Address) {
	address1, _ := cstrings.RandomHexStr(32)
	address2, _ := cstrings.RandomHexStr(32)
	address3, _ := cstrings.RandomHexStr(32)
	contractAddress := common.HexToAddress(address1)
	ownerAddr := common.HexToAddress(address2)
	ownerAddresses := []common.Address{common.HexToAddress(address2), common.HexToAddress(address3)}
	contributorAddresses := ownerAddresses
	appExpiry := big.NewInt(232424242)
	unstakedDeposit := new(big.Int)
	unstakedDeposit.SetString("100000000000000000000", 10)
	challengeID := big.NewInt(10)

	signature, _ := cstrings.RandomHexStr(32)
	authorAddr, _ := cstrings.RandomHexStr(32)

	contentHashHex, _ := cstrings.RandomHexStr(32)
	contentHashBytes := []byte(contentHashHex)
	fixedContentHash := [32]byte{}
	copy(fixedContentHash[:], contentHashBytes)

	charter := model.NewCharter(&model.CharterParams{
		URI:         "/charter/uri",
		ContentID:   big.NewInt(0),
		RevisionID:  big.NewInt(30),
		Signature:   []byte(signature),
		Author:      common.HexToAddress(authorAddr),
		ContentHash: fixedContentHash,
		Timestamp:   big.NewInt(12345678),
	})
	testListingParams := &model.NewListingParams{
		Name:                 "test_listing",
		ContractAddress:      contractAddress,
		Whitelisted:          true,
		LastState:            model.GovernanceStateAppWhitelisted,
		URL:                  "url_string",
		Charter:              charter,
		Owner:                ownerAddr,
		OwnerAddresses:       ownerAddresses,
		ContributorAddresses: contributorAddresses,
		CreatedDateTs:        1257894000,
		ApplicationDateTs:    1257894000,
		ApprovalDateTs:       1257894000,
		LastUpdatedDateTs:    1257894000,
		AppExpiry:            appExpiry,
		UnstakedDeposit:      unstakedDeposit,
		ChallengeID:          challengeID,
	}
	testListing := model.NewListing(testListingParams)
	return testListing, contractAddress
}

func TestCharterAsMapFromMap(t *testing.T) {
	signature, _ := cstrings.RandomHexStr(32)
	authorAddr, _ := cstrings.RandomHexStr(32)

	contentHashHex, _ := cstrings.RandomHexStr(32)
	contentHashBytes := []byte(contentHashHex)
	fixedContentHash := [32]byte{}
	copy(fixedContentHash[:], contentHashBytes)

	charter := model.NewCharter(&model.CharterParams{
		URI:         "/charter/uri",
		ContentID:   big.NewInt(0),
		RevisionID:  big.NewInt(30),
		Signature:   []byte(signature),
		Author:      common.HexToAddress(authorAddr),
		ContentHash: fixedContentHash,
		Timestamp:   big.NewInt(12345678),
	})

	newCharterMap := charter.AsMap()
	newCharter := &model.Charter{}
	err := newCharter.FromMap(newCharterMap)
	if err != nil {
		t.Errorf("Should have not returned error from FromMap: err: %v", err)
	}
	if charter.URI() != newCharter.URI() {
		t.Errorf("Should have had same URI")
	}
	if charter.ContentID().Cmp(newCharter.ContentID()) != 0 {
		t.Errorf("Should have had same content ID")
	}
	if charter.RevisionID().Cmp(newCharter.RevisionID()) != 0 {
		t.Errorf("Should have had same revision ID")
	}
	if !bytes.Equal(charter.Signature(), newCharter.Signature()) {
		t.Errorf("Should have had same signature")
	}
	if charter.Author().Hex() != newCharter.Author().Hex() {
		t.Errorf("Should have had same author addr")
	}
	chart1Hash := charter.ContentHash()
	chart2Hash := newCharter.ContentHash()
	if !bytes.Equal(chart1Hash[:], chart2Hash[:]) {
		t.Errorf("Should have had same content hash")
	}
	if charter.Timestamp().Cmp(newCharter.Timestamp()) != 0 {
		t.Errorf("Should have had same timestamp")
	}
}

func TestListingCharterAsMapFromMap(t *testing.T) {
	listing, _ := setupSampleListing()
	charter := listing.Charter()
	newCharterMap := charter.AsMap()

	newCharter := &model.Charter{}
	err := newCharter.FromMap(newCharterMap)
	if err != nil {
		t.Errorf("Should have not returned error from FromMap: err: %v", err)
	}
	if charter.URI() != newCharter.URI() {
		t.Errorf("Should have had same URI")
	}
	if charter.ContentID().Cmp(newCharter.ContentID()) != 0 {
		t.Errorf("Should have had same content ID")
	}
	if charter.RevisionID().Cmp(newCharter.RevisionID()) != 0 {
		t.Errorf("Should have had same revision ID")
	}
	if !bytes.Equal(charter.Signature(), newCharter.Signature()) {
		t.Errorf("Should have had same signature")
	}
	if charter.Author().Hex() != newCharter.Author().Hex() {
		t.Errorf("Should have had same author addr")
	}
	chart1Hash := charter.ContentHash()
	chart2Hash := newCharter.ContentHash()
	if !bytes.Equal(chart1Hash[:], chart2Hash[:]) {
		t.Errorf("Should have had same content hash")
	}
	if charter.Timestamp().Cmp(newCharter.Timestamp()) != 0 {
		t.Errorf("Should have had same timestamp")
	}
}
