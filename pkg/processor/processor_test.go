package processor_test

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/joincivil/civil-events-crawler/pkg/contractutils"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	"github.com/joincivil/civil-events-crawler/pkg/utils"

	"github.com/joincivil/civil-events-processor/pkg/generated/contract"
	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/processor"
)

var (
	contractAddress = "0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d"
	testAddress     = "0xDFe273082089bB7f70Ee36Eebcde64832FE97E55"
	testEvent       = &contract.CivilTCRContractApplication{
		ListingAddress: common.HexToAddress(testAddress),
		Deposit:        big.NewInt(1000),
		AppEndDate:     big.NewInt(1653860896),
		Data:           "DATA",
		Applicant:      common.HexToAddress(testAddress),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888888,
			TxHash:      common.Hash{},
			TxIndex:     2,
			BlockHash:   common.Hash{},
			Index:       2,
			Removed:     false,
		},
	}
)

func setupCivilEvent() (*crawlermodel.CivilEvent, error) {
	return crawlermodel.NewCivilEventFromContractEvent("Application", "CivilTCRContract", common.HexToAddress(contractAddress),
		testEvent, utils.CurrentEpochSecsInInt())
}

type TestAggregatePersister struct{}

// GetListingsByAddress returns a slice of Listings based on addresses
func (t *TestAggregatePersister) ListingsByAddress(addresses []common.Address) ([]*model.Listing, error) {
	return []*model.Listing{}, nil
}

// GetListingByAddress retrieves listings based on addresses
func (t *TestAggregatePersister) ListingByAddress(address common.Address) (*model.Listing, error) {
	return &model.Listing{}, nil
}

// CreateListing creates a new listing
func (t *TestAggregatePersister) CreateListing(listing *model.Listing) error {
	return nil
}

// UpdateListing updates fields on an existing listing
func (t *TestAggregatePersister) UpdateListing(listing *model.Listing) error {
	return nil
}

// DeleteListing removes a listing
func (t *TestAggregatePersister) DeleteListing(listing *model.Listing) error {
	return nil
}

// GetContentRevisions retrieves content items based on criteria
func (t *TestAggregatePersister) ContentRevisions(address common.Address,
	contentID uint64) ([]*model.ContentRevision, error) {
	return []*model.ContentRevision{}, nil
}

// GetContentRevision retrieves content items based on criteria
func (t *TestAggregatePersister) ContentRevision(address common.Address, contentID uint64,
	revisionID uint64) (*model.ContentRevision, error) {
	return &model.ContentRevision{}, nil
}

// CreateContentRevision creates a new content item
func (t *TestAggregatePersister) CreateContentRevision(revision *model.ContentRevision) error {
	return nil
}

// UpdateContentRevision updates fields on an existing content item
func (t *TestAggregatePersister) UpdateContentRevision(revision *model.ContentRevision) error {
	return nil
}

// DeleteContentRevision removes a content item
func (t *TestAggregatePersister) DeleteContentRevision(revision *model.ContentRevision) error {
	return nil
}

// GetGovernanceEventsByListingAddress retrieves governance events based on criteria
func (t *TestAggregatePersister) GovernanceEventsByListingAddress(address common.Address) ([]*model.GovernanceEvent, error) {
	return []*model.GovernanceEvent{}, nil
}

// CreateGovernanceEvent creates a new governance event
func (t *TestAggregatePersister) CreateGovernanceEvent(govEvent *model.GovernanceEvent) error {
	return nil
}

// UpdateGovernanceEvent updates fields on an existing governance event
func (t *TestAggregatePersister) UpdateGovernanceEvent(govEvent *model.GovernanceEvent) error {
	return nil
}

// DeleteGovenanceEvent removes a governance event
func (t *TestAggregatePersister) DeleteGovenanceEvent(govEvent *model.GovernanceEvent) error {
	return nil
}

func TestEventProcessor(t *testing.T) {
	gasLimit := uint64(8000000)
	client, _ := contractutils.SetupSimulatedClient(gasLimit)
	persister := &TestAggregatePersister{}
	proc := processor.NewEventProcessor(client, persister, persister, persister)
	numEvents := 10
	events := make([]*crawlermodel.CivilEvent, numEvents)
	for i := 0; i < numEvents; i++ {
		event, _ := setupCivilEvent()
		events[i] = event
	}
	err := proc.Process(events)
	if err != nil {
		t.Errorf("Error processing events: err: %v", err)
	}
}
