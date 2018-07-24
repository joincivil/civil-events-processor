package processor_test

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/joincivil/civil-events-crawler/pkg/contractutils"
	"github.com/joincivil/civil-events-crawler/pkg/generated/contract"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	"github.com/joincivil/civil-events-crawler/pkg/utils"

	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/processor"
)

var (
	editorAddress = "0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d"
	testAddress   = "0xDFe273082089bB7f70Ee36Eebcde64832FE97E55"
	testAddress2  = "0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d"
)

type TestPersister struct {
	listings  map[string]*model.Listing
	revisions map[string][]*model.ContentRevision
	govEvents map[string][]*model.GovernanceEvent
}

// GetListingsByAddress returns a slice of Listings based on addresses
func (t *TestPersister) ListingsByAddress(addresses []common.Address) ([]*model.Listing, error) {
	results := []*model.Listing{}
	for _, address := range addresses {
		listing, err := t.ListingByAddress(address)
		if err == nil {
			results = append(results, listing)
		}
	}
	return results, nil
}

// GetListingByAddress retrieves listings based on addresses
func (t *TestPersister) ListingByAddress(address common.Address) (*model.Listing, error) {
	listing := t.listings[address.Hex()]
	return listing, nil
}

// CreateListing creates a new listing
func (t *TestPersister) CreateListing(listing *model.Listing) error {
	addressHex := listing.ContractAddress().Hex()
	if t.listings == nil {
		t.listings = map[string]*model.Listing{}
	}
	t.listings[addressHex] = listing
	return nil
}

// UpdateListing updates fields on an existing listing
func (t *TestPersister) UpdateListing(listing *model.Listing) error {
	addressHex := listing.ContractAddress().Hex()
	if t.listings == nil {
		t.listings = map[string]*model.Listing{}
	}
	t.listings[addressHex] = listing
	return nil
}

// DeleteListing removes a listing
func (t *TestPersister) DeleteListing(listing *model.Listing) error {
	addressHex := listing.ContractAddress().Hex()
	if t.listings == nil {
		t.listings = map[string]*model.Listing{}
	}
	delete(t.listings, addressHex)
	return nil
}

// GetContentRevisions retrieves content revisions
func (t *TestPersister) ContentRevisions(address common.Address,
	contentID *big.Int) ([]*model.ContentRevision, error) {
	addressHex := address.Hex()
	addrRevs, ok := t.revisions[addressHex]
	if !ok {
		return []*model.ContentRevision{}, nil
	}
	contentRevisions := []*model.ContentRevision{}
	for _, rev := range addrRevs {
		if rev.ContractContentID() == contentID {
			contentRevisions = append(contentRevisions, rev)
		}
	}

	return contentRevisions, nil
}

// GetContentRevision retrieves content revisions
func (t *TestPersister) ContentRevision(address common.Address, contentID *big.Int,
	revisionID *big.Int) (*model.ContentRevision, error) {
	contentRevisions, err := t.ContentRevisions(address, contentID)
	if err != nil {
		return nil, nil
	}
	for _, rev := range contentRevisions {
		if rev.ContractRevisionID() == revisionID {
			return rev, nil
		}
	}
	return nil, nil
}

// CreateContentRevision creates a new content item
func (t *TestPersister) CreateContentRevision(revision *model.ContentRevision) error {
	addressHex := revision.ListingAddress().Hex()
	addrRevs, ok := t.revisions[addressHex]
	if !ok {
		t.revisions = map[string][]*model.ContentRevision{}
		t.revisions[addressHex] = []*model.ContentRevision{revision}
		return nil
	}
	addrRevs = append(addrRevs, revision)
	t.revisions[addressHex] = addrRevs
	return nil
}

// UpdateContentRevision updates fields on an existing content item
func (t *TestPersister) UpdateContentRevision(revision *model.ContentRevision) error {
	addressHex := revision.ListingAddress().Hex()
	addrRevs, ok := t.revisions[addressHex]
	if !ok {
		t.revisions = map[string][]*model.ContentRevision{}
		t.revisions[addressHex] = []*model.ContentRevision{revision}
		return nil
	}
	for index, rev := range addrRevs {
		if rev.ContractContentID() == revision.ContractContentID() &&
			rev.ContractRevisionID() == revision.ContractRevisionID() {
			addrRevs[index] = revision
		}
	}
	return nil
}

// DeleteContentRevision removes a content item
func (t *TestPersister) DeleteContentRevision(revision *model.ContentRevision) error {
	contentRevisions, err := t.ContentRevisions(
		revision.ListingAddress(),
		revision.ContractContentID(),
	)
	if err != nil {
		return nil
	}
	revisionID := revision.ContractRevisionID()
	updateRevs := []*model.ContentRevision{}
	for _, rev := range contentRevisions {
		if rev.ContractRevisionID() != revisionID {
			updateRevs = append(updateRevs, rev)
		}
	}
	t.revisions[revision.ListingAddress().Hex()] = updateRevs
	return nil
}

// GetGovernanceEventsByListingAddress retrieves governance events based on criteria
func (t *TestPersister) GovernanceEventsByListingAddress(address common.Address) ([]*model.GovernanceEvent, error) {
	addressHex := address.Hex()
	govEvents := t.govEvents[addressHex]
	return govEvents, nil
}

// CreateGovernanceEvent creates a new governance event
func (t *TestPersister) CreateGovernanceEvent(govEvent *model.GovernanceEvent) error {
	addressHex := govEvent.ListingAddress().Hex()
	events, ok := t.govEvents[addressHex]
	if !ok {
		t.govEvents = map[string][]*model.GovernanceEvent{}
		t.govEvents[addressHex] = []*model.GovernanceEvent{govEvent}
		return nil
	}
	events = append(events, govEvent)
	t.govEvents[addressHex] = events
	return nil
}

// UpdateGovernanceEvent updates fields on an existing governance event
func (t *TestPersister) UpdateGovernanceEvent(govEvent *model.GovernanceEvent) error {
	addressHex := govEvent.ListingAddress().Hex()
	events, ok := t.govEvents[addressHex]
	if !ok {
		t.govEvents[addressHex] = []*model.GovernanceEvent{govEvent}
		return nil
	}
	for index, event := range events {
		if event.GovernanceEventType() == govEvent.GovernanceEventType() &&
			event.SenderAddress() == govEvent.SenderAddress() &&
			event.CreationDateTs() == govEvent.CreationDateTs() {
			events[index] = govEvent
		}
	}
	return nil
}

// DeleteGovenanceEvent removes a governance event
func (t *TestPersister) DeleteGovenanceEvent(govEvent *model.GovernanceEvent) error {
	addressHex := govEvent.ListingAddress().Hex()
	events, ok := t.govEvents[addressHex]
	if !ok {
		t.govEvents[addressHex] = []*model.GovernanceEvent{govEvent}
		return nil
	}
	updatedEvents := []*model.GovernanceEvent{}
	for _, event := range events {
		if event.GovernanceEventType() != govEvent.GovernanceEventType() ||
			event.SenderAddress() != govEvent.SenderAddress() ||
			event.CreationDateTs() != govEvent.CreationDateTs() {
			updatedEvents = append(updatedEvents, event)
		}
	}
	t.govEvents[addressHex] = updatedEvents
	return nil
}

type TestScraper struct{}

func (t *TestScraper) ScrapeContent(uri string) (*model.ScraperContent, error) {
	return &model.ScraperContent{}, nil
}

func (t *TestScraper) ScrapeMetadata(uri string) (*model.ScraperContentMetadata, error) {
	return &model.ScraperContentMetadata{}, nil
}

func TestEventProcessor(t *testing.T) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &TestPersister{}
	scraper := &TestScraper{}
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister,
		scraper, scraper)

	applied1 := &contract.CivilTCRContractApplication{
		ListingAddress: contracts.NewsroomAddr,
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
	applied2 := &contract.CivilTCRContractApplication{
		ListingAddress: contracts.NewsroomAddr,
		Deposit:        big.NewInt(1000),
		AppEndDate:     big.NewInt(1653860896),
		Data:           "DATA",
		Applicant:      common.HexToAddress(testAddress),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888890,
			TxHash:      common.Hash{},
			TxIndex:     4,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false,
		},
	}
	revision1 := &contract.NewsroomContractRevisionUpdated{
		Editor:     common.HexToAddress(editorAddress),
		ContentId:  big.NewInt(0),
		RevisionId: big.NewInt(0),
		Uri:        "http://joincivil.com/1",
		Raw: types.Log{
			Address:     contracts.NewsroomAddr,
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 888889,
			TxHash:      common.Hash{},
			TxIndex:     3,
			BlockHash:   common.Hash{},
			Index:       4,
			Removed:     false,
		},
	}

	events := []*crawlermodel.Event{}
	event1, _ := crawlermodel.NewEventFromContractEvent(
		"_Application",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		applied1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event1)
	event2, _ := crawlermodel.NewEventFromContractEvent(
		"RevisionUpdated",
		"NewsroomContract",
		contracts.NewsroomAddr,
		revision1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event2)
	event3, _ := crawlermodel.NewEventFromContractEvent(
		"_Application",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		applied2,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event3)

	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	if len(persister.listings) == 0 {
		t.Error("Should have seen at least 1 listing")
	}
	if len(persister.revisions) == 0 {
		t.Error("Should have seen at least 1 revision")
	}
	if len(persister.govEvents) == 0 {
		t.Error("Should have seen at least 1 governance event")
	}
	listing := persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.LastGovernanceState() != model.GovernanceStateApplied {
		t.Errorf("Listing should have had governance state of applied")
	}
	if listing.Whitelisted() {
		t.Errorf("Should not be whitelisted")
	}
	if listing.CharterURI() != "newsroom.com/charter" {
		t.Errorf("Listing charter URI is not correct")
	}
	if listing.ContractAddress() != contracts.NewsroomAddr {
		t.Errorf("Should have the correct newsroom address")
	}
	if len(listing.OwnerAddresses()) <= 0 {
		t.Errorf("Should have at least one owner address")
	}
}

func TestEventProcessorChallenge(t *testing.T) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &TestPersister{}
	scraper := &TestScraper{}
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister,
		scraper, scraper)

	applied1 := &contract.CivilTCRContractApplication{
		ListingAddress: contracts.NewsroomAddr,
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
	challenge1 := &contract.CivilTCRContractChallenge{
		ListingAddress: contracts.NewsroomAddr,
		ChallengeID:    big.NewInt(120),
		Data:           "DATA",
		CommitEndDate:  big.NewInt(1653860896),
		RevealEndDate:  big.NewInt(1653860896),
		Challenger:     common.HexToAddress(testAddress),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888890,
			TxHash:      common.Hash{},
			TxIndex:     4,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false,
		},
	}

	events := []*crawlermodel.Event{}
	event1, _ := crawlermodel.NewEventFromContractEvent(
		"_Challenge",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		challenge1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event1)

	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	if len(persister.listings) == 0 {
		t.Error("Should have seen at least 1 listing")
	}
	if len(persister.govEvents) == 0 {
		t.Error("Should have seen at least 1 governance event")
	}
	listing := persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.LastGovernanceState() != model.GovernanceStateChallenged {
		t.Errorf("Listing should have had governance state of applied")
	}
	if listing.Whitelisted() {
		t.Errorf("Should not be whitelisted")
	}
	if listing.CharterURI() != "newsroom.com/charter" {
		t.Errorf("Listing charter URI is not correct")
	}
	if listing.ContractAddress() != contracts.NewsroomAddr {
		t.Errorf("Should have the correct newsroom address")
	}
	if len(listing.OwnerAddresses()) <= 0 {
		t.Errorf("Should have at least one owner address")
	}

	events = []*crawlermodel.Event{}
	event2, _ := crawlermodel.NewEventFromContractEvent(
		"_Application",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		applied1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event2)
	event3, _ := crawlermodel.NewEventFromContractEvent(
		"_Challenge",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		challenge1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event3)

	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	if len(persister.listings) == 0 {
		t.Error("Should have seen at least 1 listing")
	}
	if len(persister.govEvents) == 0 {
		t.Error("Should have seen at least 1 governance event")
	}
	listing = persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.LastGovernanceState() != model.GovernanceStateChallenged {
		t.Errorf("Listing should have had governance state of applied")
	}
	if listing.Whitelisted() {
		t.Errorf("Should not be whitelisted")
	}
	if listing.CharterURI() != "newsroom.com/charter" {
		t.Errorf("Listing charter URI is not correct")
	}
	if listing.ContractAddress() != contracts.NewsroomAddr {
		t.Errorf("Should have the correct newsroom address")
	}
	if len(listing.OwnerAddresses()) <= 0 {
		t.Errorf("Should have at least one owner address")
	}
}

func TestEventProcessorAppWhitelisted(t *testing.T) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &TestPersister{}
	scraper := &TestScraper{}
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister,
		scraper, scraper)

	whitelisted1 := &contract.CivilTCRContractApplicationWhitelisted{
		ListingAddress: contracts.NewsroomAddr,
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888895,
			TxHash:      common.Hash{},
			TxIndex:     8,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false,
		},
	}

	events := []*crawlermodel.Event{}
	event1, _ := crawlermodel.NewEventFromContractEvent(
		"_ApplicationWhitelisted",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		whitelisted1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event1)

	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	if len(persister.listings) == 0 {
		t.Error("Should have seen at least 1 listing")
	}
	if len(persister.govEvents) == 0 {
		t.Error("Should have seen at least 1 governance event")
	}
	listing := persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.LastGovernanceState() != model.GovernanceStateAppWhitelisted {
		t.Errorf("Listing should have had governance state of whitelisted")
	}
	if !listing.Whitelisted() {
		t.Errorf("Should have been whitelisted")
	}
	if listing.CharterURI() != "newsroom.com/charter" {
		t.Errorf("Listing charter URI is not correct")
	}
	if listing.ContractAddress() != contracts.NewsroomAddr {
		t.Errorf("Should have the correct newsroom address")
	}
	if len(listing.OwnerAddresses()) <= 0 {
		t.Errorf("Should have at least one owner address")
	}

	events = []*crawlermodel.Event{}
	event1, _ = crawlermodel.NewEventFromContractEvent(
		"_ApplicationWhitelisted",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		whitelisted1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event1)
	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	listing = persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.LastGovernanceState() != model.GovernanceStateAppWhitelisted {
		t.Errorf("Listing should have had governance state of whitelisted")
	}
}

func TestEventProcessorApplicationRemoved(t *testing.T) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &TestPersister{}
	scraper := &TestScraper{}
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister,
		scraper, scraper)

	removed1 := &contract.CivilTCRContractApplicationRemoved{
		ListingAddress: contracts.NewsroomAddr,
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888895,
			TxHash:      common.Hash{},
			TxIndex:     8,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false,
		},
	}

	events := []*crawlermodel.Event{}
	event1, _ := crawlermodel.NewEventFromContractEvent(
		"_ApplicationRemoved",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		removed1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event1)

	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	if len(persister.listings) == 0 {
		t.Error("Should have seen at least 1 listing")
	}
	if len(persister.govEvents) == 0 {
		t.Error("Should have seen at least 1 governance event")
	}
	listing := persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.LastGovernanceState() != model.GovernanceStateAppRemoved {
		t.Errorf("Listing should have had governance state of whitelisted")
	}
	if listing.Whitelisted() {
		t.Errorf("Should not have been whitelisted")
	}
	if listing.CharterURI() != "newsroom.com/charter" {
		t.Errorf("Listing charter URI is not correct")
	}
	if listing.ContractAddress() != contracts.NewsroomAddr {
		t.Errorf("Should have the correct newsroom address")
	}
	if len(listing.OwnerAddresses()) <= 0 {
		t.Errorf("Should have at least one owner address")
	}

	events = []*crawlermodel.Event{}
	event1, _ = crawlermodel.NewEventFromContractEvent(
		"_ApplicationRemoved",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		removed1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event1)
	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	listing = persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.LastGovernanceState() != model.GovernanceStateAppRemoved {
		t.Errorf("Listing should have had governance state of whitelisted")
	}
}

func TestEventProcessorListingRemoved(t *testing.T) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &TestPersister{}
	scraper := &TestScraper{}
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister,
		scraper, scraper)

	removed1 := &contract.CivilTCRContractListingRemoved{
		ListingAddress: contracts.NewsroomAddr,
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888897,
			TxHash:      common.Hash{},
			TxIndex:     9,
			BlockHash:   common.Hash{},
			Index:       8,
			Removed:     false,
		},
	}

	events := []*crawlermodel.Event{}
	event1, _ := crawlermodel.NewEventFromContractEvent(
		"_ListingRemoved",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		removed1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event1)

	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	if len(persister.listings) == 0 {
		t.Error("Should have seen at least 1 listing")
	}
	if len(persister.govEvents) == 0 {
		t.Error("Should have seen at least 1 governance event")
	}
	listing := persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.LastGovernanceState() != model.GovernanceStateRemoved {
		t.Errorf("Listing should have had governance state of listing removed")
	}
	if listing.Whitelisted() {
		t.Errorf("Should not be whitelisted")
	}
	if listing.CharterURI() != "newsroom.com/charter" {
		t.Errorf("Listing charter URI is not correct")
	}
	if listing.ContractAddress() != contracts.NewsroomAddr {
		t.Errorf("Should have the correct newsroom address")
	}
	if len(listing.OwnerAddresses()) <= 0 {
		t.Errorf("Should have at least one owner address")
	}

	events = []*crawlermodel.Event{}
	event1, _ = crawlermodel.NewEventFromContractEvent(
		"_ListingRemoved",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		removed1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event1)
	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	listing = persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.LastGovernanceState() != model.GovernanceStateRemoved {
		t.Errorf("Listing should have had governance state of listing removed")
	}
}

func TestEventProcessorListingWithdrawn(t *testing.T) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &TestPersister{}
	scraper := &TestScraper{}
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister,
		scraper, scraper)

	removed1 := &contract.CivilTCRContractListingWithdrawn{
		ListingAddress: contracts.NewsroomAddr,
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888897,
			TxHash:      common.Hash{},
			TxIndex:     9,
			BlockHash:   common.Hash{},
			Index:       8,
			Removed:     false,
		},
	}

	events := []*crawlermodel.Event{}
	event1, _ := crawlermodel.NewEventFromContractEvent(
		"_ListingWithdrawn",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		removed1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event1)

	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	if len(persister.listings) == 0 {
		t.Error("Should have seen at least 1 listing")
	}
	if len(persister.govEvents) == 0 {
		t.Error("Should have seen at least 1 governance event")
	}
	listing := persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.LastGovernanceState() != model.GovernanceStateWithdrawn {
		t.Errorf("Listing should have had governance state of listing withdrawn")
	}
	if listing.Whitelisted() {
		t.Errorf("Should not be whitelisted")
	}
	if listing.CharterURI() != "newsroom.com/charter" {
		t.Errorf("Listing charter URI is not correct")
	}
	if listing.ContractAddress() != contracts.NewsroomAddr {
		t.Errorf("Should have the correct newsroom address")
	}
	if len(listing.OwnerAddresses()) <= 0 {
		t.Errorf("Should have at least one owner address")
	}

	events = []*crawlermodel.Event{}
	event1, _ = crawlermodel.NewEventFromContractEvent(
		"_ListingWithdrawn",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		removed1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event1)
	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	listing = persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.LastGovernanceState() != model.GovernanceStateWithdrawn {
		t.Errorf("Listing should have had governance state of listing withdrawn")
	}
}

func TestEventProcessorNewsroomNameChanged(t *testing.T) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &TestPersister{}
	scraper := &TestScraper{}
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister,
		scraper, scraper)

	applied1 := &contract.CivilTCRContractApplication{
		ListingAddress: contracts.NewsroomAddr,
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

	newName := "ThisIsANewName"
	namechanged1 := &contract.NewsroomContractNameChanged{
		NewName: newName,
		Raw: types.Log{
			Address:     contracts.NewsroomAddr,
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888891,
			TxHash:      common.Hash{},
			TxIndex:     1,
			BlockHash:   common.Hash{},
			Index:       10,
			Removed:     false,
		},
	}

	events := []*crawlermodel.Event{}
	event1, _ := crawlermodel.NewEventFromContractEvent(
		"NameChanged",
		"NewsroomContract",
		contracts.NewsroomAddr,
		namechanged1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event1)
	err = proc.Process(events)
	if err == nil {
		t.Errorf("Should have failed processing events due to non existent listing")
	}

	events = []*crawlermodel.Event{}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_Application",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		applied1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event)
	event1, _ = crawlermodel.NewEventFromContractEvent(
		"NameChanged",
		"NewsroomContract",
		contracts.NewsroomAddr,
		namechanged1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event1)

	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	if len(persister.listings) == 0 {
		t.Error("Should have seen at least 1 listing")
	}
	listing := persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.Name() != newName {
		t.Errorf("Should have updated the name of the newsroom")
	}
}

func TestCivilProcessorOwnershipTransferred(t *testing.T) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &TestPersister{}
	scraper := &TestScraper{}
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister,
		scraper, scraper)

	applied1 := &contract.CivilTCRContractApplication{
		ListingAddress: contracts.NewsroomAddr,
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
	ownership1 := &contract.NewsroomContractOwnershipTransferred{
		PreviousOwner: common.HexToAddress(testAddress),
		NewOwner:      common.HexToAddress(testAddress2),
		Raw: types.Log{
			Address:     contracts.NewsroomAddr,
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888891,
			TxHash:      common.Hash{},
			TxIndex:     1,
			BlockHash:   common.Hash{},
			Index:       10,
			Removed:     false,
		},
	}
	events := []*crawlermodel.Event{}
	event1, _ := crawlermodel.NewEventFromContractEvent(
		"OwnershipTransferred",
		"NewsroomContract",
		contracts.NewsroomAddr,
		ownership1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event1)
	err = proc.Process(events)
	if err == nil {
		t.Errorf("Should have failed processing events due to non existent listing")
	}

	events = []*crawlermodel.Event{}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_Application",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		applied1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event)
	event1, _ = crawlermodel.NewEventFromContractEvent(
		"OwnershipTransferred",
		"NewsroomContract",
		contracts.NewsroomAddr,
		ownership1,
		utils.CurrentEpochSecsInInt(),
	)
	events = append(events, event1)

	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	if len(persister.listings) == 0 {
		t.Error("Should have seen at least 1 listing")
	}
	listing := persister.listings[contracts.NewsroomAddr.Hex()]
	if len(listing.OwnerAddresses()) != 1 {
		t.Errorf("Should still only have 1 owner for the listing")
	}
	if listing.OwnerAddresses()[0] != common.HexToAddress(testAddress2) {
		t.Errorf("Should have updated the listing with new owner")
	}

}
