package processor_test

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"math/big"
	"reflect"
	"testing"

	"github.com/joincivil/civil-events-crawler/pkg/contractutils"
	"github.com/joincivil/civil-events-crawler/pkg/generated/contract"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	"github.com/joincivil/civil-events-crawler/pkg/utils"

	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/processor"
)

const (
	// https://civil-develop.go-vip.co/crawler-pod/wp-json/civil-newsroom-protocol/v1/revisions/11
	testCivilMetadata = `{"title":"This is a test post","revisionContentHash":"0x9e4acfe532c8458abfc1f1d30c4eaf986fee52cf1f65c9548f1dc437fb6dfd38","revisionContentUrl":"https:\/\/civil-develop.go-vip.co\/crawler-pod\/wp-json\/civil-newsroom-protocol\/v1\/revisions-content\/0x9e4acfe532c8458abfc1f1d30c4eaf986fee52cf1f65c9548f1dc437fb6dfd38\/","canonicalUrl":"https:\/\/civil-develop.go-vip.co\/crawler-pod\/2018\/07\/25\/this-is-a-test-post\/","slug":"this-is-a-test-post","description":"I'm being described","authors":[{"byline":"Walker Flynn"}],"images":[{"url":"https:\/\/civil-develop.go-vip.co\/crawler-pod\/wp-content\/uploads\/sites\/20\/2018\/07\/Messages-Image3453599984.png","hash":"0x72ca80ed96a2b1ca20bf758a2142a678c0bc316e597161d0572af378e52b2e80","h":960,"w":697}],"tags":["news"],"primaryTag":"news","revisionDate":"2018-07-25 17:17:20","originalPublishDate":"2018-07-25 17:17:07","credibilityIndicators":{"original_reporting":"1","on_the_ground":false,"sources_cited":"1","subject_specialist":false},"opinion":false,"civilSchemaVersion":"1.0.0"}`
)

var (
	editorAddress = "0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d"
	testAddress   = "0xDFe273082089bB7f70Ee36Eebcde64832FE97E55"
	testAddress2  = "0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d"
)

type TestPersister struct {
	listings   map[string]*model.Listing
	revisions  map[string][]*model.ContentRevision
	govEvents  map[string][]*model.GovernanceEvent
	challenges map[int]*model.Challenge
	appeals    map[int]*model.Appeal
	polls      map[int]*model.Poll
	timestamp  int64
}

// ListingsByCriteria returns a slice of Listings based on ListingCriteria
func (t *TestPersister) ListingsByCriteria(criteria *model.ListingCriteria) ([]*model.Listing, error) {
	listings := make([]*model.Listing, len(t.listings))
	index := 0
	for _, listing := range t.listings {
		listings[index] = listing
		index++
	}
	return listings, nil
}

// istingsByAddresses returns a slice of Listings based on addresses
func (t *TestPersister) ListingsByAddresses(addresses []common.Address) ([]*model.Listing, error) {
	results := []*model.Listing{}
	for _, address := range addresses {
		listing, err := t.ListingByAddress(address)
		if err == nil {
			results = append(results, listing)
		}
	}
	return results, nil
}

// ListingByAddress retrieves a listing based on address
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
func (t *TestPersister) UpdateListing(listing *model.Listing, updatedFields []string) error {
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

// ContentRevisionsByCriteria retrieves content revisions by ContentRevisionCriteria
func (t *TestPersister) ContentRevisionsByCriteria(criteria *model.ContentRevisionCriteria) (
	[]*model.ContentRevision, error) {
	revisions := make([]*model.ContentRevision, len(t.revisions))
	index := 0
	for _, contentRevisions := range t.revisions {
		revisions[index] = contentRevisions[len(contentRevisions)-1]
		index++
	}
	return revisions, nil
}

// ContentRevisions retrieves content revisions
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
func (t *TestPersister) UpdateContentRevision(revision *model.ContentRevision, updatedFields []string) error {
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

// GovernanceEventsByCriteria retrieves content revisions by GovernanceEventCriteria
func (t *TestPersister) GovernanceEventsByCriteria(criteria *model.GovernanceEventCriteria) (
	[]*model.GovernanceEvent, error) {
	events := make([]*model.GovernanceEvent, len(t.govEvents))
	index := 0
	for _, event := range t.govEvents {
		events[index] = event[len(event)-1]
		index++
	}
	return events, nil
}

// GovernanceEventByChallengeID retrieves challenge by challengeID
func (t *TestPersister) GovernanceEventByChallengeID(challengeID int) (*model.GovernanceEvent, error) {
	// NOTE(IS): Placeholder for now
	govEvent := &model.GovernanceEvent{}
	return govEvent, nil
}

// GovernanceEventsByChallengeIDs retrieves challenges by challengeIDs
func (t *TestPersister) GovernanceEventsByChallengeIDs(challengeIDs []int) ([]*model.GovernanceEvent, error) {
	// NOTE(IS): Placeholder for now
	govEvents := []*model.GovernanceEvent{}
	return govEvents, nil
}

// GovernanceEventsByTxHash gets governance events based on txhash
func (t *TestPersister) GovernanceEventsByTxHash(txHash common.Hash) ([]*model.GovernanceEvent, error) {
	// NOTE(IS): Placeholder for now
	govEvents := []*model.GovernanceEvent{}
	return govEvents, nil
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
func (t *TestPersister) UpdateGovernanceEvent(govEvent *model.GovernanceEvent, updatedFields []string) error {
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
func (t *TestPersister) DeleteGovernanceEvent(govEvent *model.GovernanceEvent) error {
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

// ChallengeByChallengeID gets a challenge by challengeID
func (t *TestPersister) ChallengeByChallengeID(challengeID int) (*model.Challenge, error) {
	challenge := t.challenges[challengeID]
	return challenge, nil
}

// ChallengesByChallengeIDs returns a slice of challenges based on challenge IDs
func (t *TestPersister) ChallengesByChallengeIDs(challengeIDs []int) ([]*model.Challenge, error) {
	results := []*model.Challenge{}
	for _, challengeID := range challengeIDs {
		challenge, err := t.ChallengeByChallengeID(challengeID)
		if err == nil {
			results = append(results, challenge)
		}
	}
	return results, nil
}

// ChallengesByListingAddress gets a list of challenges by listing
func (t *TestPersister) ChallengesByListingAddress(addr common.Address) ([]*model.Challenge, error) {
	challenges := make([]*model.Challenge, len(t.challenges))
	index := 0
	for _, val := range t.challenges {
		challenges[index] = val
		index++
	}
	return challenges, nil
}

// CreateChallenge creates a new challenge
func (t *TestPersister) CreateChallenge(challenge *model.Challenge) error {
	challengeID := int(challenge.ChallengeID().Int64())
	if t.challenges == nil {
		t.challenges = map[int]*model.Challenge{}
	}
	t.challenges[challengeID] = challenge
	return nil
}

// UpdateChallenge updates a challenge
func (t *TestPersister) UpdateChallenge(challenge *model.Challenge, updatedFields []string) error {
	challengeID := int(challenge.ChallengeID().Int64())
	if t.challenges == nil {
		t.challenges = map[int]*model.Challenge{}
	}
	t.challenges[challengeID] = challenge
	return nil
}

// PollByPollID gets a poll by pollID
func (t *TestPersister) PollByPollID(pollID int) (*model.Poll, error) {
	poll := t.polls[pollID]
	return poll, nil
}

// PollsByPollIDs returns a slice of polls based on poll IDs
func (t *TestPersister) PollsByPollIDs(pollIDs []int) ([]*model.Poll, error) {
	results := []*model.Poll{}
	for _, pollID := range pollIDs {
		poll, err := t.PollByPollID(pollID)
		if err == nil {
			results = append(results, poll)
		}
	}
	return results, nil
}

// CreatePoll creates a new poll
func (t *TestPersister) CreatePoll(poll *model.Poll) error {
	pollID := int(poll.PollID().Int64())
	if t.polls == nil {
		t.polls = map[int]*model.Poll{}
	}
	t.polls[pollID] = poll
	return nil
}

// UpdatePoll updates a poll
func (t *TestPersister) UpdatePoll(poll *model.Poll, updatedFields []string) error {
	pollID := int(poll.PollID().Int64())
	if t.polls == nil {
		t.polls = map[int]*model.Poll{}
	}
	t.polls[pollID] = poll
	return nil
}

// AppealByChallengeID gets an appeal by challengeID
func (t *TestPersister) AppealByChallengeID(challengeID int) (*model.Appeal, error) {
	appeal := t.appeals[challengeID]
	return appeal, nil
}

// AppealsByChallengeIDs returns a slice of appeals based on challenge IDs
func (t *TestPersister) AppealsByChallengeIDs(challengeIDs []int) ([]*model.Appeal, error) {
	results := []*model.Appeal{}
	for _, challengeID := range challengeIDs {
		appeal, err := t.AppealByChallengeID(challengeID)
		if err == nil {
			results = append(results, appeal)
		}
	}
	return results, nil
}

// CreateAppeal creates a new appeal
func (t *TestPersister) CreateAppeal(appeal *model.Appeal) error {
	challengeID := int(appeal.OriginalChallengeID().Int64())
	if t.appeals == nil {
		t.appeals = map[int]*model.Appeal{}
	}
	t.appeals[challengeID] = appeal
	return nil
}

// UpdateAppeal updates an appeal
func (t *TestPersister) UpdateAppeal(appeal *model.Appeal, updatedFields []string) error {
	challengeID := int(appeal.OriginalChallengeID().Int64())
	if t.appeals == nil {
		t.appeals = map[int]*model.Appeal{}
	}
	t.appeals[challengeID] = appeal
	return nil
}

func (t *TestPersister) TimestampOfLastEventForCron() (int64, error) {
	return t.timestamp, nil
}

func (t *TestPersister) UpdateTimestampForCron(timestamp int64) error {
	t.timestamp = timestamp
	return nil
}

type TestScraper struct{}

func (t *TestScraper) ScrapeContent(uri string) (*model.ScraperContent, error) {
	return &model.ScraperContent{}, nil
}

func (t *TestScraper) ScrapeCivilMetadata(uri string) (*model.ScraperCivilMetadata, error) {
	metadata := model.NewScraperCivilMetadata()
	err := json.Unmarshal([]byte(testCivilMetadata), metadata)
	if err != nil {
		return nil, err
	}
	return metadata, nil
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
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister, persister,
		persister, persister, scraper, scraper, scraper)

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
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event1)
	event2, _ := crawlermodel.NewEventFromContractEvent(
		"RevisionUpdated",
		"NewsroomContract",
		contracts.NewsroomAddr,
		revision1,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
	)
	events = append(events, event2)
	event3, _ := crawlermodel.NewEventFromContractEvent(
		"_Application",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		applied2,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
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
	if listing.Charter().URI() != "newsroom.com/charter" {
		t.Errorf("Listing charter URI is not correct")
	}
	if listing.ContractAddress() != contracts.NewsroomAddr {
		t.Errorf("Should have the correct newsroom address")
	}
	if len(listing.OwnerAddresses()) <= 0 {
		t.Errorf("Should have at least one owner address")
	}
	if !reflect.DeepEqual(listing.UnstakedDeposit(), big.NewInt(1000)) {
		t.Errorf("UnstakedDeposit value is not correct: %v", listing.UnstakedDeposit())
	}
}

func TestEventProcessorChallenge(t *testing.T) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &TestPersister{}
	scraper := &TestScraper{}
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister, persister,
		persister, persister, scraper, scraper, scraper)

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
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
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
	if listing.Charter().URI() != "newsroom.com/charter" {
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
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event2)
	event3, _ := crawlermodel.NewEventFromContractEvent(
		"_Challenge",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		challenge1,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
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
	if listing.Charter().URI() != "newsroom.com/charter" {
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
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister, persister,
		persister, persister, scraper, scraper, scraper)

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
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
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
	if listing.Charter().URI() != "newsroom.com/charter" {
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
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
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
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister, persister,
		persister, persister, scraper, scraper, scraper)

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
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
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
	if listing.Charter().URI() != "newsroom.com/charter" {
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
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
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
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister, persister,
		persister, persister, scraper, scraper, scraper)

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
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
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
	if listing.Charter().URI() != "newsroom.com/charter" {
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
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
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
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister, persister,
		persister, persister, scraper, scraper, scraper)

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
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
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
	if listing.Charter().URI() != "newsroom.com/charter" {
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
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
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
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister, persister,
		persister, persister, scraper, scraper, scraper)

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
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event1)
	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should have processed events for non existent listing")
	}

	events = []*crawlermodel.Event{}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_Application",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		applied1,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event)
	event1, _ = crawlermodel.NewEventFromContractEvent(
		"NameChanged",
		"NewsroomContract",
		contracts.NewsroomAddr,
		namechanged1,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event1)

	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events %v", err)
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
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister, persister,
		persister, persister, scraper, scraper, scraper)

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
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event1)
	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should have processed events for non existent listing, %v", err)
	}

	events = []*crawlermodel.Event{}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_Application",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		applied1,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event)
	event1, _ = crawlermodel.NewEventFromContractEvent(
		"OwnershipTransferred",
		"NewsroomContract",
		contracts.NewsroomAddr,
		ownership1,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
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

func TestEventProcessorChallengeUpdate(t *testing.T) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &TestPersister{}
	scraper := &TestScraper{}
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister, persister,
		persister, persister, scraper, scraper, scraper)

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
		"_Application",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		applied1,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	event2, _ := crawlermodel.NewEventFromContractEvent(
		"_Challenge",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		challenge1,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
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
	// check for nil challenge id value
	listing := persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.ChallengeID() != nil {
		t.Errorf("Challenge ID should have been nil but it is %v", listing.ChallengeID())
	}

	events = []*crawlermodel.Event{}
	events = append(events, event2)
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

	// check that challenge id value is changed
	listing = persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.ChallengeID() != challenge1.ChallengeID {
		t.Errorf("Challenge ID should have been %v but it is %v", challenge1.ChallengeID, listing.ChallengeID())
	}

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

	event3, _ := crawlermodel.NewEventFromContractEvent(
		"_ApplicationWhitelisted",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		whitelisted1,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
	)

	events = []*crawlermodel.Event{}
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

	// check for challengeid reset
	listing = persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.ChallengeID().Int64() != 0 {
		t.Errorf("Challenge ID should have been reset to 0 but it is %v", listing.ChallengeID())
	}
	// TODO(IS): Unstaked deposit is 0 bc we don't have the sequence of events leading up to this
	// new challenge, then test another reset event
	events = []*crawlermodel.Event{}

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
	event4, _ := crawlermodel.NewEventFromContractEvent(
		"_ApplicationRemoved",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		removed1,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event2, event4)
	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	// check for challengeid reset
	listing = persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.ChallengeID().Int64() != 0 {
		t.Errorf("Challenge ID should have been reset to 0 but it is %v", listing.ChallengeID())
	}

	if listing.AppExpiry().Int64() != 0 {
		t.Errorf("AppExpiry should have been reset to 0 but it is %v", listing.AppExpiry())
	}

	// new challenge, then test another reset event
	events = []*crawlermodel.Event{}

	removed2 := &contract.CivilTCRContractListingRemoved{
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

	event5, _ := crawlermodel.NewEventFromContractEvent(
		"_ListingRemoved",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		removed2,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)

	events = append(events, event2, event5)
	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	// check for challengeid reset
	listing = persister.listings[contracts.NewsroomAddr.Hex()]
	if listing.ChallengeID().Int64() != 0 {
		t.Errorf("Challenge ID should have been reset to 0 but it is %v", listing.ChallengeID())
	}
}

func TestEmptyContractAddress(t *testing.T) {
	tcrAddress := common.Address{}
	if tcrAddress != (common.Address{}) {
		t.Error("2 blank common.Address types should be equal")
	}

}

func setupAppeal(t *testing.T, challengeID *big.Int) (*processor.EventProcessor, *contractutils.AllTestContracts,
	[]*crawlermodel.Event, *TestPersister) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &TestPersister{}
	scraper := &TestScraper{}
	proc := processor.NewEventProcessor(contracts.Client, persister, persister, persister, persister,
		persister, persister, scraper, scraper, scraper)

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
		ChallengeID:    challengeID,
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
	appealRequested := &contract.CivilTCRContractAppealRequested{
		ListingAddress: contracts.NewsroomAddr,
		ChallengeID:    challengeID,
		AppealFeePaid:  big.NewInt(1000),
		Requester:      common.HexToAddress(testAddress),
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

	event1, _ := crawlermodel.NewEventFromContractEvent(
		"_Application",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		applied1,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	event2, _ := crawlermodel.NewEventFromContractEvent(
		"_Challenge",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		challenge1,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	event3, _ := crawlermodel.NewEventFromContractEvent(
		"_AppealRequested",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		appealRequested,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events := []*crawlermodel.Event{event1, event2, event3}
	err = proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return proc, contracts, events, persister
}

func TestProcessTCRAppealRequested(t *testing.T) {
	challengeID := big.NewInt(120)
	_, _, _, persister := setupAppeal(t, challengeID)

	persistedChallenge := persister.challenges[int(challengeID.Int64())]
	if persistedChallenge == nil {
		t.Error("Should not have returned nil challenge")
	}

	persistedAppeal := persister.appeals[int(challengeID.Int64())]
	if persistedAppeal == nil {
		t.Error("Should not have rreturned nil appeal")
	}
}

func TestProcessTCRGrantedAppealChallenged(t *testing.T) {
	challengeID := big.NewInt(120)
	appealChallengeID := big.NewInt(130)
	proc, contracts, _, persister := setupAppeal(t, challengeID)

	appealChallenged := &contract.CivilTCRContractGrantedAppealChallenged{
		ListingAddress:    contracts.NewsroomAddr,
		ChallengeID:       challengeID,
		AppealChallengeID: appealChallengeID,
		Data:              "DATA",
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888889,
			TxHash:      common.Hash{},
			TxIndex:     2,
			BlockHash:   common.Hash{},
			Index:       2,
			Removed:     false,
		},
	}
	event4, _ := crawlermodel.NewEventFromContractEvent(
		"_GrantedAppealChallenged",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		appealChallenged,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events := []*crawlermodel.Event{event4}
	err := proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	persistedChallenge := persister.challenges[int(appealChallengeID.Int64())]
	if persistedChallenge == nil {
		t.Error("Should not have returned nil challenge")
	}

}

func TestProcessTCRAppealGranted(t *testing.T) {
	challengeID := big.NewInt(120)
	proc, contracts, _, persister := setupAppeal(t, challengeID)

	persistedAppeal := persister.appeals[int(challengeID.Int64())]
	if persistedAppeal == nil {
		t.Error("Should not have rreturned nil appeal")
	}
	if persistedAppeal.AppealGranted() {
		t.Error("Appeal granted field should be false")
	}

	appealGranted := &contract.CivilTCRContractAppealGranted{
		ListingAddress: contracts.NewsroomAddr,
		ChallengeID:    challengeID,
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888888,
			TxHash:      common.Hash{},
			TxIndex:     2,
			BlockHash:   common.Hash{},
			Index:       3,
			Removed:     false,
		},
	}

	event4, _ := crawlermodel.NewEventFromContractEvent(
		"_AppealGranted",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		appealGranted,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events := []*crawlermodel.Event{event4}
	err := proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	persistedChallenge := persister.challenges[int(challengeID.Int64())]
	if persistedChallenge == nil {
		t.Error("Should not have returned nil challenge")
	}

	// check that field in appeal granted has changed
	// TODO(IS): should check more but then events should be created by simulated backend
	if !persistedAppeal.AppealGranted() {
		t.Error("Appeal granted field should be true")
	}

}

func TestProcessTCRGrantedAppealConfirmed(t *testing.T) {
	challengeID := big.NewInt(120)
	appealChallengeID := big.NewInt(130)
	proc, contracts, _, persister := setupAppeal(t, challengeID)

	appealChallenged := &contract.CivilTCRContractGrantedAppealChallenged{
		ListingAddress:    contracts.NewsroomAddr,
		ChallengeID:       challengeID,
		AppealChallengeID: appealChallengeID,
		Data:              "DATA",
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888889,
			TxHash:      common.Hash{},
			TxIndex:     2,
			BlockHash:   common.Hash{},
			Index:       2,
			Removed:     false,
		},
	}

	appealConfirmed := &contract.CivilTCRContractGrantedAppealConfirmed{
		ListingAddress:    contracts.NewsroomAddr,
		ChallengeID:       challengeID,
		AppealChallengeID: appealChallengeID,
		RewardPool:        big.NewInt(1010101),
		TotalTokens:       big.NewInt(110101),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888889,
			TxHash:      common.Hash{},
			TxIndex:     2,
			BlockHash:   common.Hash{},
			Index:       2,
			Removed:     false,
		},
	}

	event4, _ := crawlermodel.NewEventFromContractEvent(
		"_GrantedAppealChallenged",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		appealChallenged,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)

	event5, _ := crawlermodel.NewEventFromContractEvent(
		"_GrantedAppealConfirmed",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		appealConfirmed,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events := []*crawlermodel.Event{event4, event5}
	err := proc.Process(events)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	persistedChallenge := persister.challenges[int(challengeID.Int64())]
	if persistedChallenge == nil {
		t.Error("Should not have returned nil challenge")
	}

	if !persistedChallenge.Resolved() {
		t.Error("Resolved field of persisted challenge should be true")
	}

}
