package processor_test

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/joincivil/civil-events-crawler/pkg/contractutils"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/processor"

	"github.com/joincivil/go-common/pkg/generated/contract"
	ctime "github.com/joincivil/go-common/pkg/time"
)

const (
	// https://civil-develop.go-vip.co/crawler-pod/wp-json/civil-newsroom-protocol/v1/revisions/11
	testCivilMetadata = `{"title":"This is a test post","revisionContentHash":"0x9e4acfe532c8458abfc1f1d30c4eaf986fee52cf1f65c9548f1dc437fb6dfd38","revisionContentUrl":"https:\/\/civil-develop.go-vip.co\/crawler-pod\/wp-json\/civil-newsroom-protocol\/v1\/revisions-content\/0x9e4acfe532c8458abfc1f1d30c4eaf986fee52cf1f65c9548f1dc437fb6dfd38\/","canonicalUrl":"https:\/\/civil-develop.go-vip.co\/crawler-pod\/2018\/07\/25\/this-is-a-test-post\/","slug":"this-is-a-test-post","description":"I'm being described","authors":[{"byline":"Walker Flynn"}],"images":[{"url":"https:\/\/civil-develop.go-vip.co\/crawler-pod\/wp-content\/uploads\/sites\/20\/2018\/07\/Messages-Image3453599984.png","hash":"0x72ca80ed96a2b1ca20bf758a2142a678c0bc316e597161d0572af378e52b2e80","h":960,"w":697}],"tags":["news"],"primaryTag":"news","revisionDate":"2018-07-25 17:17:20","originalPublishDate":"2018-07-25 17:17:07","credibilityIndicators":{"original_reporting":"1","on_the_ground":false,"sources_cited":"1","subject_specialist":false},"opinion":false,"civilSchemaVersion":"1.0.0"}`
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

func indexAddressInSlice(slice []common.Address, target common.Address) int {
	// if address in slice return idx, else return -1
	for idx, addr := range slice {
		if target.Hex() == addr.Hex() {
			return idx
		}
	}
	return -1
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
	// This is more of a placeholder
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
	challenges := []*model.Challenge{}
	for _, chal := range t.challenges {
		listingAddress := chal.ListingAddress()
		if listingAddress.Hex() == addr.Hex() {
			challenges = append(challenges, chal)
		}
	}
	return challenges, nil
}

func (t *TestPersister) ChallengesByListingAddresses(addr []common.Address) ([][]*model.Challenge, error) {
	challenges := make([][]*model.Challenge, len(addr))
	for _, chal := range t.challenges {
		listingAddress := chal.ListingAddress()
		addrIdx := indexAddressInSlice(addr, listingAddress)
		if addrIdx != -1 {
			challenges[addrIdx] = append(challenges[addrIdx], chal)
		}
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

func TestEmptyContractAddress(t *testing.T) {
	tcrAddress := common.Address{}
	if tcrAddress != (common.Address{}) {
		t.Error("2 blank common.Address types should be equal")
	}
}

func setupEventList(t *testing.T, contracts *contractutils.AllTestContracts) []*crawlermodel.Event {
	events := []*crawlermodel.Event{}
	application := &contract.CivilTCRContractApplication{
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
			Removed:     false},
	}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_Application",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		application,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event)
	revision := &contract.NewsroomContractRevisionUpdated{
		Editor:     common.HexToAddress(editorAddress),
		ContentId:  big.NewInt(0),
		RevisionId: big.NewInt(0),
		Uri:        "http://joincivil.com/charter",
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
	event, _ = crawlermodel.NewEventFromContractEvent(
		"RevisionUpdated",
		"NewsroomContract",
		contracts.NewsroomAddr,
		revision,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
	)
	events = append(events, event)
	challenge1 := &contract.CivilTCRContractChallenge{
		ListingAddress: contracts.NewsroomAddr,
		ChallengeID:    challengeID1,
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
	event, _ = crawlermodel.NewEventFromContractEvent(
		"_Challenge",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		challenge1,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event)
	pollCreated := &contract.CivilPLCRVotingContractPollCreated{
		VoteQuorum:    big.NewInt(100),
		CommitEndDate: big.NewInt(1653860896),
		RevealEndDate: big.NewInt(1653860896),
		PollID:        pollID1,
		Creator:       common.HexToAddress(testAddress),
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
		}}
	event, _ = crawlermodel.NewEventFromContractEvent(
		"_PollCreated",
		"CivilPLCRVotingContract",
		contracts.PlcrAddr,
		pollCreated,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event)
	appealRequested := &contract.CivilTCRContractAppealRequested{
		ListingAddress: contracts.NewsroomAddr,
		ChallengeID:    challengeID1,
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
	event, _ = crawlermodel.NewEventFromContractEvent(
		"_AppealRequested",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		appealRequested,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event)
	return events
}

func TestProcessor(t *testing.T) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &TestPersister{}
	scraper := &TestScraper{}
	processorParams := &processor.NewEventProcessorParams{
		Client:               contracts.Client,
		ListingPersister:     persister,
		RevisionPersister:    persister,
		GovEventPersister:    persister,
		ChallengePersister:   persister,
		PollPersister:        persister,
		AppealPersister:      persister,
		ContentScraper:       scraper,
		MetadataScraper:      scraper,
		CivilMetadataScraper: scraper,
	}
	proc := processor.NewEventProcessor(processorParams)
	events := setupEventList(t, contracts)
	err = proc.Process(events)
	if err != nil {
		t.Fatalf("Error processing events: %v", err)
	}
	if len(persister.listings) != 1 {
		t.Errorf("Should have only seen 1 listing but saw %v", len(persister.listings))
	}
	if len(persister.govEvents[contracts.NewsroomAddr.Hex()]) != 3 {
		t.Errorf("Should have seen 2 govEvents but saw %v", len(persister.govEvents[contracts.NewsroomAddr.Hex()]))
	}
	if len(persister.challenges) != 1 {
		t.Errorf("Should have seen 1 challenge but saw %v", len(persister.challenges))
	}
	if len(persister.revisions[contracts.NewsroomAddr.Hex()]) != 1 {
		t.Errorf("Should have seen 1 revision but saw %v", len(persister.revisions))
	}
	if len(persister.polls) != 1 {
		t.Errorf("Should have seen 2 polls but saw %v", len(persister.polls))
	}
	if len(persister.appeals) != 1 {
		t.Errorf("Should have seen 1 appeal but saw %v", len(persister.appeals))
	}

}
