// +build integration

// This is an integration test file for pubsubProcessor. Pubsub simulator needs to be running.
// Run this using go test -tags=integration

package processor_test

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/joincivil/civil-events-crawler/pkg/contractutils"
	"github.com/joincivil/civil-events-crawler/pkg/generated/contract"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	crawlerutils "github.com/joincivil/civil-events-crawler/pkg/utils"
	"github.com/joincivil/civil-events-processor/pkg/processor"
)

func TestPubsubBuildPayload(t *testing.T) {

	pubsub, err := crawlerutils.NewGooglePubSub("civil-media")

	if err != nil {
		return nil, err
	}

	err = pubsub.CreateTopic("governance-events-staging")

	if err != nil {
		t.Errorf("Should have created a topic")
	}

	err = pubsub.CreateSubscription("governance-events-staging", "test-subscription")

	if err != nil {
		t.Errorf("Should not prevented the creation of a the same subscription")
	}

	err = pubsub.StartPublishers()

	if err != nil {
		return nil, err
	}

	appealGranted := &contract.CivilTCRContractAppealGranted{
		ListingAddress: contracts.NewsroomAddr,
		ChallengeID:    challengeID1,
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
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_AppealGranted",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		appealGranted,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)

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
		GooglePubSub:         pubsub,
	}

	proc := processor.NewEventProcessor(processorParams)

	err = ps.StartSubscribers("test-subscription")

	if err != nil {
		t.Fatalf("Should have started up subscription: err: %v", err)
	}

	proc.Process([]*crawlermodel.Event{event})

	go func() {
		time.Sleep(2 * time.Second)
	}()

	numResults := 0
	resultChan := make(chan bool)

	go func() {
		select {
		case <-pubsub.SubscribeChan:
			resultChan <- true
		}
	}()

	select {
	case <-resultChan:
		numResults++
	}

	if numResults != 1 {
		t.Errorf("Should have received a message from pub sub")
	}

	err = pubsub.StopPublishers()
	if err != nil {
		t.Fatalf("Should have stopped publishers: err: %v", err)
	}
	err = pubsub.StopSubscribers()
	if err != nil {
		t.Fatalf("Should have stopped publishers: err: %v", err)
	}

	// pubSubBuildPayload
}
func TestPubsubProcessor(t *testing.T) {
	pubsub, err := crawlerutils.NewGooglePubSub("civil-media")

	if err != nil {
		return nil, err
	}

	err = pubsub.StartPublishers()

	if err != nil {
		return nil, err
	}

}