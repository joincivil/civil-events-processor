// +build integration

// This is an integration test file for processorpubsub.go. Pubsub simulator needs to be running.
// Run this using go test -tags=integration
package processormain_test

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	contractutils "github.com/joincivil/civil-events-crawler/pkg/contractutils"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	crawlerpubsub "github.com/joincivil/civil-events-crawler/pkg/pubsub"
	"github.com/joincivil/civil-events-processor/pkg/processor"
	"github.com/joincivil/civil-events-processor/pkg/processormain"
	"github.com/joincivil/civil-events-processor/pkg/testutils"
	"github.com/joincivil/go-common/pkg/generated/contract"
	cstring "github.com/joincivil/go-common/pkg/strings"
	ctime "github.com/joincivil/go-common/pkg/time"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	topicName       = "testTopic"
	subName         = "testSubscription"
	projectID       = "civil-media"
	contractAddress = "0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d"
)

// TestEventPersister is a persistence used for testing event persistence
type TestEventPersister struct {
	events []*crawlermodel.Event
}

func (ep *TestEventPersister) RetrieveEvents(criteria *crawlermodel.RetrieveEventsCriteria) ([]*crawlermodel.Event, error) {
	return ep.events, nil
}

func (ep *TestEventPersister) SaveEvents(events []*crawlermodel.Event) error {
	ep.events = append(ep.events, events...)
	return nil
}

func returnRandomTestApplicationEvent(t *testing.T) *contract.CivilTCRContractApplication {
	testAddress, _ := cstring.RandomHexStr(20)
	return &contract.CivilTCRContractApplication{
		ListingAddress: common.HexToAddress(testAddress),
		Deposit:        big.NewInt(1000),
		AppEndDate:     big.NewInt(1653860896),
		Data:           "DATA",
		Applicant:      common.HexToAddress(testAddress),
		Raw: types.Log{
			Address: common.HexToAddress(contractAddress),
			Topics: []common.Hash{
				common.HexToHash("0x09cd8dcaf170a50a26316b5fe0727dd9fb9581a688d65e758b16a1650da65c0b"),
				common.HexToHash("0x0000000000000000000000002652c60cf04bbf6bb6cc8a5e6f1c18143729d440"),
				common.HexToHash("0x00000000000000000000000025bf9a1595d6f6c70e6848b60cba2063e4d9e552"),
			},
			Data:        []byte("thisisadatastring"),
			BlockNumber: 8888888,
			TxHash:      common.Hash{},
			TxIndex:     2,
			BlockHash:   common.Hash{},
			Index:       2,
			Removed:     false,
		},
	}
}

func returnFilteredTestEvents(t *testing.T, numEvents int) []*crawlermodel.Event {
	appEvents := make([]*crawlermodel.Event, numEvents)
	for i := 0; i < numEvents; i++ {
		appEvent := returnRandomTestApplicationEvent(t)
		event, err := crawlermodel.NewEventFromContractEvent(
			"Application",
			"CivilTCRContract",
			common.HexToAddress(contractAddress),
			appEvent,
			ctime.CurrentEpochSecsInInt64()-int64(1000-i),
			crawlermodel.Watcher,
		)
		if err != nil {
			t.Errorf("Error creating new event %v", err)
		}
		appEvents[i] = event
	}
	return appEvents
}

func returnWatchedTestEvent(t *testing.T) *crawlermodel.Event {
	appEvent := returnRandomTestApplicationEvent(t)
	event, err := crawlermodel.NewEventFromContractEvent(
		"Application",
		"CivilTCRContract",
		common.HexToAddress(contractAddress),
		appEvent,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
	)
	if err != nil {
		t.Errorf("Error creating new event %v", err)
	}
	return event
}

func returnContractApplicationWatchedEvent(t *testing.T, address common.Address) *crawlermodel.Event {
	appEvent := &contract.CivilTCRContractApplication{
		ListingAddress: address,
		Deposit:        big.NewInt(1000),
		AppEndDate:     big.NewInt(1653860896),
		Data:           "DATA",
		Applicant:      address,
		Raw: types.Log{
			Address: common.HexToAddress(contractAddress),
			Topics: []common.Hash{
				common.HexToHash("0x09cd8dcaf170a50a26316b5fe0727dd9fb9581a688d65e758b16a1650da65c0b"),
				common.HexToHash("0x0000000000000000000000002652c60cf04bbf6bb6cc8a5e6f1c18143729d440"),
				common.HexToHash("0x00000000000000000000000025bf9a1595d6f6c70e6848b60cba2063e4d9e552"),
			},
			Data:        []byte("thisisadatastring"),
			BlockNumber: 8888888,
			TxHash:      common.Hash{},
			TxIndex:     2,
			BlockHash:   common.Hash{},
			Index:       2,
			Removed:     false,
		},
	}
	event, err := crawlermodel.NewEventFromContractEvent(
		"Application",
		"CivilTCRContract",
		common.HexToAddress(contractAddress),
		appEvent,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
	)
	if err != nil {
		t.Errorf("Error creating new event %v", err)
	}
	return event
}

func setupCrawlerPubSub(t *testing.T) *crawlerpubsub.CrawlerPubSub {
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8042")
	ps, err := crawlerpubsub.NewCrawlerPubSub(projectID, topicName)
	if err != nil {
		t.Errorf("Error starting pubsub %v", err)
	}

	te, err := ps.GooglePubsub.TopicExists(topicName)
	if err != nil {
		t.Errorf("Error checking if topic exists %v", err)
	}
	if te {
		err := ps.GooglePubsub.DeleteTopic(topicName)
		if err != nil {
			t.Errorf("Should have deleted existing topic")
		}
	}
	err = ps.GooglePubsub.CreateTopic(topicName)
	if err != nil {
		t.Errorf("Should have created a topic")
	}

	err = ps.GooglePubsub.StartPublishers()
	if err != nil {
		t.Errorf("Error starting publishers %v", err)
	}
	se, err := ps.GooglePubsub.SubscriptionExists(subName)
	if err != nil {
		t.Errorf("Error checking if subscription exists %v", err)
	}
	if se {
		err = ps.GooglePubsub.DeleteSubscription(subName)
		if err != nil {
			t.Errorf("Should have deleted existing subscription")
		}
	}

	err = ps.GooglePubsub.CreateSubscription(topicName, subName)
	if err != nil {
		t.Errorf("Error creating subscription %v", err)
	}
	err = ps.GooglePubsub.StartSubscribers(subName)
	if err != nil {
		t.Errorf("Error starting subscribers %v", err)
	}
	return ps
}

func TestProcessorPubSub(t *testing.T) {
	cps := setupCrawlerPubSub(t)
	testPersister := &testutils.TestPersister{}
	testScraper := &testutils.TestScraper{}
	testEventPersister := &TestEventPersister{}
	persisters := &processormain.InitializedPersisters{
		Cron:            testPersister,
		Event:           testEventPersister,
		Listing:         testPersister,
		ContentRevision: testPersister,
		GovernanceEvent: testPersister,
		Challenge:       testPersister,
		Poll:            testPersister,
		Appeal:          testPersister,
	}
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}

	quitChan := make(chan bool)
	filteredEvents := returnFilteredTestEvents(t, 4)

	watchedEvent := returnWatchedTestEvent(t)
	watchedEvent2 := returnContractApplicationWatchedEvent(t, contracts.NewsroomAddr)

	proc := processor.NewEventProcessor(&processor.NewEventProcessorParams{
		Client:               contracts.Client,
		ListingPersister:     testPersister,
		RevisionPersister:    testPersister,
		GovEventPersister:    testPersister,
		ChallengePersister:   testPersister,
		PollPersister:        testPersister,
		AppealPersister:      testPersister,
		ContentScraper:       testScraper,
		MetadataScraper:      testScraper,
		CivilMetadataScraper: testScraper,
	})
	var wg sync.WaitGroup
	wg.Add(2)

	go processormain.RunProcessorPubSub(persisters, cps.GooglePubsub, proc, quitChan, &wg)

	go func() {
		// Save filtered fake events here to Events persistence
		_ = testEventPersister.SaveEvents(filteredEvents)
		cps.PublishFilteringFinishedMessage()
		// Make some fake watched event
		_ = testEventPersister.SaveEvents([]*crawlermodel.Event{watchedEvent, watchedEvent2})
		cps.PublishWatchedEventMessage(watchedEvent)
		cps.PublishWatchedEventMessage(watchedEvent2)

		time.Sleep(10 * time.Second)
		quitChan <- true
		wg.Done()
	}()
	wg.Wait()

	listing, _ := persisters.Listing.ListingByAddress(contracts.NewsroomAddr)
	if listing == nil {
		t.Errorf("Should have gotten listing with listing address %v in persistence", contracts.NewsroomAddr.Hex())
	}
	timestampFromCron, _ := persisters.Cron.TimestampOfLastEventForCron()
	if timestampFromCron != watchedEvent.Timestamp() {
		t.Errorf("Processor did not run correctly, last timestamp is wrong: %v, %v", timestampFromCron, watchedEvent.Timestamp())
	}

}