// +build integration

// This is an integration test file for pubsubProcessor. Pubsub simulator needs to be running.
// Run this using go test -tags=integration

package processor_test

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/joincivil/civil-events-crawler/pkg/contractutils"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	"github.com/joincivil/civil-events-processor/pkg/processor"
	"github.com/joincivil/civil-events-processor/pkg/testutils"

	"github.com/joincivil/go-common/pkg/generated/contract"
	"github.com/joincivil/go-common/pkg/pubsub"

	ctime "github.com/joincivil/go-common/pkg/time"
)

func TestPubsubEventsBuildPayload(t *testing.T) {
	topicName := "governance-events-staging"
	subscriptionName := "test-subscription"

	pubsub, err := pubsub.NewGooglePubSub("civil-media")
	if err != nil {
		t.Fatalf("Error initializing pubsub: %v", err)
	}

	err = pubsub.CreateTopic(topicName)
	if err != nil {
		t.Errorf("Should have created a topic: err: %v", err)
	}

	err = pubsub.CreateSubscription(topicName, subscriptionName)
	if err != nil {
		t.Errorf("Should not prevented the creation of a the same subscription: err: %v", err)
	}

	err = pubsub.StartPublishers()
	if err != nil {
		t.Fatalf("Error starting pubsub: %v", err)
	}

	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
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
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
	)

	persister := &testutils.TestPersister{}
	processorParams := &processor.NewEventProcessorParams{
		Client:                 contracts.Client,
		ListingPersister:       persister,
		RevisionPersister:      persister,
		GovEventPersister:      persister,
		ChallengePersister:     persister,
		PollPersister:          persister,
		AppealPersister:        persister,
		TokenTransferPersister: persister,
		GooglePubSub:           pubsub,
		PubSubEventsTopicName:  topicName,
	}
	proc := processor.NewEventProcessor(processorParams)

	err = pubsub.StartSubscribers(subscriptionName)
	if err != nil {
		t.Fatalf("Should have started up subscription: err: %v", err)
	}

	proc.Process([]*crawlermodel.Event{event})

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
	case <-time.After(time.Second * 5):
		t.Errorf("Should not have timed out")
	}

	if numResults != 1 {
		t.Errorf("Should have received a message from pub sub")
	}

	err = pubsub.StopPublishers()
	if err != nil {
		t.Errorf("Should have stopped publishers: err: %v", err)
	}
	err = pubsub.StopSubscribers()
	if err != nil {
		t.Errorf("Should have stopped publishers: err: %v", err)
	}
	err = pubsub.DeleteSubscription(subscriptionName)
	if err != nil {
		t.Errorf("Should have deleted the subscription")
	}
	err = pubsub.DeleteTopic(topicName)
	if err != nil {
		t.Errorf("Should have deleted the topic")
	}

}

func TestPubsubTokensBuildPayload(t *testing.T) {
	topicName := "token-event-staging"
	subscriptionName := "test-token-subscription"

	pubsub, err := pubsub.NewGooglePubSub("civil-media")
	if err != nil {
		t.Fatalf("Error initializing pubsub: %v", err)
	}

	err = pubsub.CreateTopic(topicName)
	if err != nil {
		t.Errorf("Should have created a topic: err: %v", err)
	}

	err = pubsub.CreateSubscription(topicName, subscriptionName)
	if err != nil {
		t.Errorf("Should not prevented the creation of a the same subscription: err: %v", err)
	}

	err = pubsub.StartPublishers()
	if err != nil {
		t.Fatalf("Error starting pubsub: %v", err)
	}

	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}

	tokenTransfer := &contract.CVLTokenContractTransfer{
		From:  common.HexToAddress(testAddress),
		To:    common.HexToAddress(testAddress),
		Value: big.NewInt(100000000),
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
	event, _ := crawlermodel.NewEventFromContractEvent(
		"Transfer",
		"CVLTokenContract",
		contracts.TokenAddr,
		tokenTransfer,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
	)

	persister := &testutils.TestPersister{}
	processorParams := &processor.NewEventProcessorParams{
		Client:                 contracts.Client,
		ListingPersister:       persister,
		RevisionPersister:      persister,
		GovEventPersister:      persister,
		ChallengePersister:     persister,
		PollPersister:          persister,
		AppealPersister:        persister,
		TokenTransferPersister: persister,
		GooglePubSub:           pubsub,
		PubSubTokenTopicName:   topicName,
	}

	proc := processor.NewEventProcessor(processorParams)

	err = pubsub.StartSubscribers(subscriptionName)
	if err != nil {
		t.Fatalf("Should have started up subscription: err: %v", err)
	}

	proc.Process([]*crawlermodel.Event{event})

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
	case <-time.After(time.Second * 5):
		t.Errorf("Should not have timed out")
	}

	if numResults != 1 {
		t.Errorf("Should have received a message from pub sub")
	}

	err = pubsub.StopPublishers()
	if err != nil {
		t.Errorf("Should have stopped publishers: err: %v", err)
	}
	err = pubsub.StopSubscribers()
	if err != nil {
		t.Errorf("Should have stopped publishers: err: %v", err)
	}
	err = pubsub.DeleteSubscription(subscriptionName)
	if err != nil {
		t.Errorf("Should have deleted the subscription")
	}
	err = pubsub.DeleteTopic(topicName)
	if err != nil {
		t.Errorf("Should have deleted the topic")
	}

}
func TestPubsubProcessor(t *testing.T) {
	pubsub, err := pubsub.NewGooglePubSub("civil-media")
	if err != nil {
		t.Fatalf("Error initializing pubsub: err: %v", err)
	}

	err = pubsub.StartPublishers()
	if err != nil {
		t.Fatalf("Error starting publishers: err: %v", err)
	}

}
