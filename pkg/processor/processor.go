package processor

import (
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	log "github.com/golang/glog"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	"github.com/joincivil/go-common/pkg/pubsub"

	"github.com/joincivil/civil-events-processor/pkg/model"
	"sync"
)

func isStringInSlice(slice []string, target string) bool {
	for _, str := range slice {
		if target == str {
			return true
		}
	}
	return false
}

// NewEventProcessor is a convenience function to init an EventProcessor
func NewEventProcessor(params *NewEventProcessorParams) *EventProcessor {
	tcrEventProcessor := NewTcrEventProcessor(
		params.Client,
		params.ListingPersister,
		params.ChallengePersister,
		params.AppealPersister,
		params.GovEventPersister,
	)
	plcrEventProcessor := NewPlcrEventProcessor(
		params.Client,
		params.PollPersister,
	)
	newsroomEventProcessor := NewNewsroomEventProcessor(
		params.Client,
		params.ListingPersister,
		params.RevisionPersister,
		params.ContentScraper,
		params.MetadataScraper,
		params.CivilMetadataScraper,
	)
	return &EventProcessor{
		tcrEventProcessor:      tcrEventProcessor,
		plcrEventProcessor:     plcrEventProcessor,
		newsroomEventProcessor: newsroomEventProcessor,
		googlePubSub:           params.GooglePubSub,
		googlePubSubTopicName:  params.GooglePubSubTopicName,
	}
}

// NewEventProcessorParams defines the params needed to be passed to the processor
type NewEventProcessorParams struct {
	Client                bind.ContractBackend
	ListingPersister      model.ListingPersister
	RevisionPersister     model.ContentRevisionPersister
	GovEventPersister     model.GovernanceEventPersister
	ChallengePersister    model.ChallengePersister
	PollPersister         model.PollPersister
	AppealPersister       model.AppealPersister
	ContentScraper        model.ContentScraper
	MetadataScraper       model.MetadataScraper
	CivilMetadataScraper  model.CivilMetadataScraper
	GooglePubSub          *pubsub.GooglePubSub
	GooglePubSubTopicName string
}

// EventProcessor handles the processing of raw events into aggregated data
// for use via the API.
type EventProcessor struct {
	tcrEventProcessor      *TcrEventProcessor
	plcrEventProcessor     *PlcrEventProcessor
	newsroomEventProcessor *NewsroomEventProcessor
	googlePubSub           *pubsub.GooglePubSub
	googlePubSubTopicName  string
	mutex                  sync.Mutex
}

// Process runs the processor with the given set of raw CivilEvents
func (e *EventProcessor) Process(events []*crawlermodel.Event) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	var err error
	var ran bool

	if !e.pubsubEnabled() {
		log.Info("Events pubsub is disabled, to enable set the project ID and topic in the config.")
	}

	for _, event := range events {
		if event == nil {
			log.Errorf("Nil event found, should not be nil")
			continue
		}
		ran, err = e.newsroomEventProcessor.Process(event)
		if err != nil {
			log.Errorf("Error processing newsroom event: err: %v\n", err)
		}
		if ran {
			continue
		}
		ran, err = e.tcrEventProcessor.Process(event)
		if err != nil {
			log.Errorf("Error processing civil tcr event: err: %v\n", err)
		}
		if ran {
			err = e.sendEventToPubsub(event)
			if err != nil {
				log.Errorf("Error publishing to pubsub: err %v\n", err)
			}
			continue
		}
		_, err = e.plcrEventProcessor.Process(event)
		if err != nil {
			log.Errorf("Error processing plcr event: err: %v\n", err)
		}

	}
	return err
}

func (e *EventProcessor) sendEventToPubsub(event *crawlermodel.Event) error {
	if !e.pubsubEnabled() {
		return nil
	}

	return e.pubSub(event)
}
