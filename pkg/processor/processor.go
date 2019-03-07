package processor

import (
	"github.com/davecgh/go-spew/spew"
	log "github.com/golang/glog"
	"github.com/shurcooL/graphql"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"

	"github.com/joincivil/civil-events-processor/pkg/model"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	"github.com/joincivil/go-common/pkg/pubsub"
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
	)
	cvlTokenProcessor := NewCvlTokenEventProcessor(
		params.Client,
		params.GraphQLClient,
		params.TokenTransferPersister,
	)
	return &EventProcessor{
		tcrEventProcessor:      tcrEventProcessor,
		plcrEventProcessor:     plcrEventProcessor,
		newsroomEventProcessor: newsroomEventProcessor,
		cvlTokenProcessor:      cvlTokenProcessor,
		googlePubSub:           params.GooglePubSub,
		googlePubSubTopicName:  params.GooglePubSubTopicName,
	}
}

// NewEventProcessorParams defines the params needed to be passed to the processor
type NewEventProcessorParams struct {
	Client                 bind.ContractBackend
	ListingPersister       model.ListingPersister
	RevisionPersister      model.ContentRevisionPersister
	GovEventPersister      model.GovernanceEventPersister
	ChallengePersister     model.ChallengePersister
	PollPersister          model.PollPersister
	AppealPersister        model.AppealPersister
	TokenTransferPersister model.TokenTransferPersister
	GooglePubSub           *pubsub.GooglePubSub
	GooglePubSubTopicName  string
	GraphQLClient          *graphql.Client
}

// EventProcessor handles the processing of raw events into aggregated data
// for use via the API.
type EventProcessor struct {
	tcrEventProcessor      *TcrEventProcessor
	plcrEventProcessor     *PlcrEventProcessor
	newsroomEventProcessor *NewsroomEventProcessor
	cvlTokenProcessor      *CvlTokenEventProcessor
	googlePubSub           *pubsub.GooglePubSub
	googlePubSubTopicName  string
}

// Process runs the processor with the given set of raw CivilEvents
func (e *EventProcessor) Process(events []*crawlermodel.Event) error {
	var err error
	var ran bool

	if !e.pubsubEnabled() {
		log.Info("Events pubsub is disabled, to enable set the project ID and topic in the config.")
	}

	for _, event := range events {
		if log.V(2) {
			log.Infof("Process event: %v", spew.Sprintf("%#+v", event))
		}

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

		ran, err = e.plcrEventProcessor.Process(event)
		if err != nil {
			log.Errorf("Error processing plcr event: err: %v\n", err)
		}
		if ran {
			continue
		}

		_, err = e.cvlTokenProcessor.Process(event)
		if err != nil {
			log.Errorf("Error processing token transfer event: err: %v\n", err)
		}

	}
	log.Info("Finished Processing")
	return err
}

func (e *EventProcessor) sendEventToPubsub(event *crawlermodel.Event) error {
	if !e.pubsubEnabled() {
		return nil
	}

	return e.pubSub(event)
}
