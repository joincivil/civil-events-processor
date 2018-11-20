package processor

import (
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	log "github.com/golang/glog"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	"github.com/joincivil/civil-events-processor/pkg/model"
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
		params.GovEventPersister)
	plcrEventProcessor := NewPlcrEventProcessor(
		params.Client,
		params.PollPersister)
	newsroomEventProcessor := NewNewsroomEventProcessor(
		params.Client,
		params.ListingPersister,
		params.RevisionPersister,
		params.ContentScraper,
		params.MetadataScraper,
		params.CivilMetadataScraper)
	return &EventProcessor{
		tcrEventProcessor:      tcrEventProcessor,
		plcrEventProcessor:     plcrEventProcessor,
		newsroomEventProcessor: newsroomEventProcessor,
	}
}

// NewEventProcessorParams defines the params needed to be passed to the processor
type NewEventProcessorParams struct {
	Client               bind.ContractBackend
	ListingPersister     model.ListingPersister
	RevisionPersister    model.ContentRevisionPersister
	GovEventPersister    model.GovernanceEventPersister
	ChallengePersister   model.ChallengePersister
	PollPersister        model.PollPersister
	AppealPersister      model.AppealPersister
	ContentScraper       model.ContentScraper
	MetadataScraper      model.MetadataScraper
	CivilMetadataScraper model.CivilMetadataScraper
}

// EventProcessor handles the processing of raw events into aggregated data
// for use via the API.
type EventProcessor struct {
	tcrEventProcessor      *TcrEventProcessor
	plcrEventProcessor     *PlcrEventProcessor
	newsroomEventProcessor *NewsroomEventProcessor
}

// Process runs the processor with the given set of raw CivilEvents
func (e *EventProcessor) Process(events []*crawlermodel.Event) error {
	var err error
	var ran bool
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
			continue
		}
		_, err = e.plcrEventProcessor.Process(event)
		if err != nil {
			log.Errorf("Error processing plcr event: err: %v\n", err)
		}

	}
	return err
}
