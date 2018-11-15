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
func NewEventProcessor(client bind.ContractBackend, listingPersister model.ListingPersister,
	revisionPersister model.ContentRevisionPersister, govEventPersister model.GovernanceEventPersister,
	challengePersister model.ChallengePersister, pollPersister model.PollPersister, appealPersister model.AppealPersister,
	contentScraper model.ContentScraper, metadataScraper model.MetadataScraper,
	civilMetadataScraper model.CivilMetadataScraper) *EventProcessor {
	tcrEventProcessor := NewTcrEventProcessor(client, listingPersister, challengePersister, appealPersister,
		govEventPersister)
	plcrEventProcessor := NewPlcrEventProcessor(client, pollPersister)
	newsroomEventProcessor := NewNewsroomEventProcessor(client, listingPersister, revisionPersister,
		contentScraper, metadataScraper, civilMetadataScraper)
	return &EventProcessor{
		client:                 client,
		tcrEventProcessor:      tcrEventProcessor,
		plcrEventProcessor:     plcrEventProcessor,
		newsroomEventProcessor: newsroomEventProcessor,
	}
}

// EventProcessor handles the processing of raw events into aggregated data
// for use via the API.
type EventProcessor struct {
	client bind.ContractBackend
	// govEventPersister      model.GovernanceEventPersister
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
		ran, err = e.newsroomEventProcessor.process(event)
		if err != nil {
			log.Errorf("Error processing newsroom event: err: %v\n", err)
		}
		if ran {
			continue
		}
		ran, err = e.tcrEventProcessor.process(event)
		if err != nil {
			log.Errorf("Error processing civil tcr event: err: %v\n", err)
		}
		if ran {
			continue
		}
		_, err = e.plcrEventProcessor.process(event)
		if err != nil {
			log.Errorf("Error processing plcr event: err: %v\n", err)
		}

	}
	return err
}
