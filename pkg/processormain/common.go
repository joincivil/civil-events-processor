package processormain

import (
	"fmt"
	log "github.com/golang/glog"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/helpers"
	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/utils"
	cpubsub "github.com/joincivil/go-common/pkg/pubsub"
)

// SaveLastEventInformation saves the last timestamp and event hash info to the cron table
func SaveLastEventInformation(persister model.CronPersister, events []*crawlermodel.Event,
	lastTs int64) error {
	updated := false
	numEvents := 0
	for _, event := range events {
		timestamp := event.Timestamp()
		if timestamp > lastTs {
			lastTs = timestamp
			updated = true
			numEvents = 1
		} else if timestamp == lastTs {
			numEvents++
		}
	}
	eventHashes := make([]string, numEvents)
	i := 0
	if updated {
		for _, event := range events {
			if event.Timestamp() == lastTs {
				eventHashes[i] = event.Hash()
				i++
			}
		}
		err := persister.UpdateTimestampForCron(lastTs)
		if err != nil {
			return fmt.Errorf("Error updating event hashes in cron table: %v", err)
		}
		err = persister.UpdateEventHashesForCron(eventHashes)
		if err != nil {
			return fmt.Errorf("Error updating event hashes in cron table: %v", err)
		}
	}
	return nil
}

func initPubSubEvents(config *utils.ProcessorConfig, ps *cpubsub.GooglePubSub) (*cpubsub.GooglePubSub, error) {
	// If no events topic name, disable
	if config.PubSubEventsTopicName == "" {
		return nil, nil
	}
	err := ps.StartPublishers()
	return ps, err
}

// InitializedPersisters contains initialized persisters needed to run processor
type InitializedPersisters struct {
	Cron            model.CronPersister
	Event           crawlermodel.EventDataPersister
	Listing         model.ListingPersister
	ContentRevision model.ContentRevisionPersister
	GovernanceEvent model.GovernanceEventPersister
	Challenge       model.ChallengePersister
	Poll            model.PollPersister
	Appeal          model.AppealPersister
}

// InitPersisters inits the persisters from the config file
func InitPersisters(config *utils.ProcessorConfig) (*InitializedPersisters, error) {
	cronPersister, err := helpers.CronPersister(config)
	if err != nil {
		log.Errorf("Error getting the cron persister: %v", err)
		return nil, err
	}
	eventPersister, err := helpers.EventPersister(config)
	if err != nil {
		log.Errorf("Error getting the event persister: %v", err)
		return nil, err
	}
	listingPersister, err := helpers.ListingPersister(config)
	if err != nil {
		log.Errorf("Error w listingPersister: err: %v", err)
		return nil, err
	}
	contentRevisionPersister, err := helpers.ContentRevisionPersister(config)
	if err != nil {
		log.Errorf("Error w contentRevisionPersister: err: %v", err)
		return nil, err
	}
	governanceEventPersister, err := helpers.GovernanceEventPersister(config)
	if err != nil {
		log.Errorf("Error w governanceEventPersister: err: %v", err)
		return nil, err
	}
	challengePersister, err := helpers.ChallengePersister(config)
	if err != nil {
		log.Errorf("Error w ChallengePersister: err: %v", err)
		return nil, err
	}
	pollPersister, err := helpers.PollPersister(config)
	if err != nil {
		log.Errorf("Error w PollPersister: err: %v", err)
		return nil, err
	}
	appealPersister, err := helpers.AppealPersister(config)
	if err != nil {
		log.Errorf("Error w AppealPersister: err: %v", err)
		return nil, err
	}
	return &InitializedPersisters{
		Cron:            cronPersister,
		Event:           eventPersister,
		Listing:         listingPersister,
		ContentRevision: contentRevisionPersister,
		GovernanceEvent: governanceEventPersister,
		Challenge:       challengePersister,
		Poll:            pollPersister,
		Appeal:          appealPersister,
	}, nil
}
