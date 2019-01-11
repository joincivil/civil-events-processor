package processormain

import (
	"os"
	"runtime"
	"time"

	log "github.com/golang/glog"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/robfig/cron"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	cpubsub "github.com/joincivil/go-common/pkg/pubsub"

	"github.com/joincivil/civil-events-processor/pkg/helpers"
	"github.com/joincivil/civil-events-processor/pkg/processor"
	"github.com/joincivil/civil-events-processor/pkg/utils"
)

const (
	checkRunSecs = 5
)

func checkCron(cr *cron.Cron) {
	entries := cr.Entries()
	for _, entry := range entries {
		log.Infof("Proc run times: prev: %v, next: %v\n", entry.Prev, entry.Next)
	}
}

func initPubSubForCron(config *utils.ProcessorConfig) (*cpubsub.GooglePubSub, error) {
	// If no project ID, disable
	if config.PubSubProjectID == "" {
		return nil, nil
	}

	ps, err := cpubsub.NewGooglePubSub(config.PubSubProjectID)
	if err != nil {
		return nil, err
	}

	return initPubSubEmail(config, ps)
}

func runProcessorCron(config *utils.ProcessorConfig, persisters *InitializedPersisters) {
	lastTs, err := persisters.Cron.TimestampOfLastEventForCron()
	if err != nil {
		log.Errorf("Error getting last event timestamp: %v", err)
		return
	}

	events, err := persisters.Event.RetrieveEvents(
		&crawlermodel.RetrieveEventsCriteria{
			FromTs: lastTs,
		},
	)

	if err != nil {
		log.Errorf("Error retrieving events: err: %v", err)
		return
	}

	if len(events) > 0 {
		client, err := ethclient.Dial(config.EthAPIURL)
		if err != nil {
			log.Errorf("Error connecting to eth API: err: %v", err)
			return
		}
		defer client.Close()

		pubsub, err := initPubSubForCron(config)
		if err != nil {
			log.Errorf("Error initializing pubsub: err: %v", err)
			return
		}

		proc := processor.NewEventProcessor(&processor.NewEventProcessorParams{
			Client:                client,
			ListingPersister:      persisters.Listing,
			RevisionPersister:     persisters.ContentRevision,
			GovEventPersister:     persisters.GovernanceEvent,
			ChallengePersister:    persisters.Challenge,
			PollPersister:         persisters.Poll,
			AppealPersister:       persisters.Appeal,
			ContentScraper:        helpers.ContentScraper(config),
			MetadataScraper:       helpers.MetadataScraper(config),
			CivilMetadataScraper:  helpers.CivilMetadataScraper(config),
			GooglePubSub:          pubsub,
			GooglePubSubTopicName: config.PubSubEmailTopicName,
		})
		err = proc.Process(events)
		if err != nil {
			log.Errorf("Error processing events: err: %v", err)
			return
		}

		err = saveLastEventTimestamp(persisters.Cron, events, lastTs)
		if err != nil {
			log.Errorf("Error saving last timestamp %v: err: %v", lastTs, err)
			return
		}
	}

	log.Infof("Done running processor: %v", runtime.NumGoroutine())
}

// ProcessorCronMain contains the logic to run the processor using a cronjob
func ProcessorCronMain(config *utils.ProcessorConfig, persisters *InitializedPersisters) {
	cr := cron.New()
	err := cr.AddFunc(config.CronConfig, func() { runProcessorCron(config, persisters) })
	if err != nil {
		log.Errorf("Error starting: err: %v", err)
		os.Exit(1)
	}
	cr.Start()

	// Blocks here while the cron process runs
	for range time.After(checkRunSecs * time.Second) {
		checkCron(cr)
	}
}
