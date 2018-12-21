package main

import (
	"flag"
	"os"
	"runtime"
	"time"

	log "github.com/golang/glog"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/robfig/cron"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	crawlerutils "github.com/joincivil/civil-events-crawler/pkg/utils"

	"github.com/joincivil/civil-events-processor/pkg/helpers"
	"github.com/joincivil/civil-events-processor/pkg/model"
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

func saveLastEventTimestamp(persister model.CronPersister, events []*crawlermodel.Event,
	lastTs int64) error {
	updated := false
	for _, event := range events {
		timestamp := event.Timestamp()
		if timestamp > lastTs {
			lastTs = timestamp
			updated = true
		}
	}
	if updated {
		return persister.UpdateTimestampForCron(lastTs)
	}
	return nil
}

func initPubSub(config *utils.ProcessorConfig) (*crawlerutils.GooglePubSub, error) {

	// TODO(jorgelo): Put "project id" in configuration.
	pubsub, err := crawlerutils.NewGooglePubSub("civil-media")

	if err != nil {
		return nil, err
	}

	err = pubsub.StartPublishers()

	if err != nil {
		return nil, err
	}

	return pubsub, nil
}

type initializedPersisters struct {
	cron            model.CronPersister
	event           crawlermodel.EventDataPersister
	listing         model.ListingPersister
	contentRevision model.ContentRevisionPersister
	governanceEvent model.GovernanceEventPersister
	challenge       model.ChallengePersister
	poll            model.PollPersister
	appeal          model.AppealPersister
}

func initPersisters(config *utils.ProcessorConfig) (*initializedPersisters, error) {
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
	return &initializedPersisters{
		cron:            cronPersister,
		event:           eventPersister,
		listing:         listingPersister,
		contentRevision: contentRevisionPersister,
		governanceEvent: governanceEventPersister,
		challenge:       challengePersister,
		poll:            pollPersister,
		appeal:          appealPersister,
	}, nil
}

func runProcessor(config *utils.ProcessorConfig, persisters *initializedPersisters) {
	lastTs, err := persisters.cron.TimestampOfLastEventForCron()
	if err != nil {
		log.Errorf("Error getting last event timestamp: %v", err)
		return
	}

	events, err := persisters.event.RetrieveEvents(
		&crawlermodel.RetrieveEventsCriteria{
			FromTs: lastTs,
			Count:  1,
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

		pubsub, err := initPubSub(config)

		if err != nil {
			log.Errorf("Error initializing pubsub: err: %v", err)
			return
		}

		proc := processor.NewEventProcessor(&processor.NewEventProcessorParams{
			Client:               client,
			ListingPersister:     persisters.listing,
			RevisionPersister:    persisters.contentRevision,
			GovEventPersister:    persisters.governanceEvent,
			ChallengePersister:   persisters.challenge,
			PollPersister:        persisters.poll,
			AppealPersister:      persisters.appeal,
			ContentScraper:       helpers.ContentScraper(config),
			MetadataScraper:      helpers.MetadataScraper(config),
			CivilMetadataScraper: helpers.CivilMetadataScraper(config),
			GooglePubSub:         pubsub,
		})
		err = proc.Process(events)
		if err != nil {
			log.Errorf("Error retrieving events: err: %v", err)
			return
		}

		err = saveLastEventTimestamp(persisters.cron, events, lastTs)
		if err != nil {
			log.Errorf("Error saving last timestamp %v: err: %v", lastTs, err)
			return
		}
	}

	log.Infof("Done running processor: %v", runtime.NumGoroutine())
}

func main() {
	config := &utils.ProcessorConfig{}
	flag.Usage = func() {
		config.OutputUsage()
		os.Exit(0)
	}
	flag.Parse()

	err := config.PopulateFromEnv()
	if err != nil {
		config.OutputUsage()
		log.Errorf("Invalid processor config: err: %v\n", err)
		os.Exit(2)
	}

	persisters, err := initPersisters(config)
	if err != nil {
		log.Errorf("Error initializing persister: err: %v", err)
		os.Exit(2)
	}

	cr := cron.New()
	err = cr.AddFunc(config.CronConfig, func() { runProcessor(config, persisters) })
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
