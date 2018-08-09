package main

import (
	"flag"
	log "github.com/golang/glog"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/robfig/cron"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	crawlerpersist "github.com/joincivil/civil-events-crawler/pkg/persistence"

	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/persistence"
	"github.com/joincivil/civil-events-processor/pkg/processor"
	"github.com/joincivil/civil-events-processor/pkg/scraper"
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

func eventPersister(config *utils.ProcessorConfig) crawlermodel.EventDataPersister {
	if config.PersisterType == utils.PersisterTypePostgresql {
		persister, err := crawlerpersist.NewPostgresPersister(
			config.PersisterPostgresAddress,
			config.PersisterPostgresPort,
			config.PersisterPostgresUser,
			config.PersisterPostgresPw,
			config.PersisterPostgresDbname,
		)
		if err != nil {
			log.Errorf("Error connecting to Postgresql, stopping...; err: %v", err)
			os.Exit(1)
		}
		return persister
	}
	nullPersister := &crawlerpersist.NullPersister{}
	return nullPersister
}

func listingPersister(config *utils.ProcessorConfig) model.ListingPersister {
	p := persister(config)
	return p.(model.ListingPersister)
}

func contentRevisionPersister(config *utils.ProcessorConfig) model.ContentRevisionPersister {
	p := persister(config)
	return p.(model.ContentRevisionPersister)
}

func governanceEventPersister(config *utils.ProcessorConfig) model.GovernanceEventPersister {
	p := persister(config)
	return p.(model.GovernanceEventPersister)
}

func persister(config *utils.ProcessorConfig) interface{} {
	// if config.PersisterType == utils.PersisterTypePostgresql {
	// 	return postgresPersister(config)
	// }
	// Default to the NullPersister
	return &persistence.NullPersister{}
}

func civilMetadataScraper(config *utils.ProcessorConfig) model.CivilMetadataScraper {
	return &scraper.CivilMetadataScraper{}
}

func contentScraper(config *utils.ProcessorConfig) model.ContentScraper {
	return &scraper.NullScraper{}
}

func metadataScraper(config *utils.ProcessorConfig) model.MetadataScraper {
	return &scraper.NullScraper{}
}

func runProcessor(config *utils.ProcessorConfig) {
	client, err := ethclient.Dial(config.EthAPIURL)
	if err != nil {
		log.Errorf("Error connecting to eth API: err: %v", err)
		return
	}

	eventPersister := eventPersister(config)
	events, err := eventPersister.RetrieveEvents(&crawlermodel.RetrieveEventsCriteria{})
	if err != nil {
		log.Errorf("Error retrieving events: err: %v", err)
		return
	}

	cronPersister := persistence.NewCronPersister()

	proc := processor.NewEventProcessor(
		client,
		listingPersister(config),
		contentRevisionPersister(config),
		governanceEventPersister(config),
		cronPersister,
		contentScraper(config),
		metadataScraper(config),
		civilMetadataScraper(config),
	)
	err = proc.Process(events)
	if err != nil {
		log.Errorf("Error retrieving events: err: %v", err)
		return
	}
	log.Info("Done running processor")
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
		log.Errorf("Invalid crawler config: err: %v\n", err)
		os.Exit(2)
	}

	cr := cron.New()
	err = cr.AddFunc(config.CronConfig, func() { runProcessor(config) })
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
