package processormain

import (
	"encoding/json"
	"errors"
	log "github.com/golang/glog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/ethereum/go-ethereum/ethclient"

	"cloud.google.com/go/pubsub"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	crawlerps "github.com/joincivil/civil-events-crawler/pkg/pubsub"

	cpubsub "github.com/joincivil/go-common/pkg/pubsub"

	"github.com/joincivil/civil-events-processor/pkg/helpers"
	"github.com/joincivil/civil-events-processor/pkg/processor"
	"github.com/joincivil/civil-events-processor/pkg/utils"
)

func initPubSub(config *utils.ProcessorConfig) (*cpubsub.GooglePubSub, error) {
	// If no project ID, quit
	if config.PubSubProjectID == "" {
		return nil, errors.New("Need PubSubProjectID")
	}

	ps, err := cpubsub.NewGooglePubSub(config.PubSubProjectID)
	if err != nil {
		return nil, err
	}
	return ps, err
}

func initPubSubSubscribers(config *utils.ProcessorConfig, ps *cpubsub.GooglePubSub) error {
	// If no crawl topic name, quit
	if config.PubSubCrawlTopicName == "" {
		return errors.New("Pubsub topic name should be specified")
	}

	// If no subscription name, quit
	if config.PubSubCrawlSubName == "" {
		return errors.New("Pubsub subscription name should be specified")
	}

	return ps.StartSubscribers(config.PubSubCrawlSubName)
}

func processMessageGetEvents(msg *pubsub.Message) (*crawlerps.CrawlerPubSubMessage, error) {
	mess := &crawlerps.CrawlerPubSubMessage{}
	err := json.Unmarshal(msg.Data, mess)
	return mess, err
}

// RunProcessorPubSub gets messages from pubsub
func RunProcessorPubSub(persisters *InitializedPersisters, ps *cpubsub.GooglePubSub,
	proc *processor.EventProcessor, quit <-chan bool) {
Loop:
	for {
		select {
		case msg, ok := <-ps.SubscribeChan:
			if !ok {
				log.Errorf("Sending on closed channel")
				break Loop
			}

			messData, err := processMessageGetEvents(msg)
			if err != nil {
				log.Errorf("Error processing message: err: %v", err)
			}

			lastTs, err := persisters.Cron.TimestampOfLastEventForCron()
			if err != nil {
				log.Errorf("Error getting last event timestamp: %v", err)
				return
			}

			if messData.Hash != "" && messData.Timestamp < lastTs {
				log.Errorf("Timestamp %v is less than last persisted timestamp for event with hash %v",
					messData.Timestamp, messData.Hash)
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

			log.Infof("Finished processing events from message\n")
		case <-quit:
			log.Infof("Quitting")
			break Loop
		}
	}
}

func cleanup(ps *cpubsub.GooglePubSub) {
	err := ps.StopSubscribers()
	if err != nil {
		log.Errorf("Error stopping subscribers: err: %v", err)
	}
	log.Info("Subscribers stopped")
}

func setupKillNotify(ps *cpubsub.GooglePubSub, quitChan chan<- bool) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		quitChan <- true
		cleanup(ps)
		os.Exit(1)
	}()
}

// ProcessorPubSubMain runs the processor using pubsub
func ProcessorPubSubMain(config *utils.ProcessorConfig, persisters *InitializedPersisters) {
	ps, err := initPubSub(config)
	if err != nil {
		log.Errorf("Error initializing pubsub: err: %v", err)
		return
	}
	quitChan := make(chan bool)
	// Setup pubsub for getting subscriptions
	err = initPubSubSubscribers(config, ps)
	if err != nil {
		log.Errorf("Error starting subscribers for pubsub: err: %v", err)
	}

	// Setup pubsub for events. This can be nil
	eventsPs, err := initPubSubEvents(config, ps)
	if err != nil {
		log.Errorf("Error starting publishers for events: err: %v", err)
		return
	}

	client, err := ethclient.Dial(config.EthAPIURL)
	if err != nil {
		log.Errorf("Error connecting to eth API: err: %v", err)
		return
	}
	defer client.Close()

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
		GooglePubSub:          eventsPs,
		GooglePubSubTopicName: config.PubSubEventsTopicName,
	})

	RunProcessorPubSub(persisters, ps, proc, quitChan)

	setupKillNotify(ps, quitChan)

	log.Infof("Done running processor: %v", runtime.NumGoroutine())
}
