package processormain

import (
	"encoding/json"
	"errors"
	log "github.com/golang/glog"
	"os"
	"os/signal"
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
	return ps.StartSubscribersWithConfig(
		cpubsub.SubscribeConfig{
			Name:    config.PubSubCrawlSubName,
			AutoAck: false,
		},
	)
}

func processMessageGetEvents(msg *pubsub.Message) (*crawlerps.CrawlerPubSubMessage, error) {
	mess := &crawlerps.CrawlerPubSubMessage{}
	err := json.Unmarshal(msg.Data, mess)
	return mess, err
}

func isNewsroomException(crawlerMsg *crawlerps.CrawlerPubSubMessage) bool {
	if crawlerMsg.NewsroomException && crawlerMsg.ContractAddress != "" {
		return true
	}
	return false
}

// RunProcessorPubSub runs processor upon receiving messages from pubsub
func RunProcessorPubSub(persisters *InitializedPersisters, ps *cpubsub.GooglePubSub,
	proc *processor.EventProcessor, quit <-chan bool) {
	log.Info("Start listening for messages")
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
			var retrieveCriteria *crawlermodel.RetrieveEventsCriteria
			if isNewsroomException(messData) {
				log.Infof("Received newsroom exception message with ID: %v from crawler", msg.ID)
				retrieveCriteria = &crawlermodel.RetrieveEventsCriteria{
					FromTs:          lastTs,
					ContractAddress: messData.ContractAddress,
				}
			} else {
				log.Infof("Received regular message with ID: %v from crawler", msg.ID)
				lastHashes, cronTableErr := persisters.Cron.EventHashesOfLastTimestampForCron()
				if cronTableErr != nil {
					log.Errorf("Error getting event hashes for last timestamp seen in cron: %v", cronTableErr)
					return
				}
				retrieveCriteria = &crawlermodel.RetrieveEventsCriteria{
					FromTs:        lastTs,
					ExcludeHashes: lastHashes,
				}
			}
			events, err := persisters.Event.RetrieveEvents(retrieveCriteria)
			if err != nil {
				log.Errorf("Error retrieving events: err: %v", err)
				return
			}
			err = proc.Process(events)
			if err != nil {
				log.Errorf("Error processing events: err: %v", err)
				return
			}
			// Manually acknowledge message receipt after processing is successful
			msg.Ack()
			// NOTE(IS): Only save lastTs if this message isn't a NewsroomException
			if !isNewsroomException(messData) {
				err := SaveLastEventInformation(persisters.Cron, events, lastTs)
				if err != nil {
					log.Errorf("Error saving last seen event info %v: err: %v", lastTs, err)
					return
				}
			}
			log.Infof("Finished processing events from message\n")
		case <-quit:
			log.Infof("Quitting")
			break Loop
		}
	}
}

func cleanup(ps *cpubsub.GooglePubSub, quitChan chan<- bool) {
	err := ps.StopSubscribers()
	if err != nil {
		log.Errorf("Error stopping subscribers: err: %v", err)
	}
	close(quitChan)
	log.Info("Subscribers stopped")
}

func setupKillNotify(ps *cpubsub.GooglePubSub, quitChan chan<- bool) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cleanup(ps, quitChan)
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
	setupKillNotify(ps, quitChan)
	defer func() {
		cleanup(ps, quitChan)
	}()

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

	// First run processor without pubsub:
	lastTs, lastHashes, err := GetLastEventInformation(persisters)
	if err != nil {
		return
	}
	events, err := persisters.Event.RetrieveEvents(
		&crawlermodel.RetrieveEventsCriteria{
			FromTs:        lastTs,
			ExcludeHashes: lastHashes,
		},
	)
	if err != nil {
		log.Errorf("Error retrieving events: err: %v", err)
		return
	}
	if len(events) > 0 {
		RunProcessor(proc, persisters, events, lastTs)
	}
	RunProcessorPubSub(persisters, ps, proc, quitChan)
}
