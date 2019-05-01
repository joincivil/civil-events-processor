package processormain

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	log "github.com/golang/glog"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	"github.com/joincivil/civil-events-processor/pkg/helpers"
	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/processor"
	"github.com/joincivil/civil-events-processor/pkg/utils"

	cerrors "github.com/joincivil/go-common/pkg/errors"
	cpubsub "github.com/joincivil/go-common/pkg/pubsub"
)

// InitErrorReporter inits an error reporter struct
func InitErrorReporter(config *utils.ProcessorConfig) (cerrors.ErrorReporter, error) {
	errRepConfig := &cerrors.MetaErrorReporterConfig{
		StackDriverProjectID:      "civil-media",
		StackDriverServiceName:    "processor",
		StackDriverServiceVersion: "1.0",
		SentryDSN:                 config.SentryDsn,
		SentryDebug:               false,
		SentryEnv:                 config.SentryEnv,
		SentryLoggerName:          "processor_logger",
		SentryRelease:             "1.0",
		SentrySampleRate:          1.0,
	}
	reporter, err := cerrors.NewMetaErrorReporter(errRepConfig)
	if err != nil {
		log.Errorf("Error creating meta reporter: %v", err)
		return nil, err
	}
	if reporter == nil {
		log.Infof("Enabling null error reporter")
		return &cerrors.NullErrorReporter{}, nil
	}
	return reporter, nil
}

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
		log.Infof("Updating timestamp %v, eventHashes %v", lastTs, eventHashes)
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

// SetupKillNotify inits cleanup hook when a kill command is sent to the process
func SetupKillNotify(persisters *InitializedPersisters) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		ClosePersisters(persisters)
		os.Exit(1)
	}()
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
	Cron              model.CronPersister
	Event             crawlermodel.EventDataPersister
	Listing           model.ListingPersister
	ContentRevision   model.ContentRevisionPersister
	GovernanceEvent   model.GovernanceEventPersister
	Challenge         model.ChallengePersister
	Poll              model.PollPersister
	Appeal            model.AppealPersister
	TokenTransfer     model.TokenTransferPersister
	ParameterProposal model.ParamProposalPersister
	UserChallengeData model.UserChallengeDataPersister
}

// InitPersisters inits the persisters from the config file
func InitPersisters(config *utils.ProcessorConfig) (*InitializedPersisters, error) {
	cronPersister, err := helpers.CronPersister(config, config.VersionNumber)
	if err != nil {
		log.Errorf("Error getting the cron persister: %v", err)
		return nil, err
	}
	eventPersister, err := helpers.EventPersister(config)
	if err != nil {
		log.Errorf("Error getting the event persister: %v", err)
		return nil, err
	}
	listingPersister, err := helpers.ListingPersister(config, config.VersionNumber)
	if err != nil {
		log.Errorf("Error w listingPersister: err: %v", err)
		return nil, err
	}
	contentRevisionPersister, err := helpers.ContentRevisionPersister(config, config.VersionNumber)
	if err != nil {
		log.Errorf("Error w contentRevisionPersister: err: %v", err)
		return nil, err
	}
	governanceEventPersister, err := helpers.GovernanceEventPersister(config, config.VersionNumber)
	if err != nil {
		log.Errorf("Error w governanceEventPersister: err: %v", err)
		return nil, err
	}
	challengePersister, err := helpers.ChallengePersister(config, config.VersionNumber)
	if err != nil {
		log.Errorf("Error w ChallengePersister: err: %v", err)
		return nil, err
	}
	pollPersister, err := helpers.PollPersister(config, config.VersionNumber)
	if err != nil {
		log.Errorf("Error w PollPersister: err: %v", err)
		return nil, err
	}
	appealPersister, err := helpers.AppealPersister(config, config.VersionNumber)
	if err != nil {
		log.Errorf("Error w AppealPersister: err: %v", err)
		return nil, err
	}
	transferPersister, err := helpers.TokenTransferPersister(config, config.VersionNumber)
	if err != nil {
		log.Errorf("Error w transferPersister: err: %v", err)
		return nil, err
	}
	paramProposalPersister, err := helpers.ParameterizerPersister(config, config.VersionNumber)
	if err != nil {
		log.Errorf("Error w paramProposalPersister %v", err)
		return nil, err
	}
	userChallengeDataPersister, err := helpers.UserChallengeDataPersister(config, config.VersionNumber)
	if err != nil {
		log.Errorf("Error w userChallengeDataPersister %v", err)
		return nil, err
	}
	return &InitializedPersisters{
		Cron:              cronPersister,
		Event:             eventPersister,
		Listing:           listingPersister,
		ContentRevision:   contentRevisionPersister,
		GovernanceEvent:   governanceEventPersister,
		Challenge:         challengePersister,
		Poll:              pollPersister,
		Appeal:            appealPersister,
		TokenTransfer:     transferPersister,
		ParameterProposal: paramProposalPersister,
		UserChallengeData: userChallengeDataPersister,
	}, nil
}

// ClosePersisters closes all the initialized persisters via Close()
func ClosePersisters(persisters *InitializedPersisters) {
	err := persisters.Cron.Close()
	if err != nil {
		log.Errorf("Error closing persister: err: %v", err)
	}
	// err = persisters.Event.Close()
	// if err != nil {
	// 	log.Errorf("Error closing persister: err: %v", err)
	// }
	err = persisters.Listing.Close()
	if err != nil {
		log.Errorf("Error closing persister: err: %v", err)
	}
	err = persisters.ContentRevision.Close()
	if err != nil {
		log.Errorf("Error closing persister: err: %v", err)
	}
	err = persisters.GovernanceEvent.Close()
	if err != nil {
		log.Errorf("Error closing persister: err: %v", err)
	}
	err = persisters.Challenge.Close()
	if err != nil {
		log.Errorf("Error closing persister: err: %v", err)
	}
	err = persisters.Poll.Close()
	if err != nil {
		log.Errorf("Error closing persister: err: %v", err)
	}
	err = persisters.Appeal.Close()
	if err != nil {
		log.Errorf("Error closing persister: err: %v", err)
	}
	err = persisters.TokenTransfer.Close()
	if err != nil {
		log.Errorf("Error closing persister: err: %v", err)
	}
	err = persisters.ParameterProposal.Close()
	if err != nil {
		log.Errorf("Error closing persister: err: %v", err)
	}
	err = persisters.UserChallengeData.Close()
	if err != nil {
		log.Errorf("Error closing persister: err: %v", err)
	}
}

// GetLastEventInformation gets the timestamp and associated hashes for the last events processed
func GetLastEventInformation(persisters *InitializedPersisters) (int64, []string, error) {
	lastTs, err := persisters.Cron.TimestampOfLastEventForCron()
	if err != nil {
		log.Errorf("Error getting last event timestamp: %v", err)
		return lastTs, []string{}, err
	}

	lastHashes, err := persisters.Cron.EventHashesOfLastTimestampForCron()
	if err != nil {
		log.Errorf("Error getting event hashes for last timestamp seen in cron: %v", err)
		return lastTs, lastHashes, err
	}
	return lastTs, lastHashes, nil
}

// RunProcessor runs the processor
func RunProcessor(proc *processor.EventProcessor, persisters *InitializedPersisters,
	events []*crawlermodel.Event, lastTs int64, errRep cerrors.ErrorReporter) {

	err := proc.Process(events)
	if err != nil {
		log.Errorf("Error processing events: err: %v", err)
		errRep.Error(err, nil)
		return
	}

	err = SaveLastEventInformation(persisters.Cron, events, lastTs)
	if err != nil {
		log.Errorf("Error saving last seen event info %v: err: %v", lastTs, err)
		errRep.Error(err, nil)
		return
	}
}
