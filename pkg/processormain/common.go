package processormain

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/golang/glog"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	crawlerhelpers "github.com/joincivil/civil-events-crawler/pkg/helpers"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	"github.com/joincivil/civil-events-processor/pkg/helpers"
	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/persistence"
	"github.com/joincivil/civil-events-processor/pkg/processor"
	"github.com/joincivil/civil-events-processor/pkg/utils"

	cerrors "github.com/joincivil/go-common/pkg/errors"
	cpubsub "github.com/joincivil/go-common/pkg/pubsub"
)

const (
	maxOpenConns    = 5
	maxIdleConns    = 5
	connMaxLifetime = time.Second * 180 // 3 mins
)

// InitErrorReporter inits an error reporter struct
func InitErrorReporter(config *utils.ProcessorConfig) (cerrors.ErrorReporter, error) {
	errRepConfig := &cerrors.MetaErrorReporterConfig{
		StackDriverProjectID:      config.StackDriverProjectID,
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
	Persister                   *persistence.PostgresPersister
	Cron                        model.CronPersister
	Event                       crawlermodel.EventDataPersister
	Listing                     model.ListingPersister
	ContentRevision             model.ContentRevisionPersister
	GovernanceEvent             model.GovernanceEventPersister
	Challenge                   model.ChallengePersister
	Poll                        model.PollPersister
	Appeal                      model.AppealPersister
	TokenTransfer               model.TokenTransferPersister
	ParameterProposal           model.ParamProposalPersister
	Parameter                   model.ParameterPersister
	UserChallengeData           model.UserChallengeDataPersister
	MultiSig                    model.MultiSigPersister
	MultiSigOwner               model.MultiSigOwnerPersister
	GovernmentParameterProposal model.GovernmentParamProposalPersister
	GovernmentParameter         model.GovernmentParameterPersister
}

func initSqlxDB(config *utils.ProcessorConfig) (*sqlx.DB, error) {
	psqlInfo := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config.PersisterPostgresAddress,
		config.PersisterPostgresPort,
		config.PersisterPostgresUser,
		config.PersisterPostgresPw,
		config.PersisterPostgresDbname,
	)
	db, err := sqlx.Connect("postgres", psqlInfo)
	if err != nil {
		return nil, errors.Wrap(err, "error connecting to sqlx")
	}

	if config.PersisterPostgresMaxConns != nil {
		db.SetMaxOpenConns(*config.PersisterPostgresMaxConns)
	} else {
		// Default value
		db.SetMaxOpenConns(maxOpenConns)
	}
	if config.PersisterPostgresMaxIdle != nil {
		db.SetMaxIdleConns(*config.PersisterPostgresMaxIdle)
	} else {
		// Default value
		db.SetMaxIdleConns(maxIdleConns)
	}
	if config.PersisterPostgresConnLife != nil {
		db.SetConnMaxLifetime(time.Second * time.Duration(*config.PersisterPostgresConnLife))
	} else {
		// Default value
		db.SetConnMaxLifetime(connMaxLifetime)
	}

	return db, nil
}

// InitPersisters inits the persisters from the config file
func InitPersisters(config *utils.ProcessorConfig) (*InitializedPersisters, error) {
	db, err := initSqlxDB(config)
	if err != nil {
		log.Errorf("Error init sqlx db: %v", err)
		return nil, err
	}

	eventPersister, err := crawlerhelpers.EventPersisterFromSqlx(db)
	if err != nil {
		log.Errorf("Error getting the event persister: %v", err)
		return nil, err
	}

	persister, err := helpers.PersisterFromSqlx(db, config.VersionNumber)
	if err != nil {
		log.Errorf("Error getting the persister: %v", err)
		return nil, err
	}

	return &InitializedPersisters{
		Persister:                   persister.(*persistence.PostgresPersister),
		Cron:                        persister.(model.CronPersister),
		Event:                       eventPersister,
		Listing:                     persister.(model.ListingPersister),
		ContentRevision:             persister.(model.ContentRevisionPersister),
		GovernanceEvent:             persister.(model.GovernanceEventPersister),
		Challenge:                   persister.(model.ChallengePersister),
		Poll:                        persister.(model.PollPersister),
		Appeal:                      persister.(model.AppealPersister),
		TokenTransfer:               persister.(model.TokenTransferPersister),
		ParameterProposal:           persister.(model.ParamProposalPersister),
		Parameter:                   persister.(model.ParameterPersister),
		UserChallengeData:           persister.(model.UserChallengeDataPersister),
		MultiSig:                    persister.(model.MultiSigPersister),
		MultiSigOwner:               persister.(model.MultiSigOwnerPersister),
		GovernmentParameterProposal: persister.(model.GovernmentParamProposalPersister),
		GovernmentParameter:         persister.(model.GovernmentParameterPersister),
	}, nil
}

// ClosePersisters closes all the initialized persisters via Close()
func ClosePersisters(persisters *InitializedPersisters) {
	err := persisters.Persister.Close()
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
	}

	err = SaveLastEventInformation(persisters.Cron, events, lastTs)
	if err != nil {
		log.Errorf("Error saving last seen event info %v: err: %v", lastTs, err)
		errRep.Error(err, nil)
	}
}
