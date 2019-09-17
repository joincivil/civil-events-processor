package processormain

import (
	"os"
	"runtime"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	log "github.com/golang/glog"
	"github.com/robfig/cron"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	"github.com/joincivil/civil-events-processor/pkg/processor"
	"github.com/joincivil/civil-events-processor/pkg/utils"

	cerrors "github.com/joincivil/go-common/pkg/errors"
	cpubsub "github.com/joincivil/go-common/pkg/pubsub"
	ctime "github.com/joincivil/go-common/pkg/time"
)

const (
	checkRunSecs = 5
	// If the process has been locked for longer than this time,
	// should just unlock it.
	processLockTimeoutSecs = 60 * 30 // 30 mins
)

var (
	processLockTs = 0
)

func checkCron(cr *cron.Cron) {
	entries := cr.Entries()
	for _, entry := range entries {
		log.Infof("Proc run times: prev: %v, next: %v\n", entry.Prev, entry.Next)
	}
}

func isProcessLocked() bool {
	if processLockTs > 0 {
		if (ctime.CurrentEpochSecsInInt() - processLockTs) <= processLockTimeoutSecs {
			return true
		}
		resetProcessLock()
	}
	return false
}

func setProcessLock() {
	processLockTs = ctime.CurrentEpochSecsInInt()
}

func resetProcessLock() {
	processLockTs = 0
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

	return initPubSubEvents(config, ps)
}

// RunProcessor runs the processor
func runProcessorCron(config *utils.ProcessorConfig, persisters *InitializedPersisters,
	errRep cerrors.ErrorReporter) {
	// Check the process lock to ensure we run one of these at a time.
	if isProcessLocked() {
		log.Infof("Cron process already running, stopping run...")
		return
	}
	// Set the lock and make sure lock is reset when done
	setProcessLock()
	defer resetProcessLock()

	lastTs, lastHashes, err := GetLastEventInformation(persisters)
	if err != nil {
		errRep.Error(err, nil)
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
		errRep.Error(err, nil)
		return
	}

	if len(events) > 0 {
		client, err := ethclient.Dial(config.EthAPIURL)
		if err != nil {
			log.Errorf("Error connecting to eth API: err: %v", err)
			errRep.Error(err, nil)
			return
		}
		defer client.Close()

		pubsub, err := initPubSubForCron(config)
		if err != nil {
			log.Errorf("Error initializing pubsub: err: %v", err)
			errRep.Error(err, nil)
			return
		}

		proc := processor.NewEventProcessor(&processor.NewEventProcessorParams{
			Client:                     client,
			ListingPersister:           persisters.Listing,
			RevisionPersister:          persisters.ContentRevision,
			GovEventPersister:          persisters.GovernanceEvent,
			ChallengePersister:         persisters.Challenge,
			PollPersister:              persisters.Poll,
			AppealPersister:            persisters.Appeal,
			TokenTransferPersister:     persisters.TokenTransfer,
			ParameterProposalPersister: persisters.ParameterProposal,
			ParameterPersister:         persisters.Parameter,
			UserChallengeDataPersister: persisters.UserChallengeData,
			GooglePubSub:               pubsub,
			PubSubEventsTopicName:      config.PubSubEventsTopicName,
			PubSubTokenTopicName:       config.PubSubTokenTopicName,
			ErrRep:                     errRep,
		})

		RunProcessor(proc, persisters, events, lastTs, errRep)
	}

	log.Infof("Done running processor: %v", runtime.NumGoroutine())
}

// ProcessorCronMain contains the logic to run the processor using a cronjob
func ProcessorCronMain(config *utils.ProcessorConfig, persisters *InitializedPersisters) {
	errRep, err := InitErrorReporter(config)
	if err != nil {
		log.Errorf("Error init error reporting: err: %+v\n", err)
		os.Exit(2)
	}

	err = persisters.Parameter.CreateDefaultValues(config)
	if err != nil {
		log.Errorf("Error creating default values: err: %v", err)
		errRep.Error(err, nil)
		os.Exit(1)
	}
	// runProcessorCron one startup before waiting for cron to trigger
	runProcessorCron(config, persisters, errRep)

	cr := cron.New()
	err = cr.AddFunc(config.CronConfig, func() { runProcessorCron(config, persisters, errRep) })
	if err != nil {
		log.Errorf("Error starting: err: %v", err)
		errRep.Error(err, nil)
		os.Exit(1)
	}
	cr.Start()

	// Blocks here while the cron process runs
	for range time.After(checkRunSecs * time.Second) {
		checkCron(cr)
	}
}
