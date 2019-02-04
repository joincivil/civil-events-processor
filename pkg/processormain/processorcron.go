package processormain

import (
	log "github.com/golang/glog"
	"github.com/robfig/cron"
	"os"
	"time"

	"github.com/joincivil/civil-events-processor/pkg/utils"
	cpubsub "github.com/joincivil/go-common/pkg/pubsub"
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

	return initPubSubEvents(config, ps)
}

// ProcessorCronMain contains the logic to run the processor using a cronjob
func ProcessorCronMain(config *utils.ProcessorConfig, persisters *InitializedPersisters) {
	cr := cron.New()
	err := cr.AddFunc(config.CronConfig, func() { RunProcessor(config, persisters) })
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
