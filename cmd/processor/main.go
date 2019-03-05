package main

import (
	"flag"
	"os"

	log "github.com/golang/glog"

	"github.com/joincivil/civil-events-processor/pkg/processormain"
	"github.com/joincivil/civil-events-processor/pkg/utils"
)

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

	persisters, err := processormain.InitPersisters(config)
	if err != nil {
		log.Errorf("Error initializing persister: err: %v", err)
		os.Exit(2)
	}

	processormain.SetupKillNotify(persisters)

	if config.CronConfig != "" {
		processormain.ProcessorCronMain(config, persisters)
	} else {
		processormain.ProcessorPubSubMain(config, persisters)
	}

}
