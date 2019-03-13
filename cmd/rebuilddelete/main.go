// Package main contains logic to delete old versions of tables
package main

import (
	"flag"
	"fmt"
	log "github.com/golang/glog"
	"os"

	cpersist "github.com/joincivil/civil-events-crawler/pkg/persistence"
	cpostgres "github.com/joincivil/civil-events-crawler/pkg/persistence/postgres"
	"github.com/joincivil/civil-events-processor/pkg/persistence"
	"github.com/joincivil/civil-events-processor/pkg/persistence/postgres"

	cutils "github.com/joincivil/civil-events-crawler/pkg/utils"
)

func main() {
	config := &cutils.RebuildConfig{}
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
	// this should look in db and get all versions which aren't new
	persister, err := cpersist.NewPostgresPersister(
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

	versions, err := persister.OldVersions(persistence.ProcessorServiceName)
	if err != nil {
		log.Errorf("Error getting versions, stopping...; err: %v", err)
		os.Exit(1)
	}
	tableNames := []string{
		postgres.AppealTableBaseName,
		postgres.ChallengeTableBaseName,
		postgres.ContentRevisionTableBaseName,
		postgres.CronTableBaseName,
		postgres.GovernanceEventTableBaseName,
		postgres.ListingTableBaseName,
		postgres.PollTableBaseName,
		postgres.TokenTransferTableBaseName,
	}
	fmt.Println(versions)
	for _, version := range versions {
		for _, tableName := range tableNames {
			currTableName := fmt.Sprintf("%s_%s", tableName, version)
			log.Infof("Attempting to delete table %v", currTableName)
			err := persister.DropTable(currTableName)
			if err != nil {
				log.Errorf("Error deleting %v table, stopping...; err: %v", currTableName, err)
			}
			log.Infof("Successfully deleted table %v", currTableName)
			err = persister.UpdateExistenceFalseForVersionTable(cpostgres.VersionTableName, version,
				persistence.ProcessorServiceName)
			if err != nil {
				log.Errorf("Error updating exists field for %v table, stopping...; err: %v", currTableName, err)
			}
		}

	}

	// NOTE(IS): Not deleting versions from version table so we can keep track.
	log.Info("Rebuild completed")
}
