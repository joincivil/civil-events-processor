// Package helpers contains various common helper functions.
// Normally they are shared functions used by the cmds.
package helpers

import (
	// log "github.com/golang/glog"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	crawlerpersist "github.com/joincivil/civil-events-crawler/pkg/persistence"

	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/persistence"

	cconfig "github.com/joincivil/go-common/pkg/config"
)

// CronPersister is a helper function to return the correct cron persister based on
// the given configuration
func CronPersister(config cconfig.PersisterConfig, versionNumber string) (model.CronPersister, error) {
	p, err := Persister(config, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.CronPersister), nil
}

// EventPersister is a helper function to return the correct event persister based on
// the given configuration
func EventPersister(config cconfig.PersisterConfig) (crawlermodel.EventDataPersister, error) {
	if config.PersistType() == cconfig.PersisterTypePostgresql {
		persister, err := crawlerpersist.NewPostgresPersister(
			config.PostgresAddress(),
			config.PostgresPort(),
			config.PostgresUser(),
			config.PostgresPw(),
			config.PostgresDbname(),
		)
		if err != nil {
			return nil, err
		}
		// Pass nil to crawler persistence so it uses the latest version
		err = persister.CreateVersionTable(nil)
		if err != nil {
			return nil, err
		}
		return persister, nil
	}
	nullPersister := &crawlerpersist.NullPersister{}
	return nullPersister, nil
}

// ListingPersister is a helper function to return the correct listing persister based on
// the given configuration
func ListingPersister(config cconfig.PersisterConfig, versionNumber string) (model.ListingPersister, error) {
	p, err := Persister(config, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.ListingPersister), nil
}

// ContentRevisionPersister is a helper function to return the correct revision persister based on
// the given configuration
func ContentRevisionPersister(config cconfig.PersisterConfig, versionNumber string) (model.ContentRevisionPersister, error) {
	p, err := Persister(config, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.ContentRevisionPersister), nil
}

// GovernanceEventPersister is a helper function to return the correct gov event persister based on
// the given configuration
func GovernanceEventPersister(config cconfig.PersisterConfig, versionNumber string) (model.GovernanceEventPersister, error) {
	p, err := Persister(config, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.GovernanceEventPersister), nil
}

// ChallengePersister is a helper function to return the correct challenge persister based on
// the given configuration
func ChallengePersister(config cconfig.PersisterConfig, versionNumber string) (model.ChallengePersister, error) {
	p, err := Persister(config, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.ChallengePersister), nil
}

// AppealPersister is a helper function to return the correct appeals persister based on
// the given configuration
func AppealPersister(config cconfig.PersisterConfig, versionNumber string) (model.AppealPersister, error) {
	p, err := Persister(config, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.AppealPersister), nil
}

// PollPersister is a helper function to return the correct poll persister based on
// the given configuration
func PollPersister(config cconfig.PersisterConfig, versionNumber string) (model.PollPersister, error) {
	p, err := Persister(config, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.PollPersister), nil
}

// TokenTransferPersister is a helper function to return the token transfer persister based on
// the given configuration
func TokenTransferPersister(config cconfig.PersisterConfig, versionNumber string) (model.TokenTransferPersister, error) {
	p, err := Persister(config, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.TokenTransferPersister), nil
}

// ParameterizerPersister is a helper function to return the parameterizerpersister based
// on the given configureation
func ParameterizerPersister(config cconfig.PersisterConfig, versionNumber string) (model.ParamProposalPersister, error) {
	p, err := Persister(config, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.ParamProposalPersister), nil
}

// UserChallengeDataPersister is a helper function to return the userchallengedatapersister based
// on the given configuration.
func UserChallengeDataPersister(config cconfig.PersisterConfig, versionNumber string) (model.UserChallengeDataPersister, error) {
	p, err := Persister(config, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.UserChallengeDataPersister), nil
}

func Persister(config cconfig.PersisterConfig, versionNumber string) (interface{}, error) {
	if config.PersistType() == cconfig.PersisterTypePostgresql {
		return postgresPersister(config, versionNumber)
	}
	// Default to the NullPersister
	return &persistence.NullPersister{}, nil
}

func postgresPersister(config cconfig.PersisterConfig, versionNumber string) (*persistence.PostgresPersister, error) {
	persister, err := persistence.NewPostgresPersister(
		config.PostgresAddress(),
		config.PostgresPort(),
		config.PostgresUser(),
		config.PostgresPw(),
		config.PostgresDbname(),
	)
	if err != nil {
		// log.Errorf("Error connecting to Postgresql, stopping...; err: %v", err)
		return nil, err
	}
	// Version table created by eventPersister, so just store version
	err = persister.InitProcessorVersion(&versionNumber)
	if err != nil {
		return nil, err
	}
	// Attempts to create all the necessary tables here
	err = persister.CreateTables()
	if err != nil {
		// log.Errorf("Error creating tables, stopping...; err: %v", err)
		return nil, err
	}
	// Attempts to create all the necessary table indices here
	err = persister.CreateIndices()
	if err != nil {
		// log.Errorf("Error creating table indices, stopping...; err: %v", err)
		return nil, err
	}
	// Attempts to run all the necessary table updates/migrations here
	err = persister.RunMigrations()
	if err != nil {
		// log.Errorf("Error running migrations, stopping...; err: %v", err)
		return nil, err
	}
	return persister, nil
}
