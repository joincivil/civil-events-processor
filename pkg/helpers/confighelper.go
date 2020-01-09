// Package helpers contains various common helper functions.
// Normally they are shared functions used by the cmds.
package helpers

import (
	// log "github.com/golang/glog"

	"github.com/jmoiron/sqlx"
	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/persistence"

	cconfig "github.com/joincivil/go-common/pkg/config"
)

// Persister is a helper function to return an interface{} that is a initialized
// persister type
func Persister(config cconfig.PersisterConfig, versionNumber string) (interface{}, error) {
	if config.PersistType() == cconfig.PersisterTypePostgresql {
		return postgresPersister(config, versionNumber)
	}
	// Default to the NullPersister
	return &persistence.NullPersister{}, nil
}

// PersisterFromSqlx is a helper function to return an interface{} given an
// initialized sqlx.DB struct
func PersisterFromSqlx(db *sqlx.DB, versionNumber string) (interface{}, error) {
	persister, err := persistence.NewPostgresPersisterFromSqlx(db)
	if err != nil {
		return nil, err
	}

	err = initTablesAndData(persister, versionNumber)
	if err != nil {
		return nil, err
	}

	return persister, nil
}

// CronPersister is a helper function to return the correct cron persister based on
// the given configuration
func CronPersister(config cconfig.PersisterConfig, versionNumber string) (model.CronPersister, error) {
	p, err := Persister(config, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.CronPersister), nil
}

// CronPersisterFromSqlx is a helper function to return the correct cron persister based on
// the given configuration
func CronPersisterFromSqlx(db *sqlx.DB, versionNumber string) (model.CronPersister, error) {
	p, err := PersisterFromSqlx(db, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.CronPersister), nil
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

// ListingPersisterFromSqlx is a helper function to return the correct listing persister based on
// the given configuration
func ListingPersisterFromSqlx(db *sqlx.DB, versionNumber string) (model.ListingPersister, error) {
	p, err := PersisterFromSqlx(db, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.ListingPersister), nil
}

// MultiSigPersister is a helper function to return the correct multi sig persister based on
// the given configuration
func MultiSigPersister(config cconfig.PersisterConfig, versionNumber string) (model.MultiSigPersister, error) {
	p, err := Persister(config, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.MultiSigPersister), nil
}

// MultiSigPersisterFromSqlx is a helper function to return the correct multi sig persister based on
// the given configuration
func MultiSigPersisterFromSqlx(db *sqlx.DB, versionNumber string) (model.MultiSigPersister, error) {
	p, err := PersisterFromSqlx(db, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.MultiSigPersister), nil
}

// MultiSigOwnerPersister is a helper function to return the correct multi sig owner persister based on
// the given configuration
func MultiSigOwnerPersister(config cconfig.PersisterConfig, versionNumber string) (model.MultiSigOwnerPersister, error) {
	p, err := Persister(config, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.MultiSigOwnerPersister), nil
}

// MultiSigOwnerPersisterFromSqlx is a helper function to return the correct multi sig persister based on
// the given configuration
func MultiSigOwnerPersisterFromSqlx(db *sqlx.DB, versionNumber string) (model.MultiSigOwnerPersister, error) {
	p, err := PersisterFromSqlx(db, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.MultiSigOwnerPersister), nil
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

// ContentRevisionPersisterFromSqlx is a helper function to return the correct revision persister based on
// the given configuration
func ContentRevisionPersisterFromSqlx(db *sqlx.DB, versionNumber string) (model.ContentRevisionPersister, error) {
	p, err := PersisterFromSqlx(db, versionNumber)
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

// GovernanceEventPersisterFromSqlx is a helper function to return the correct gov event persister based on
// the given configuration
func GovernanceEventPersisterFromSqlx(db *sqlx.DB, versionNumber string) (model.GovernanceEventPersister, error) {
	p, err := PersisterFromSqlx(db, versionNumber)
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

// ChallengePersisterFromSqlx is a helper function to return the correct challenge persister based on
// the given configuration
func ChallengePersisterFromSqlx(db *sqlx.DB, versionNumber string) (model.ChallengePersister, error) {
	p, err := PersisterFromSqlx(db, versionNumber)
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

// AppealPersister is a helper function to return the correct appeals persister based on
// the given configuration
func AppealPersisterFromSqlx(db *sqlx.DB, versionNumber string) (model.AppealPersister, error) {
	p, err := PersisterFromSqlx(db, versionNumber)
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

// PollPersister is a helper function to return the correct poll persister based on
// the given configuration
func PollPersisterFromSqlx(db *sqlx.DB, versionNumber string) (model.PollPersister, error) {
	p, err := PersisterFromSqlx(db, versionNumber)
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

// TokenTransferPersisterFromSqlx is a helper function to return the token transfer persister based on
// the given configuration
func TokenTransferPersisterFromSqlx(db *sqlx.DB, versionNumber string) (model.TokenTransferPersister, error) {
	p, err := PersisterFromSqlx(db, versionNumber)
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

// ParameterizerPersisterFromSqlx is a helper function to return the parameterizerpersister based
// on the given configureation
func ParameterizerPersisterFromSqlx(db *sqlx.DB, versionNumber string) (model.ParamProposalPersister, error) {
	p, err := PersisterFromSqlx(db, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.ParamProposalPersister), nil
}

// ParameterPersister is a helper function to return the parameterpersister based
// on the given configureation
func ParameterPersister(config cconfig.PersisterConfig, versionNumber string) (model.ParameterPersister, error) {
	p, err := Persister(config, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.ParameterPersister), nil
}

// ParameterPersisterFromSqlx is a helper function to return the parameterpersister based
// on the given sqlx.Db
func ParameterPersisterFromSqlx(db *sqlx.DB, versionNumber string) (model.ParameterPersister, error) {
	p, err := PersisterFromSqlx(db, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.ParameterPersister), nil
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

// UserChallengeDataPersisterFromSqlx is a helper function to return the userchallengedatapersister based
// on the given sqlx.DB.
func UserChallengeDataPersisterFromSqlx(db *sqlx.DB, versionNumber string) (
	model.UserChallengeDataPersister, error) {
	p, err := PersisterFromSqlx(db, versionNumber)
	if err != nil {
		return nil, err
	}
	return p.(model.UserChallengeDataPersister), nil
}

func postgresPersister(config cconfig.PersisterConfig, versionNumber string) (*persistence.PostgresPersister, error) {
	persister, err := persistence.NewPostgresPersister(
		config.Address(),
		config.Port(),
		config.User(),
		config.Password(),
		config.Dbname(),
		config.PoolMaxConns(),
		config.PoolMaxIdleConns(),
		config.PoolConnLifetimeSecs(),
	)
	if err != nil {
		return nil, err
	}
	err = initTablesAndData(persister, versionNumber)
	if err != nil {
		return nil, err
	}

	return persister, nil
}

func initTablesAndData(persister *persistence.PostgresPersister, versionNumber string) error {
	// Version table created by eventPersister, so just store version
	err := persister.InitProcessorVersion(&versionNumber)
	if err != nil {
		return err
	}
	// Attempts to create all the necessary tables here
	err = persister.CreateTables()
	if err != nil {
		return err
	}
	// Attempts to create all the necessary table indices here
	err = persister.CreateIndices()
	if err != nil {
		return err
	}
	// Attempts to run all the necessary table updates/migrations here
	err = persister.RunMigrations()
	if err != nil {
		return err
	}
	return nil
}
