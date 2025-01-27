// Package utils contains various common utils separate by utility types
package utils

import (
	"errors"
	"fmt"
	"strings"

	"github.com/kelseyhightower/envconfig"
	"github.com/robfig/cron"

	cconfig "github.com/joincivil/go-common/pkg/config"
	cstrings "github.com/joincivil/go-common/pkg/strings"
)

const (
	envVarPrefixProcessor = "processor"
)

// NOTE(PN): After envconfig populates ProcessorConfig with the environment vars,
// there is nothing preventing the ProcessorConfig fields from being mutated.

// ProcessorConfig is the master config for the processor derived from environment
// variables.
//
// If CronConfig is set, will use the cron process.  If it is not set, will setup
// the subscription to the crawler pubsub to listen for trigger events.
type ProcessorConfig struct {
	CronConfig string `envconfig:"cron_config" desc:"Cron config string * * * * *"`
	EthAPIURL  string `envconfig:"eth_api_url" required:"true" desc:"Ethereum API address"`

	PubSubProjectID         string `split_words:"true" desc:"Sets GPubSub project ID. If not set, will not push or pull events."`
	PubSubEventsTopicName   string `split_words:"true" desc:"Sets GPubSub topic name for governance events. If not set, will not push events."`
	PubSubTokenTopicName    string `split_words:"true" desc:"Sets GPubSub topic name for cvltoken events. If not set, will not push events."`
	PubSubMultiSigTopicName string `split_words:"true" desc:"Sets GPubSub topic name for multi sig events. If not set, will not push events."`
	PubSubCrawlTopicName    string `split_words:"true" desc:"Sets GPubSub topic name for crawler. Set if using pubsub to run the processor."`
	PubSubCrawlSubName      string `split_words:"true" desc:"Sets GPubSub subscription name. Needs to be set to run processor using pubsub updates."`

	PersisterType             cconfig.PersisterType `ignored:"true"`
	PersisterTypeName         string                `split_words:"true" required:"true" desc:"Sets the persister type to use"`
	PersisterPostgresAddress  string                `split_words:"true" desc:"If persister type is Postgresql, sets the address"`
	PersisterPostgresPort     int                   `split_words:"true" desc:"If persister type is Postgresql, sets the port"`
	PersisterPostgresDbname   string                `split_words:"true" desc:"If persister type is Postgresql, sets the database name"`
	PersisterPostgresUser     string                `split_words:"true" desc:"If persister type is Postgresql, sets the database user"`
	PersisterPostgresPw       string                `split_words:"true" desc:"If persister type is Postgresql, sets the database password"`
	PersisterPostgresMaxConns *int                  `split_words:"true" desc:"If persister type is Postgresql, sets the max conns in pool"`
	PersisterPostgresMaxIdle  *int                  `split_words:"true" desc:"If persister type is Postgresql, sets the max idle conns in pool"`
	PersisterPostgresConnLife *int                  `split_words:"true" desc:"If persister type is Postgresql, sets the max conn lifetime in secs"`

	VersionNumber string `split_words:"true" desc:"Sets the version to use for Postgres tables"`

	StackDriverProjectID string `split_words:"true" desc:"Sets the Stackdriver project ID"`
	SentryDsn            string `split_words:"true" desc:"Sets the Sentry DSN"`
	SentryEnv            string `split_words:"true" desc:"Sets the Sentry environment"`

	ParameterizerDefaultValues       map[string]string `split_words:"true" required:"true" desc:"<parameter name>:<parameter value>. Delimit contract address with '|' for multiple addresses"`
	GovernmentParameterDefaultValues map[string]string `split_words:"true" required:"true" desc:"<parameter name>:<parameter value>. Delimit contract address with '|' for multiple addresses"`
}

// PersistType returns the persister type, implements PersisterConfig
func (c *ProcessorConfig) PersistType() cconfig.PersisterType {
	return c.PersisterType
}

// PostgresAddress returns the postgres persister address, implements PersisterConfig
func (c *ProcessorConfig) Address() string {
	return c.PersisterPostgresAddress
}

// PostgresPort returns the postgres persister port, implements PersisterConfig
func (c *ProcessorConfig) Port() int {
	return c.PersisterPostgresPort
}

// PostgresDbname returns the postgres persister db name, implements PersisterConfig
func (c *ProcessorConfig) Dbname() string {
	return c.PersisterPostgresDbname
}

// PostgresUser returns the postgres persister user, implements PersisterConfig
func (c *ProcessorConfig) User() string {
	return c.PersisterPostgresUser
}

// PostgresPw returns the postgres persister password, implements PersisterConfig
func (c *ProcessorConfig) Password() string {
	return c.PersisterPostgresPw
}

// PoolMaxConns returns the max conns for a pool, if configured, implements PersisterConfig
func (c *ProcessorConfig) PoolMaxConns() *int {
	return c.PersisterPostgresMaxConns
}

// PoolMaxIdleConns returns the max idleconns for a pool, if configured, implements PersisterConfig
func (c *ProcessorConfig) PoolMaxIdleConns() *int {
	return c.PersisterPostgresMaxIdle
}

// PoolConnLifetimeSecs returns the conn lifetime for a pool, if configured, implements PersisterConfig
func (c *ProcessorConfig) PoolConnLifetimeSecs() *int {
	return c.PersisterPostgresConnLife
}

// ParameterizerDefaults returns the parameterizer default values
func (c *ProcessorConfig) ParameterizerDefaults() map[string]string {
	return c.ParameterizerDefaultValues
}

// GovernmentParameterDefaults returns the government parameter default values
func (c *ProcessorConfig) GovernmentParameterDefaults() map[string]string {
	return c.GovernmentParameterDefaultValues
}

// OutputUsage prints the usage string to os.Stdout
func (c *ProcessorConfig) OutputUsage() {
	cconfig.OutputUsage(c, envVarPrefixProcessor, envVarPrefixProcessor)
}

// PopulateFromEnv processes the environment vars, populates ProcessorConfig
// with the respective values, and validates the values.
func (c *ProcessorConfig) PopulateFromEnv() error {
	envEnvVar := fmt.Sprintf("%v_ENV", strings.ToUpper(envVarPrefixProcessor))
	err := cconfig.PopulateFromDotEnv(envEnvVar)
	if err != nil {
		return err
	}

	err = envconfig.Process(envVarPrefixProcessor, c)
	if err != nil {
		return err
	}
	if c.CronConfig != "" {
		err = c.validateCronConfig()
		if err != nil {
			return err
		}
	}

	err = c.validateAPIURL()
	if err != nil {
		return err
	}

	err = c.populatePersisterType()
	if err != nil {
		return err
	}

	err = c.validateParameterizerDefaultValues()
	if err != nil {
		return err
	}

	err = c.validateGovernmentParameterDefaultValues()
	if err != nil {
		return err
	}

	return c.validatePersister()
}

func (c *ProcessorConfig) validateCronConfig() error {
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	_, err := parser.Parse(c.CronConfig)
	if err != nil {
		return fmt.Errorf("Invalid cron config: '%v'", c.CronConfig)
	}
	return nil
}

func (c *ProcessorConfig) validateAPIURL() error {
	if c.EthAPIURL == "" || !cstrings.IsValidEthAPIURL(c.EthAPIURL) {
		return fmt.Errorf("Invalid eth API URL: '%v'", c.EthAPIURL)
	}
	return nil
}

func (c *ProcessorConfig) validatePersister() error {
	var err error
	if c.PersisterType == cconfig.PersisterTypePostgresql {
		err = validatePostgresqlPersisterParams(
			c.PersisterPostgresAddress,
			c.PersisterPostgresPort,
			c.PersisterPostgresDbname,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ProcessorConfig) validateParameterizerDefaultValues() error {
	for paramName, paramValue := range c.ParameterizerDefaultValues {
		if paramName == "" {
			return fmt.Errorf("Invalid parameter name: '%v'", paramName)
		}
		if paramValue == "" {
			return fmt.Errorf("Invalid parameter value: '%v'", paramValue)
		}
	}
	return nil
}

func (c *ProcessorConfig) validateGovernmentParameterDefaultValues() error {
	for paramName, paramValue := range c.GovernmentParameterDefaultValues {
		if paramName == "" {
			return fmt.Errorf("Invalid parameter name: '%v'", paramName)
		}
		if paramValue == "" {
			return fmt.Errorf("Invalid parameter value: '%v'", paramValue)
		}
	}
	return nil
}

func (c *ProcessorConfig) populatePersisterType() error {
	var err error
	c.PersisterType, err = cconfig.PersisterTypeFromName(c.PersisterTypeName)
	return err
}

func validatePostgresqlPersisterParams(address string, port int, dbname string) error {
	if address == "" {
		return errors.New("Postgresql address required")
	}
	if port == 0 {
		return errors.New("Postgresql port required")
	}
	if dbname == "" {
		return errors.New("Postgresql db name required")
	}
	return nil
}
