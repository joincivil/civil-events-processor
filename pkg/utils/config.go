// Package utils contains various common utils separate by utility types
package utils

import (
	"errors"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/kelseyhightower/envconfig"
	"github.com/robfig/cron"

	crawlerutils "github.com/joincivil/civil-events-crawler/pkg/utils"
)

// PersisterType is the type of persister to use.
type PersisterType int

const (
	// PersisterTypeInvalid is an invalid persister value
	PersisterTypeInvalid PersisterType = iota

	// PersisterTypeNone is a persister that does nothing but return default values
	PersisterTypeNone

	// PersisterTypePostgresql is a persister that uses PostgreSQL as the backend
	PersisterTypePostgresql
)

var (
	// PersisterNameToType maps valid persister names to the types above
	PersisterNameToType = map[string]PersisterType{
		"none":       PersisterTypeNone,
		"postgresql": PersisterTypePostgresql,
	}
)

const (
	envVarPrefix = "processor"

	usageListFormat = `The processor is configured via environment vars only. The following environment variables can be used:
{{range .}}
{{usage_key .}}
  description: {{usage_description .}}
  type:        {{usage_type .}}
  default:     {{usage_default .}}
  required:    {{usage_required .}}
{{end}}
`
)

// NOTE(PN): After envconfig populates ProcessorConfig with the environment vars,
// there is nothing preventing the ProcessorConfig fields from being mutated.

// ProcessorConfig is the master config for the processor derived from environment
// variables.
type ProcessorConfig struct {
	CronConfig string `envconfig:"cron_config" required:"true" desc:"Cron config string * * * * *"`
	EthAPIURL  string `envconfig:"eth_api_url" required:"true" desc:"Ethereum API address"`

	PersisterType            PersisterType `ignored:"true"`
	PersisterTypeName        string        `split_words:"true" required:"true" desc:"Sets the persister type to use"`
	PersisterPostgresAddress string        `split_words:"true" desc:"If persister type is Postgresql, sets the address"`
	PersisterPostgresPort    int           `split_words:"true" desc:"If persister type is Postgresql, sets the port"`
	PersisterPostgresDbname  string        `split_words:"true" desc:"If persister type is Postgresql, sets the database name"`
	PersisterPostgresUser    string        `split_words:"true" desc:"If persister type is Postgresql, sets the database user"`
	PersisterPostgresPw      string        `split_words:"true" desc:"If persister type is Postgresql, sets the database password"`
}

// OutputUsage prints the usage string to os.Stdout
func (c *ProcessorConfig) OutputUsage() {
	tabs := tabwriter.NewWriter(os.Stdout, 1, 0, 4, ' ', 0)
	_ = envconfig.Usagef(envVarPrefix, c, tabs, usageListFormat) // nolint: gosec
	_ = tabs.Flush()                                             // nolint: gosec
}

// PopulateFromEnv processes the environment vars, populates ProcessorConfig
// with the respective values, and validates the values.
func (c *ProcessorConfig) PopulateFromEnv() error {
	err := envconfig.Process(envVarPrefix, c)
	if err != nil {
		return err
	}

	err = c.validateCronConfig()
	if err != nil {
		return err
	}

	err = c.validateAPIURL()
	if err != nil {
		return err
	}

	err = c.populatePersisterType()
	if err != nil {
		return err
	}

	return c.validatePersister()
}

func (c *ProcessorConfig) validateCronConfig() error {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	_, err := parser.Parse(c.CronConfig)
	if err != nil {
		return fmt.Errorf("Invalid cron config: '%v'", c.CronConfig)
	}
	return nil
}

func (c *ProcessorConfig) validateAPIURL() error {
	if c.EthAPIURL == "" || !crawlerutils.IsValidEthAPIURL(c.EthAPIURL) {
		return fmt.Errorf("Invalid eth API URL: '%v'", c.EthAPIURL)
	}
	return nil
}

func (c *ProcessorConfig) validatePersister() error {
	var err error
	if c.PersisterType == PersisterTypePostgresql {
		err = c.validatePostgresqlPersister()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ProcessorConfig) validatePostgresqlPersister() error {
	if c.PersisterPostgresAddress == "" {
		return errors.New("Postgresql address required")
	}
	if c.PersisterPostgresPort == 0 {
		return errors.New("Postgresql port required")
	}
	if c.PersisterPostgresDbname == "" {
		return errors.New("Postgresql db name required")
	}
	return nil
}

func (c *ProcessorConfig) populatePersisterType() error {
	var err error
	c.PersisterType, err = PersisterTypeFromName(c.PersisterTypeName)
	return err
}

// PersisterTypeFromName returns the correct persisterType from the string name
func PersisterTypeFromName(typeStr string) (PersisterType, error) {
	pType, ok := PersisterNameToType[typeStr]
	if !ok {
		validNames := make([]string, len(PersisterNameToType))
		index := 0
		for name := range PersisterNameToType {
			validNames[index] = name
			index++
		}
		return PersisterTypeInvalid,
			fmt.Errorf("Invalid persister value: %v; valid types %v", typeStr, validNames)
	}
	return pType, nil
}
