package main

// This script checks the Google Function datastore against our events database.
// Specifically it will check if the TxHashes for the events in our DB exist in
// our datastore.  This will both allow us to check for any discrepancies, but
// also allow us to prevent oddly timed or old tweets/emails to be fired if
// there needs to be a rebuild of the processor data.

// TODO(PN): Later to be expanded to PUT missing entries into the datastore. Only
// checks for missing entries right now.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/datastore"
	"github.com/kelseyhightower/envconfig"
	"google.golang.org/api/iterator"

	crawlmodel "github.com/joincivil/civil-events-crawler/pkg/model"
	crawlpersist "github.com/joincivil/civil-events-crawler/pkg/persistence"

	cconfig "github.com/joincivil/go-common/pkg/config"
	ceth "github.com/joincivil/go-common/pkg/eth"
)

const (
	googleCredsEnvVarName = "GOOGLE_APPLICATION_CREDENTIALS"
	dsKind                = "GovernanceEvent"
)

// Config configures this script
type Config struct {
	WetRun                   bool   `split_words:"true" desc:"If set to true, will perform mutations on the data"`
	PersisterPostgresAddress string `split_words:"true" desc:"If persister type is Postgresql, sets the address"`
	PersisterPostgresPort    int    `split_words:"true" desc:"If persister type is Postgresql, sets the port"`
	PersisterPostgresDbname  string `split_words:"true" desc:"If persister type is Postgresql, sets the database name"`
	PersisterPostgresUser    string `split_words:"true" desc:"If persister type is Postgresql, sets the database user"`
	PersisterPostgresPw      string `split_words:"true" desc:"If persister type is Postgresql, sets the database password"`
	TcrContractAddress       string `split_words:"true" desc:"Sets up the contract address of the TCR"`
	DsNamespace              string `split_words:"true" desc:"Sets up the datastore namespace to use"`
}

// PopulateFromEnv processes the environment vars, populates Config
func (c *Config) PopulateFromEnv() error {
	return envconfig.Process("dscheck", c)
}

// OutputUsage prints the usage string to os.Stdout
func (c *Config) OutputUsage() {
	cconfig.OutputUsage(c, "dscheck", "dscheck")
}

// Relies on the GOOGLE_CREDENTIALS envvar
func datastoreClient() (*datastore.Client, error) {
	googleCredsEnvVar := os.Getenv(googleCredsEnvVarName)
	if googleCredsEnvVar == "" {
		return nil, errors.New("Required Google envvars do not appear to be set")
	}

	ctx := context.Background()

	client, err := datastore.NewClient(ctx, "civil-media")
	if err != nil {
		return nil, err
	}

	return client, nil
}

func eventsPersister(config *Config) (*crawlpersist.PostgresPersister, error) {
	return crawlpersist.NewPostgresPersister(
		config.PersisterPostgresAddress,
		config.PersisterPostgresPort,
		config.PersisterPostgresUser,
		config.PersisterPostgresPw,
		config.PersisterPostgresDbname,
	)
}

func run(config *Config) {
	dsClient, err := datastoreClient()
	if err != nil {
		fmt.Printf("error with datastore: err: %v\n", err)
		os.Exit(2)
	}

	eventsPersister, err := eventsPersister(config)
	if err != nil {
		fmt.Printf("error with persister: err: %v\n", err)
		os.Exit(2)
	}

	offset := 0
	count := 500
	ctx := context.Background()

	// normalize addr
	config.TcrContractAddress = ceth.NormalizeEthAddress(config.TcrContractAddress)

Loop:
	for {
		events, err := eventsPersister.RetrieveEvents(
			&crawlmodel.RetrieveEventsCriteria{
				ContractAddress: config.TcrContractAddress,
				Offset:          offset,
				Count:           count,
			},
		)
		if err != nil {
			fmt.Printf("error retrieving events: err: %v\n", err)
			os.Exit(2)
		}

		for _, event := range events {
			txHash := strings.ToLower(event.TxHash().Hex())
			query := datastore.NewQuery(dsKind).
				Filter("txHash =", txHash).
				Namespace(config.DsNamespace).
				KeysOnly()

			it := dsClient.Run(ctx, query)

			resultFound := false
			for {
				_, err := it.Next(nil)
				if err == iterator.Done {
					break
				}
				if err != nil {
					fmt.Printf(
						"error retrieving from ds: eventType: %v, err: %v\n",
						event.EventType(),
						err,
					)
					continue
				}
				resultFound = true
			}

			if !resultFound {
				fmt.Printf("Missing event: %v, payload: %v, txHash: %v\n",
					event.EventType(),
					event.EventPayload(),
					event.TxHash().Hex(),
				)
			}
		}

		if len(events) < count {
			break Loop
		}

		offset += count
	}
	fmt.Printf("Done.\n")
}

func main() {
	config := &Config{}
	flag.Usage = func() {
		config.OutputUsage()
		os.Exit(0)
	}
	flag.Parse()

	err := cconfig.PopulateFromDotEnv("DSCHECK_ENV")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(2)
	}

	err = config.PopulateFromEnv()
	if err != nil {
		config.OutputUsage()
		os.Exit(2)
	}

	run(config)
}
