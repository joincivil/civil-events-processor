// Package time_test contains tests for the config utils
package utils_test

import (
	"os"
	"testing"

	"github.com/joincivil/civil-events-processor/pkg/utils"
)

func TestCrawlerConfig(t *testing.T) {
	os.Setenv(
		"PROCESSOR_CRON_CONFIG",
		"* * * * *",
	)
	os.Setenv(
		"PROCESSOR_ETH_API_URL",
		"http://ethaddress.com",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_TYPE_NAME",
		"postgresql",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_ADDRESS",
		"localhost",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_PORT",
		"5432",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_DBNAME",
		"civil_crawler",
	)
	config := &utils.ProcessorConfig{}
	err := config.PopulateFromEnv()
	if err != nil {
		t.Errorf("Failed to populate from environment: err: %v", err)
	}
}

func TestBadPersisterNameCrawlerConfig(t *testing.T) {
	os.Setenv(
		"PROCESSOR_CRON_CONFIG",
		"* * * * *",
	)
	os.Setenv(
		"PROCESSOR_ETH_API_URL",
		"http://ethaddress.com",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_TYPE_NAME",
		"postgresql",
	)
	//Bad persister name
	os.Setenv(
		"PROCESSOR_PERSISTER_TYPE_NAME",
		"mysql",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_ADDRESS",
		"localhost",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_PORT",
		"5432",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_DBNAME",
		"civil_crawler",
	)
	config := &utils.ProcessorConfig{}
	err := config.PopulateFromEnv()
	if err == nil {
		t.Errorf("Should have failed to allow bad persister type from environment: err: %v", err)
	}
}

func TestBadPersisterPostgresqlAddressCrawlerConfig(t *testing.T) {
	os.Setenv(
		"PROCESSOR_CRON_CONFIG",
		"* * * * *",
	)
	os.Setenv(
		"PROCESSOR_ETH_API_URL",
		"http://ethaddress.com",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_TYPE_NAME",
		"postgresql",
	)
	//Bad persister postgresql address
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_ADDRESS",
		"",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_PORT",
		"5432",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_DBNAME",
		"civil_crawler",
	)
	config := &utils.ProcessorConfig{}
	err := config.PopulateFromEnv()
	if err == nil {
		t.Errorf("Should have failed to allow bad postgres address from environment: err: %v", err)
	}
}

func TestBadPersisterPostgresqlPortCrawlerConfig(t *testing.T) {
	os.Setenv(
		"PROCESSOR_CRON_CONFIG",
		"* * * * *",
	)
	os.Setenv(
		"PROCESSOR_ETH_API_URL",
		"http://ethaddress.com",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_TYPE_NAME",
		"postgresql",
	)
	//Bad persister postgresql address
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_ADDRESS",
		"localhost",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_PORT",
		"0",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_DBNAME",
		"civil_crawler",
	)
	config := &utils.ProcessorConfig{}
	err := config.PopulateFromEnv()
	if err == nil {
		t.Errorf("Should have failed to allow bad postgres port from environment: err: %v", err)
	}
}

func TestBadPersisterPostgresqlDBNameCrawlerConfig(t *testing.T) {
	os.Setenv(
		"PROCESSOR_CRON_CONFIG",
		"* * * * *",
	)
	os.Setenv(
		"PROCESSOR_ETH_API_URL",
		"http://ethaddress.com",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_TYPE_NAME",
		"postgresql",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_ADDRESS",
		"localhost",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_PORT",
		"5432",
	)
	//Bad persister dbname
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_DBNAME",
		"",
	)
	config := &utils.ProcessorConfig{}
	err := config.PopulateFromEnv()
	if err == nil {
		t.Errorf("Should have failed to allow bad postgres dbname from environment: err: %v", err)
	}
}

func TestBadCronConfigCrawlerConfig1(t *testing.T) {
	os.Setenv(
		"PROCESSOR_CRON_CONFIG",
		"* *",
	)
	os.Setenv(
		"PROCESSOR_ETH_API_URL",
		"http://ethaddress.com",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_TYPE_NAME",
		"postgresql",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_ADDRESS",
		"localhost",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_PORT",
		"5432",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_DBNAME",
		"civil_crawler",
	)
	config := &utils.ProcessorConfig{}
	err := config.PopulateFromEnv()
	if err == nil {
		t.Errorf("Should have failed config: err: %v", err)
	}
}

func TestBadCronConfigCrawlerConfig2(t *testing.T) {
	os.Setenv(
		"PROCESSOR_CRON_CONFIG",
		"* * * * 145",
	)
	os.Setenv(
		"PROCESSOR_ETH_API_URL",
		"http://ethaddress.com",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_TYPE_NAME",
		"postgresql",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_ADDRESS",
		"localhost",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_PORT",
		"5432",
	)
	os.Setenv(
		"PROCESSOR_PERSISTER_POSTGRES_DBNAME",
		"civil_crawler",
	)
	config := &utils.ProcessorConfig{}
	err := config.PopulateFromEnv()
	if err == nil {
		t.Errorf("Should have failed config: err: %v", err)
	}
}
