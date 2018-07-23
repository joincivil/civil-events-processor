// +build integration

// This is an integration test file for postgrespersister. Postgres needs to be running.
// Run this using go test -tags=integration
// Run benchmark test using go test -tags=integration -bench=.
package persistence

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/persistence/postgres"
	"reflect"
	"testing"
)

const (
	postgresPort   = 5432
	postgresDBName = "civil_crawler"
	postgresUser   = "docker"
	postgresPswd   = "docker"
	postgresHost   = "localhost"
)

// random hex string generation
func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func setupDBConnection() (*PostgresPersister, error) {
	postgresPersister, err := NewPostgresPersister(postgresHost, postgresPort, postgresUser, postgresPswd, postgresDBName)
	return postgresPersister, err
}

func setupTestTable(tableName string) (*PostgresPersister, error) {
	persister, err := setupDBConnection()
	if err != nil {
		return persister, fmt.Errorf("Error connecting to DB: %v", err)
	}
	var schema string
	switch tableName {
	case "listing_test":
		schema = postgres.ListingSchemaString(tableName)
	case "content_revision_test":
		schema = postgres.ContentRevisionSchemaString(tableName)
	case "governance_event_test":
		schema = postgres.GovernanceEventSchemaString(tableName)
	}
	_, err = persister.db.Query(schema)
	if err != nil {
		return persister, fmt.Errorf("Couldn't create test table %s: %v", tableName, err)
	}
	return persister, nil
}

func deleteTestTable(persister *PostgresPersister, tableName string) error {
	var err error
	switch tableName {
	case "listing_test":
		_, err = persister.db.Query("DROP TABLE listing_test;")
	case "content_revision_test":
		_, err = persister.db.Query("DROP TABLE content_revision_test;")
	case "governance_event_test":
		_, err = persister.db.Query("DROP TABLE governance_event_test;")
	}
	if err != nil {
		return fmt.Errorf("Couldn't delete test table %s: %v", tableName, err)
	}
	return nil
}

func setupSampleListing() (*model.Listing, common.Address) {
	address1, _ := randomHex(32)
	address2, _ := randomHex(32)
	address3, _ := randomHex(32)
	contractAddress := common.HexToAddress(address1)
	ownerAddresses := []common.Address{common.HexToAddress(address2), common.HexToAddress(address3)}
	contributorAddresses := ownerAddresses
	testListing := model.NewListing("test_listing", contractAddress, true,
		model.GovernanceStateAppWhitelisted, "url_string", "charterURI", ownerAddresses,
		contributorAddresses, 1257894000, 1257894000, 1257894000, 1257894000)
	return testListing, contractAddress
}

func TestDBConnection(t *testing.T) {
	persister, err := setupDBConnection()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	var result int
	err = persister.db.QueryRow("SELECT 1;").Scan(&result)
	if err != nil {
		t.Errorf("Error querying DB: %v", err)
	}
	if result != 1 {
		t.Errorf("Wrong result from DB")
	}
}

func checkTableExists(tableName string, persister *PostgresPersister) error {
	var exists bool
	queryString := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
        FROM   information_schema.tables 
        WHERE  table_schema = 'public'
        AND    table_name = '%s'
        );`, tableName)
	err := persister.db.QueryRow(queryString).Scan(&exists)
	if err != nil {
		return fmt.Errorf("Couldn't get %s table", tableName)
	}
	if !exists {
		return fmt.Errorf("%s table does not exist", tableName)
	}
	return nil
}

func TestTableSetup(t *testing.T) {
	// run function to create tables, and test table exists
	persister, err := setupDBConnection()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	err = persister.CreateTables()
	if err != nil {
		t.Errorf("Error creating tables: %v", err)
	}
	err = checkTableExists("listing", persister)
	if err != nil {
		t.Error(err)
	}
	err = checkTableExists("content_revision", persister)
	if err != nil {
		t.Error(err)
	}
	err = checkTableExists("governance_event", persister)
	if err != nil {
		t.Error(err)
	}
}

func TestCreateListing(t *testing.T) {
	tableName := "listing_test"
	// create fake listing in listing_test
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)
	modelListing, _ := setupSampleListing()
	// save to test table
	queryStringCreate := persister.createListingQueryString(tableName)
	err = persister.saveListingToTable(queryStringCreate, modelListing)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}
	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete test listing table: %v", err)
	}
}

// TestListingByAddress tests that the query we are using to get Listing works
func TestListingByAddress(t *testing.T) {
	tableName := "listing_test"
	// create fake listing in listing_test
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)
	modelListing, modelListingAddress := setupSampleListing()
	// save to test table
	queryStringCreate := persister.createListingQueryString("listing_test")
	persister.saveListingToTable(queryStringCreate, modelListing)
	// retrieve from test table
	dbListing := &postgres.Listing{}
	queryStringRetrieve := persister.listingByAddressQuery("listing_test")
	dbListing, err = persister.getListingFromTableByAddress(queryStringRetrieve, modelListingAddress)
	if err != nil {
		t.Errorf("Wasn't able to get listing from postgres table: %v", err)
	}
	modelListingFromDB := dbListing.DbToListingData()
	// check that retrieved fields match with inserted listing
	if !reflect.DeepEqual(modelListing, modelListingFromDB) {
		t.Errorf("listing from DB: %v, doesn't match inserted listing: %v", modelListingFromDB, modelListing)
	}
	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete test listing table: %v", err)
	}
}

// TestUpdateListing tests that updating the Listing works
func TestUpdateListing(t *testing.T) {

}

// TestDeleteListing tests that the deleting the Listing works
func TestDeleteListing(t *testing.T) {

}
