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
	crawlerutils "github.com/joincivil/civil-events-crawler/pkg/utils"
	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/persistence/postgres"
	"math/big"
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

// randomHex generates a random hex string
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

/*
General DB tests
*/

// TestDBConnection tests that we can connect to DB
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

/*
Helpers for listing table tests:
*/

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

/*
All tests for listing table:
*/

// TestCreateListing tests that a listing is created
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
	queryStringCreate := persister.insertIntoDBQueryString(tableName, postgres.Listing{})
	err = persister.saveListingToTable(queryStringCreate, modelListing)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}
	// check that listing is there
	var numRowsb int
	err = persister.db.QueryRow(`SELECT COUNT(*) FROM listing_test`).Scan(&numRowsb)
	if numRowsb != 1 {
		t.Errorf("Number of rows in table should be 0 but is: %v", numRowsb)
	}

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete test listing table: %v", err)
	}
}

// TestListingByAddress tests that the query we are using to get Listing works
func TestListingByAddress(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)
	// create fake listing in listing_test
	modelListing, modelListingAddress := setupSampleListing()

	// save to test table
	queryStringCreate := persister.insertIntoDBQueryString(tableName, postgres.Listing{})
	err = persister.saveListingToTable(queryStringCreate, modelListing)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}

	// retrieve from test table
	dbListing := &postgres.Listing{}
	queryStringRetrieve := persister.listingByAddressQuery("listing_test")
	dbListing, err = persister.getListingFromTableByAddress(queryStringRetrieve, modelListingAddress)
	if err != nil {
		t.Errorf("Wasn't able to get listing from postgres table: %v", err)
	}
	modelListingFromDB := dbListing.DbToListingData()
	// check that retrieved fields match with inserted listing
	// TODO(IS): more correct for this to be in a different test. This test should probably be on its own
	if !reflect.DeepEqual(modelListing, modelListingFromDB) {
		t.Errorf("listing from DB: %v, doesn't match inserted listing: %v", modelListingFromDB, modelListing)
	}
	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete test listing table: %v", err)
	}
}

// TODO(IS): test retrieving multiple listings

// TestUpdateListing tests that updating the Listing works
func TestUpdateListing(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	modelListing, modelListingAddress := setupSampleListing()

	// save this to table
	queryStringCreate := persister.insertIntoDBQueryString(tableName, postgres.Listing{})
	err = persister.saveListingToTable(queryStringCreate, modelListing)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}

	// modify fields
	updatedFields := []string{"Name", "Whitelisted"}
	modelListing.SetName("New Name")
	modelListing.SetWhitelisted(false)

	// test update
	queryString, err := persister.updateListingQuery(updatedFields, tableName)
	if err != nil {
		t.Errorf("Error generating update listing query: %v", err)
	}

	dbListing := postgres.NewListing(modelListing)
	_, err = persister.db.NamedQuery(queryString, dbListing)
	if err != nil {
		t.Errorf("Error updating fields: %v", err)
	}

	//check here that update happened
	updatedDbListing := &postgres.Listing{}
	queryStringRetrieve := persister.listingByAddressQuery("listing_test")
	updatedDbListing, err = persister.getListingFromTableByAddress(queryStringRetrieve, modelListingAddress)
	if err != nil {
		t.Errorf("Wasn't able to get listing from postgres table: %v", err)
	}
	if updatedDbListing.Name != "New Name" {
		t.Errorf("Name field was not updated correctly. %v", updatedDbListing.Name)
	}
	if updatedDbListing.Whitelisted != false {
		t.Errorf("Whitelisted field was not updated correctly. %v", updatedDbListing.Whitelisted)
	}

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete test listing table: %v", err)
	}

}

// TestDeleteListing tests that the deleting the Listing works
func TestDeleteListing(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	modelListing, _ := setupSampleListing()

	// save this to table
	queryStringCreate := persister.insertIntoDBQueryString(tableName, postgres.Listing{})
	err = persister.saveListingToTable(queryStringCreate, modelListing)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}

	var numRowsb int
	err = persister.db.QueryRow(`SELECT COUNT(*) FROM listing_test`).Scan(&numRowsb)
	if err != nil {
		t.Errorf("Problem getting count from table: %v", err)
	}
	if numRowsb != 1 {
		t.Errorf("Number of rows in table should be 1 but is: %v", numRowsb)
	}

	//delete rows
	queryString := persister.deleteListingQuery(tableName)
	dbListing := postgres.NewListing(modelListing)
	_, err = persister.db.NamedQuery(queryString, dbListing)

	var numRows int
	err = persister.db.QueryRow(`SELECT COUNT(*) FROM listing_test`).Scan(&numRows)
	if err != nil {
		t.Errorf("Problem getting count from table: %v", err)
	}
	if numRows != 0 {
		t.Errorf("Number of rows in table should be 0 but is: %v", numRows)
	}
}

/*
Helpers for content_revision table tests:
*/

func setupSampleContentRevision() (*model.ContentRevision, common.Address, *big.Int, *big.Int) {
	address1, _ := randomHex(32)
	address2, _ := randomHex(32)
	address3, _ := randomHex(32)
	listingAddr := common.HexToAddress(address1)
	payload := model.ArticlePayload{}
	payloadHash := address2
	editorAddress := common.HexToAddress(address3)
	contractContentID := big.NewInt(3434343334)
	contractRevisionID := big.NewInt(676767676676)
	revisionURI := "revisionURI"
	revisionDateTs := crawlerutils.CurrentEpochSecsInInt64()
	testContentRevision := model.NewContentRevision(listingAddr, payload, payloadHash, editorAddress,
		contractContentID, contractRevisionID, revisionURI, revisionDateTs)
	return testContentRevision, listingAddr, contractContentID, contractRevisionID
}

/*
All tests for content_revision table:
*/

// TestCreateListing tests that a ContentRevision is created
func TestCreateContentRevision(t *testing.T) {
	tableName := "content_revision_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// sample contentRevision
	modelContentRevision, _, _, _ := setupSampleContentRevision()

	// insert to table
	queryStringCreate := persister.insertIntoDBQueryString(tableName, postgres.ContentRevision{})
	err = persister.saveContentRevisionToTable(queryStringCreate, modelContentRevision)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}
	// check row is there
	var numRowsb int
	err = persister.db.QueryRow(`SELECT COUNT(*) FROM content_revision_test`).Scan(&numRowsb)
	if numRowsb != 1 {
		t.Errorf("Number of rows in table should be 0 but is: %v", numRowsb)
	}

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete test listing table: %v", err)
	}
}

// TestContentRevision tests that a content revision is created
func TestContentRevision(t *testing.T) {
	tableName := "content_revision_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// sample contentRevision
	modelContentRevision, listingAddr, contentID, revisionID := setupSampleContentRevision()

	// insert to table
	queryStringCreate := persister.insertIntoDBQueryString(tableName, postgres.ContentRevision{})
	err = persister.saveContentRevisionToTable(queryStringCreate, modelContentRevision)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}

	// retrieve from table
	queryString := persister.contentRevisionQuery("content_revision_test")
	_, err = persister.getContentRevisionFromTable(queryString, listingAddr.Hex(), contentID.Int64(), revisionID.Int64())
	if err != nil {
		t.Errorf("Wasn't able to get content revision from postgres table: %v", err)
	}

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete test listing table: %v", err)
	}
}

// TODO(IS): test multiple contentrevisions

// TODO(IS): test update content revision

// TestDeleteContentRevision tests that the deleting the ContentRevision works
func TestDeleteContentRevision(t *testing.T) {
	tableName := "content_revision_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// sample contentRevision
	modelContentRevision, _, _, _ := setupSampleContentRevision()

	// insert to table
	queryStringCreate := persister.insertIntoDBQueryString(tableName, postgres.ContentRevision{})
	err = persister.saveContentRevisionToTable(queryStringCreate, modelContentRevision)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}

	var numRowsb int
	err = persister.db.QueryRow(`SELECT COUNT(*) FROM content_revision_test`).Scan(&numRowsb)
	if err != nil {
		t.Errorf("Problem getting count from table: %v", err)
	}
	if numRowsb != 1 {
		t.Errorf("Number of rows in table should be 1 but is: %v", numRowsb)
	}

	//delete rows
	queryString := persister.deleteContentRevisionQuery(tableName)
	dbContentRevision := postgres.NewContentRevision(modelContentRevision)
	_, err = persister.db.NamedQuery(queryString, dbContentRevision)

	var numRows int
	err = persister.db.QueryRow(`SELECT COUNT(*) FROM content_revision_test`).Scan(&numRows)
	if err != nil {
		t.Errorf("Problem getting count from table: %v", err)
	}
	if numRows != 0 {
		t.Errorf("Number of rows in table should be 0 but is: %v", numRows)
	}
}

/*
Helpers for governance_event table tests:
*/

func setupSampleGovernanceEvent() (*model.GovernanceEvent, common.Address) {
	address1, _ := randomHex(32)
	address2, _ := randomHex(32)
	listingAddr := common.HexToAddress(address1)
	senderAddress := common.HexToAddress(address2)
	metadata := model.Metadata{}
	governanceEventType := "governanceeventtypehere"
	creationDateTs := crawlerutils.CurrentEpochSecsInInt64()
	lastUpdatedDateTs := crawlerutils.CurrentEpochSecsInInt64() + 1
	testGovernanceEvent := model.NewGovernanceEvent(listingAddr, senderAddress, metadata, governanceEventType,
		creationDateTs, lastUpdatedDateTs)
	return testGovernanceEvent, listingAddr
}

/*
All tests for governance_event table:
*/

// TestCreateGovernanceEvent tests that a GovernanceEvent is created
func TestCreateGovernanceEvent(t *testing.T) {
	tableName := "governance_event_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// sample contentRevision
	modelGovernanceEvent, _ := setupSampleGovernanceEvent()

	// insert to table
	queryStringCreate := persister.insertIntoDBQueryString(tableName, postgres.GovernanceEvent{})
	err = persister.saveGovEventToTable(queryStringCreate, modelGovernanceEvent)
	if err != nil {
		t.Errorf("error saving GovernanceEvent: %v", err)
	}
	// check row is there
	var numRowsb int
	err = persister.db.QueryRow(`SELECT COUNT(*) FROM governance_event_test`).Scan(&numRowsb)
	if numRowsb != 1 {
		t.Errorf("Number of rows in table should be 0 but is: %v", numRowsb)
	}

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete test listing table: %v", err)
	}
}

// TestGovernanceEventsByListingAddress tests that a GovernanceEvent is properly retrieved
func TestGovernanceEventsByListingAddress(t *testing.T) {
	tableName := "governance_event_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// sample contentRevision
	modelGovernanceEvent, listingAddr := setupSampleGovernanceEvent()

	// insert to table
	queryStringCreate := persister.insertIntoDBQueryString(tableName, postgres.GovernanceEvent{})
	err = persister.saveGovEventToTable(queryStringCreate, modelGovernanceEvent)
	if err != nil {
		t.Errorf("error saving GovernanceEvent: %v", err)
	}

	// retrieve from table
	queryString := persister.govEventsQuery(tableName)
	dbGovEvents, err := persister.getGovEventsFromTable(queryString, listingAddr.Hex())
	if err != nil {
		t.Errorf("Wasn't able to get governance event from postgres table: %v", err)
	}

	if len(dbGovEvents) != 1 {
		t.Errorf("length of governance events should be 1 but is: %v", len(dbGovEvents))
	}

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete governance_event_test table: %v", err)
	}
}

// TODO(IS): test update gov event

// TestDeleteContentRevision tests that the deleting the ContentRevision works
// TODO(IS) : this will delete more than you want. need to put some kind of hash for the gov event.
func TestDeleteGovernanceEvent(t *testing.T) {
	tableName := "governance_event_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// sample contentRevision
	modelGovernanceEvent, _ := setupSampleGovernanceEvent()

	// insert to table
	queryStringCreate := persister.insertIntoDBQueryString(tableName, postgres.GovernanceEvent{})
	err = persister.saveGovEventToTable(queryStringCreate, modelGovernanceEvent)
	if err != nil {
		t.Errorf("error saving governance event: %v", err)
	}

	var numRowsb int
	err = persister.db.QueryRow(`SELECT COUNT(*) FROM governance_event_test`).Scan(&numRowsb)
	if err != nil {
		t.Errorf("Problem getting count from table: %v", err)
	}
	if numRowsb != 1 {
		t.Errorf("Number of rows in table should be 1 but is: %v", numRowsb)
	}

	//delete rows
	queryString := persister.deleteGovEventQuery(tableName)
	dbGovernanceEvent := postgres.NewGovernanceEvent(modelGovernanceEvent)
	_, err = persister.db.NamedQuery(queryString, dbGovernanceEvent)

	var numRows int
	err = persister.db.QueryRow(`SELECT COUNT(*) FROM governance_event_test`).Scan(&numRows)
	if err != nil {
		t.Errorf("Problem getting count from table: %v", err)
	}
	if numRows != 0 {
		t.Errorf("Number of rows in table should be 0 but is: %v", numRows)
	}
}

// TODO (IS): tests here to make sure that each dbtype that you save in the db is the same as the one
// you pull out.
