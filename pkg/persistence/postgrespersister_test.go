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
	mathrand "math/rand"
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
	err = postgresPersister.CreateTables()
	if err != nil {
		fmt.Errorf("Error setting up tables in db: %v", err)
	}
	return postgresPersister, err
}

func setupTestTable(tableName string) (*PostgresPersister, error) {
	persister, err := setupDBConnection()
	if err != nil {
		return persister, fmt.Errorf("Error connecting to DB: %v", err)
	}
	var queryString string
	switch tableName {
	case "listing_test":
		queryString = postgres.CreateListingTableQueryString(tableName)
	case "content_revision_test":
		queryString = postgres.CreateContentRevisionTableQueryString(tableName)
	case "governance_event_test":
		queryString = postgres.CreateGovernanceEventTableQueryString(tableName)
	case "cron_test":
		queryString = postgres.CreateCronTableQueryString(tableName)
	}
	_, err = persister.db.Query(queryString)
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
	case "cron_test":
		_, err = persister.db.Query("DROP TABLE cron_test;")
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
	err = checkTableExists("cron", persister)
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

func setupSampleListings(numListings int) ([]*model.Listing, []common.Address) {
	listings := make([]*model.Listing, numListings)
	addresses := make([]common.Address, numListings)
	for i := 0; i < numListings; i++ {
		listings[i], addresses[i] = setupSampleListing()
	}
	return listings, addresses
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
	err = persister.createListingForTable(modelListing, tableName)
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
	err = persister.createListingForTable(modelListing, tableName)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}

	// retrieve from test table
	_, err = persister.listingByAddressFromTable(modelListingAddress, tableName)

	if err != nil {
		t.Errorf("Wasn't able to get listing from postgres table: %v", err)
	}

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete test listing table: %v", err)
	}
}

// TestDBListingToModelListing tests that the db listing can be properly converted to model listing
func TestDBListingToModelListing(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// create fake listing in listing_test
	modelListing, modelListingAddress := setupSampleListing()

	// save to test table
	err = persister.createListingForTable(modelListing, tableName)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}

	// retrieve from test table
	modelListingFromDB, err := persister.listingByAddressFromTable(modelListingAddress, tableName)
	if err != nil {
		t.Errorf("Wasn't able to get listing from postgres table: %v", err)
	}
	// check that retrieved fields match with inserted listing
	if !reflect.DeepEqual(modelListing, modelListingFromDB) {
		t.Errorf("listing from DB: %v, doesn't match inserted listing: %v", modelListingFromDB, modelListing)
	}

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete test listing table: %v", err)
	}
}

// Test retrieving multiple listings
func TestListingsByAddresses(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	// defer deleteTestTable(persister, tableName)

	// create fake listings in listing_test
	numListings := 3
	modelListings, modelListingAddresses := setupSampleListings(numListings)

	// Insert
	for _, list := range modelListings {
		err := persister.createListingForTable(list, tableName)
		if err != nil {
			t.Errorf("Couldn't save listing to table: %v", err)
		}
	}
	//retrieve listings
	dbListings, err := persister.listingsByAddressesFromTable(modelListingAddresses, tableName)
	if err != nil {
		t.Errorf("Error retrieving multiple listings: %v", err)
	}
	if len(dbListings) != numListings {
		t.Errorf("Only retrieved %v listings but should have retrieved %v", len(dbListings), numListings)
	}

	//delete test table
	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete test listing table: %v", err)
	}
}

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
	err = persister.createListingForTable(modelListing, tableName)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}

	// modify fields
	updatedFields := []string{"Name", "Whitelisted"}
	modelListing.SetName("New Name")
	modelListing.SetWhitelisted(false)

	// test update
	err = persister.updateListingInTable(modelListing, updatedFields, tableName)
	if err != nil {
		t.Errorf("Error updating fields: %v", err)
	}

	//check here that update happened
	updatedDbListing, err := persister.listingByAddressFromTable(modelListingAddress, tableName)
	if err != nil {
		t.Errorf("Wasn't able to get listing from postgres table: %v", err)
	}
	if updatedDbListing.Name() != "New Name" {
		t.Errorf("Name field was not updated correctly. %v", updatedDbListing.Name())
	}
	if updatedDbListing.Whitelisted() != false {
		t.Errorf("Whitelisted field was not updated correctly. %v", updatedDbListing.Whitelisted())
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
	err = persister.createListingForTable(modelListing, tableName)
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
	err = persister.deleteListingFromTable(modelListing, tableName)
	if err != nil {
		t.Errorf("Error deleting listing: %v", err)
	}

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

func setupRandomSampleContentRevision() (*model.ContentRevision, common.Address, *big.Int, *big.Int) {
	address, _ := randomHex(32)
	listingAddr := common.HexToAddress(address)
	contractContentID := big.NewInt(mathrand.Int63())
	return setupSampleContentRevision(listingAddr, contractContentID)
}

func setupSampleContentRevision(listingAddr common.Address, contractContentID *big.Int) (*model.ContentRevision, common.Address, *big.Int, *big.Int) {
	address2, _ := randomHex(32)
	address3, _ := randomHex(32)
	payload := model.ArticlePayload{}
	payloadHash := address2
	editorAddress := common.HexToAddress(address3)
	contractRevisionID := big.NewInt(mathrand.Int63())
	revisionURI := "revisionURI"
	revisionDateTs := crawlerutils.CurrentEpochSecsInInt64()
	testContentRevision := model.NewContentRevision(listingAddr, payload, payloadHash, editorAddress,
		contractContentID, contractRevisionID, revisionURI, revisionDateTs)
	return testContentRevision, listingAddr, contractContentID, contractRevisionID
}

func setupSampleContentRevisionsSameAddressContentID(numRevisions int) ([]*model.ContentRevision, common.Address, *big.Int, []*big.Int) {
	address, _ := randomHex(32)
	listingAddr := common.HexToAddress(address)
	contractContentID := big.NewInt(mathrand.Int63())
	testContentRevisions := make([]*model.ContentRevision, numRevisions)
	testContractRevisionIDs := make([]*big.Int, numRevisions)
	for i := 0; i < numRevisions; i++ {
		testContentRevision, _, _, testContractRevisionID := setupSampleContentRevision(listingAddr, contractContentID)
		testContentRevisions[i] = testContentRevision
		testContractRevisionIDs[i] = testContractRevisionID
	}
	return testContentRevisions, listingAddr, contractContentID, testContractRevisionIDs
}

/*
All tests for content_revision table:
*/

// TestCreateContentRevision tests that a ContentRevision is created
func TestCreateContentRevision(t *testing.T) {
	tableName := "content_revision_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// sample contentRevision
	modelContentRevision, _, _, _ := setupRandomSampleContentRevision()

	// insert to table
	err = persister.createContentRevisionForTable(modelContentRevision, tableName)
	if err != nil {
		t.Errorf("error saving content revision: %v", err)
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

// TestContentRevision tests that a content revision can be retrieved
func TestContentRevision(t *testing.T) {
	tableName := "content_revision_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// sample contentRevision
	modelContentRevision, listingAddr, contentID, revisionID := setupRandomSampleContentRevision()

	// insert to table
	err = persister.createContentRevisionForTable(modelContentRevision, tableName)
	if err != nil {
		t.Errorf("error saving content revision: %v", err)
	}

	// retrieve from table
	_, err = persister.contentRevisionFromTable(listingAddr, contentID, revisionID, tableName)
	if err != nil {
		t.Errorf("Wasn't able to get content revision from postgres table: %v", err)
	}

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete test listing table: %v", err)
	}
}

// TestDBCRToModelCR tests that the db listing can be properly converted to model listing
func TestDBCRToModelCR(t *testing.T) {
	tableName := "content_revision_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// sample contentRevision
	modelContentRevision, listingAddr, contentID, revisionID := setupRandomSampleContentRevision()

	// insert to table
	err = persister.createContentRevisionForTable(modelContentRevision, tableName)
	if err != nil {
		t.Errorf("error saving content revision: %v", err)
	}

	// retrieve from table
	modelCRFromDB, err := persister.contentRevisionFromTable(listingAddr, contentID, revisionID, tableName)
	if err != nil {
		t.Errorf("Wasn't able to get content revision from postgres table: %v", err)
	}

	// do a deep equal
	// check that retrieved fields match with inserted listing
	if !reflect.DeepEqual(modelContentRevision, modelCRFromDB) {
		t.Errorf("listing from DB: %v, doesn't match inserted listing: %v", modelCRFromDB, modelContentRevision)
	}

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete test listing table: %v", err)
	}
}

// TestContentRevision tests that multiple content revisions can be retrieved
func TestContentRevisions(t *testing.T) {
	tableName := "content_revision_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// create multiple contentRevisions
	numRevisions := 10
	testContentRevisions, listingAddr, contractContentID, testContractRevisionIDs := setupSampleContentRevisionsSameAddressContentID(numRevisions)

	// save all to table
	for _, contRev := range testContentRevisions {
		err = persister.createContentRevisionForTable(contRev, tableName)
		if err != nil {
			t.Errorf("Couldn't save content revision to table: %v", err)
		}
	}

	// retrieve from table
	dbContentRevisions, err := persister.contentRevisionsFromTable(listingAddr, contractContentID, tableName)
	if err != nil {
		t.Errorf("Error with persister.ContentRevisions(): %v", err)
	}

	// test various things
	// test length of both
	if len(dbContentRevisions) != numRevisions {
		t.Errorf("Only retrieved %v listings but should have retrieved %v", len(dbContentRevisions), numRevisions)
	}
	// order will be the same:
	contractRevisionIDsFromDB := make([]*big.Int, numRevisions)
	for i, contrev := range dbContentRevisions {
		contractRevisionIDsFromDB[i] = contrev.ContractRevisionID()
	}
	reflect.DeepEqual(contractRevisionIDsFromDB, testContractRevisionIDs)

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete test listing table: %v", err)
	}

}

// Test update content revision -- TODO(IS) create an update where address, contentid, revisionid don't change
func TestUpdateContentRevision(t *testing.T) {
}

// TestDeleteContentRevision tests that the deleting the ContentRevision works
func TestDeleteContentRevision(t *testing.T) {
	tableName := "content_revision_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// sample contentRevision
	modelContentRevision, _, _, _ := setupRandomSampleContentRevision()

	// insert to table
	err = persister.createContentRevisionForTable(modelContentRevision, tableName)
	if err != nil {
		t.Errorf("error saving content revision: %v", err)
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
	err = persister.deleteContentRevisionFromTable(modelContentRevision, tableName)
	if err != nil {
		t.Errorf("Coud not delete content revision: %v", err)
	}

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

func setupSampleGovernanceEvent() (*model.GovernanceEvent, common.Address, string) {
	address1, _ := randomHex(32)
	address2, _ := randomHex(32)
	listingAddr := common.HexToAddress(address1)
	senderAddress := common.HexToAddress(address2)
	metadata := model.Metadata{}
	governanceEventType := "governanceeventtypehere"
	creationDateTs := crawlerutils.CurrentEpochSecsInInt64()
	lastUpdatedDateTs := crawlerutils.CurrentEpochSecsInInt64() + 1
	eventHash, _ := randomHex(5)
	testGovernanceEvent := model.NewGovernanceEvent(listingAddr, senderAddress, metadata, governanceEventType,
		creationDateTs, lastUpdatedDateTs, eventHash)
	return testGovernanceEvent, listingAddr, eventHash
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
	modelGovernanceEvent, _, _ := setupSampleGovernanceEvent()

	// insert to table
	err = persister.createGovernanceEventInTable(modelGovernanceEvent, tableName)
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
	modelGovernanceEvent, listingAddr, _ := setupSampleGovernanceEvent()

	// insert to table
	err = persister.createGovernanceEventInTable(modelGovernanceEvent, tableName)
	if err != nil {
		t.Errorf("error saving GovernanceEvent: %v", err)
	}

	// retrieve from table
	dbGovEvents, err := persister.governanceEventsByListingAddressFromTable(listingAddr, tableName)
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

// TestDBGovEventToModelGovEvent tests that the db listing can be properly converted to model listing
func TestDBGovEventToModelGovEvent(t *testing.T) {
	tableName := "governance_event_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// sample contentRevision
	modelGovernanceEvent, listingAddr, _ := setupSampleGovernanceEvent()

	// insert to table
	err = persister.createGovernanceEventInTable(modelGovernanceEvent, tableName)
	if err != nil {
		t.Errorf("error saving GovernanceEvent: %v", err)
	}

	// retrieve from table
	dbGovEvents, err := persister.governanceEventsByListingAddressFromTable(listingAddr, tableName)
	if err != nil {
		t.Errorf("Wasn't able to get governance event from postgres table: %v", err)
	}
	modelGovEventFromDB := dbGovEvents[0]

	// do a deep equal
	// check that retrieved fields match with inserted listing
	if !reflect.DeepEqual(modelGovernanceEvent, modelGovEventFromDB) {
		t.Errorf("listing from DB: %v, doesn't match inserted listing: %v", modelGovEventFromDB, modelGovernanceEvent)
	}

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete governance_event_test table: %v", err)
	}

}

// TODO(IS): test update gov event -- test update that's not by listing

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
	modelGovernanceEvent, _, _ := setupSampleGovernanceEvent()

	// insert to table
	err = persister.createGovernanceEventInTable(modelGovernanceEvent, tableName)
	if err != nil {
		t.Errorf("error saving GovernanceEvent: %v", err)
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
	err = persister.deleteGovenanceEventFromTable(modelGovernanceEvent, tableName)
	if err != nil {
		t.Errorf("Error deleting governance event: %v", err)
	}

	var numRows int
	err = persister.db.QueryRow(`SELECT COUNT(*) FROM governance_event_test`).Scan(&numRows)
	if err != nil {
		t.Errorf("Problem getting count from table: %v", err)
	}
	if numRows != 0 {
		t.Errorf("Number of rows in table should be 0 but is: %v", numRows)
	}
}

/*
All tests for cron table:
- test to see that
*/

func TestTypeExistsInCronTable(t *testing.T) {
	tableName := "cron_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// insert something
	queryString := fmt.Sprintf("INSERT INTO %s(data_persisted, data_type) VALUES(0, 'timestamp')", tableName)
	_, err = persister.db.Exec(queryString)
	// fmt.Println(err)
	if err != nil {
		t.Errorf("Inserting into the cron table should have worked but it didn't, %v", err)
	}

	// test that we can confirm this exists:
	exists, err := persister.typeExistsInCronTable(tableName, "timestamp")
	if err != nil {
		t.Errorf("Error getting type exists in table, %v", err)
	}

	if exists != postgres.TimestampToString(0) {
		t.Errorf("Value returned should be 0 but it is %v", err)
	}

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete governance_event_test table: %v", err)
	}
}

func TestTimestampOfLastEventForCron(t *testing.T) {
	tableName := "cron_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(persister, tableName)

	// There should be no rows in the table. In this case lastCronTimestamp should insert a nil value.
	timestamp, err := persister.lastCronTimestampFromTable(tableName)
	if err != nil {
		t.Errorf("Error retrieving from cron table: %v", err)
	}
	if timestamp != int64(0) {
		t.Errorf("Timestamp should be 0 but it is %v", timestamp)
	}

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete governance_event_test table: %v", err)
	}
}

func TestUpdateTimestampForCron(t *testing.T) {
	tableName := "cron_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	// defer deleteTestTable(persister, tableName)

	newTimestamp := int64(1212121212)
	err = persister.updateCronTimestampInTable(newTimestamp, tableName)
	if err != nil {
		t.Errorf("Error updating cron table, %v", err)
	}

	// retrieve timestamp to make sure it was updated
	timestamp, err := persister.lastCronTimestampFromTable(tableName)
	if err != nil {
		t.Errorf("Error retrieving from cron table: %v", err)
	}

	if timestamp != newTimestamp {
		t.Errorf("Timestamp should be %v but it is %v", newTimestamp, timestamp)
	}

	// Update again, make sure it works NOTE THIS DOESN'T WORK!
	newTimestamp2 := int64(121212121233)
	err = persister.updateCronTimestampInTable(newTimestamp2, tableName)
	if err != nil {
		t.Errorf("Error updating cron table, %v", err)
	}

	// retrieve timestamp to make sure it was updated
	timestamp2, err := persister.lastCronTimestampFromTable(tableName)
	if err != nil {
		t.Errorf("Error retrieving from cron table: %v", err)
	}

	if timestamp2 != newTimestamp2 {
		t.Errorf("Timestamp should be %v but it is %v", newTimestamp2, timestamp2)
	}

	err = deleteTestTable(persister, tableName)
	if err != nil {
		t.Errorf("Could not delete governance_event_test table: %v", err)
	}
}
