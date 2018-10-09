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
	"time"
)

const (
	postgresPort     = 5432
	postgresDBName   = "civil_crawler"
	postgresUser     = "docker"
	postgresPswd     = "docker"
	postgresHost     = "localhost"
	govTestTableName = "governance_event_test"
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

func deleteTestTable(t *testing.T, persister *PostgresPersister, tableName string) {
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
		t.Errorf("Couldn't delete test table %s: %v", tableName, err)
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
	ownerAddr := common.HexToAddress(address2)
	ownerAddresses := []common.Address{common.HexToAddress(address2), common.HexToAddress(address3)}
	contributorAddresses := ownerAddresses
	appExpiry := big.NewInt(232424242)
	unstakedDeposit := new(big.Int)
	unstakedDeposit.SetString("100000000000000000000", 10)
	challengeID := big.NewInt(10)
	testListing := model.NewListing("test_listing", contractAddress, true,
		model.GovernanceStateAppWhitelisted, "url_string", "charterURI", ownerAddr, ownerAddresses,
		contributorAddresses, 1257894000, 1257894000, 1257894000, 1257894000, appExpiry, unstakedDeposit,
		challengeID)
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
	defer deleteTestTable(t, persister, tableName)
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
}

// TestListingByAddress tests that the query we are using to get Listing works
func TestListingByAddress(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)
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

}

// TestListingByAddress tests that the query we are using to get Listing works
func TestListingByAddressDoesNotExist(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)
	// create fake listing in listing_test
	modelListing, _ := setupSampleListing()

	// save to test table
	err = persister.createListingForTable(modelListing, tableName)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}
	bogusAddress := common.Address{}
	// retrieve from test table
	nullListing, err := persister.listingByAddressFromTable(bogusAddress, tableName)

	if err != nil {
		t.Errorf("Wasn't able to get listing from postgres table: %v", err)
	}
	if nullListing != nil {
		t.Errorf("Shouldn't have retrieved a listing at all %v", err)
	}

}

// TestDBListingToModelListing tests that the db listing can be properly converted to model listing
func TestDBListingToModelListing(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

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

}

// Test retrieving multiple listings
func TestListingsByAddresses(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

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

}

//shuffle function
func shuffleListingAddresses(slice []common.Address) []common.Address {
	for i := range slice {
		j := mathrand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
	return slice
}

// Test that orered function for IN query returns results in order:
func TestListingByAddressesInOrder(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

	numListings := 10
	modelListings, modelListingAddresses := setupSampleListings(numListings)
	// shuffle array
	modelListingAddresses = shuffleListingAddresses(modelListingAddresses)

	// Insert
	for _, list := range modelListings {
		err := persister.createListingForTable(list, tableName)
		if err != nil {
			t.Errorf("Couldn't save listing to table: %v", err)
		}
	}
	//retrieve listings
	dbListings, err := persister.listingsByAddressesFromTableInOrder(modelListingAddresses, tableName)
	if err != nil {
		t.Errorf("Error retrieving multiple listings: %v", err)
	}
	for i, listingAddress := range modelListingAddresses {
		modelListingAddress := dbListings[i].ContractAddress().Hex()
		if modelListingAddress != listingAddress.Hex() {
			t.Errorf("Order of addresses don't match up for index %v", i)
		}
	}

}

// There are nil addresses that slip through
func TestListingByAddressesInOrderAddressNotFound(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

	numListings := 10
	modelListings, modelListingAddresses := setupSampleListings(numListings)

	// Insert
	for _, list := range modelListings {
		err := persister.createListingForTable(list, tableName)
		if err != nil {
			t.Errorf("Couldn't save listing to table: %v", err)
		}
	}

	// Add nil listing
	modelListingAddresses = append(modelListingAddresses, common.Address{})

	//retrieve listings
	dbListings, err := persister.listingsByAddressesFromTableInOrder(modelListingAddresses, tableName)
	if err != nil {
		t.Errorf("Error retrieving multiple listings: %v", err)
	}

	for i, listingAddress := range modelListingAddresses {
		if dbListings[i] != nil {
			modelListingAddress := dbListings[i].ContractAddress().Hex()
			if modelListingAddress != listingAddress.Hex() {
				t.Errorf("Order of addresses don't match up for index %v", i)
			}
		}

	}

}

// TestUpdateListing tests that updating the Listing works
func TestUpdateListing(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

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

}

// TestDeleteListing tests that the deleting the Listing works
func TestDeleteListing(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

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
	defer deleteTestTable(t, persister, tableName)

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
}

// TestContentRevision tests that a content revision can be retrieved
func TestContentRevision(t *testing.T) {
	tableName := "content_revision_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

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

}

// TestDBCRToModelCR tests that the db listing can be properly converted to model listing
func TestDBCRToModelCR(t *testing.T) {
	tableName := "content_revision_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

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
}

// TestContentRevision tests that multiple content revisions can be retrieved
func TestContentRevisions(t *testing.T) {
	tableName := "content_revision_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

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
	defer deleteTestTable(t, persister, tableName)

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

func setupSampleGovernanceEvent(randListing bool) (*model.GovernanceEvent, common.Address, string, common.Hash) {
	var listingAddr common.Address
	address2, _ := randomHex(32)
	if randListing {
		address1, _ := randomHex(32)
		listingAddr = common.HexToAddress(address1)
	} else {
		// keep listingAddress constant
		listingAddr = common.HexToAddress("0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d")
	}

	senderAddress := common.HexToAddress(address2)
	metadata := model.Metadata{}
	governanceEventType := "governanceeventtypehere"
	creationDateTs := crawlerutils.CurrentEpochSecsInInt64()
	lastUpdatedDateTs := crawlerutils.CurrentEpochSecsInInt64() + 1
	eventHash, _ := randomHex(5)
	blockNumber := uint64(88888)
	tHash, _ := randomHex(5)
	txHash := common.HexToHash(tHash)
	txIndex := uint(4)
	blockHash := common.Hash{}
	index := uint(2)
	testGovernanceEvent := model.NewGovernanceEvent(listingAddr, senderAddress, metadata, governanceEventType,
		creationDateTs, lastUpdatedDateTs, eventHash, blockNumber, txHash, txIndex, blockHash, index)
	return testGovernanceEvent, listingAddr, eventHash, txHash
}

func setupGovEventTable(t *testing.T) *PostgresPersister {
	persister, err := setupTestTable(govTestTableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	return persister
}

func createAndSaveTestGovEvent(t *testing.T, persister *PostgresPersister, randListing bool) (*model.GovernanceEvent, common.Address, string, common.Hash) {
	// sample govEvent
	modelGovernanceEvent, listingAddr, eventHash, txHash := setupSampleGovernanceEvent(false)

	// insert to table
	err := persister.createGovernanceEventInTable(modelGovernanceEvent, govTestTableName)
	if err != nil {
		t.Errorf("error saving GovernanceEvent: %v", err)
	}
	return modelGovernanceEvent, listingAddr, eventHash, txHash
}

/*
All tests for governance_event table:
*/

// TestCreateGovernanceEvent tests that a GovernanceEvent is created
func TestCreateGovernanceEvent(t *testing.T) {
	persister := setupGovEventTable(t)
	defer deleteTestTable(t, persister, govTestTableName)

	_, _, _, _ = createAndSaveTestGovEvent(t, persister, false)

	// check row is there
	var numRowsb int
	err := persister.db.QueryRow(`SELECT COUNT(*) FROM governance_event_test`).Scan(&numRowsb)
	if err != nil {
		t.Errorf("Problem getting count from table: %v", err)
	}
	if numRowsb != 1 {
		t.Errorf("Number of rows in table should be 0 but is: %v", numRowsb)
	}
}

// TestGovernanceEventsByListingAddress tests that a GovernanceEvent is properly retrieved
func TestGovernanceEventsByListingAddress(t *testing.T) {
	persister := setupGovEventTable(t)
	defer deleteTestTable(t, persister, govTestTableName)

	_, listingAddr, _, _ := createAndSaveTestGovEvent(t, persister, false)

	// retrieve from table
	dbGovEvents, err := persister.governanceEventsByListingAddressFromTable(listingAddr, govTestTableName)
	if err != nil {
		t.Errorf("Wasn't able to get governance event from postgres table: %v", err)
	}

	if len(dbGovEvents) != 1 {
		t.Errorf("length of governance events should be 1 but is: %v", len(dbGovEvents))
	}

}

// TestDBGovEventToModelGovEvent tests that the db listing can be properly converted to model listing
func TestDBGovEventToModelGovEvent(t *testing.T) {
	persister := setupGovEventTable(t)
	defer deleteTestTable(t, persister, govTestTableName)

	modelGovernanceEvent, listingAddr, _, _ := createAndSaveTestGovEvent(t, persister, false)

	// retrieve from table
	dbGovEvents, err := persister.governanceEventsByListingAddressFromTable(listingAddr, govTestTableName)
	if err != nil {
		t.Errorf("Wasn't able to get governance event from postgres table: %v", err)
	}
	modelGovEventFromDB := dbGovEvents[0]

	// do a deep equal
	// check that retrieved fields match with inserted listing
	if !reflect.DeepEqual(modelGovernanceEvent, modelGovEventFromDB) {
		t.Errorf("listing from DB: %v, doesn't match inserted listing: %v", modelGovEventFromDB, modelGovernanceEvent)
	}

}

// TODO(IS): test update gov event -- test update that's not by listing

// TestDeleteGovernanceEvent tests that the deleting the Governance Event works
// TODO(IS) : this will delete more than you want. need to put some kind of hash for the gov event.
func TestDeleteGovernanceEvent(t *testing.T) {
	persister := setupGovEventTable(t)
	defer deleteTestTable(t, persister, govTestTableName)

	modelGovernanceEvent, _, _, _ := createAndSaveTestGovEvent(t, persister, false)

	var numRowsb int
	err := persister.db.QueryRow(`SELECT COUNT(*) FROM governance_event_test`).Scan(&numRowsb)
	if err != nil {
		t.Errorf("Problem getting count from table: %v", err)
	}
	if numRowsb != 1 {
		t.Errorf("Number of rows in table should be 1 but is: %v", numRowsb)
	}

	//delete rows
	err = persister.deleteGovernanceEventFromTable(modelGovernanceEvent, govTestTableName)
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

// TestGovEventsByCriteria tests GovernanceEvent by criteria query
func TestGovEventsByCriteria(t *testing.T) {
	persister := setupGovEventTable(t)
	defer deleteTestTable(t, persister, govTestTableName)
	var listingAddr common.Address
	var timeMiddle int64
	var timeStart int64
	var modelGovernanceEvent *model.GovernanceEvent
	// create some governance events w constant listing address and save them to DB
	for i := 1; i <= 30; i++ {
		// TODO: just set timestamp for event bc there is still a probability these times won't be what you think.
		if i < 20 {
			timeStart = crawlerutils.CurrentEpochSecsInInt64()
		}
		if i == 20 {
			time.Sleep(1 * time.Second)
			timeMiddle = crawlerutils.CurrentEpochSecsInInt64()
		}
		modelGovernanceEvent, listingAddr, _, _ = createAndSaveTestGovEvent(t, persister, true)
	}

	govEvents, err := persister.governanceEventsByCriteriaFromTable(&model.GovernanceEventCriteria{
		ListingAddress: listingAddr.Hex(),
		Count:          1,
	}, govTestTableName)

	if err != nil {
		t.Errorf("Wasn't able to get governance events from postgres table: %v", err)
	}

	if len(govEvents) != 1 {
		t.Errorf("Should have only retrieved one governance event but got %v", len(govEvents))
	}

	if modelGovernanceEvent.ListingAddress() != listingAddr {
		t.Errorf("Listing address is %v but should be %v ", modelGovernanceEvent.ListingAddress().Hex(), listingAddr.Hex())
	}

	govEvents, err = persister.governanceEventsByCriteriaFromTable(&model.GovernanceEventCriteria{
		ListingAddress: listingAddr.Hex(),
		CreatedFromTs:  timeStart,
	}, govTestTableName)

	if err != nil {
		t.Errorf("Wasn't able to get governance events from postgres table: %v", err)
	}

	if len(govEvents) != 11 {
		t.Errorf("Should have retrieved 11 governance events but only got %v", len(govEvents))
	}

	govEvents, err = persister.governanceEventsByCriteriaFromTable(&model.GovernanceEventCriteria{
		ListingAddress:  listingAddr.Hex(),
		CreatedBeforeTs: timeMiddle,
	}, govTestTableName)

	if err != nil {
		t.Errorf("Wasn't able to get governance events from postgres table: %v", err)
	}
	if len(govEvents) != 19 {
		t.Errorf("Should have retrieved 19 governance events but only got %v", len(govEvents))
	}

}

// TestGovEventsByCriteria tests GovernanceEvent by criteria query
func TestGovEventsByTxHash(t *testing.T) {
	tableName := "governance_event_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

	// sample governanceEvent
	modelGovernanceEvent, _, _, txHash := setupSampleGovernanceEvent(true)
	modelGovernanceEvent2, _, _, _ := setupSampleGovernanceEvent(true)

	// insert to table
	err = persister.createGovernanceEventInTable(modelGovernanceEvent, tableName)
	if err != nil {
		t.Errorf("error saving GovernanceEvent: %v", err)
	}

	err = persister.createGovernanceEventInTable(modelGovernanceEvent2, tableName)
	if err != nil {
		t.Errorf("error saving GovernanceEvent: %v", err)
	}

	govEvents, err := persister.governanceEventsByTxHashFromTable(txHash, tableName)

	if err != nil {
		t.Errorf("Wasn't able to get governance event from postgres table: %v", err)
	}

	// confirm txHash from query result
	if len(govEvents) != 1 {
		t.Errorf("Should have only received 1 governance event from txHash query but received %v", len(govEvents))
	}

	blockData := govEvents[0].BlockData()
	if blockData.TxHash() != txHash.Hex() {
		t.Errorf("Hash should be %v but is %v", txHash, blockData.TxHash())
	}

}

func TestChallengeQuery(t *testing.T) {
	tableName := "governance_event_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

	challengeIDs := []int{1, 2, 3}
	query := persister.govEventsByChallengeIDQuery(govTestTableName, challengeIDs)
	correctQuery := "SELECT listing_address, sender_address, metadata, gov_event_type, creation_date, last_updated, event_hash, block_data FROM governance_event_test WHERE gov_event_type='Challenge' AND metadata ->>'ChallengeID' IN ('1','2','3');"
	if query != correctQuery {
		t.Errorf("ChallengeID query for governance_events is not correct, should be %v, but is %v", query, correctQuery)
	}
	challengeIDs2 := []int{1}
	query2 := persister.govEventsByChallengeIDQuery(govTestTableName, challengeIDs2)
	correctQuery2 := "SELECT listing_address, sender_address, metadata, gov_event_type, creation_date, last_updated, event_hash, block_data FROM governance_event_test WHERE gov_event_type='Challenge' AND metadata ->>'ChallengeID' IN ('1');"
	if query2 != correctQuery2 {
		t.Errorf("ChallengeID query for governance_events is not correct, should be %v, but is %v", query2, correctQuery2)
	}
}

func setupSampleGovernanceChallengeEvent(randListing bool) (*model.GovernanceEvent, int) {
	var listingAddr common.Address
	address2, _ := randomHex(32)
	if randListing {
		address1, _ := randomHex(32)
		listingAddr = common.HexToAddress(address1)
	} else {
		// keep listingAddress constant
		listingAddr = common.HexToAddress("0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d")
	}
	challengeID := mathrand.Intn(100)
	senderAddress := common.HexToAddress(address2)
	metadata := model.Metadata{
		"Data":           "ipfs://QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
		"Challenger":     "0xe562d05067eded7a722ed73b9ebfaaedc60970a1",
		"ChallengeID":    challengeID,
		"CommitEndDate":  1527266803,
		"RevealEndDate":  1527268603,
		"ListingAddress": "0xa28ca9c9a7979c33cf73d3f406cd765e2d68c965"}
	governanceEventType := "Challenge"
	creationDateTs := crawlerutils.CurrentEpochSecsInInt64()
	lastUpdatedDateTs := crawlerutils.CurrentEpochSecsInInt64() + 1
	eventHash, _ := randomHex(5)
	blockNumber := uint64(88888)
	tHash, _ := randomHex(5)
	txHash := common.HexToHash(tHash)
	txIndex := uint(4)
	blockHash := common.Hash{}
	index := uint(2)
	testGovernanceEvent := model.NewGovernanceEvent(listingAddr, senderAddress, metadata, governanceEventType,
		creationDateTs, lastUpdatedDateTs, eventHash, blockNumber, txHash, txIndex, blockHash, index)
	return testGovernanceEvent, challengeID
}

func TestChallengesByID(t *testing.T) {
	tableName := "governance_event_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

	challengeEvent, challengeID := setupSampleGovernanceChallengeEvent(true)
	// insert to table
	err = persister.createGovernanceEventInTable(challengeEvent, tableName)
	if err != nil {
		t.Errorf("error saving GovernanceEvent: %v", err)
	}

	// Try with just one ID
	challengeIDs := []int{challengeID}
	govEvents, err := persister.govEventsByChallengeIDFromTable(challengeIDs, tableName)

	if len(govEvents) != 1 {
		t.Errorf("Wrong number of events returned: %v. Should be 1.", len(govEvents))
	}
	govEvent := govEvents[0]
	if int(govEvent.Metadata()["ChallengeID"].(float64)) != challengeID {
		t.Errorf("ChallengeID is %v but it should be %v", int(govEvent.Metadata()["ChallengeID"].(float64)),
			challengeID)
	}

	// Multiple IDs
	challengeIDs = []int{}
	for i := 0; i < 6; i++ {
		challengeEvent, challengeID := setupSampleGovernanceChallengeEvent(true)
		challengeIDs = append(challengeIDs, challengeID)
		// insert to table
		err = persister.createGovernanceEventInTable(challengeEvent, tableName)
		if err != nil {
			t.Errorf("error saving GovernanceEvent: %v", err)
		}
	}

	govEvents, err = persister.govEventsByChallengeIDFromTable(challengeIDs, tableName)
	if len(govEvents) != 6 {
		t.Errorf("Wrong number of events returned: %v. Should be 6.", len(govEvents))
	}

	// Will be in order that you put it in DB so you can do this
	for i, govEvent := range govEvents {
		if int(govEvent.Metadata()["ChallengeID"].(float64)) != challengeIDs[i] {
			t.Errorf("ChallengeID from DB is %v but should be %v",
				int(govEvent.Metadata()["ChallengeID"].(float64)), challengeID)
		}
	}

}

func TestChallengesByIDOrder(t *testing.T) {
	// check that challenges are in order
	tableName := "governance_event_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

	challengeEvent, challengeID := setupSampleGovernanceChallengeEvent(true)
	// insert to table
	err = persister.createGovernanceEventInTable(challengeEvent, tableName)
	if err != nil {
		t.Errorf("error saving GovernanceEvent: %v", err)
	}

	// Try with just one ID
	challengeIDs := []int{challengeID}
	govEvents, err := persister.govEventsByChallengeIDFromTable(challengeIDs, tableName)

	if len(govEvents) != 1 {
		t.Errorf("Wrong number of events returned: %v. Should be 1.", len(govEvents))
	}
	govEvent := govEvents[0]
	if int(govEvent.Metadata()["ChallengeID"].(float64)) != challengeID {
		t.Errorf("ChallengeID is %v but it should be %v", int(govEvent.Metadata()["ChallengeID"].(float64)),
			challengeID)
	}

	// Multiple IDs
	challengeIDs = []int{}
	for i := 0; i < 6; i++ {
		challengeEvent, challengeID := setupSampleGovernanceChallengeEvent(true)
		challengeIDs = append(challengeIDs, challengeID)
		// insert to table
		err = persister.createGovernanceEventInTable(challengeEvent, tableName)
		if err != nil {
			t.Errorf("error saving GovernanceEvent: %v", err)
		}
	}

	challengeIDs = shuffleInts(challengeIDs)
	govEvents, err = persister.govEventsByChallengeIDFromTable(challengeIDs, tableName)
	if len(govEvents) != 6 {
		t.Errorf("Wrong number of events returned: %v. Should be 6.", len(govEvents))
	}

	for i, govEvent := range govEvents {
		if int(govEvent.Metadata()["ChallengeID"].(float64)) != challengeIDs[i] {
			t.Errorf("ChallengeID from DB is %v but should be %v",
				int(govEvent.Metadata()["ChallengeID"].(float64)), challengeIDs[i])
		}
	}

	// IDs that don't exist
	nilID := 300
	challengeIDs = append(challengeIDs, nilID)
	challengeIDs = shuffleInts(challengeIDs)
	govEvents, err = persister.govEventsByChallengeIDFromTable(challengeIDs, tableName)
	if len(govEvents) != 7 {
		t.Errorf("Wrong number of events returned: %v. Should be 6.", len(govEvents))
	}
	// emptyGovEvent := model.GovernanceEvent{}
	for i, govEvent := range govEvents {
		if govEvent != nil {
			if int(govEvent.Metadata()["ChallengeID"].(float64)) != challengeIDs[i] {
				t.Errorf("ChallengeID from DB is %v but should be %v",
					int(govEvent.Metadata()["ChallengeID"].(float64)), challengeIDs[i])
			}
		} else {
			if challengeIDs[i] != nilID {
				t.Errorf("This challenge should be null but it is %v", govEvent)
			}
		}
	}

}

//shuffle function
func shuffleInts(slice []int) []int {
	for i := range slice {
		j := mathrand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
	return slice
}

func TestNilChallenges(t *testing.T) {
	// check that challenges are in order
	tableName := "governance_event_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

	challengeEvent, _ := setupSampleGovernanceChallengeEvent(true)
	// insert to table
	err = persister.createGovernanceEventInTable(challengeEvent, tableName)
	if err != nil {
		t.Errorf("error saving GovernanceEvent: %v", err)
	}

	// Try with just one ID
	challengeIDs := []int{0}
	govEvents, err := persister.govEventsByChallengeIDFromTable(challengeIDs, tableName)

	if len(govEvents) != 1 {
		t.Errorf("Should have only returned 1 listing")
	}

	for _, event := range govEvents {
		if event != nil {
			t.Errorf("Should have gotten nil event but got %v", event)
		}

	}

	// Try with just one ID
	challengeIDs = []int{0, 300}
	govEvents, err = persister.govEventsByChallengeIDFromTable(challengeIDs, tableName)

	if len(govEvents) != 2 {
		t.Errorf("Should have only returned 1 listing")
	}

	for _, event := range govEvents {
		if event != nil {
			t.Errorf("Should have gotten nil event but got %v", event)
		}

	}

}

// also test that you can create multiple queries and cnxn pools are working

/*
All tests for cron table:
*/

func TestTypeExistsInCronTable(t *testing.T) {
	tableName := "cron_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

	// insert something
	queryString := fmt.Sprintf("INSERT INTO %s(data_persisted, data_type) VALUES(0, 'timestamp')", tableName)
	_, err = persister.db.Exec(queryString)
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

}

func TestTimestampOfLastEventForCron(t *testing.T) {
	tableName := "cron_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

	// There should be no rows in the table. In this case lastCronTimestamp should insert a nil value.
	timestamp, err := persister.lastCronTimestampFromTable(tableName)
	if err != nil {
		t.Errorf("Error retrieving from cron table: %v", err)
	}
	if timestamp != int64(0) {
		t.Errorf("Timestamp should be 0 but it is %v", timestamp)
	}

}

func TestUpdateTimestampForCron(t *testing.T) {
	tableName := "cron_test"
	persister, err := setupTestTable(tableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

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
}
