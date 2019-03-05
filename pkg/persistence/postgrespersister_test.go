// +build integration

// This is an integration test file for postgrespersister. Postgres needs to be running.
// Run this using go test -tags=integration
// Run benchmark test using go test -tags=integration -bench=.
package persistence

import (
	"bytes"
	"fmt"
	"math/big"
	mathrand "math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/persistence/postgres"

	cpersist "github.com/joincivil/go-common/pkg/persistence"
	cstrings "github.com/joincivil/go-common/pkg/strings"
	ctime "github.com/joincivil/go-common/pkg/time"
)

const (
	postgresPort               = 5432
	postgresDBName             = "civil_crawler"
	postgresUser               = "docker"
	postgresPswd               = "docker"
	postgresHost               = "localhost"
	govTestTableName           = "governance_event_test"
	challengeTestTableName     = "challenge_test"
	pollTestTableName          = "poll_test"
	appealTestTableName        = "appeal_test"
	tokenTransferTestTableName = "token_transfer_test"

	testAddress = "0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d"
)

func setupDBConnection() (*PostgresPersister, error) {
	postgresPersister, err := NewPostgresPersister(postgresHost, postgresPort, postgresUser, postgresPswd, postgresDBName)
	if err != nil {
		return nil, fmt.Errorf("Error setting up new persister: err: %v", err)
	}
	err = postgresPersister.CreateTables()
	if err != nil {
		return nil, fmt.Errorf("Error setting up tables in db: %v", err)
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
	case "challenge_test":
		queryString = postgres.CreateChallengeTableQueryString(tableName)
	case "poll_test":
		queryString = postgres.CreatePollTableQueryString(tableName)
	case "appeal_test":
		queryString = postgres.CreateAppealTableQueryString(tableName)
	case "token_transfer_test":
		queryString = postgres.CreateTokenTransferTableQueryString(tableName)
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
	case "challenge_test":
		_, err = persister.db.Query("DROP TABLE challenge_test;")
	case "poll_test":
		_, err = persister.db.Query("DROP TABLE poll_test;")
	case "appeal_test":
		_, err = persister.db.Query("DROP TABLE appeal_test;")
	case "token_transfer_test":
		_, err = persister.db.Query("DROP TABLE token_transfer_test;")
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
	address1, _ := cstrings.RandomHexStr(32)
	address2, _ := cstrings.RandomHexStr(32)
	address3, _ := cstrings.RandomHexStr(32)
	contractAddress := common.HexToAddress(address1)
	ownerAddr := common.HexToAddress(address2)
	ownerAddresses := []common.Address{common.HexToAddress(address2), common.HexToAddress(address3)}
	contributorAddresses := ownerAddresses
	appExpiry := big.NewInt(232424242)
	unstakedDeposit := new(big.Int)
	unstakedDeposit.SetString("100000000000000000000", 10)
	challengeID := big.NewInt(10)

	signature, _ := cstrings.RandomHexStr(32)
	authorAddr, _ := cstrings.RandomHexStr(32)

	contentHashHex, _ := cstrings.RandomHexStr(32)
	contentHashBytes := []byte(contentHashHex)
	fixedContentHash := [32]byte{}
	copy(fixedContentHash[:], contentHashBytes)

	charter := model.NewCharter(&model.CharterParams{
		URI:         "/charter/uri",
		ContentID:   big.NewInt(0),
		RevisionID:  big.NewInt(30),
		Signature:   []byte(signature),
		Author:      common.HexToAddress(authorAddr),
		ContentHash: fixedContentHash,
		Timestamp:   big.NewInt(12345678),
	})
	testListingParams := &model.NewListingParams{
		Name:                 "test_listing",
		ContractAddress:      contractAddress,
		Whitelisted:          true,
		LastState:            model.GovernanceStateAppWhitelisted,
		URL:                  "url_string",
		Charter:              charter,
		Owner:                ownerAddr,
		OwnerAddresses:       ownerAddresses,
		ContributorAddresses: contributorAddresses,
		CreatedDateTs:        1257894000,
		ApplicationDateTs:    1257894000,
		ApprovalDateTs:       1257894000,
		LastUpdatedDateTs:    1257894000,
		AppExpiry:            appExpiry,
		UnstakedDeposit:      unstakedDeposit,
		ChallengeID:          challengeID,
	}
	testListing := model.NewListing(testListingParams)
	return testListing, contractAddress
}

func setupSampleListingUnchallenged() (*model.Listing, common.Address) {
	address1, _ := cstrings.RandomHexStr(32)
	address2, _ := cstrings.RandomHexStr(32)
	address3, _ := cstrings.RandomHexStr(32)
	contractAddress := common.HexToAddress(address1)
	ownerAddr := common.HexToAddress(address2)
	ownerAddresses := []common.Address{common.HexToAddress(address2), common.HexToAddress(address3)}
	contributorAddresses := ownerAddresses
	appExpiry := big.NewInt(232424242)
	unstakedDeposit := new(big.Int)
	unstakedDeposit.SetString("100000000000000000000", 10)

	signature, _ := cstrings.RandomHexStr(32)
	authorAddr, _ := cstrings.RandomHexStr(32)

	contentHashHex, _ := cstrings.RandomHexStr(32)
	contentHashBytes := []byte(contentHashHex)
	fixedContentHash := [32]byte{}
	copy(fixedContentHash[:], contentHashBytes)

	charter := model.NewCharter(&model.CharterParams{
		URI:         "/charter/uri",
		ContentID:   big.NewInt(0),
		RevisionID:  big.NewInt(30),
		Signature:   []byte(signature),
		Author:      common.HexToAddress(authorAddr),
		ContentHash: fixedContentHash,
		Timestamp:   big.NewInt(12345678),
	})
	testListingParams := &model.NewListingParams{
		Name:                 "test_listing",
		ContractAddress:      contractAddress,
		Whitelisted:          false,
		LastState:            model.GovernanceStateAppWhitelisted,
		URL:                  "url_string",
		Charter:              charter,
		Owner:                ownerAddr,
		OwnerAddresses:       ownerAddresses,
		ContributorAddresses: contributorAddresses,
		CreatedDateTs:        1257894000,
		ApplicationDateTs:    1257894000,
		ApprovalDateTs:       1257894000,
		LastUpdatedDateTs:    1257894000,
		AppExpiry:            appExpiry,
		UnstakedDeposit:      unstakedDeposit,
	}
	testListing := model.NewListing(testListingParams)
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
	defer persister.Close()
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
	if err != nil {
		t.Errorf("Error querying row: err: %v", err)
	}
	if numRowsb != 1 {
		t.Errorf("Number of rows in table should be 0 but is: %v", numRowsb)
	}
}

// TestListingByAddress tests that the query we are using to get Listing works
func TestListingByAddress(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	defer persister.Close()
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

// TestListingCharterByAddress tests that the query we are using to get Listing works
func TestListingCharterByAddress(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	defer persister.Close()
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
	dbListing, err := persister.listingByAddressFromTable(modelListingAddress, tableName)

	if err != nil {
		t.Errorf("Wasn't able to get listing from postgres table: %v", err)
	}

	charter := modelListing.Charter()
	dbCharter := dbListing.Charter()

	if charter.URI() != dbCharter.URI() {
		t.Errorf("Should have had same URI")
	}
	if charter.ContentID().Cmp(dbCharter.ContentID()) != 0 {
		t.Errorf("Should have had same content ID")
	}
	if charter.RevisionID().Cmp(dbCharter.RevisionID()) != 0 {
		t.Errorf("Should have had same revision ID")
	}
	if !bytes.Equal(charter.Signature(), dbCharter.Signature()) {
		t.Errorf("Should have had same signature")
	}
	if charter.Author().Hex() != dbCharter.Author().Hex() {
		t.Errorf("Should have had same author addr")
	}
	chart1Hash := charter.ContentHash()
	chart2Hash := dbCharter.ContentHash()
	if !bytes.Equal(chart1Hash[:], chart2Hash[:]) {
		t.Errorf("Should have had same content hash")
	}
	if charter.Timestamp().Cmp(dbCharter.Timestamp()) != 0 {
		t.Errorf("Should have had same timestamp")
	}
}

// TestListingByAddress tests that the query we are using to get Listing works
func TestListingByAddressDoesNotExist(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	defer persister.Close()
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

	if err != cpersist.ErrPersisterNoResults {
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
	defer persister.Close()
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
	defer persister.Close()
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
	dbListings, err := persister.listingsByAddressesFromTableInOrder(modelListingAddresses, tableName)
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
	defer persister.Close()
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

	// Empty input results
	_, err = persister.listingsByAddressesFromTableInOrder([]common.Address{}, tableName)
	if err == nil {
		t.Errorf("Should have received an error on listing addresses")
	}
	if err != cpersist.ErrPersisterNoResults {
		t.Errorf("Should have received an ErrPersisterNoResults on empty listing addresses: err: %v", err)
	}

	// What happens with a non-existent listing address
	dbListings, err = persister.listingsByAddressesFromTableInOrder(
		[]common.Address{common.HexToAddress("0x39eD84CE90Bc48DD76C4760DD0F90997Ba274F9d")},
		tableName,
	)
	if err != nil {
		t.Errorf("Should have received an error on bad listing address")
	}
	if len(dbListings) != 1 {
		t.Errorf("Should have received 1 item in the listings from DB")
	}
	if dbListings[0] != nil {
		t.Errorf("Should have received a nil value for an unfound listing")
	}

}

// There are nil addresses that slip through
func TestListingByAddressesInOrderAddressNotFound(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	defer persister.Close()
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

func TestNilResultsListing(t *testing.T) {
	// Query for listings that don't exist and make sure expected behavior is returned
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)
	randHex, _ := cstrings.RandomHexStr(32)
	randHex2, _ := cstrings.RandomHexStr(32)
	testAddress1 := common.HexToAddress(randHex)
	testAddress2 := common.HexToAddress(randHex2)

	listingRes, err := persister.listingByAddressFromTable(testAddress1, tableName)
	if err != cpersist.ErrPersisterNoResults {
		t.Errorf("Error message should be ErrPersisterNoResults, but is %v", err)
	}
	if listingRes != nil {
		t.Errorf("Listing response should be nil")
	}

	// This does not use cpersist.ErrPersisterNoResults
	listingsRes, err := persister.listingsByAddressesFromTableInOrder([]common.Address{testAddress1, testAddress2},
		tableName)
	if err != nil {
		t.Errorf("Error should be nil but is %v", err)
	}

	for _, listing := range listingsRes {
		if listing != nil {
			t.Errorf("Listing should be nil but it is %v", listing)
		}
	}

}

// TestUpdateListing tests that updating the Listing works
func TestUpdateListing(t *testing.T) {
	tableName := "listing_test"
	persister, err := setupTestTable(tableName)
	defer persister.Close()
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
	defer persister.Close()
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

func TestListingsByCriteria(t *testing.T) {
	tableName := "listing_test"
	joinTableName := "challenge_test"
	persister, err := setupTestTable(tableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}

	_, err = setupTestTable(joinTableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

	// whitelisted modellisting with active challenge
	modelListingWhitelistedActiveChallenge, _ := setupSampleListing()
	challenge := setupChallengeByChallengeID(10, false)
	// Create another modelListing that was rejected after challenge succeeded
	modelListingRejected, _ := setupSampleListing()
	modelListingRejected.SetWhitelisted(false)
	modelListingRejected.SetChallengeID(big.NewInt(0))
	modelListingRejected.SetAppExpiry(big.NewInt(0))
	// modelListing that is still in application phase, not whitelisted
	modelListingApplicationPhase, _ := setupSampleListingUnchallenged()
	appExpiry := big.NewInt(ctime.CurrentEpochSecsInInt64() + 100)
	modelListingApplicationPhase.SetAppExpiry(appExpiry)
	// modellisting that is whitelisted, never had a challenge
	modelListingWhitelisted, _ := setupSampleListingUnchallenged()
	modelListingWhitelisted.SetWhitelisted(true)
	// Create another modelListing where challenge failed
	modelListingNoChallenge, _ := setupSampleListing()
	modelListingNoChallenge.SetChallengeID(big.NewInt(0))
	// modelListing that passed application phase but not challenged so ready to be whitelisted
	modelListingPastApplicationPhase, _ := setupSampleListingUnchallenged()
	appExpiry = big.NewInt(ctime.CurrentEpochSecsInInt64() - 100)
	modelListingPastApplicationPhase.SetAppExpiry(appExpiry)

	// save to test table
	err = persister.createListingForTable(modelListingWhitelistedActiveChallenge, tableName)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}
	err = persister.createListingForTable(modelListingRejected, tableName)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}
	err = persister.createListingForTable(modelListingApplicationPhase, tableName)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}
	err = persister.createListingForTable(modelListingWhitelisted, tableName)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}
	err = persister.createListingForTable(modelListingNoChallenge, tableName)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}
	err = persister.createListingForTable(modelListingPastApplicationPhase, tableName)
	if err != nil {
		t.Errorf("error saving listing: %v", err)
	}
	err = persister.createChallengeInTable(challenge, joinTableName)
	if err != nil {
		t.Errorf("error saving challenge: %v", err)
	}

	listingsFromDB, err := persister.listingsByCriteriaFromTable(&model.ListingCriteria{
		RejectedOnly: true,
	}, tableName, joinTableName)
	if err != nil {
		t.Errorf("Error getting listing by criteria: %v", err)
	}
	if len(listingsFromDB) != 1 {
		t.Errorf("Only one listing should have been returned but there are %v", len(listingsFromDB))
	}
	if listingsFromDB[0].Whitelisted() {
		t.Error("Listing should not be whitelisted.")
	}
	if !reflect.DeepEqual(listingsFromDB[0].ChallengeID(), big.NewInt(0)) {
		t.Error("Listing should have challengeID = 0")
	}

	listingsFromDB, err = persister.listingsByCriteriaFromTable(&model.ListingCriteria{
		Offset: 0,
		Count:  10,
	}, tableName, joinTableName)
	if err != nil {
		t.Errorf("Error getting listing by criteria: %v", err)
	}
	if len(listingsFromDB) != 6 {
		t.Error("Number of listings should be 6")
	}
	if listingsFromDB[0].ContractAddress().Hex() != modelListingWhitelistedActiveChallenge.ContractAddress().Hex() {
		t.Error("First listing is incorrect, ordering might be wrong")
	}
	if listingsFromDB[1].ContractAddress().Hex() != modelListingRejected.ContractAddress().Hex() {
		t.Error("Second listing is incorrect, ordering might be wrong")
	}
	if listingsFromDB[2].ContractAddress().Hex() != modelListingApplicationPhase.ContractAddress().Hex() {
		t.Error("Third listing is incorrect, ordering might be wrong")
	}
	if listingsFromDB[3].ContractAddress().Hex() != modelListingWhitelisted.ContractAddress().Hex() {
		t.Error("Fourth listing is incorrect, ordering might be wrong")
	}
	if listingsFromDB[4].ContractAddress().Hex() != modelListingNoChallenge.ContractAddress().Hex() {
		t.Error("Fifth listing is incorrect, ordering might be wrong")
	}
	if listingsFromDB[5].ContractAddress().Hex() != modelListingPastApplicationPhase.ContractAddress().Hex() {
		t.Error("Last listing is incorrect, ordering might be wrong")
	}

	listingsFromDB, err = persister.listingsByCriteriaFromTable(&model.ListingCriteria{
		ActiveChallenge: true,
	}, tableName, joinTableName)
	if err != nil {
		t.Errorf("Error getting listing by criteria: %v", err)
	}
	if len(listingsFromDB) != 1 {
		t.Errorf("One listing should have been returned but there are %v", len(listingsFromDB))
	}
	if listingsFromDB[0].ChallengeID().Int64() <= int64(0) {
		t.Error("Listing should have challengeID > 0")
	}
	if !listingsFromDB[0].Whitelisted() {
		t.Error("Listing should be currently whitelisted")
	}

	listingsFromDB, err = persister.listingsByCriteriaFromTable(&model.ListingCriteria{
		CurrentApplication: true,
	}, tableName, joinTableName)
	if err != nil {
		t.Errorf("Error getting listing by criteria: %v", err)
	}
	if len(listingsFromDB) != 2 {
		t.Errorf("Two listings should have been returned but there are %v", len(listingsFromDB))
	}

	listingsFromDB, err = persister.listingsByCriteriaFromTable(&model.ListingCriteria{
		ActiveChallenge:    true,
		CurrentApplication: true,
	}, tableName, joinTableName)
	if err != nil {
		t.Errorf("Error getting listing by criteria: %v", err)
	}
	if len(listingsFromDB) != 3 {
		t.Errorf("Three listings should have been returned but there are %v", len(listingsFromDB))
	}

	// Update active challenge listing resolved = true
	challenge.SetResolved(true)
	err = persister.updateChallengeInTable(challenge, []string{"Resolved"}, joinTableName)
	if err != nil {
		t.Errorf("Error updating challenge: %v", err)
	}
	listingsFromDB, err = persister.listingsByCriteriaFromTable(&model.ListingCriteria{
		ActiveChallenge:    true,
		CurrentApplication: true,
	}, tableName, joinTableName)
	if err != nil {
		t.Errorf("Error getting listing by criteria: %v", err)
	}
	if len(listingsFromDB) != 2 {
		t.Errorf("Two listings should have been returned but there are %v", len(listingsFromDB))
	}

	listingsFromDB, err = persister.listingsByCriteriaFromTable(&model.ListingCriteria{
		WhitelistedOnly: true,
	}, tableName, joinTableName)
	if err != nil {
		t.Errorf("Error getting listing by criteria: %v", err)
	}
	if len(listingsFromDB) != 3 {
		t.Errorf("Three listings should have been returned but there are %v", len(listingsFromDB))
	}

}

/*
Helpers for content_revision table tests:
*/

func setupRandomSampleContentRevision() (*model.ContentRevision, common.Address, *big.Int, *big.Int) {
	address, _ := cstrings.RandomHexStr(32)
	listingAddr := common.HexToAddress(address)
	contractContentID := big.NewInt(mathrand.Int63())
	return setupSampleContentRevision(listingAddr, contractContentID)
}

func setupSampleContentRevision(listingAddr common.Address, contractContentID *big.Int) (*model.ContentRevision, common.Address, *big.Int, *big.Int) {
	address2, _ := cstrings.RandomHexStr(32)
	address3, _ := cstrings.RandomHexStr(32)
	payload := model.ArticlePayload{}
	payloadHash := address2
	editorAddress := common.HexToAddress(address3)
	contractRevisionID := big.NewInt(mathrand.Int63())
	revisionURI := "revisionURI"
	revisionDateTs := ctime.CurrentEpochSecsInInt64()
	testContentRevision := model.NewContentRevision(listingAddr, payload, payloadHash, editorAddress,
		contractContentID, contractRevisionID, revisionURI, revisionDateTs)
	return testContentRevision, listingAddr, contractContentID, contractRevisionID
}

func setupSampleContentRevisionsSameAddressContentID(numRevisions int) ([]*model.ContentRevision, common.Address, *big.Int, []*big.Int) {
	address, _ := cstrings.RandomHexStr(32)
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
	defer persister.Close()
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
	if err != nil {
		t.Errorf("Error querying row: err: %v", err)
	}
	if numRowsb != 1 {
		t.Errorf("Number of rows in table should be 0 but is: %v", numRowsb)
	}
}

// TestContentRevision tests that a content revision can be retrieved
func TestContentRevision(t *testing.T) {
	tableName := "content_revision_test"
	persister, err := setupTestTable(tableName)
	defer persister.Close()
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
	defer persister.Close()
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
	defer persister.Close()
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

func TestNilResultsContentRevision(t *testing.T) {
	contRevTableName := "content_revision_test"
	persister, err := setupTestTable(contRevTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, contRevTableName)
	address1, _ := cstrings.RandomHexStr(32)
	contractAddress := common.HexToAddress(address1)
	contentID := big.NewInt(0)
	revisionID := big.NewInt(0)
	cr, err := persister.contentRevisionFromTable(contractAddress, contentID, revisionID, contRevTableName)
	if err != cpersist.ErrPersisterNoResults {
		t.Errorf("Error message is not %v but %v", cpersist.ErrPersisterNoResults, err)
	}
	if cr != nil {
		t.Errorf("Content Revision should be nil but is %v", cr)
	}
}

// Test update content revision -- TODO(IS) create an update where address, contentid, revisionid don't change
func TestUpdateContentRevision(t *testing.T) {
}

// TestDeleteContentRevision tests that the deleting the ContentRevision works
func TestDeleteContentRevision(t *testing.T) {
	tableName := "content_revision_test"
	persister, err := setupTestTable(tableName)
	defer persister.Close()
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
	if randListing {
		address1, _ := cstrings.RandomHexStr(32)
		listingAddr = common.HexToAddress(address1)
	} else {
		// keep listingAddress constant
		listingAddr = common.HexToAddress(testAddress)
	}

	metadata := model.Metadata{}
	governanceEventType := "governanceeventtypehere"
	creationDateTs := ctime.CurrentEpochSecsInInt64()
	lastUpdatedDateTs := ctime.CurrentEpochSecsInInt64() + 1
	eventHash, _ := cstrings.RandomHexStr(5)
	blockNumber := uint64(88888)
	tHash, _ := cstrings.RandomHexStr(5)
	txHash := common.HexToHash(tHash)
	txIndex := uint(4)
	blockHash := common.Hash{}
	index := uint(2)
	testGovernanceEvent := model.NewGovernanceEvent(listingAddr, metadata, governanceEventType,
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
	defer persister.Close()
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

func TestNilResultsGovernanceEvent(t *testing.T) {
	persister := setupGovEventTable(t)
	defer persister.Close()
	defer deleteTestTable(t, persister, govTestTableName)
	txHashSample, _ := cstrings.RandomHexStr(30)
	txHash := common.HexToHash(txHashSample)
	govEvent, err := persister.governanceEventsByTxHashFromTable(txHash, govTestTableName)
	if err != nil {
		t.Errorf("Error should be nil but is %v", err)
	}
	if len(govEvent) != 0 {
		t.Errorf("govEvent list should be empty but is %v", govEvent)
	}
}

// TestGovernanceEventsByListingAddress tests that a GovernanceEvent is properly retrieved
func TestGovernanceEventsByListingAddress(t *testing.T) {
	persister := setupGovEventTable(t)
	defer persister.Close()
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
	defer persister.Close()
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
	defer persister.Close()
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
	defer persister.Close()
	defer deleteTestTable(t, persister, govTestTableName)
	var listingAddr common.Address
	var timeMiddle int64
	var timeStart int64
	var modelGovernanceEvent *model.GovernanceEvent
	// create some governance events w constant listing address and save them to DB
	for i := 1; i <= 30; i++ {
		// TODO: just set timestamp for event bc there is still a probability these times won't be what you think.
		if i < 20 {
			timeStart = ctime.CurrentEpochSecsInInt64()
		}
		if i == 20 {
			time.Sleep(1 * time.Second)
			timeMiddle = ctime.CurrentEpochSecsInInt64()
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

// TestGovEventsByCriteria tests GovernanceEvent by txhash query
func TestGovEventsByTxHash(t *testing.T) {
	tableName := "governance_event_test"
	persister, err := setupTestTable(tableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

	// sample governanceEvent
	modelGovernanceEvent, _, _, txHash := setupSampleGovernanceEvent(true)
	time.Sleep(3)
	modelGovernanceEvent2, _, _, _ := setupSampleGovernanceEvent(true)

	// insert to table
	err = persister.createGovernanceEventInTable(modelGovernanceEvent2, tableName)
	if err != nil {
		t.Errorf("error saving GovernanceEvent: %v", err)
	}

	err = persister.createGovernanceEventInTable(modelGovernanceEvent, tableName)
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
	// This also tests that the first govEvent is the most recent one although they were saved out of order
	blockData := govEvents[0].BlockData()
	if blockData.TxHash() != txHash.Hex() {
		t.Errorf("Hash should be %v but is %v", txHash, blockData.TxHash())
	}

}

func setupSampleGovernanceChallengeEvent(randListing bool) (*model.GovernanceEvent, int) {
	var listingAddr common.Address
	if randListing {
		address1, _ := cstrings.RandomHexStr(32)
		listingAddr = common.HexToAddress(address1)
	} else {
		// keep listingAddress constant
		listingAddr = common.HexToAddress(testAddress)
	}
	challengeID := mathrand.Intn(100)
	metadata := model.Metadata{
		"Data":           "ipfs://QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
		"Challenger":     "0xe562d05067eded7a722ed73b9ebfaaedc60970a1",
		"ChallengeID":    challengeID,
		"CommitEndDate":  1527266803,
		"RevealEndDate":  1527268603,
		"ListingAddress": "0xa28ca9c9a7979c33cf73d3f406cd765e2d68c965"}
	governanceEventType := "Challenge"
	creationDateTs := ctime.CurrentEpochSecsInInt64()
	lastUpdatedDateTs := ctime.CurrentEpochSecsInInt64() + 1
	eventHash, _ := cstrings.RandomHexStr(5)
	blockNumber := uint64(88888)
	tHash, _ := cstrings.RandomHexStr(5)
	txHash := common.HexToHash(tHash)
	txIndex := uint(4)
	blockHash := common.Hash{}
	index := uint(2)
	testGovernanceEvent := model.NewGovernanceEvent(listingAddr, metadata, governanceEventType,
		creationDateTs, lastUpdatedDateTs, eventHash, blockNumber, txHash, txIndex, blockHash, index)
	return testGovernanceEvent, challengeID
}

//shuffle function
func shuffleInts(slice []int) []int {
	for i := range slice {
		j := mathrand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
	return slice
}

// also test that you can create multiple queries and cnxn pools are working

/*
All tests for challenge table:
*/
func setupChallengeByChallengeID(challengeIDInt int, resolved bool) *model.Challenge {
	listingAddr := common.HexToAddress(testAddress)
	challengeID := big.NewInt(int64(challengeIDInt))
	statement := ""
	address2, _ := cstrings.RandomHexStr(32)
	challenger := common.HexToAddress(address2)
	stake := new(big.Int)
	stake.SetString("100000000000000000000", 10)
	rewardPool := new(big.Int)
	rewardPool.SetString("50000000000000000000", 10)
	totalTokens := big.NewInt(232323223232)

	requestAppealExpiry := big.NewInt(1231312)
	testChallenge := model.NewChallenge(challengeID, listingAddr, statement, rewardPool,
		challenger, resolved, stake, totalTokens, requestAppealExpiry, int64(1212141313))
	return testChallenge
}

func setupSampleChallenge(randListing bool) (*model.Challenge, int) {
	var listingAddr common.Address
	address2, _ := cstrings.RandomHexStr(32)
	if randListing {
		address1, _ := cstrings.RandomHexStr(32)
		listingAddr = common.HexToAddress(address1)
	} else {
		// keep listingAddress constant
		listingAddr = common.HexToAddress(testAddress)
	}
	// challengeIDInt := 50
	challengeIDInt := mathrand.Intn(10000)
	challengeID := big.NewInt(int64(challengeIDInt))
	statement := ""
	challenger := common.HexToAddress(address2)
	stake := new(big.Int)
	stake.SetString("100000000000000000000", 10)
	rewardPool := new(big.Int)
	rewardPool.SetString("50000000000000000000", 10)
	totalTokens := big.NewInt(232323223232)

	requestAppealExpiry := big.NewInt(1231312)
	testChallenge := model.NewChallenge(challengeID, listingAddr, statement, rewardPool,
		challenger, false, stake, totalTokens, requestAppealExpiry, int64(1212141313))
	return testChallenge, challengeIDInt
}

func setupChallengeTable(t *testing.T) *PostgresPersister {
	persister, err := setupTestTable(challengeTestTableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	return persister
}

func createAndSaveTestChallenge(t *testing.T, persister *PostgresPersister, randListing bool) (*model.Challenge, int) {
	// sample challenge
	modelChallenge, challengeID := setupSampleChallenge(randListing)

	// insert to table
	err := persister.createChallengeInTable(modelChallenge, challengeTestTableName)
	if err != nil {
		t.Errorf("error saving challenge: %v", err)
	}
	return modelChallenge, challengeID
}

func TestCreateChallenge(t *testing.T) {
	persister, err := setupTestTable(challengeTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, challengeTestTableName)
	_, _ = createAndSaveTestChallenge(t, persister, true)

}

func TestGetChallenge(t *testing.T) {
	persister, err := setupTestTable(challengeTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, challengeTestTableName)
	modelChallenge, challengeID := createAndSaveTestChallenge(t, persister, true)

	challengesFromDB, err := persister.challengesByChallengeIDsInTableInOrder(
		[]int{challengeID}, challengeTestTableName)
	if err != nil {
		t.Errorf("Error getting value from DB: %v", err)
	}
	if len(challengesFromDB) == 0 {
		t.Errorf("Didn't get anything from DB challenge test")
	}
	challengeFromDB := challengesFromDB[0]

	if !reflect.DeepEqual(modelChallenge.ChallengeID(), challengeFromDB.ChallengeID()) {
		t.Error("Mismatch in challenge ID")
	}
	if !reflect.DeepEqual(modelChallenge.ListingAddress(), challengeFromDB.ListingAddress()) {
		t.Error("Mismatch in listingaddress")
	}
	if !reflect.DeepEqual(modelChallenge.Statement(), challengeFromDB.Statement()) {
		t.Error("Mismatch in statement")
	}
	if !reflect.DeepEqual(modelChallenge.RewardPool(), challengeFromDB.RewardPool()) {
		t.Error("Mismatch in rewardpool")
	}
	if !reflect.DeepEqual(modelChallenge.Stake(), challengeFromDB.Stake()) {
		t.Error("Mismatch in stake")
	}

	if !reflect.DeepEqual(modelChallenge.TotalTokens(), challengeFromDB.TotalTokens()) {
		t.Error("Mismatch in total tokens")
	}
	if !reflect.DeepEqual(modelChallenge.Challenger(), challengeFromDB.Challenger()) {
		t.Error("Mismatch in challenger")
	}
	if !reflect.DeepEqual(modelChallenge.LastUpdatedDateTs(), challengeFromDB.LastUpdatedDateTs()) {
		t.Error("Mismatch in ts")
	}

	_, err = persister.challengesByChallengeIDsInTableInOrder(
		[]int{}, challengeTestTableName)
	if err == nil {
		t.Errorf("Should have received an error on empty challenges ID")
	}
	if err != cpersist.ErrPersisterNoResults {
		t.Errorf("Should have received an ErrPersisterNoResults on empty challenges ID: err: %v", err)
	}

	challengesFromDB, err = persister.challengesByChallengeIDsInTableInOrder(
		[]int{1002040929}, challengeTestTableName)
	if err != nil {
		t.Errorf("Should have received an error on empty challenges ID")
	}
	if len(challengesFromDB) != 1 {
		t.Errorf("Should have received 1 item in the challenges from DB")
	}
	if challengesFromDB[0] != nil {
		t.Errorf("Should have received a nil value for an unfound challenge")
	}

}

func TestGetChallengesForListingAddresses(t *testing.T) {
	persister, err := setupTestTable(challengeTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, challengeTestTableName)

	// Multiple for a single address
	challenge1, _ := createAndSaveTestChallenge(t, persister, false)
	_, _ = createAndSaveTestChallenge(t, persister, false)
	_, _ = createAndSaveTestChallenge(t, persister, false)

	// A few random challenges
	challenge4, _ := createAndSaveTestChallenge(t, persister, true)
	challenge5, _ := createAndSaveTestChallenge(t, persister, true)

	addrs := []common.Address{
		challenge1.ListingAddress(),
		challenge4.ListingAddress(),
		challenge5.ListingAddress(),
	}

	listingChallenges, err := persister.challengesByListingAddressesInTable(
		addrs,
		challengeTestTableName,
	)
	if err != nil {
		t.Errorf("Error getting values from DB: %v", err)
	}
	if len(listingChallenges) == 0 {
		t.Errorf("Should have gotten some results")
	}
	if len(listingChallenges) != 3 {
		t.Errorf("Should have gotten 3 results")
	}

	for index, addr := range addrs {
		addrChallenges := listingChallenges[index]
		if addr.Hex() == challenge1.ListingAddress().Hex() {
			if len(addrChallenges) != 3 {
				t.Errorf("Should have gotten 3 challenges for %v", addr.Hex())
			}
			challenge := addrChallenges[0]
			if challenge.ListingAddress().Hex() != challenge1.ListingAddress().Hex() {
				t.Errorf("Should have matched addresses, might be out of order: addr %v", addr.Hex())
			}
		}
		if addr.Hex() == challenge4.ListingAddress().Hex() {
			if len(addrChallenges) != 1 {
				t.Errorf("Should have gotten 1 challenge for %v", addr.Hex())
			}
			challenge := addrChallenges[0]
			if challenge.ListingAddress().Hex() != challenge4.ListingAddress().Hex() {
				t.Errorf("Should have matched addresses, might be out of order: addr %v", addr.Hex())
			}
		}
		if addr.Hex() == challenge5.ListingAddress().Hex() {
			if len(addrChallenges) != 1 {
				t.Errorf("Should have gotten 1 challenge for %v", addr.Hex())
			}
			challenge := addrChallenges[0]
			if challenge.ListingAddress().Hex() != challenge5.ListingAddress().Hex() {
				t.Errorf("Should have matched addresses, might be out of order: addr %v", addr.Hex())
			}
		}
	}

	_, err = persister.challengesByListingAddressesInTable(
		[]common.Address{},
		challengeTestTableName,
	)
	if err == nil {
		t.Errorf("Should have received an error on empty addresses")
	}
	if err != cpersist.ErrPersisterNoResults {
		t.Errorf("Should have received an ErrPersisterNoResults on empty addresses: err: %v", err)
	}

	listingChallenges, err = persister.challengesByListingAddressesInTable(
		[]common.Address{common.HexToAddress("0x39eD84CE90Bc48DD76C4760DD0F90997Ba274F9d")},
		challengeTestTableName,
	)
	if err != nil {
		t.Errorf("Should have received an error on bad address")
	}
	if len(listingChallenges) != 1 {
		t.Errorf("Should have received 1 item in the challenges from DB")
	}
	if listingChallenges[0] != nil {
		t.Errorf("Should have received a nil value for an unfound challenge")
	}

}

func TestGetChallengesForListingAddress(t *testing.T) {
	persister, err := setupTestTable(challengeTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, challengeTestTableName)
	_, _ = createAndSaveTestChallenge(t, persister, false)
	_, _ = createAndSaveTestChallenge(t, persister, false)
	_, _ = createAndSaveTestChallenge(t, persister, false)

	challengesFromDB, err := persister.challengesByListingAddressInTable(
		common.HexToAddress(testAddress),
		challengeTestTableName,
	)
	if err != nil {
		t.Errorf("Error getting value from DB: %v", err)
	}

	if len(challengesFromDB) == 0 {
		t.Errorf("Should have gotten some results for address")
	}
	if len(challengesFromDB) != 3 {
		t.Errorf("Should have gotten 3 results for address")
	}

	previousChallengeID := big.NewInt(-1)
	for _, ch := range challengesFromDB {
		if ch.ListingAddress().Hex() != testAddress {
			t.Errorf("Should have gotten all challenges for a single address")
		}
		if ch.ChallengeID().Cmp(previousChallengeID) != 1 {
			t.Errorf(
				"Should have returned the list in order: %v, %v",
				ch.ChallengeID(),
				previousChallengeID,
			)
		}
		previousChallengeID = ch.ChallengeID()
	}
}

func TestNilResultsChallenges(t *testing.T) {
	persister, err := setupTestTable(challengeTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, challengeTestTableName)

	challenge, err := persister.challengeByChallengeIDFromTable(0, challengeTestTableName)
	if err != cpersist.ErrPersisterNoResults {
		t.Errorf("Error should be %v but is %v", cpersist.ErrPersisterNoResults, err)
	}
	if challenge != nil {
		t.Errorf("Challenge should be nil but is %v", challenge)
	}

	blankAddress := common.Address{}
	challenges, err := persister.challengesByListingAddressInTable(blankAddress, challengeTestTableName)
	if err != cpersist.ErrPersisterNoResults {
		t.Errorf("Error should be no results %v", err)
	}
	if challenges != nil {
		t.Errorf("Challenges should be nil but is %v", challenges)
	}

}

func TestUpdateChallenge(t *testing.T) {
	persister, err := setupTestTable(challengeTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, challengeTestTableName)
	_, challengeID := createAndSaveTestChallenge(t, persister, true)

	challengesFromDB, err := persister.challengesByChallengeIDsInTableInOrder([]int{challengeID}, challengeTestTableName)
	if err != nil {
		t.Errorf("Error getting value from DB: %v", err)
	}
	if len(challengesFromDB) == 0 {
		t.Errorf("Didn't get anything from DB challenge test")
	}
	challengeFromDB := challengesFromDB[0]
	newTotalTokens := big.NewInt(int64(231231312312))
	challengeFromDB.SetTotalTokens(newTotalTokens)

	err = persister.updateChallengeInTable(challengeFromDB, []string{"TotalTokens"}, challengeTestTableName)
	if err != nil {
		t.Errorf("Error updating challenge: %v", err)
	}

	challengesFromDB, err = persister.challengesByChallengeIDsInTableInOrder([]int{challengeID}, challengeTestTableName)
	if err != nil {
		t.Errorf("Error getting value from DB: %v", err)
	}
	if len(challengesFromDB) == 0 {
		t.Errorf("Didn't get anything from DB challenge test")
	}
	challengeFromDBModified := challengesFromDB[0]
	if !reflect.DeepEqual(challengeFromDBModified.TotalTokens(), newTotalTokens) {
		t.Error("Val was not updated")
	}
}

/*
All tests for poll table:
*/

func setupSamplePoll(randListing bool) (*model.Poll, *big.Int) {
	pollID := big.NewInt(23)
	return model.NewPoll(
		pollID,
		big.NewInt(232232323),
		big.NewInt(232232350),
		big.NewInt(40),
		big.NewInt(50),
		big.NewInt(50),
		int64(232232323),
	), pollID
}

func setupPollTable(t *testing.T) *PostgresPersister {
	persister, err := setupTestTable(pollTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	return persister
}

func createAndSaveTestPoll(t *testing.T, persister *PostgresPersister, randListing bool) (*model.Poll, *big.Int) {
	// sample poll
	modelPoll, pollID := setupSamplePoll(randListing)

	// insert to table
	err := persister.createPollInTable(modelPoll, pollTestTableName)
	if err != nil {
		t.Errorf("error saving poll: %v", err)
	}
	return modelPoll, pollID
}

func TestCreatePoll(t *testing.T) {
	persister, err := setupTestTable(pollTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, pollTestTableName)
	_, _ = createAndSaveTestPoll(t, persister, true)

}

func TestNilResultsPoll(t *testing.T) {
	persister, err := setupTestTable(pollTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, pollTestTableName)

	pollID := 0
	poll, err := persister.pollByPollIDFromTable(pollID)
	if poll != nil {
		t.Errorf("Poll should be nil but is %v", poll)
	}
	if err != cpersist.ErrPersisterNoResults {
		t.Errorf("Error should be %v but is %v", cpersist.ErrPersisterNoResults, err)
	}
}

func TestUpdatePoll(t *testing.T) {
	persister, err := setupTestTable(pollTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, pollTestTableName)
	_, pollID := createAndSaveTestPoll(t, persister, true)

	pollsFromDB, err := persister.pollsByPollIDsInTableInOrder([]int{int(pollID.Int64())}, pollTestTableName)
	if err != nil {
		t.Errorf("Error getting value from DB: %v", err)
	}
	if len(pollsFromDB) == 0 {
		t.Errorf("Didn't get anything from DB poll test")
	}
	pollFromDB := pollsFromDB[0]

	newVotes := big.NewInt(30)
	pollFromDB.UpdateVotesFor(newVotes)

	err = persister.updatePollInTable(pollFromDB, []string{"VotesFor"}, pollTestTableName)
	if err != nil {
		t.Errorf("Error updating poll %v", err)
	}

	pollsFromDB, err = persister.pollsByPollIDsInTableInOrder([]int{int(pollID.Int64())}, pollTestTableName)
	if err != nil {
		t.Errorf("Error getting value from DB: %v", err)
	}
	if len(pollsFromDB) == 0 {
		t.Errorf("Didn't get anything from DB challenge test")
	}
	pollFromDBModified := pollsFromDB[0]
	if !reflect.DeepEqual(pollFromDBModified.VotesFor(), pollFromDB.VotesFor()) {
		t.Errorf("Error updating poll table")
	}
}

/*
All tests for appeal table:
*/

func setupSampleAppeal(randListing bool) (*model.Appeal, *big.Int) {
	originalChallengeID := big.NewInt(23)
	address2, _ := cstrings.RandomHexStr(32)
	return model.NewAppeal(
		originalChallengeID,
		common.HexToAddress(address2),
		big.NewInt(2322),
		big.NewInt(401123243),
		true,
		"",
		int64(232323),
		"",
	), originalChallengeID
}

func setupAppealTable(t *testing.T) *PostgresPersister {
	persister, err := setupTestTable(appealTestTableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	return persister
}

func createAndSaveTestAppeal(t *testing.T, persister *PostgresPersister, randListing bool) (*model.Appeal, *big.Int) {
	// sample appeal
	modelAppeal, challengeID := setupSampleAppeal(randListing)
	// insert to table
	err := persister.createAppealInTable(modelAppeal, appealTestTableName)
	if err != nil {
		t.Errorf("error saving appeal: %v", err)
	}
	return modelAppeal, challengeID
}

func TestCreateAppeal(t *testing.T) {
	persister, err := setupTestTable(appealTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, appealTestTableName)
	_, _ = createAndSaveTestAppeal(t, persister, true)

}

func TestUpdateAppeal(t *testing.T) {
	persister, err := setupTestTable(appealTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, appealTestTableName)
	_, challengeID := createAndSaveTestAppeal(t, persister, true)

	appealsFromDB, err := persister.appealsByChallengeIDsInTableInOrder([]int{int(challengeID.Int64())}, appealTestTableName)
	if err != nil {
		t.Errorf("Error getting value from DB: %v", err)
	}
	if len(appealsFromDB) == 0 {
		t.Errorf("Didn't get anything from DB poll test")
	}
	appealFromDB := appealsFromDB[0]

	newChallengeID := big.NewInt(100)
	appealFromDB.SetAppealChallengeID(newChallengeID)

	err = persister.updateAppealInTable(appealFromDB, []string{"AppealChallengeID"}, appealTestTableName)
	if err != nil {
		t.Errorf("Error updating appeal %v", err)
	}

	appealsFromDB, err = persister.appealsByChallengeIDsInTableInOrder([]int{int(challengeID.Int64())}, appealTestTableName)
	if err != nil {
		t.Errorf("Error getting value from DB: %v", err)
	}
	if len(appealsFromDB) == 0 {
		t.Errorf("Didn't get anything from DB challenge test")
	}
	appealFromDBModified := appealsFromDB[0]
	if !reflect.DeepEqual(appealFromDBModified.AppealChallengeID(), appealFromDB.AppealChallengeID()) {
		t.Errorf("Error updating appeal table")
	}
}

func TestNilResultsAppeal(t *testing.T) {
	persister, err := setupTestTable(appealTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, appealTestTableName)

	challengeID := 0
	appeal, err := persister.appealByChallengeIDFromTable(challengeID)
	if appeal != nil {
		t.Errorf("Appeal should be nil but is %v", appeal)
	}
	if err != cpersist.ErrPersisterNoResults {
		t.Errorf("Error should be %v but is %v", cpersist.ErrPersisterNoResults, err)
	}
}

/*
All tests for cron table:
*/

func TestTypeExistsInCronTable(t *testing.T) {
	tableName := "cron_test"
	persister, err := setupTestTable(tableName)
	defer persister.Close()
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

	if exists != ctime.TimestampToString(0) {
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
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

	newTimestamp := int64(1212121212)
	err = persister.updateCronTimestampInTable(newTimestamp, tableName)
	if err != nil {
		t.Errorf("Error updating cron table, %v", err)
	}

	timestamp, err := persister.lastCronTimestampFromTable(tableName)
	if err != nil {
		t.Errorf("Error retrieving from cron table: %v", err)
	}

	if timestamp != newTimestamp {
		t.Errorf("Timestamp should be %v but it is %v", newTimestamp, timestamp)
	}

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

func TestLastEventHashesFromTable(t *testing.T) {
	tableName := "cron_test"
	persister, err := setupTestTable(tableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

	// There should be no rows in the table. In this case lastEventHashes should insert a nil value.
	eventHashes, err := persister.lastEventHashesFromTable(tableName)
	if err != nil {
		t.Errorf("Error retrieving from cron table: %v", err)
	}
	if strings.Join(eventHashes, ",") != "" {
		t.Errorf("Event Hashes should be empty but are %v", eventHashes)
	}
}

func TestUpdateEventHashes(t *testing.T) {
	tableName := "cron_test"
	persister, err := setupTestTable(tableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tableName)

	newEventHashes := []string{"testhash1", "testhash2"}
	err = persister.updateEventHashesInTable(newEventHashes, tableName)
	if err != nil {
		t.Errorf("Error updating cron table, %v", err)
	}

	eventHashes, err := persister.lastEventHashesFromTable(tableName)
	if err != nil {
		t.Errorf("Error retrieving from cron table: %v", err)
	}

	if !reflect.DeepEqual(eventHashes, newEventHashes) {
		t.Errorf("EventHashes should be %v but is %v", newEventHashes, eventHashes)
	}

}

/*
 * All tests for token transfer table:
 */

func setupSampleTokenTransfer() *model.TokenTransfer {
	address1, _ := cstrings.RandomHexStr(32)
	address2, _ := cstrings.RandomHexStr(32)
	hex1, _ := cstrings.RandomHexStr(30)
	hex2, _ := cstrings.RandomHexStr(30)
	params := &model.TokenTransferParams{
		ToAddress:    common.HexToAddress(address1),
		FromAddress:  common.HexToAddress(address2),
		Amount:       big.NewInt(int64(mathrand.Intn(1000))),
		TransferDate: ctime.CurrentEpochSecsInInt64(),
		BlockNumber:  uint64(mathrand.Intn(1000000)),
		TxHash:       common.HexToHash(hex1),
		TxIndex:      uint(mathrand.Intn(20)),
		BlockHash:    common.HexToHash(hex2),
		Index:        uint(mathrand.Intn(20)),
	}
	return model.NewTokenTransfer(params)
}

func setupTokenTransferTable(t *testing.T) *PostgresPersister {
	persister, err := setupTestTable(tokenTransferTestTableName)
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	return persister
}

func createAndSaveTestTokenTransfer(t *testing.T, persister *PostgresPersister) *model.TokenTransfer {
	transfer := setupSampleTokenTransfer()
	err := persister.createTokenTransferInTable(transfer, tokenTransferTestTableName)
	if err != nil {
		t.Errorf("error saving token transfer: %v", err)
	}
	return transfer
}

func TestCreateTokenTransfer(t *testing.T) {
	persister, err := setupTestTable(tokenTransferTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tokenTransferTestTableName)
	_ = createAndSaveTestTokenTransfer(t, persister)
}

func TestGetTokenTransfersForToAddress(t *testing.T) {
	persister, err := setupTestTable(tokenTransferTestTableName)
	defer persister.Close()
	if err != nil {
		t.Errorf("Error connecting to DB: %v", err)
	}
	defer deleteTestTable(t, persister, tokenTransferTestTableName)
	transfer := createAndSaveTestTokenTransfer(t, persister)

	purchases, err := persister.tokenTransfersByToAddressFromTable(
		transfer.ToAddress(),
		tokenTransferTestTableName,
	)
	if err != nil {
		t.Errorf("Should have not gotten error from transfer query: err: %v", err)
	}
	if len(purchases) != 1 {
		t.Errorf("Should have gotten 1 result for transfers")
	}
	purchase := purchases[0]

	if purchase.ToAddress().Hex() != transfer.ToAddress().Hex() {
		t.Errorf("Should have gotten the same to address")
	}
	if purchase.FromAddress().Hex() != transfer.FromAddress().Hex() {
		t.Errorf("Should have gotten the same from address")
	}
	if purchase.Amount().Int64() != transfer.Amount().Int64() {
		t.Errorf("Should have gotten the same amount")
	}
	if purchase.TransferDate() != transfer.TransferDate() {
		t.Errorf("Should have gotten the transfer date")
	}
}
