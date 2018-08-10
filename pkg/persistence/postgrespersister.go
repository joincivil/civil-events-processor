// Package persistence contains components to interact with the DB
package persistence // import "github.com/joincivil/civil-events-processor/pkg/persistence"

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jmoiron/sqlx"
	// driver for postgresql
	_ "github.com/lib/pq"

	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/persistence/postgres"
)

const (
	listingTableName  = "listing"
	contRevTableName  = "content_revision"
	govEventTableName = "governance_event"
	cronTableName     = "cron"

	// Could make this configurable later if needed
	maxOpenConns    = 20
	maxIdleConns    = 5
	connMaxLifetime = time.Nanosecond
)

// NewPostgresPersister creates a new postgres persister
func NewPostgresPersister(host string, port int, user string, password string, dbname string) (*PostgresPersister, error) {
	pgPersister := &PostgresPersister{}
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sqlx.Connect("postgres", psqlInfo)
	if err != nil {
		return pgPersister, fmt.Errorf("Error connecting to sqlx: %v", err)
	}
	pgPersister.db = db
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(connMaxLifetime)
	return pgPersister, nil
}

// PostgresPersister holds the DB connection and persistence
type PostgresPersister struct {
	db *sqlx.DB
}

// ListingsByAddresses returns a slice of Listings based on addresses
func (p *PostgresPersister) ListingsByAddresses(addresses []common.Address) ([]*model.Listing, error) {
	return p.listingsByAddressesFromTable(addresses, listingTableName)
}

// ListingByAddress retrieves listings based on addresses
func (p *PostgresPersister) ListingByAddress(address common.Address) (*model.Listing, error) {
	return p.listingByAddressFromTable(address, listingTableName)
}

// CreateListing creates a new listing
func (p *PostgresPersister) CreateListing(listing *model.Listing) error {
	return p.createListingForTable(listing, listingTableName)
}

// UpdateListing updates fields on an existing listing
func (p *PostgresPersister) UpdateListing(listing *model.Listing, updatedFields []string) error {
	return p.updateListingInTable(listing, updatedFields, listingTableName)
}

// DeleteListing removes a listing
func (p *PostgresPersister) DeleteListing(listing *model.Listing) error {
	return p.deleteListingFromTable(listing, listingTableName)
}

// CreateContentRevision creates a new content revision
func (p *PostgresPersister) CreateContentRevision(revision *model.ContentRevision) error {
	return p.createContentRevisionForTable(revision, contRevTableName)
}

// ContentRevision retrieves a specific content revision for newsroom content
func (p *PostgresPersister) ContentRevision(address common.Address, contentID *big.Int, revisionID *big.Int) (*model.ContentRevision, error) {
	return p.contentRevisionFromTable(address, contentID, revisionID, contRevTableName)
}

// ContentRevisions retrieves the revisions for content on a listing
func (p *PostgresPersister) ContentRevisions(address common.Address, contentID *big.Int) ([]*model.ContentRevision, error) {
	return p.contentRevisionsFromTable(address, contentID, contRevTableName)
}

// UpdateContentRevision updates fields on an existing content revision
func (p *PostgresPersister) UpdateContentRevision(revision *model.ContentRevision, updatedFields []string) error {
	return p.updateContentRevisionInTable(revision, updatedFields, contRevTableName)
}

// DeleteContentRevision removes a content revision
func (p *PostgresPersister) DeleteContentRevision(revision *model.ContentRevision) error {
	return p.deleteContentRevisionFromTable(revision, contRevTableName)
}

// GovernanceEventsByListingAddress retrieves governance events based on criteria
func (p *PostgresPersister) GovernanceEventsByListingAddress(address common.Address) ([]*model.GovernanceEvent, error) {
	return p.governanceEventsByListingAddressFromTable(address, govEventTableName)
}

// CreateGovernanceEvent creates a new governance event
func (p *PostgresPersister) CreateGovernanceEvent(govEvent *model.GovernanceEvent) error {
	return p.createGovernanceEventInTable(govEvent, govEventTableName)
}

// UpdateGovernanceEvent updates fields on an existing governance event
func (p *PostgresPersister) UpdateGovernanceEvent(govEvent *model.GovernanceEvent, updatedFields []string) error {
	return p.updateGovernanceEventInTable(govEvent, updatedFields, govEventTableName)
}

// DeleteGovenanceEvent removes a governance event
func (p *PostgresPersister) DeleteGovenanceEvent(govEvent *model.GovernanceEvent) error {
	return p.deleteGovenanceEventFromTable(govEvent, govEventTableName)
}

// TimestampOfLastEventForCron returns the last timestamp from cron
func (p *PostgresPersister) TimestampOfLastEventForCron() (int64, error) {
	return p.lastCronTimestampFromTable(cronTableName)
}

// UpdateTimestampForCron updates the timestamp saved in cron table
func (p *PostgresPersister) UpdateTimestampForCron(timestamp int64) error {
	return p.updateCronTimestampInTable(timestamp, cronTableName)
}

// CreateTables creates the tables for processor if they don't exist
func (p *PostgresPersister) CreateTables() error {
	// this needs to get all the event tables for processor
	contRevTableQuery := postgres.CreateContentRevisionTableQuery()
	govEventTableQuery := postgres.CreateGovernanceEventTableQuery()
	listingTableQuery := postgres.CreateListingTableQuery()
	cronTableQuery := postgres.CreateCronTableQuery()

	_, err := p.db.Exec(contRevTableQuery)
	if err != nil {
		return fmt.Errorf("Error creating content_revision table in postgres: %v", err)
	}
	_, err = p.db.Exec(govEventTableQuery)
	if err != nil {
		return fmt.Errorf("Error creating governance_event table in postgres: %v", err)
	}
	_, err = p.db.Exec(listingTableQuery)
	if err != nil {
		return fmt.Errorf("Error creating listing table in postgres: %v", err)
	}
	_, err = p.db.Exec(cronTableQuery)
	if err != nil {
		return fmt.Errorf("Error creating listing table in postgres: %v", err)
	}
	return err
}

func (p *PostgresPersister) insertIntoDBQueryString(tableName string, dbModelStruct interface{}) string {
	fieldNames, fieldNamesColon := postgres.StructFieldsForQuery(dbModelStruct, true)
	queryString := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s);", tableName, fieldNames, fieldNamesColon) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) updateDBQueryBuffer(updatedFields []string, tableName string, dbModelStruct interface{}) (bytes.Buffer, error) {
	var queryBuf bytes.Buffer
	queryBuf.WriteString("UPDATE ") // nolint: gosec
	queryBuf.WriteString(tableName) // nolint: gosec
	queryBuf.WriteString(" SET ")   // nolint: gosec
	for idx, field := range updatedFields {
		dbFieldName, err := postgres.DbFieldNameFromModelName(dbModelStruct, field)
		if err != nil {
			return queryBuf, fmt.Errorf("Error getting %s from %s table DB struct tag: %v", field, tableName, err)
		}
		queryBuf.WriteString(fmt.Sprintf("%s=:%s", dbFieldName, dbFieldName)) // nolint: gosec
		if idx+1 < len(updatedFields) {
			queryBuf.WriteString(", ") // nolint: gosec
		}
	}
	return queryBuf, nil
}

func (p *PostgresPersister) listingsByAddressesFromTable(addresses []common.Address, tableName string) ([]*model.Listing, error) {
	listings := []*model.Listing{}
	for _, address := range addresses {
		listing, err := p.listingByAddressFromTable(address, tableName)
		if err != nil {
			if err == sql.ErrNoRows {
				err = model.ErrPersisterNoResults
			}
			return listings, err
		}
		listings = append(listings, listing)
	}
	return listings, nil
}

func (p *PostgresPersister) listingByAddressFromTable(address common.Address, tableName string) (*model.Listing, error) {
	dbListing := postgres.Listing{}
	queryString := p.listingByAddressQuery(tableName)
	err := p.db.Get(&dbListing, queryString, address.Hex())
	if err != nil {
		if err == sql.ErrNoRows {
			err = model.ErrPersisterNoResults
		}
		return nil, fmt.Errorf("Wasn't able to get listing from postgres table: %v", err)
	}
	listing := dbListing.DbToListingData()
	return listing, err
}

func (p *PostgresPersister) listingByAddressQuery(tableName string) string {
	fieldNames, _ := postgres.StructFieldsForQuery(postgres.Listing{}, false)
	queryString := fmt.Sprintf("SELECT %s FROM %s WHERE contract_address=$1;", fieldNames, tableName) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) createListingForTable(listing *model.Listing, tableName string) error {
	dbListing := postgres.NewListing(listing)
	queryString := p.insertIntoDBQueryString(tableName, postgres.Listing{})
	_, err := p.db.NamedExec(queryString, dbListing)
	if err != nil {
		return fmt.Errorf("Error saving listing to table: %v", err)
	}
	return nil
}

func (p *PostgresPersister) updateListingInTable(listing *model.Listing, updatedFields []string, tableName string) error {
	queryString, err := p.updateListingQuery(updatedFields, tableName)
	if err != nil {
		return fmt.Errorf("Error creating query string for update: %v ", err)
	}
	dbListing := postgres.NewListing(listing)
	_, err = p.db.NamedExec(queryString, dbListing)
	if err != nil {
		return fmt.Errorf("Error updating fields in db: %v", err)
	}
	return nil
}

func (p *PostgresPersister) updateListingQuery(updatedFields []string, tableName string) (string, error) {
	queryString, err := p.updateDBQueryBuffer(updatedFields, tableName, postgres.Listing{})
	if err != nil {
		return "", err
	}
	queryString.WriteString(" WHERE contract_address=:contract_address;") // nolint: gosec
	return queryString.String(), nil
}

func (p *PostgresPersister) deleteListingFromTable(listing *model.Listing, tableName string) error {
	dbListing := postgres.NewListing(listing)
	queryString := p.deleteListingQuery(tableName)
	_, err := p.db.NamedExec(queryString, dbListing)
	if err != nil {
		return fmt.Errorf("Error deleting listing in db: %v", err)
	}
	return nil
}

func (p *PostgresPersister) deleteListingQuery(tableName string) string {
	queryString := fmt.Sprintf("DELETE FROM %s WHERE contract_address=:contract_address", tableName) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) createContentRevisionForTable(revision *model.ContentRevision, tableName string) error {
	queryString := p.insertIntoDBQueryString(tableName, postgres.ContentRevision{})
	dbContRev := postgres.NewContentRevision(revision)
	_, err := p.db.NamedExec(queryString, dbContRev)
	if err != nil {
		return fmt.Errorf("Error saving contentRevision to table: %v", err)
	}
	return nil
}

func (p *PostgresPersister) contentRevisionFromTable(address common.Address, contentID *big.Int, revisionID *big.Int, tableName string) (*model.ContentRevision, error) {
	contRev := &model.ContentRevision{}
	dbContRev := postgres.ContentRevision{}
	queryString := p.contentRevisionQuery(tableName)
	err := p.db.Get(&dbContRev, queryString, address.Hex(), contentID.Int64(), revisionID.Int64())
	if err != nil {
		if err == sql.ErrNoRows {
			err = model.ErrPersisterNoResults
		}
		return contRev, fmt.Errorf("Wasn't able to get ContentRevision from postgres table: %v", err)
	}
	contRev = dbContRev.DbToContentRevisionData()
	return contRev, err
}

func (p *PostgresPersister) contentRevisionQuery(tableName string) string {
	fieldNames, _ := postgres.StructFieldsForQuery(postgres.ContentRevision{}, false)
	queryString := fmt.Sprintf("SELECT %s FROM %s WHERE (listing_address=$1 AND contract_content_id=$2 AND contract_revision_id=$3)", fieldNames, tableName) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) contentRevisionsFromTable(address common.Address, contentID *big.Int, tableName string) ([]*model.ContentRevision, error) {
	contRevs := []*model.ContentRevision{}
	dbContRevs := []postgres.ContentRevision{}
	queryString := p.contentRevisionsQuery(tableName)
	err := p.db.Select(&dbContRevs, queryString, address.Hex(), contentID.Int64())
	if err != nil {
		if err == sql.ErrNoRows {
			err = model.ErrPersisterNoResults
		}
		return contRevs, fmt.Errorf("Wasn't able to get ContentRevisions from postgres table: %v", err)
	}
	for _, dbContRev := range dbContRevs {
		contRevs = append(contRevs, dbContRev.DbToContentRevisionData())
	}
	return contRevs, err
}

func (p *PostgresPersister) contentRevisionsQuery(tableName string) string {
	fieldNames, _ := postgres.StructFieldsForQuery(postgres.ContentRevision{}, false)
	queryString := fmt.Sprintf("SELECT %s FROM %s WHERE (listing_address=$1 AND contract_content_id=$2)", fieldNames, tableName) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) updateContentRevisionInTable(revision *model.ContentRevision, updatedFields []string, tableName string) error {
	queryString, err := p.updateContentRevisionQuery(updatedFields, tableName)
	if err != nil {
		return fmt.Errorf("Error creating query string for update: %v ", err)
	}
	dbContentRevision := postgres.NewContentRevision(revision)
	_, err = p.db.NamedExec(queryString, dbContentRevision)
	if err != nil {
		return fmt.Errorf("Error updating fields in db: %v", err)
	}
	return nil
}

func (p *PostgresPersister) updateContentRevisionQuery(updatedFields []string, tableName string) (string, error) {
	queryString, err := p.updateDBQueryBuffer(updatedFields, tableName, postgres.ContentRevision{})
	if err != nil {
		return "", err
	}
	queryString.WriteString(" WHERE (listing_address=:listing_address AND contract_content_id=:contract_content_id AND contract_revision_id=:contract_revision_id);") // nolint: gosec
	return queryString.String(), nil
}

func (p *PostgresPersister) deleteContentRevisionFromTable(revision *model.ContentRevision, tableName string) error {
	dbContRev := postgres.NewContentRevision(revision)
	queryString := p.deleteContentRevisionQuery(tableName)
	_, err := p.db.NamedExec(queryString, dbContRev)
	if err != nil {
		return fmt.Errorf("Error deleting content revision in db: %v", err)
	}
	return nil
}

func (p *PostgresPersister) deleteContentRevisionQuery(tableName string) string {
	queryString := fmt.Sprintf("DELETE FROM %s WHERE (listing_address=:listing_address AND contract_content_id=:contract_content_id AND contract_revision_id=:contract_revision_id)", tableName) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) governanceEventsByListingAddressFromTable(address common.Address, tableName string) ([]*model.GovernanceEvent, error) {
	govEvents := []*model.GovernanceEvent{}
	queryString := p.govEventsQuery(tableName)
	dbGovEvents := []postgres.GovernanceEvent{}
	err := p.db.Select(&dbGovEvents, queryString, address.Hex())
	if err != nil {
		if err == sql.ErrNoRows {
			err = model.ErrPersisterNoResults
		}
		return govEvents, fmt.Errorf("Error retrieving governance events from table: %v", err)
	}
	for _, dbGovEvent := range dbGovEvents {
		govEvents = append(govEvents, dbGovEvent.DbToGovernanceData())
	}
	return govEvents, nil
}

func (p *PostgresPersister) govEventsQuery(tableName string) string {
	fieldNames, _ := postgres.StructFieldsForQuery(postgres.GovernanceEvent{}, false)
	queryString := fmt.Sprintf("SELECT %s FROM %s WHERE listing_address=$1", fieldNames, tableName) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) createGovernanceEventInTable(govEvent *model.GovernanceEvent, tableName string) error {
	dbGovEvent := postgres.NewGovernanceEvent(govEvent)
	queryString := p.insertIntoDBQueryString(tableName, postgres.GovernanceEvent{})
	_, err := p.db.NamedExec(queryString, dbGovEvent)
	if err != nil {
		return fmt.Errorf("Error saving GovernanceEvent to table: %v", err)
	}
	return nil
}

func (p *PostgresPersister) updateGovernanceEventInTable(govEvent *model.GovernanceEvent, updatedFields []string, tableName string) error {
	queryString, err := p.updateGovEventsQuery(updatedFields, tableName)
	if err != nil {
		return fmt.Errorf("Error creating query string for update: %v ", err)
	}
	dbGovEvent := postgres.NewGovernanceEvent(govEvent)
	_, err = p.db.NamedExec(queryString, dbGovEvent)
	if err != nil {
		return fmt.Errorf("Error updating fields in db: %v", err)
	}
	return nil
}

func (p *PostgresPersister) updateGovEventsQuery(updatedFields []string, tableName string) (string, error) {
	queryString, err := p.updateDBQueryBuffer(updatedFields, tableName, postgres.GovernanceEvent{})
	if err != nil {
		return "", err
	}
	queryString.WriteString(" WHERE event_hash=:event_hash;") // nolint: gosec
	return queryString.String(), nil
}

func (p *PostgresPersister) deleteGovenanceEventFromTable(govEvent *model.GovernanceEvent, tableName string) error {
	dbGovEvent := postgres.NewGovernanceEvent(govEvent)
	queryString := p.deleteGovEventQuery(tableName)
	_, err := p.db.NamedExec(queryString, dbGovEvent)
	if err != nil {
		return fmt.Errorf("Error deleting governanceEvent in db: %v", err)
	}
	return nil
}

func (p *PostgresPersister) deleteGovEventQuery(tableName string) string {
	queryString := fmt.Sprintf("DELETE FROM %s WHERE event_hash=:event_hash;", tableName) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) lastCronTimestampFromTable(tableName string) (int64, error) {
	// First make sure that the database has one row:
	numRowsb, err := p.countNumRows(tableName)
	if err != nil {
		return 0, fmt.Errorf("Error counting rows in cron table before update: %v", err)
	}

	cronData := postgres.CronData{}
	if numRowsb == 0 {
		queryStringInit := fmt.Sprintf("INSERT into %s(timestamp) values(0)", tableName) // nolint: gosec
		_, err = p.db.Exec(queryStringInit)
		if err != nil {
			return cronData.Timestamp, fmt.Errorf("Error saving timestamp to table: %v", err)
		}
	}
	queryString := fmt.Sprintf("SELECT * from %s", tableName) // nolint: gosec
	err = p.db.Get(&cronData, queryString)
	if err != nil {
		return cronData.Timestamp, err
	}
	return cronData.Timestamp, nil
}

func (p *PostgresPersister) updateCronTimestampInTable(timestamp int64, tableName string) error {
	// First make sure that the database has one row:
	numRowsb, err := p.countNumRows(tableName)
	if err != nil {
		return fmt.Errorf("Error counting rows in cron table before update: %v", err)
	}

	cronData := postgres.NewCron(timestamp)
	if numRowsb == 0 {
		queryString := fmt.Sprintf("INSERT INTO %s(timestamp) values (:timestamp);", tableName) // nolint: gosec
		_, err := p.db.NamedExec(queryString, cronData)
		if err != nil {
			return fmt.Errorf("Error saving listing to table: %v", err)
		}
	} else {
		updatedFields := []string{"timestamp"}
		queryString, err := p.updateDBQueryBuffer(updatedFields, tableName, cronData)
		if err != nil {
			return err
		}
		_, err = p.db.NamedExec(queryString.String(), cronData)
		if err != nil {
			return fmt.Errorf("Error updating fields in db: %v", err)
		}
	}

	return nil
}

func (p *PostgresPersister) countNumRows(tableName string) (int, error) {
	var numRowsb int
	queryString := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tableName) // nolint: gosec
	// NOTE: connection is closed upon Scan
	err := p.db.QueryRow(queryString).Scan(&numRowsb)
	if err != nil {
		return 0, fmt.Errorf("Problem getting count from table: %v", err)
	}
	return numRowsb, err
}
