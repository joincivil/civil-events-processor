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

// CreateTables creates the tables for processor if they don't exist
func (p *PostgresPersister) CreateTables() error {
	// this needs to get all the event tables for processor
	contentRevisionSchema := postgres.CreateContentRevisionTableQuery()
	governanceEventSchema := postgres.CreateGovernanceEventTableQuery()
	listingSchema := postgres.CreateListingTableQuery()

	_, err := p.db.Exec(contentRevisionSchema)
	if err != nil {
		return fmt.Errorf("Error creating content_revision table in postgres: %v", err)
	}
	_, err = p.db.Exec(governanceEventSchema)
	if err != nil {
		return fmt.Errorf("Error creating governance_event table in postgres: %v", err)
	}
	_, err = p.db.Exec(listingSchema)
	if err != nil {
		return fmt.Errorf("Error creating listing table in postgres: %v", err)
	}
	return err
}

// ListingsByAddresses returns a slice of Listings based on addresses
func (p *PostgresPersister) ListingsByAddresses(addresses []common.Address) ([]*model.Listing, error) {
	listings := []*model.Listing{}
	for _, address := range addresses {
		listing, err := p.ListingByAddress(address)
		if err != nil {
			if err == sql.ErrNoRows {
				return listings, model.ErrPersisterNoResults
			}
			return listings, fmt.Errorf("Wasn't able to get listings from postgres table: %v", err)
		}
		listings = append(listings, listing)
	}
	return listings, nil
}

// ListingByAddress retrieves listings based on addresses
func (p *PostgresPersister) ListingByAddress(address common.Address) (*model.Listing, error) {
	queryString := p.listingByAddressQuery(listingTableName)
	dbListing, err := p.listingFromTableByAddress(queryString, address.Hex())
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, model.ErrPersisterNoResults
		}
		return nil, fmt.Errorf("Wasn't able to get listing from postgres table: %v", err)
	}
	listing := dbListing.DbToListingData()
	return listing, err
}

// CreateListing creates a new listing
func (p *PostgresPersister) CreateListing(listing *model.Listing) error {
	queryString := p.insertIntoDBQueryString(listingTableName, postgres.Listing{})
	return p.saveListingToTable(queryString, listing)
}

// UpdateListing updates fields on an existing listing
func (p *PostgresPersister) UpdateListing(listing *model.Listing, updatedFields []string) error {
	queryString, err := p.updateListingQuery(updatedFields, listingTableName)
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

// DeleteListing removes a listing
func (p *PostgresPersister) DeleteListing(listing *model.Listing) error {
	dbListing := postgres.NewListing(listing)
	queryString := p.deleteListingQuery(listingTableName)
	_, err := p.db.NamedExec(queryString, dbListing)
	if err != nil {
		return fmt.Errorf("Error deleting listing in db: %v", err)
	}
	return nil
}

// CreateContentRevision creates a new content revision
func (p *PostgresPersister) CreateContentRevision(revision *model.ContentRevision) error {
	queryString := p.insertIntoDBQueryString(contRevTableName, postgres.ContentRevision{})
	return p.saveContentRevisionToTable(queryString, revision)
}

// ContentRevision retrieves a specific content revision for newsroom content
func (p *PostgresPersister) ContentRevision(address common.Address, contentID *big.Int, revisionID *big.Int) (*model.ContentRevision, error) {
	queryString := p.contentRevisionQuery(contRevTableName)
	dbContRev, err := p.contentRevisionFromTable(queryString, address.Hex(), contentID.Int64(), revisionID.Int64())
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, model.ErrPersisterNoResults
		}
		return nil, fmt.Errorf("Wasn't able to get ContentRevision from postgres table: %v", err)
	}
	contRev := dbContRev.DbToContentRevisionData()
	return contRev, err
}

// ContentRevisions retrieves the revisions for content on a listing
func (p *PostgresPersister) ContentRevisions(address common.Address, contentID *big.Int) ([]*model.ContentRevision, error) {
	contRevs := []*model.ContentRevision{}
	queryString := p.contentRevisionsQuery(contRevTableName)
	dbContRevs, err := p.contentRevisionsFromTable(queryString, address.Hex(), contentID.Int64())
	if err != nil {
		if err == sql.ErrNoRows {
			return contRevs, model.ErrPersisterNoResults
		}
		return contRevs, fmt.Errorf("Wasn't able to get ContentRevisions from postgres table: %v", err)
	}
	for _, dbContRev := range dbContRevs {
		contRevs = append(contRevs, dbContRev.DbToContentRevisionData())
	}
	return contRevs, err
}

// UpdateContentRevision updates fields on an existing content revision
func (p *PostgresPersister) UpdateContentRevision(revision *model.ContentRevision, updatedFields []string) error {
	queryString, err := p.updateContentRevisionQuery(updatedFields, contRevTableName)
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

// DeleteContentRevision removes a content revision
func (p *PostgresPersister) DeleteContentRevision(revision *model.ContentRevision) error {
	dbContRev := postgres.NewContentRevision(revision)
	queryString := p.deleteContentRevisionQuery(contRevTableName)
	_, err := p.db.NamedExec(queryString, dbContRev)
	if err != nil {
		return fmt.Errorf("Error deleting content revision in db: %v", err)
	}
	return nil
}

// GovernanceEventsByListingAddress retrieves governance events based on criteria
func (p *PostgresPersister) GovernanceEventsByListingAddress(address common.Address) ([]*model.GovernanceEvent, error) {
	govEvents := []*model.GovernanceEvent{}
	queryString := p.govEventsQuery(govEventTableName)
	dbGovEvents, err := p.govEventsFromTable(queryString, address.Hex())
	if err == sql.ErrNoRows {
		err = model.ErrPersisterNoResults
	}
	for i, dbGovEvent := range dbGovEvents {
		govEvents[i] = dbGovEvent.DbToGovernanceData()
	}
	return govEvents, err
}

// CreateGovernanceEvent creates a new governance event
func (p *PostgresPersister) CreateGovernanceEvent(govEvent *model.GovernanceEvent) error {
	queryString := p.insertIntoDBQueryString(govEventTableName, postgres.GovernanceEvent{})
	return p.saveGovEventToTable(queryString, govEvent)
}

// UpdateGovernanceEvent updates fields on an existing governance event based on eventHash
func (p *PostgresPersister) UpdateGovernanceEvent(govEvent *model.GovernanceEvent, updatedFields []string) error {
	queryString, err := p.updateGovEventsQuery(updatedFields, govEventTableName)
	if err != nil {
		return fmt.Errorf("Error creating query string for update: %v ", err)
	}
	// get values to fill in query
	dbGovEvent := postgres.NewGovernanceEvent(govEvent)
	_, err = p.db.NamedExec(queryString, dbGovEvent)
	if err != nil {
		return fmt.Errorf("Error updating fields in db: %v", err)
	}
	return nil
}

// DeleteGovenanceEvent removes a governance event based on eventHash
func (p *PostgresPersister) DeleteGovenanceEvent(govEvent *model.GovernanceEvent) error {
	dbGovEvent := postgres.NewGovernanceEvent(govEvent)
	queryString := p.deleteGovEventQuery(govEventTableName)
	_, err := p.db.NamedExec(queryString, dbGovEvent)
	if err != nil {
		return fmt.Errorf("Error deleting governanceEvent in db: %v", err)
	}
	return nil
}

func (p *PostgresPersister) insertIntoDBQueryString(tableName string, dbModelStruct interface{}) string {
	fieldNames, fieldNamesColon := postgres.StructFieldsForQuery(dbModelStruct, true)
	queryString := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s);", tableName, fieldNames, fieldNamesColon) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) updateDBQueryBuffer(updatedFields []string, tableName string, dbModelStruct interface{}) (bytes.Buffer, error) {
	var queryString bytes.Buffer
	queryString.WriteString("UPDATE ") // nolint: gosec
	queryString.WriteString(tableName) // nolint: gosec
	queryString.WriteString(" SET ")   // nolint: gosec
	for idx, field := range updatedFields {
		dbFieldName, err := postgres.DbFieldNameFromModelName(dbModelStruct, field)
		if err != nil {
			return queryString, fmt.Errorf("Error getting %s from %s table DB struct tag: %v", field, tableName, err)
		}
		queryString.WriteString(fmt.Sprintf("%s=:%s", dbFieldName, dbFieldName)) // nolint: gosec
		if idx+1 < len(updatedFields) {
			queryString.WriteString(", ") // nolint: gosec
		}
	}
	return queryString, nil
}

func (p *PostgresPersister) listingFromTableByAddress(query string, address string) (*postgres.Listing, error) {
	dbListing := postgres.Listing{}
	err := p.db.Get(&dbListing, query, address)
	return &dbListing, err
}

func (p *PostgresPersister) listingByAddressQuery(tableName string) string {
	fieldNames, _ := postgres.StructFieldsForQuery(postgres.Listing{}, false)
	queryString := fmt.Sprintf("SELECT %s FROM %s WHERE contract_address=$1;", fieldNames, tableName) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) saveListingToTable(query string, listing *model.Listing) error {
	dbListing := postgres.NewListing(listing)
	_, err := p.db.NamedExec(query, dbListing)
	if err != nil {
		return fmt.Errorf("Error saving listing to table: %v", err)
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

func (p *PostgresPersister) deleteListingQuery(tableName string) string {
	queryString := fmt.Sprintf("DELETE FROM %s WHERE contract_address=:contract_address", tableName) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) saveContentRevisionToTable(query string, revision *model.ContentRevision) error {
	dbContRev := postgres.NewContentRevision(revision)
	_, err := p.db.NamedExec(query, dbContRev)
	if err != nil {
		return fmt.Errorf("Error saving contentRevision to table: %v", err)
	}
	return nil
}

func (p *PostgresPersister) contentRevisionQuery(tableName string) string {
	fieldNames, _ := postgres.StructFieldsForQuery(postgres.ContentRevision{}, false)
	queryString := fmt.Sprintf("SELECT %s FROM %s WHERE (listing_address=$1 AND contract_content_id=$2 AND contract_revision_id=$3)", fieldNames, tableName) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) contentRevisionFromTable(query string, address string, contentID int64, revisionID int64) (postgres.ContentRevision, error) {
	dbContRev := postgres.ContentRevision{}
	err := p.db.Get(&dbContRev, query, address, contentID, revisionID)
	return dbContRev, err
}

func (p *PostgresPersister) contentRevisionsQuery(tableName string) string {
	fieldNames, _ := postgres.StructFieldsForQuery(postgres.ContentRevision{}, false)
	queryString := fmt.Sprintf("SELECT %s FROM %s WHERE (listing_address=$1 AND contract_content_id=$2)", fieldNames, tableName) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) contentRevisionsFromTable(query string, address string, contentID int64) ([]postgres.ContentRevision, error) {
	dbContRevs := []postgres.ContentRevision{}
	err := p.db.Select(&dbContRevs, query, address, contentID)
	return dbContRevs, err
}

func (p *PostgresPersister) updateContentRevisionQuery(updatedFields []string, tableName string) (string, error) {
	queryString, err := p.updateDBQueryBuffer(updatedFields, tableName, postgres.ContentRevision{})
	if err != nil {
		return "", err
	}
	queryString.WriteString(" WHERE (listing_address=:listing_address AND contract_content_id=:contract_content_id AND contract_revision_id=:contract_revision_id);") // nolint: gosec
	return queryString.String(), nil
}

func (p *PostgresPersister) deleteContentRevisionQuery(tableName string) string {
	queryString := fmt.Sprintf("DELETE FROM %s WHERE (listing_address=:listing_address AND contract_content_id=:contract_content_id AND contract_revision_id=:contract_revision_id)", tableName) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) saveGovEventToTable(query string, govEvent *model.GovernanceEvent) error {
	dbGovEvent := postgres.NewGovernanceEvent(govEvent)
	_, err := p.db.NamedExec(query, dbGovEvent)
	if err != nil {
		return fmt.Errorf("Error saving GovernanceEvent to table: %v", err)
	}
	return nil
}

func (p *PostgresPersister) govEventsFromTable(query string, address string) ([]postgres.GovernanceEvent, error) {
	dbGovEvents := []postgres.GovernanceEvent{}
	err := p.db.Select(&dbGovEvents, query, address)
	return dbGovEvents, err
}

func (p *PostgresPersister) updateGovEventsQuery(updatedFields []string, tableName string) (string, error) {
	queryString, err := p.updateDBQueryBuffer(updatedFields, tableName, postgres.GovernanceEvent{})
	if err != nil {
		return "", err
	}
	queryString.WriteString(" WHERE event_hash=:event_hash;") // nolint: gosec
	return queryString.String(), nil
}

func (p *PostgresPersister) govEventsQuery(tableName string) string {
	fieldNames, _ := postgres.StructFieldsForQuery(postgres.GovernanceEvent{}, false)
	queryString := fmt.Sprintf("SELECT %s FROM %s WHERE listing_address=$1", fieldNames, tableName) // nolint: gosec
	return queryString
}

func (p *PostgresPersister) deleteGovEventQuery(tableName string) string {
	queryString := fmt.Sprintf("DELETE FROM %s WHERE event_hash=:event_hash;", tableName) // nolint: gosec
	return queryString
}
