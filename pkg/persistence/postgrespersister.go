// Package persistence contains components to interact with the DB
package persistence // import "github.com/joincivil/civil-events-processor/pkg/persistence"

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jmoiron/sqlx"
	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/persistence/postgres"
	"math/big"
	// driver for postgresql
	_ "github.com/lib/pq"
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
	return pgPersister, nil
}

// PostgresPersister holds the DB connection and persistence
type PostgresPersister struct {
	db *sqlx.DB
}

// CreateTables creates the tables for processor if they don't exist
func (p *PostgresPersister) CreateTables() error {
	// this needs to get all the event tables for processor
	contentRevisionSchema := postgres.ContentRevisionSchema()
	governanceEventSchema := postgres.GovernanceEventSchema()
	listingSchema := postgres.ListingSchema()

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
// TODO(IS): batch query
func (p *PostgresPersister) ListingsByAddresses(addresses []common.Address) ([]*model.Listing, error) {
	listings := []*model.Listing{}
	for _, address := range addresses {
		listing, err := p.ListingByAddress(address)
		if err != nil {
			return listings, err
		}
		listings = append(listings, listing)
	}
	return listings, nil
}

// ListingByAddress retrieves listings based on addresses
func (p *PostgresPersister) ListingByAddress(address common.Address) (*model.Listing, error) {
	listing := &model.Listing{}
	queryString := p.listingByAddressQuery("listing")
	dbListing, err := p.getListingFromTableByAddress(queryString, address)
	if err != nil {
		return listing, fmt.Errorf("Wasn't able to get listing from postgres table: %v", err)
	}
	listing = dbListing.DbToListingData()
	return listing, err
}

// CreateListing creates a new listing
func (p *PostgresPersister) CreateListing(listing *model.Listing) error {
	queryString := p.insertIntoDBQueryString("listing", postgres.Listing{})
	return p.saveListingToTable(queryString, listing)
}

// UpdateListing updates fields on an existing listing
func (p *PostgresPersister) UpdateListing(listing *model.Listing, updatedFields []string) error {
	queryString, err := p.updateListingQuery(updatedFields, "listing")
	if err != nil {
		return fmt.Errorf("Error creating query string for update: %v ", err)
	}
	// get values to fill in query
	dbListing := postgres.NewListing(listing)
	_, err = p.db.NamedQuery(queryString, dbListing)
	if err != nil {
		return fmt.Errorf("Error updating fields in db: %v", err)
	}
	return nil
}

// DeleteListing removes a listing
func (p *PostgresPersister) DeleteListing(listing *model.Listing) error {
	dbListing := postgres.NewListing(listing)
	queryString := p.deleteListingQuery("listing")
	_, err := p.db.NamedQuery(queryString, dbListing)
	if err != nil {
		return fmt.Errorf("Error deleting listing in db: %v", err)
	}
	return nil
}

// CreateContentRevision creates a new content revision
func (p *PostgresPersister) CreateContentRevision(revision *model.ContentRevision) error {
	queryString := p.insertIntoDBQueryString("content_revision", postgres.ContentRevision{})
	return p.saveContentRevisionToTable(queryString, revision)
}

// ContentRevision retrieves a specific content revision for newsroom content
func (p *PostgresPersister) ContentRevision(address common.Address, contentID *big.Int, revisionID *big.Int) (*model.ContentRevision, error) {
	contRev := &model.ContentRevision{}
	queryString := p.contentRevisionQuery("content_revision")
	dbContRev, err := p.getContentRevisionFromTable(queryString, address.Hex(), contentID.Int64(), revisionID.Int64())
	if err != nil {
		return contRev, fmt.Errorf("Wasn't able to get ContentRevision from postgres table: %v", err)
	}
	contRev = dbContRev.DbToContentRevisionData()
	return contRev, err
}

// ContentRevisions retrieves the revisions for content on a listing
func (p *PostgresPersister) ContentRevisions(address common.Address, contentID *big.Int) ([]*model.ContentRevision, error) {
	contRevs := []*model.ContentRevision{}
	queryString := p.contentRevisionsQuery("content_revision")
	dbContRevs, err := p.getContentRevisionsFromTable(queryString, address.Hex(), contentID.Int64())
	if err != nil {
		return contRevs, fmt.Errorf("Wasn't able to get ContentRevisions from postgres table: %v", err)
	}
	for i, dbContRev := range dbContRevs {
		contRevs[i] = dbContRev.DbToContentRevisionData()
	}
	return contRevs, err
}

// UpdateContentRevision updates fields on an existing content revision
func (p *PostgresPersister) UpdateContentRevision(revision *model.ContentRevision, updatedFields []string) error {
	queryString, err := p.updateContentRevisionQuery(updatedFields, "listing")
	if err != nil {
		return fmt.Errorf("Error creating query string for update: %v ", err)
	}
	// get values to fill in query
	dbContentRevision := postgres.NewContentRevision(revision)
	_, err = p.db.NamedQuery(queryString, dbContentRevision)
	if err != nil {
		return fmt.Errorf("Error updating fields in db: %v", err)
	}
	return nil
}

// DeleteContentRevision removes a content revision
func (p *PostgresPersister) DeleteContentRevision(revision *model.ContentRevision) error {
	dbContRev := postgres.NewContentRevision(revision)
	queryString := p.deleteContentRevisionQuery("content_revision")
	_, err := p.db.NamedQuery(queryString, dbContRev)
	if err != nil {
		return fmt.Errorf("Error deleting content revision in db: %v", err)
	}
	return nil
}

// GovernanceEventsByListingAddress retrieves governance events based on criteria
func (p *PostgresPersister) GovernanceEventsByListingAddress(address common.Address) ([]*model.GovernanceEvent, error) {
	govEvents := []*model.GovernanceEvent{}
	queryString := p.govEventsQuery("governance_event")
	dbGovEvents, err := p.getGovEventsFromTable(queryString, address.Hex())
	for i, dbGovEvent := range dbGovEvents {
		govEvents[i] = dbGovEvent.DbToGovernanceData()
	}
	return govEvents, err
}

// CreateGovernanceEvent creates a new governance event
func (p *PostgresPersister) CreateGovernanceEvent(govEvent *model.GovernanceEvent) error {
	queryString := p.insertIntoDBQueryString("governance_event", postgres.GovernanceEvent{})
	return p.saveGovEventToTable(queryString, govEvent)
}

// UpdateGovernanceEvent updates fields on an existing governance event
// NOTE(IS): are we updating governance events? yes, this is just a generic update
func (p *PostgresPersister) UpdateGovernanceEvent(govEvent *model.GovernanceEvent, updatedFields []string) error {
	queryString, err := p.updateGovEventsQuery(updatedFields, "governance_event")
	if err != nil {
		return fmt.Errorf("Error creating query string for update: %v ", err)
	}
	// get values to fill in query
	dbGovEventRevision := postgres.NewGovernanceEvent(govEvent)
	_, err = p.db.NamedQuery(queryString, dbGovEventRevision)
	if err != nil {
		return fmt.Errorf("Error updating fields in db: %v", err)
	}
	return nil
}

// DeleteGovenanceEvent removes a governance event
func (p *PostgresPersister) DeleteGovenanceEvent(govEvent *model.GovernanceEvent) error {
	dbGovEvent := postgres.NewGovernanceEvent(govEvent)
	queryString := p.deleteGovEventQuery("governance_event")
	_, err := p.db.NamedQuery(queryString, dbGovEvent)
	if err != nil {
		return fmt.Errorf("Error deleting governanceEvent in db: %v", err)
	}
	return nil
}

func (p *PostgresPersister) insertIntoDBQueryString(tableName string, dbModelStruct interface{}) string {
	fieldNames, fieldNamesColon := postgres.GetAllStructFieldsForQuery(dbModelStruct, true)
	queryString := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s);", tableName, fieldNames, fieldNamesColon)
	return queryString
}

func (p *PostgresPersister) updateDBQueryBuffer(updatedFields []string, tableName string, dbModelStruct interface{}) (bytes.Buffer, error) {
	var queryString bytes.Buffer
	queryString.WriteString("UPDATE ")
	queryString.WriteString(tableName)
	queryString.WriteString(" SET ")
	for idx, field := range updatedFields {
		dbFieldName, err := postgres.DbFieldNameFromModelName(dbModelStruct, field)
		if err != nil {
			return queryString, fmt.Errorf("Error getting %s from %s table DB struct tag: %v", field, tableName, err)
		}
		queryString.WriteString(fmt.Sprintf("%s=:%s", dbFieldName, dbFieldName))
		if idx+1 < len(updatedFields) {
			queryString.WriteString(", ")
		}
	}
	return queryString, nil
}

func (p *PostgresPersister) getListingFromTableByAddress(query string, address common.Address) (*postgres.Listing, error) {
	dbListing := postgres.Listing{}
	err := p.db.Get(&dbListing, query, address.Hex())
	return &dbListing, err
}

func (p *PostgresPersister) listingByAddressQuery(tableName string) string {
	fieldNames, _ := postgres.GetAllStructFieldsForQuery(postgres.Listing{}, false)
	queryString := fmt.Sprintf("SELECT %s FROM %s WHERE contract_address=$1;", fieldNames, tableName)
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
	queryString.WriteString(" WHERE contract_address=:contract_address;")
	return queryString.String(), nil
}

func (p *PostgresPersister) deleteListingQuery(tableName string) string {
	queryString := fmt.Sprintf("DELETE FROM %s WHERE contract_address=:contract_address", tableName)
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
	fieldNames, _ := postgres.GetAllStructFieldsForQuery(postgres.ContentRevision{}, false)
	queryString := fmt.Sprintf("SELECT %s FROM %s WHERE (listing_address=$1 AND contract_content_id=$2 AND contract_revision_id=$3)", fieldNames, tableName)
	return queryString
}

func (p *PostgresPersister) getContentRevisionFromTable(query string, address string, contentID int64, revisionID int64) (postgres.ContentRevision, error) {
	dbContRev := postgres.ContentRevision{}
	err := p.db.Get(&dbContRev, query, address, contentID, revisionID)
	return dbContRev, err
}

func (p *PostgresPersister) contentRevisionsQuery(tableName string) string {
	fieldNames, _ := postgres.GetAllStructFieldsForQuery(postgres.ContentRevision{}, false)
	queryString := fmt.Sprintf("SELECT %s FROM %s WHERE (listing_address=$1 AND contract_content_id=$2)", fieldNames, tableName)
	return queryString
}

func (p *PostgresPersister) getContentRevisionsFromTable(query string, address string, contentID int64) ([]postgres.ContentRevision, error) {
	dbContRevs := []postgres.ContentRevision{}
	err := p.db.Select(&dbContRevs, query, address, contentID)
	return dbContRevs, err
}

func (p *PostgresPersister) updateContentRevisionQuery(updatedFields []string, tableName string) (string, error) {
	queryString, err := p.updateDBQueryBuffer(updatedFields, tableName, postgres.ContentRevision{})
	if err != nil {
		return "", err
	}
	queryString.WriteString(" WHERE listing_address=:listing_address AND contract_content_id=:contract_content_id AND contract_revision_id=:contract_revision_id;")
	return queryString.String(), nil
}

func (p *PostgresPersister) deleteContentRevisionQuery(tableName string) string {
	queryString := fmt.Sprintf("DELETE FROM %s WHERE (listing_address=:listing_address AND contract_content_id=:contract_content_id AND contract_revision_id=:contract_revision_id)", tableName)
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

func (p *PostgresPersister) getGovEventsFromTable(query string, address string) ([]postgres.GovernanceEvent, error) {
	dbGovEvents := []postgres.GovernanceEvent{}
	err := p.db.Select(&dbGovEvents, query, address)
	return dbGovEvents, err
}

func (p *PostgresPersister) updateGovEventsQuery(updatedFields []string, tableName string) (string, error) {
	queryString, err := p.updateDBQueryBuffer(updatedFields, tableName, postgres.GovernanceEvent{})
	if err != nil {
		return "", err
	}
	// TODO (IS): determine parameters to identify gov event
	queryString.WriteString(" WHERE listing_address=:listing_address;")
	return queryString.String(), nil
}

func (p *PostgresPersister) govEventsQuery(tableName string) string {
	fieldNames, _ := postgres.GetAllStructFieldsForQuery(postgres.GovernanceEvent{}, false)
	queryString := fmt.Sprintf("SELECT %s FROM %s WHERE listing_address=$1", fieldNames, tableName)
	return queryString
}

// TODO (IS): need some hash to identify the gov event.. add hash from event?
func (p *PostgresPersister) deleteGovEventQuery(tableName string) string {
	queryString := fmt.Sprintf("DELETE FROM %s WHERE listing_address=:listing_address", tableName)
	return queryString
}
