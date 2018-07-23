// Package persistence contains components to interact with the DB
package persistence // import "github.com/joincivil/civil-events-processor/pkg/persistence"

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jmoiron/sqlx"
	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/persistence/postgres"
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
// is it possible to a batch query for this?
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
	queryString := p.createListingQueryString("listing")
	return p.saveListingToTable(queryString, listing)
}

// UpdateListing updates fields on an existing listing
func (p *PostgresPersister) UpdateListing(listing *model.Listing) error {
	return nil
}

// DeleteListing removes a listing
// need some way to reference the listing
func (p *PostgresPersister) DeleteListing(listing *model.Listing) error {
	return nil
}

// // ContentRevisions retrieves the revisions for content on a listing
// func (p *PostgresPersister) ContentRevisions(address common.Address, contentID uint64) ([]*model.ContentRevision, error) {

// }

// ContentRevision retrieves a specific content revision for newsroom content
func (p *PostgresPersister) ContentRevision(address common.Address, contentID uint64, revisionID uint64) (*model.ContentRevision, error) {
	contRev := &model.ContentRevision{}
	queryString := p.contentRevisionQuery("content_revision")
	dbContRev, err := p.getContentRevisionFromTable(queryString, address, contentID, revisionID)
	if err != nil {
		return contRev, fmt.Errorf("Wasn't able to get content revision from postgres table: %v", err)
	}
	contRev, err = dbContRev.DbToContentRevisionData()
	if err != nil {
		return contRev, fmt.Errorf("Wasn't able to convert db content revision to model.ContentRevision: %v", err)
	}
	return contRev, err
}

// CreateContentRevision creates a new content revision
func (p *PostgresPersister) CreateContentRevision(revision *model.ContentRevision) error {
	return nil
}

// UpdateContentRevision updates fields on an existing content revision
func (p *PostgresPersister) UpdateContentRevision(revision *model.ContentRevision) error {
	return nil
}

// DeleteContentRevision removes a content revision
func (p *PostgresPersister) DeleteContentRevision(revision *model.ContentRevision) error {
	return nil
}

func (p *PostgresPersister) getListingFromTableByAddress(query string, address common.Address) (*postgres.Listing, error) {
	dbListing := postgres.Listing{}
	err := p.db.Get(&dbListing, query, address.Hex())
	return &dbListing, err
}

func (p *PostgresPersister) listingByAddressQuery(tableName string) string {
	queryString := fmt.Sprintf("SELECT name, contract_address, whitelisted, last_governance_state, url, charter_uri, "+
		"owner_addresses, contributor_addresses, creation_timestamp, application_timestamp, approval_timestamp, last_updated_timestamp "+
		" FROM %s WHERE contract_address=$1;", tableName)
	return queryString
}

func (p *PostgresPersister) saveListingToTable(query string, listing *model.Listing) error {
	postgresListing := postgres.NewListing(listing)
	_, err := p.db.NamedExec(query, postgresListing)
	if err != nil {
		return fmt.Errorf("Error saving listing to table: %v", err)
	}
	return nil
}

func (p *PostgresPersister) createListingQueryString(tableName string) string {
	queryString := fmt.Sprintf("INSERT INTO %s (name, contract_address, whitelisted, last_governance_state, url, charter_uri, "+
		"owner_addresses, contributor_addresses, creation_timestamp, application_timestamp, approval_timestamp, last_updated_timestamp)"+
		" VALUES (:name, :contract_address, :whitelisted, :last_governance_state, :url, :charter_uri, :owner_addresses, :contributor_addresses, "+
		":creation_timestamp, :application_timestamp, :approval_timestamp, :last_updated_timestamp);", tableName)
	return queryString
}

func (p *PostgresPersister) contentRevisionQuery(tableName string) string {
	queryString := ""
	return queryString
}

func (p *PostgresPersister) getContentRevisionFromTable(query string, address common.Address, contentID uint64, revisionID uint64) (*postgres.ContentRevision, error) {
	postgresContRev := postgres.ContentRevision{}
	return &postgresContRev, nil
}
