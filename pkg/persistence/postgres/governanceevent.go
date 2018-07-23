package postgres

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	crawlerpostgres "github.com/joincivil/civil-events-crawler/pkg/persistence/postgres"
	"github.com/joincivil/civil-events-processor/pkg/model"
)

// GovernanceEventSchema returns the query to create the governance_event table
func GovernanceEventSchema() string {
	return GovernanceEventSchemaString("governance_event")
}

// GovernanceEventSchemaString returns the query to create this table
func GovernanceEventSchemaString(tableName string) string {
	schema := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s(
            listing_address TEXT,
            sender_address TEXT,
            metadata JSONB,
            creation_date BIGINT,
            last_updated BIGINT
        );
    `, tableName)
	return schema
}

// GovernanceEvent is postgres definition of model.GovernanceEvent
type GovernanceEvent struct {
	ListingAddress string

	SenderAddress string

	Metadata crawlerpostgres.JsonbPayload

	GovernanceEventType string

	CreationDateTs int64

	LastUpdatedDateTs int64
}

// NewGovernanceEvent creates a new postgres GovernanceEvent
func (ge *GovernanceEvent) NewGovernanceEvent(governanceEvent *model.GovernanceEvent) *GovernanceEvent {
	listingAddress := governanceEvent.ListingAddress().Hex()
	senderAddress := governanceEvent.SenderAddress().Hex()
	metadata := crawlerpostgres.JsonbPayload(governanceEvent.Metadata())
	return &GovernanceEvent{
		ListingAddress:      listingAddress,
		SenderAddress:       senderAddress,
		Metadata:            metadata,
		GovernanceEventType: governanceEvent.GovernanceEventType(),
		CreationDateTs:      governanceEvent.CreationDateTs(),
		LastUpdatedDateTs:   governanceEvent.LastUpdatedDateTs(),
	}
}

// DbToGovernanceData creates a model.GovernanceEvent from postgres.GovernanceEvent
func (ge *GovernanceEvent) DbToGovernanceData() *model.GovernanceEvent {
	listingAddress := common.HexToAddress(ge.ListingAddress)
	senderAddress := common.HexToAddress(ge.SenderAddress)
	metadata := model.Metadata(ge.Metadata)
	return model.NewGovernanceEvent(listingAddress, senderAddress, metadata, ge.GovernanceEventType,
		ge.CreationDateTs, ge.LastUpdatedDateTs)
}
