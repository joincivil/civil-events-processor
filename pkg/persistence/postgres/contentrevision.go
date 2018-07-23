package postgres // import "github.com/joincivil/civil-events-processor/pkg/persistence/postgres"

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	crawlerpostgres "github.com/joincivil/civil-events-crawler/pkg/persistence/postgres"
	"github.com/joincivil/civil-events-processor/pkg/model"
)

// ContentRevisionSchema returns the query to create the content_revision table
func ContentRevisionSchema() string {
	return ContentRevisionSchemaString("content_revision")
}

// ContentRevisionSchemaString returns the query to create this table
func ContentRevisionSchemaString(tableName string) string {
	schema := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s(
            id SERIAL PRIMARY KEY,
            listing_address TEXT,
            article_payload JSONB,
            article_payload_hash TEXT,
            editor_address TEXT,
            contract_content_id BIGINT,
            contract_revision_id BIGINT,
            revision_uri TEXT,
            revision_timestamp BIGINT
        );
    `, tableName)
	return schema
}

// ContentRevision is the model for content_revision table in db
type ContentRevision struct {
	ListingAddress     string                       `db:"listing_address"`
	ArticlePayload     crawlerpostgres.JsonbPayload `db:"article_payload"`
	ArticlePayloadHash string                       `db:"article_payload_hash"`
	EditorAddress      string                       `db:"editor_address"`
	ContractContentID  uint64                       `db:"contract_content_id"`
	ContractRevisionID uint64                       `db:"contract_revision_id"`
	RevisionURI        string                       `db:"revision_uri"`
	RevisionDateTs     int64                        `db:"revision_timestamp"`
}

// NewContentRevision constructs a content_revision for DB from a model.ContentRevision
func NewContentRevision(contentRevision *model.ContentRevision) *ContentRevision {
	listingAddress := contentRevision.ListingAddress().Hex()
	articlePayload := crawlerpostgres.JsonbPayload(contentRevision.Payload())
	editorAddress := contentRevision.EditorAddress().Hex()
	return &ContentRevision{
		ListingAddress:     listingAddress,
		ArticlePayload:     articlePayload,
		ArticlePayloadHash: contentRevision.PayloadHash(),
		EditorAddress:      editorAddress,
		ContractContentID:  contentRevision.ContractContentID(),
		ContractRevisionID: contentRevision.ContractRevisionID(),
		RevisionURI:        contentRevision.RevisionURI(),
		RevisionDateTs:     contentRevision.RevisionDateTs(),
	}
}

// DbToContentRevisionData creates a model.ContentRevision from postgres ContentRevision
func (cr *ContentRevision) DbToContentRevisionData() (*model.ContentRevision, error) {
	listingAddress := common.HexToAddress(cr.ListingAddress)
	// TODO (IS): maybe should do a generic conversion of jsonb types back to map[string]interface{}
	payload := model.ArticlePayload(cr.ArticlePayload)
	editorAddress := common.HexToAddress(cr.EditorAddress)
	contractContentID := cr.ContractContentID
	contractRevisionID := cr.ContractRevisionID
	return model.NewContentRevision(listingAddress, payload, editorAddress, contractContentID,
		contractRevisionID, cr.RevisionURI, cr.RevisionDateTs)
}
