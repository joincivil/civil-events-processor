package postgres

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/joincivil/go-common/pkg/numbers"

	"github.com/joincivil/civil-events-processor/pkg/model"
	cpostgres "github.com/joincivil/go-common/pkg/persistence/postgres"
)

const (
	defaultTokenPurchaseTableName = "token_purchase"
)

// CreateTokenPurchaseTableQuery returns the query to create the token_purchase table
func CreateTokenPurchaseTableQuery() string {
	return CreateTokenPurchaseTableQueryString(defaultTokenPurchaseTableName)
}

// CreateTokenPurchaseTableQueryString returns the query to create the token_purchase table
func CreateTokenPurchaseTableQueryString(tableName string) string {
	queryString := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s(
			purchaser_address TEXT,
			source_address TEXT,
			amount NUMERIC,
			purchase_date INT,
			block_data JSONB
		);
	`, tableName)
	return queryString
}

// CreateTokenPurchaseEventTableIndices returns the query to create indices for this table
func CreateTokenPurchaseEventTableIndices() string {
	return CreateTokenPurchaseEventTableIndicesString(defaultTokenPurchaseTableName)
}

// CreateTokenPurchaseEventTableIndicesString returns the query to create indices for this table
func CreateTokenPurchaseEventTableIndicesString(tableName string) string {
	// queryString := fmt.Sprintf(`
	// `, tableName, tableName)
	// return queryString
	return ""
}

// NewTokenPurchase creates a new postgres TokenPurchase from model.TokenPurchase
func NewTokenPurchase(tokenPurchase *model.TokenPurchase) *TokenPurchase {
	purchase := &TokenPurchase{}
	purchase.PurchaserAddress = tokenPurchase.PurchaserAddress().Hex()
	purchase.SourceAddress = tokenPurchase.SourceAddress().Hex()
	purchase.Amount = numbers.BigIntToFloat64(tokenPurchase.Amount())
	purchase.PurchaseDate = tokenPurchase.PurchaseDate()
	purchase.BlockData = make(cpostgres.JsonbPayload)
	purchase.fillBlockData(tokenPurchase.BlockData())
	return purchase
}

// TokenPurchase is the postgres definition of a model.TokenPurchase
type TokenPurchase struct {
	PurchaserAddress string `db:"purchaser_address"`

	SourceAddress string `db:"source_address"`

	Amount float64 `db:"amount"` // Amount in token, not gwei

	PurchaseDate int64 `db:"purchase_date"`

	BlockData cpostgres.JsonbPayload `db:"block_data"`
}

// DbToTokenPurchase creates a model.TokenPurchase from a postgres.TokenPurchase
func (t *TokenPurchase) DbToTokenPurchase() *model.TokenPurchase {
	params := &model.TokenPurchaseParams{}
	params.PurchaserAddress = common.HexToAddress(t.PurchaserAddress)
	params.SourceAddress = common.HexToAddress(t.SourceAddress)
	params.Amount = numbers.Float64ToBigInt(t.Amount)
	params.PurchaseDate = t.PurchaseDate

	params.BlockNumber = uint64(t.BlockData["blockNumber"].(float64))
	params.BlockHash = common.HexToHash(t.BlockData["blockHash"].(string))
	params.TxHash = common.HexToHash(t.BlockData["txHash"].(string))
	// NOTE: TxIndex is stored in DB as float64
	params.TxIndex = uint(t.BlockData["txIndex"].(float64))
	// NOTE: Index is stored in DB as float64
	params.Index = uint(t.BlockData["index"].(float64))

	return model.NewTokenPurchase(params)
}

func (t *TokenPurchase) fillBlockData(blockData model.BlockData) {
	t.BlockData["blockNumber"] = blockData.BlockNumber()
	t.BlockData["txHash"] = blockData.TxHash()
	t.BlockData["txIndex"] = blockData.TxIndex()
	t.BlockData["blockHash"] = blockData.BlockHash()
	t.BlockData["index"] = blockData.Index()
}
