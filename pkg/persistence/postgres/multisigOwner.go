package postgres // import "github.com/joincivil/civil-events-processor/pkg/persistence/postgres"

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"

	"github.com/joincivil/civil-events-processor/pkg/model"
)

const (
	// MultiSigOwnerTableBaseName is the type of table this code defines
	MultiSigOwnerTableBaseName = "multisig_owner"
)

// CreateMultiSigOwnerTableQuery returns the query to create the multisig owner table
func CreateMultiSigOwnerTableQuery(tableName string) string {
	queryString := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s(
			key TEXT PRIMARY KEY,
            owner_address TEXT,
            multi_sig_address TEXT
        );
    `, tableName)
	return queryString
}

// CreateMultiSigOwnerTableIndicesQuery returns the query to create indices for this table
func CreateMultiSigOwnerTableIndicesQuery(tableName string) string {
	queryString := fmt.Sprintf(`
        CREATE INDEX IF NOT EXISTS owner_address_idx ON %s (owner_address);
        CREATE INDEX IF NOT EXISTS multi_sig_address_idx ON %s (multi_sig_address);
    `, tableName, tableName)
	return queryString
}

// MultiSigOwner is the model definition for multisig owner table in crawler db
type MultiSigOwner struct {
	Key string `db:"key"`

	OwnerAddress string `db:"owner_address"`

	MultiSigAddress string `db:"multi_sig_address"`
}

// NewMultiSigOwner constructs a multisig owner for DB from a model.MultiSigOwner
func NewMultiSigOwner(multiSigOwner *model.MultiSigOwner) *MultiSigOwner {
	key := multiSigOwner.Key()
	ownerAddress := multiSigOwner.OwnerAddress().Hex()
	multiSigAddress := multiSigOwner.MultiSigAddress().Hex()

	return &MultiSigOwner{
		Key:             key,
		OwnerAddress:    ownerAddress,
		MultiSigAddress: multiSigAddress,
	}
}

// DbToMultiSigOwnerData creates a model.MultiSig from postgres MultiSig
func (m *MultiSigOwner) DbToMultiSigOwnerData() *model.MultiSigOwner {
	key := m.Key
	ownerAddress := common.HexToAddress(m.OwnerAddress)
	multiSigAddress := common.HexToAddress(m.MultiSigAddress)

	multiSigOwnerParams := &model.NewMultiSigOwnerParams{
		Key:             key,
		OwnerAddress:    ownerAddress,
		MultiSigAddress: multiSigAddress,
	}
	return model.NewMultiSigOwner(multiSigOwnerParams)
}
