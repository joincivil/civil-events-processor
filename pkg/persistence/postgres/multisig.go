package postgres // import "github.com/joincivil/civil-events-processor/pkg/persistence/postgres"

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"

	"github.com/joincivil/civil-events-processor/pkg/model"
	cstrings "github.com/joincivil/go-common/pkg/strings"
)

const (
	// MultiSigTableBaseName is the type of table this code defines
	MultiSigTableBaseName = "multisig"
)

// CreateMultiSigTableQuery returns the query to create the multisig table
func CreateMultiSigTableQuery(tableName string) string {
	queryString := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s(
            contract_address TEXT PRIMARY KEY,
            owner_addresses TEXT,
        );
    `, tableName)
	return queryString
}

// MultiSig is the model definition for multisig table in crawler db
// NOTE(IS) : golang<->postgres doesn't support list of strings. for now, OwnerAddresses will be strings
type MultiSig struct {
	ContractAddress string `db:"contract_address"`

	// OwnerAddresses is a comma delimited string
	OwnerAddresses string `db:"owner_addresses"`
}

// NewMultiSig constructs a multisig for DB from a model.MultiSig
func NewMultiSig(multiSig *model.MultiSig) *MultiSig {
	ownerAddresses := cstrings.ListCommonAddressesToString(multiSig.OwnerAddresses())

	var contractAddress string
	if multiSig.ContractAddress() != (common.Address{}) {
		contractAddress = multiSig.ContractAddress().Hex()
	}

	return &MultiSig{
		ContractAddress: contractAddress,
		OwnerAddresses:  ownerAddresses,
	}
}

// DbToMultiSigData creates a model.MultiSig from postgres MultiSig
func (m *MultiSig) DbToMultiSigData() *model.MultiSig {
	contractAddress := common.HexToAddress(m.ContractAddress)
	ownerAddresses := cstrings.StringToCommonAddressesList(m.OwnerAddresses)

	multiSigParams := &model.NewMultiSigParams{
		ContractAddress: contractAddress,
		OwnerAddresses:  ownerAddresses,
	}
	return model.NewMultiSig(multiSigParams)
}
