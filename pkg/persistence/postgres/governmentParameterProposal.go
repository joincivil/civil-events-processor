package postgres

import (
	"fmt"
	"math/big"

	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/go-common/pkg/bytes"
	"github.com/joincivil/go-common/pkg/numbers"
)

const (
	// GovernmentParameterProposalTableBaseName is the table name for this model
	GovernmentParameterProposalTableBaseName = "government_parameter_proposal"
)

// CreateGovernmentParameterProposalTableQuery returns the query to create this table
func CreateGovernmentParameterProposalTableQuery(tableName string) string {
	queryString := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s(
            id TEXT PRIMARY KEY,
            prop_id TEXT,
            name TEXT,
            value NUMERIC,
            app_expiry INT,
            poll_id INT,
            accepted BOOL,
            expired BOOL,
            last_updated_timestamp INT
        );
    `, tableName)
	return queryString
}

// GovernmentParameterProposal is postgres definition of model.GovernmentParameterProposal
type GovernmentParameterProposal struct {
	ID string `db:"id"`

	PropID string `db:"prop_id"`

	Name string `db:"name"`

	Value float64 `db:"value"`

	AppExpiry int64 `db:"app_expiry"`

	PollID int64 `db:"poll_id"`

	Accepted bool `db:"accepted"`

	Expired bool `db:"expired"`

	LastUpdatedDateTs int64 `db:"last_updated_timestamp"`
}

// NewGovernmentParameterProposal is the model definition for government_parameter_proposal table
func NewGovernmentParameterProposal(govtParameterProposal *model.GovernmentParameterProposal) *GovernmentParameterProposal {
	value := numbers.BigIntToFloat64(govtParameterProposal.Value())
	propID := bytes.Byte32ToHexString(govtParameterProposal.PropID())
	return &GovernmentParameterProposal{
		ID:                govtParameterProposal.ID(),
		Name:              govtParameterProposal.Name(),
		Value:             value,
		PropID:            propID,
		AppExpiry:         govtParameterProposal.AppExpiry().Int64(),
		PollID:            govtParameterProposal.PollID().Int64(),
		Accepted:          govtParameterProposal.Accepted(),
		Expired:           govtParameterProposal.Expired(),
		LastUpdatedDateTs: govtParameterProposal.LastUpdatedDateTs(),
	}
}

// DbToGovernmentParameterProposalData creates a model.GovernmentParameterProposal from postgres GovernmentParameterProposal
func (p *GovernmentParameterProposal) DbToGovernmentParameterProposalData() (*model.GovernmentParameterProposal, error) {
	value := numbers.Float64ToBigInt(p.Value)
	propID, err := bytes.HexStringToByte32(p.PropID)
	if err != nil {
		return nil, err
	}
	appExpiry := big.NewInt(p.AppExpiry)
	pollID := big.NewInt(p.PollID)
	govtParameterProposalParams := &model.GovernmentParameterProposalParams{
		ID:                p.ID,
		Name:              p.Name,
		Value:             value,
		PropID:            propID,
		AppExpiry:         appExpiry,
		PollID:            pollID,
		Accepted:          p.Accepted,
		Expired:           p.Expired,
		LastUpdatedDateTs: p.LastUpdatedDateTs,
	}
	return model.NewGovernmentParameterProposal(govtParameterProposalParams), nil
}
