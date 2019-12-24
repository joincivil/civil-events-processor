package postgres

import (
	"fmt"
	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/go-common/pkg/numbers"
)

const (
	// GovernmentParameterTableBaseName is the type of table this code defines
	GovernmentParameterTableBaseName = "government_parameter"
)

// CreateGovernmentParameterTableQuery returns the query to create this table
func CreateGovernmentParameterTableQuery(tableName string) string {
	queryString := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s(
            param_name TEXT PRIMARY KEY,
            value NUMERIC
        );
    `, tableName)
	return queryString
}

// GovernmentParameter is model for parameter object
type GovernmentParameter struct {
	ParamName string `db:"param_name"`

	Value float64 `db:"value"`
}

// NewGovernmentParameter creates a new parameter
func NewGovernmentParameter(govtParameterData *model.GovernmentParameter) *GovernmentParameter {
	govtParameter := &GovernmentParameter{}
	govtParameter.ParamName = govtParameterData.ParamName()
	govtParameter.Value = numbers.BigIntToFloat64(govtParameterData.Value())

	return govtParameter
}

// DbToGovernmentParameterData creates a model.GovernmentParameter from postgres.GovernmentParameter
func (p *GovernmentParameter) DbToGovernmentParameterData() *model.GovernmentParameter {
	govtParameter := model.NewGovernmentParameter(
		p.ParamName,
		numbers.Float64ToBigInt(p.Value),
	)

	return govtParameter
}
