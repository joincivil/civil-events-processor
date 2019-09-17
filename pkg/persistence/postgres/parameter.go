package postgres

import (
	"fmt"
	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/go-common/pkg/numbers"
)

const (
	// ParameterTableBaseName is the type of table this code defines
	ParameterTableBaseName = "parameter"
)

// CreateParameterTableQuery returns the query to create this table
func CreateParameterTableQuery(tableName string) string {
	queryString := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s(
            param_name TEXT PRIMARY KEY,
            value NUMERIC
        );
    `, tableName)
	return queryString
}

// Parameter is model for parameter object
type Parameter struct {
	ParamName string `db:"param_name"`

	Value float64 `db:"value"`
}

// NewParameter creates a new parameter
func NewParameter(parameterData *model.Parameter) *Parameter {
	parameter := &Parameter{}
	parameter.ParamName = parameterData.ParamName()
	parameter.Value = numbers.BigIntToFloat64(parameterData.Value())

	return parameter
}

// DbToParameterData creates a model.Parameter from postgres.Parameter
func (p *Parameter) DbToParameterData() *model.Parameter {
	parameter := model.NewParameter(
		p.ParamName,
		numbers.Float64ToBigInt(p.Value),
	)

	return parameter
}
