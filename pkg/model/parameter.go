// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

import (
	"math/big"
)

// Parameter represents a parameter for the parameterizer
type Parameter struct {
	paramName string
	value     *big.Int
}

// NewParameter creates a new parameter object
func NewParameter(paramName string, value *big.Int) *Parameter {
	return &Parameter{
		paramName: paramName,
		value:     value,
	}
}

// ParamName returns the name of the parameter
func (p *Parameter) ParamName() string {
	return p.paramName
}

// Value returns the value of the parameter
func (p *Parameter) Value() *big.Int {
	return p.value
}

// SetValue sets value field
func (p *Parameter) SetValue(value *big.Int) {
	p.value = value
}
