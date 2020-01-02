// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

import (
	"math/big"
)

// GovernmentParameter represents a parameter for the government
type GovernmentParameter struct {
	paramName string
	value     *big.Int
}

// NewGovernmentParameter creates a new parameter object
func NewGovernmentParameter(paramName string, value *big.Int) *GovernmentParameter {
	return &GovernmentParameter{
		paramName: paramName,
		value:     value,
	}
}

// ParamName returns the name of the parameter
func (p *GovernmentParameter) ParamName() string {
	return p.paramName
}

// Value returns the value of the parameter
func (p *GovernmentParameter) Value() *big.Int {
	return p.value
}

// SetValue sets value field
func (p *GovernmentParameter) SetValue(value *big.Int) {
	p.value = value
}
