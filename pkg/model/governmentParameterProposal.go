// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

import (
	"math/big"
)

// GovernmentParameterProposalParams are params to create a new government parameter proposal
type GovernmentParameterProposalParams struct {
	ID                string
	Name              string
	Value             *big.Int
	PropID            [32]byte
	AppExpiry         *big.Int
	PollID            *big.Int
	Accepted          bool
	Expired           bool
	LastUpdatedDateTs int64
}

// NewGovernmentParameterProposal is a convenience function to create a new government parameter proposal
func NewGovernmentParameterProposal(params *GovernmentParameterProposalParams) *GovernmentParameterProposal {
	return &GovernmentParameterProposal{
		id:                params.ID,
		name:              params.Name,
		value:             params.Value,
		propID:            params.PropID,
		appExpiry:         params.AppExpiry,
		pollID:            params.PollID,
		accepted:          params.Accepted,
		expired:           params.Expired,
		lastUpdatedDateTs: params.LastUpdatedDateTs,
	}
}

// GovernmentParameterProposal represents a government parameterizer proposal
type GovernmentParameterProposal struct {
	id string

	name string

	value *big.Int

	propID [32]byte

	appExpiry *big.Int

	pollID *big.Int

	accepted bool

	expired bool

	lastUpdatedDateTs int64
}

// ID is the id of the parameter field
func (p *GovernmentParameterProposal) ID() string {
	return p.id
}

// Name is the name of the parameter field
func (p *GovernmentParameterProposal) Name() string {
	return p.name
}

// Value is the value of the parameter
func (p *GovernmentParameterProposal) Value() *big.Int {
	return p.value
}

// PropID is the id of proposal
func (p *GovernmentParameterProposal) PropID() [32]byte {
	return p.propID
}

// AppExpiry is the proposal's date of expiration
func (p *GovernmentParameterProposal) AppExpiry() *big.Int {
	return p.appExpiry
}

// PollID is the poll id of this proposal
func (p *GovernmentParameterProposal) PollID() *big.Int {
	return p.pollID
}

// Accepted is whether this proposal has been accepted
func (p *GovernmentParameterProposal) Accepted() bool {
	return p.accepted
}

// Expired is whether this proposal is expired
func (p *GovernmentParameterProposal) Expired() bool {
	return p.expired
}

// LastUpdatedDateTs is the timestamp of last update
func (p *GovernmentParameterProposal) LastUpdatedDateTs() int64 {
	return p.lastUpdatedDateTs
}

// SetAccepted sets accepted field
func (p *GovernmentParameterProposal) SetAccepted(accepted bool) {
	p.accepted = accepted
}

// SetExpired sets expired field
func (p *GovernmentParameterProposal) SetExpired(expired bool) {
	p.expired = expired
}

// SetPollID sets the pollID field
func (p *GovernmentParameterProposal) SetPollID(pollID *big.Int) {
	p.pollID = pollID
}

// SetLastUpdatedDateTs sets the value of the last time this proposal was updated
func (p *GovernmentParameterProposal) SetLastUpdatedDateTs(date int64) {
	p.lastUpdatedDateTs = date
}
