// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

import (
	"fmt"
	"io"
	"math/big"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/joincivil/civil-events-processor/pkg/utils"
)

// SortByType is a string enum of the sort type
type SortByType string

const (
	// SortByUndefined is an undefined SortBy type
	SortByUndefined SortByType = ""
	// SortByName sorts by the newsroom name
	SortByName = "NAME"
	// SortByCreated sorts by the newsroom's creation date
	SortByCreated = "CREATED"
	// SortByApplied sorts by the newsroom's application date
	SortByApplied = "APPLIED"
	// SortByWhitelisted sorts by the newsroom's whitelisted date
	SortByWhitelisted = "WHITELISTED"
)

// IsValid returns if the enum is a valid one
func (e SortByType) IsValid() bool {
	switch e {
	case SortByUndefined, SortByName, SortByCreated, SortByApplied, SortByWhitelisted:
		return true
	}
	return false
}

// String returns the string value of this enum
func (e SortByType) String() string {
	return string(e)
}

// UnmarshalGQL unmarshals from a value to the enum
func (e *SortByType) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}

	*e = SortByType(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid SortByType", str)
	}
	return nil
}

// MarshalGQL marshals from an enum to the writer
func (e SortByType) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String())) // nolint: errcheck
}

// errors must not be returned in valid conditions, such as when there is no
// record for a query.  In this case, return the empty value for the return
// type. errors must be reserved for actual internal errors.  Use ErrPersisterNoResults.

// ListingCriteria contains the retrieval criteria for the ListingsByCriteria
// query. Only one of WhitelistedOnly, RejectedOnly, ActiveChallenge, CurrentApplication can
// be true in one instance.
type ListingCriteria struct {
	Offset int `db:"offset"`
	Count  int `db:"count"`
	// Listings that are currently whitelisted, whitelisted = true
	WhitelistedOnly bool `db:"whitelisted_only"`
	// Listings that were challenged and rejected, they could have an active application.
	RejectedOnly bool `db:"rejected_only"`
	// Listings that have a challenge in progress.
	ActiveChallenge bool `db:"active_challenge"`
	// Listings that have a current application in progress, or listings that have passed their appExpiry
	// and have not been updated yet
	CurrentApplication bool  `db:"current_application"`
	CreatedFromTs      int64 `db:"created_fromts"`
	CreatedBeforeTs    int64 `db:"created_beforets"`

	// SortBy is the sort type to use when returning results
	SortBy SortByType `db:"sort_by"`
	// SortDesc returns results in desc order if true
	SortDesc bool `db:"sort_desc"`
}

// ListingPersister is the interface to store the listings data related to the processor
// and the aggregated data from the events.  Potentially to be used to service
// the APIs to pull data.
type ListingPersister interface {
	// Listings returns all listings by ListingCriteria sorted by creation ts
	ListingsByCriteria(criteria *ListingCriteria) ([]*Listing, error)
	// ListingsByAddress returns a slice of Listings in order based on addresses
	ListingsByAddresses(addresses []common.Address) ([]*Listing, error)
	// ListingByAddress retrieves listings based on addresses
	ListingByAddress(address common.Address) (*Listing, error)
	// ListingsByOwnerAddress retrieves listings based on owner address
	ListingsByOwnerAddress(address common.Address) ([]*Listing, error)
	// CreateListing creates a new listing
	CreateListing(listing *Listing) error
	// UpdateListing updates fields on an existing listing
	UpdateListing(listing *Listing, updatedFields []string) error
	// DeleteListing removes a listing
	DeleteListing(listing *Listing) error
	// ListingByCleanedNewsroomURL retrieves a listing that matches the given url
	ListingByCleanedNewsroomURL(cleanedURL string) (*Listing, error)
	// AllListingAddresses returns all addresses for listings in persistence
	AllListingAddresses() ([]string, error)
	// Close shuts down the persister
	Close() error
}

// MultiSigPersister is the interface to store the multisig data related to the processor
type MultiSigPersister interface {
	// CreateMultiSig creates a new MultiSig
	CreateMultiSig(multiSig *MultiSig) error
	// UpdateMultiSig updates fields on an existing multi sig
	UpdateMultiSig(multiSig *MultiSig, updatedFields []string) error
	// MultiSigOwners gets the owners of a multi sig
	MultiSigOwners(multiSigAddress common.Address) ([]*MultiSigOwner, error)
	// AllMultiSigAddresses returns all addresses for multi sigs in persistence
	AllMultiSigAddresses() ([]string, error)
	// Close shuts down the persister
	Close() error
}

// MultiSigOwnerPersister is the interface to store the multi sig owner data related to the processor
type MultiSigOwnerPersister interface {
	// CreateMultiSigOwner creates a new MultiSigOwner
	CreateMultiSigOwner(multiSigOwner *MultiSigOwner) error
	// DeleteMultiSigOwner deletes a multi sig owner associated with a multi sig
	DeleteMultiSigOwner(multiSigAddress common.Address, ownerAddress common.Address) error
	// MultiSigOwnersByOwner gets multi sig owners of multi sigs owned by address
	MultiSigOwnersByOwner(ownerAddress common.Address) ([]*MultiSigOwner, error)
	// Close shuts down the persister
	Close() error
}

// ContentRevisionCriteria contains the retrieval criteria for a ContentRevisionsByCriteria
// query.
type ContentRevisionCriteria struct {
	ListingAddress string `db:"listing_address"`
	ContentID      *int64 `db:"content_id"`
	RevisionID     *int64 `db:"revision_id"`
	Offset         int    `db:"offset"`
	Count          int    `db:"count"`
	LatestOnly     bool   `db:"latest_only"`
	FromTs         int64  `db:"fromts"`
	BeforeTs       int64  `db:"beforets"`
}

// ContentRevisionPersister is the interface to store the content data related to the processor
// and the aggregated data from the events.  Potentially to be used to service
// the APIs to pull data.
type ContentRevisionPersister interface {
	// ContentRevisionsByCriteria returns all content revisions by ContentRevisionCriteria
	ContentRevisionsByCriteria(criteria *ContentRevisionCriteria) ([]*ContentRevision, error)
	// ContentRevisions retrieves the revisions for content on a listing
	ContentRevisions(address common.Address, contentID *big.Int) ([]*ContentRevision, error)
	// ContentRevision retrieves a specific content revision for newsroom content
	ContentRevision(address common.Address, contentID *big.Int, revisionID *big.Int) (*ContentRevision, error)
	// CreateContentRevision creates a new content revision
	CreateContentRevision(revision *ContentRevision) error
	// UpdateContentRevision updates fields on an existing content revision
	UpdateContentRevision(revision *ContentRevision, updatedFields []string) error
	// DeleteContentRevision removes a content revision
	DeleteContentRevision(revision *ContentRevision) error
	// Close shuts down the persister
	Close() error
}

// GovernanceEventCriteria contains the retrieval criteria for a GovernanceEventsByCriteria
// query.
type GovernanceEventCriteria struct {
	ListingAddress  string `db:"listing_address"`
	Offset          int    `db:"offset"`
	Count           int    `db:"count"`
	CreatedFromTs   int64  `db:"created_fromts"`
	CreatedBeforeTs int64  `db:"created_beforets"`
}

// GovernanceEventPersister is the interface to store the governance event data related to the processor
// and the aggregated data from the events.  Potentially to be used to service
// the APIs to pull data.
type GovernanceEventPersister interface {
	//GovernanceEventsByTxHash gets governance events based on txhash
	GovernanceEventsByTxHash(txHash common.Hash) ([]*GovernanceEvent, error)
	// GovernanceEventsByCriteria retrieves governance events based on criteria
	GovernanceEventsByCriteria(criteria *GovernanceEventCriteria) ([]*GovernanceEvent, error)
	// GovernanceEventsByListingAddress retrieves governance events based on listing address
	GovernanceEventsByListingAddress(address common.Address) ([]*GovernanceEvent, error)
	// CreateGovernanceEvent creates a new governance event
	CreateGovernanceEvent(govEvent *GovernanceEvent) error
	// UpdateGovernanceEvent updates fields on an existing governance event
	UpdateGovernanceEvent(govEvent *GovernanceEvent, updatedFields []string) error
	// DeleteGovernanceEvent removes a governance event
	DeleteGovernanceEvent(govEvent *GovernanceEvent) error
	// Close shuts down the persister
	Close() error
}

// CronPersister persists information needed for the cron to run
type CronPersister interface {
	// TimestampOfLastEventForCron returns the timestamp for the last event seen by the processor
	TimestampOfLastEventForCron() (int64, error)
	// UpdateTimestampForCron updates the timestamp of the last event seen by the cron
	UpdateTimestampForCron(timestamp int64) error
	// EventHashesOfLastTimestampForCron returns the event hashes processed for the last timestamp from cron
	EventHashesOfLastTimestampForCron() ([]string, error)
	// UpdateEventHashesForCron updates the eventHashes saved in cron table
	UpdateEventHashesForCron(eventHashes []string) error
	// Close shuts down the persister
	Close() error
}

// ChallengePersister is the interface to store ChallengeData
type ChallengePersister interface {
	// ChallengeByChallengeID gets a challenge by challengeID
	ChallengeByChallengeID(challengeID int) (*Challenge, error)
	// ChallengesByChallengeIDs returns a slice of challenges in order based on challenge IDs
	ChallengesByChallengeIDs(challengeIDs []int) ([]*Challenge, error)
	// ChallengesByListingAddress gets list of challenges for a listing sorted by
	// challenge id
	ChallengesByListingAddress(addr common.Address) ([]*Challenge, error)
	// ChallengesByListingAddresses gets slice of challenges in order by challenge ID
	// for a each listing address in order of addresses
	ChallengesByListingAddresses(addr []common.Address) ([][]*Challenge, error)
	// ChallengesByChallengerAddress returns a slice of challenges started by given user
	ChallengesByChallengerAddress(addr common.Address) ([]*Challenge, error)
	// CreateChallenge creates a new challenge
	CreateChallenge(challenge *Challenge) error
	// UpdateChallenge updates a challenge
	UpdateChallenge(challenge *Challenge, updatedFields []string) error
	// Close shuts down the persister
	Close() error
}

// PollPersister is the interface to store PollData
type PollPersister interface {
	// PollByPollID gets a poll by pollID
	PollByPollID(pollID int) (*Poll, error)
	// PollsByPollIDs returns a slice of polls in order based on poll IDs
	PollsByPollIDs(pollIDs []int) ([]*Poll, error)
	// CreatePoll creates a new poll
	CreatePoll(poll *Poll) error
	// UpdatePoll updates a poll
	UpdatePoll(poll *Poll, updatedFields []string) error
	// Close shuts down the persister
	Close() error
}

// AppealPersister is the interface to store AppealData
type AppealPersister interface {
	// AppealByChallengeID gets an appeal by challengeID
	AppealByChallengeID(challengeID int) (*Appeal, error)
	// AppealsByChallengeIDs returns a slice of appeals in order based on challenge IDs
	AppealsByChallengeIDs(challengeIDs []int) ([]*Appeal, error)
	// AppealByAppealChallengeID gets an appeal by appealchallengeID
	AppealByAppealChallengeID(challengeID int) (*Appeal, error)
	// CreateAppeal creates a new appeal
	CreateAppeal(appeal *Appeal) error
	// UpdateAppeal updates an appeal
	UpdateAppeal(appeal *Appeal, updatedFields []string) error
	// Close shuts down the persister
	Close() error
}

// ParameterPersister is the interface to store ParameterData
type ParameterPersister interface {
	// ParameterByName gets a parameter by name
	ParameterByName(paramName string) (*Parameter, error)
	// ParametersByName gets a slice of parameter by name
	ParametersByName(paramName []string) ([]*Parameter, error)
	// UpdateParameter updates a parameter value
	UpdateParameter(parameter *Parameter, updatedFields []string) error
	// CreateDefaultValues creates Parameter default values
	CreateDefaultValues(config *utils.ProcessorConfig) error
	// Close shuts down the persister
	Close() error
}

// TokenTransferPersister is the persister interface to store TokenTransfer
type TokenTransferPersister interface {
	// TokenTransfersByTxHash gets a list of token transfers by txhash
	TokenTransfersByTxHash(txHash common.Hash) ([]*TokenTransfer, error)
	// TokenTransfersByToAddress gets a list of token transfers by purchaser address
	TokenTransfersByToAddress(addr common.Address) ([]*TokenTransfer, error)
	// CreateTokenTransfer creates a new token transfer
	CreateTokenTransfer(purchase *TokenTransfer) error
	// Close shuts down the persister
	Close() error
}

// ParamProposalPersister is the persister interface to store ParameterProposal
type ParamProposalPersister interface {
	// CreateParameterProposal creates a new parameter proposal
	CreateParameterProposal(paramProposal *ParameterProposal) error
	// ParamProposalByPropID gets a parameter proposal from persistence using propID
	ParamProposalByPropID(propID [32]byte, active bool) (*ParameterProposal, error)
	// ParamProposalByName gets parameter proposals by name from persistence
	ParamProposalByName(name string, active bool) ([]*ParameterProposal, error)
	// UpdateParamProposal updates parameter propsal in table
	UpdateParamProposal(paramProposal *ParameterProposal, updatedFields []string) error
	// Close shuts down the persister
	Close() error
}

// GovernmentParameterPersister is the interface to store ParameterData
type GovernmentParameterPersister interface {
	// GovernmentParameterByName gets a parameter by name
	GovernmentParameterByName(paramName string) (*GovernmentParameter, error)
	// GovernmentParametersByName gets a slice of parameter by name
	GovernmentParametersByName(paramName []string) ([]*GovernmentParameter, error)
	// UpdateGovernmentParameter updates a parameter value
	UpdateGovernmentParameter(parameter *GovernmentParameter, updatedFields []string) error
	// CreateDefaultValues creates Government Parameter default values
	CreateDefaultValues(config *utils.ProcessorConfig) error
	// Close shuts down the persister
	Close() error
}

// GovernmentParamProposalPersister is the persister interface to store ParameterProposal
type GovernmentParamProposalPersister interface {
	// CreateGovernmentParameterProposal creates a new parameter proposal
	CreateGovernmentParameterProposal(paramProposal *GovernmentParameterProposal) error
	// GovernmentParamProposalByPropID gets a parameter proposal from persistence using propID
	GovernmentParamProposalByPropID(propID [32]byte, active bool) (*GovernmentParameterProposal, error)
	// GovernmentParamProposalByName gets parameter proposals by name from persistence
	GovernmentParamProposalByName(name string, active bool) ([]*GovernmentParameterProposal, error)
	// UpdateGovernmentParamProposal updates parameter propsal in table
	UpdateGovernmentParamProposal(paramProposal *GovernmentParameterProposal, updatedFields []string) error
	// Close shuts down the persister
	Close() error
}

// UserChallengeDataCriteria contains the retrieval criteria for the UserChallengeDataByCriteria
// query
type UserChallengeDataCriteria struct {
	UserAddress    string `db:"user_address"`
	PollID         uint64 `db:"poll_id"`
	CanUserCollect bool   `db:"can_user_collect"`
	CanUserReveal  bool   `db:"can_user_reveal"`
	CanUserRescue  bool   `db:"can_user_rescue"`
	Offset         int    `db:"offset"`
	Count          int    `db:"count"`
}

// UserChallengeDataPersister is the persister interface to store UserChallengeData
type UserChallengeDataPersister interface {
	// CreateUserChallengeData creates a new UserChallengeData
	CreateUserChallengeData(userChallengeData *UserChallengeData) error
	// UserChallengeDataByCriteria retrieves UserChallengeData based on criteria
	UserChallengeDataByCriteria(criteria *UserChallengeDataCriteria) ([]*UserChallengeData, error)
	// UpdateUserChallengeData updates UserChallengeData in table.
	// user=true updates for user + pollID, user=false updates for pollID
	// Since we save on all voteCommitted events, latestVote=True only updates the latest vote
	UpdateUserChallengeData(userChallengeData *UserChallengeData, updatedFields []string,
		updateWithUserAddress bool, latestVote bool) error
	// Close shuts down the persister
	Close() error
}
