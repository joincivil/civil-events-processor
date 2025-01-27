package persistence

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/utils"
)

// NullPersister is a persister that does not save any values and always returns
// defaults for interface methods. Handy for testing and for one off use scenarios.
// Implements the ListingPersister, ContentRevisionPersister, and GovernanceEventPersister
type NullPersister struct{}

// Close does nothing
func (n *NullPersister) Close() error {
	return nil
}

// ListingsByCriteria returns all listings by ListingCriteria
func (n *NullPersister) ListingsByCriteria(criteria *model.ListingCriteria) ([]*model.Listing, error) {
	return []*model.Listing{}, nil
}

// ListingsByAddresses returns a slice of Listings based on addresses
func (n *NullPersister) ListingsByAddresses(addresses []common.Address) ([]*model.Listing, error) {
	return []*model.Listing{}, nil
}

// ListingByAddress retrieves listings based on addresses
func (n *NullPersister) ListingByAddress(address common.Address) (*model.Listing, error) {
	return &model.Listing{}, nil
}

// ListingsByOwnerAddress retrieves listings based on owner address
func (n *NullPersister) ListingsByOwnerAddress(address common.Address) ([]*model.Listing, error) {
	return []*model.Listing{}, nil
}

// ListingByCleanedNewsroomURL returns listing that matches given url
func (n *NullPersister) ListingByCleanedNewsroomURL(cleanedURL string) (*model.Listing, error) {
	return &model.Listing{}, nil
}

// CreateListing creates a new listing
func (n *NullPersister) CreateListing(listing *model.Listing) error {
	return nil
}

// UpdateListing updates fields on an existing listing
func (n *NullPersister) UpdateListing(listing *model.Listing, updatedFields []string) error {
	return nil
}

// AllListingAddresses returns all listing addresses in persistence
func (n *NullPersister) AllListingAddresses() ([]string, error) {
	return []string{}, nil
}

// DeleteListing removes a listing
func (n *NullPersister) DeleteListing(listing *model.Listing) error {
	return nil
}

// ContentRevisionsByCriteria returns all content revisions by ContentRevisionCriteria
func (n *NullPersister) ContentRevisionsByCriteria(criteria *model.ContentRevisionCriteria) ([]*model.ContentRevision, error) {
	return []*model.ContentRevision{}, nil
}

// ContentRevisions retrieves the revisions for content on a listing
func (n *NullPersister) ContentRevisions(address common.Address, contentID *big.Int) ([]*model.ContentRevision, error) {
	return []*model.ContentRevision{}, nil
}

// ContentRevision retrieves a specific content revision for newsroom content
func (n *NullPersister) ContentRevision(address common.Address, contentID *big.Int, revisionID *big.Int) (*model.ContentRevision, error) {
	return &model.ContentRevision{}, nil
}

// CreateContentRevision creates a new content revision
func (n *NullPersister) CreateContentRevision(revision *model.ContentRevision) error {
	return nil
}

// UpdateContentRevision updates fields on an existing content revision
func (n *NullPersister) UpdateContentRevision(revision *model.ContentRevision, updatedFields []string) error {
	return nil
}

// DeleteContentRevision removes a content revision
func (n *NullPersister) DeleteContentRevision(revision *model.ContentRevision) error {
	return nil
}

// GovernanceEventsByTxHash gets governance events based on txhash
func (n *NullPersister) GovernanceEventsByTxHash(txHash common.Hash) ([]*model.GovernanceEvent, error) {
	return []*model.GovernanceEvent{}, nil
}

// GovernanceEventsByCriteria retrieves governance events based on criteria
func (n *NullPersister) GovernanceEventsByCriteria(criteria *model.GovernanceEventCriteria) ([]*model.GovernanceEvent, error) {
	return []*model.GovernanceEvent{}, nil
}

// GovernanceEventsByListingAddress retrieves governance events based on criteria
func (n *NullPersister) GovernanceEventsByListingAddress(address common.Address) ([]*model.GovernanceEvent, error) {
	return []*model.GovernanceEvent{}, nil
}

// GovernanceEventByChallengeID retrieves challenge by challengeID
func (n *NullPersister) GovernanceEventByChallengeID(challengeID int) (*model.GovernanceEvent, error) {
	return &model.GovernanceEvent{}, nil
}

// GovernanceEventsByChallengeIDs retrieves challenges by challengeIDs
func (n *NullPersister) GovernanceEventsByChallengeIDs(challengeIDs []int) ([]*model.GovernanceEvent, error) {
	return []*model.GovernanceEvent{}, nil
}

// CreateGovernanceEvent creates a new governance event
func (n *NullPersister) CreateGovernanceEvent(govEvent *model.GovernanceEvent) error {
	return nil
}

// UpdateGovernanceEvent updates fields on an existing governance event
func (n *NullPersister) UpdateGovernanceEvent(govEvent *model.GovernanceEvent, updatedFields []string) error {
	return nil
}

// DeleteGovernanceEvent removes a governance event
func (n *NullPersister) DeleteGovernanceEvent(govEvent *model.GovernanceEvent) error {
	return nil
}

// TimestampOfLastEventForCron returns the timestamp for the last event seen by the processor
func (n *NullPersister) TimestampOfLastEventForCron() (int64, error) {
	return int64(0), nil
}

// UpdateTimestampForCron updates the timestamp of the last event seen by the cron
func (n *NullPersister) UpdateTimestampForCron(timestamp int64) error {
	return nil
}

// EventHashesOfLastTimestampForCron returns the event hashes processed for the last timestamp from cron
func (n *NullPersister) EventHashesOfLastTimestampForCron() ([]string, error) {
	return []string{}, nil
}

// UpdateEventHashesForCron updates the eventHashes saved in cron table
func (n *NullPersister) UpdateEventHashesForCron(eventHashes []string) error {
	return nil
}

// ChallengeByChallengeID gets a challenge by challengeID
func (n *NullPersister) ChallengeByChallengeID(challengeID int) (*model.Challenge, error) {
	return &model.Challenge{}, nil
}

// ChallengesByChallengeIDs returns a slice of challenges in order based on challenge IDs
func (n *NullPersister) ChallengesByChallengeIDs(challengeIDs []int) ([]*model.Challenge, error) {
	return []*model.Challenge{}, nil
}

// ChallengesByListingAddress gets list of challenges for a listing sorted by
// challenge id
func (n *NullPersister) ChallengesByListingAddress(addr common.Address) ([]*model.Challenge, error) {
	return []*model.Challenge{}, nil
}

// ChallengesByListingAddresses gets slice of challenges in order by challenge ID
// for a each listing address in order of addresses
func (n *NullPersister) ChallengesByListingAddresses(addr []common.Address) ([][]*model.Challenge, error) {
	return [][]*model.Challenge{}, nil
}

// ChallengesByChallengerAddress returns a slice of challenges started by given user
func (n *NullPersister) ChallengesByChallengerAddress(addr common.Address) ([]*model.Challenge, error) {
	return []*model.Challenge{}, nil
}

// CreateChallenge creates a new challenge
func (n *NullPersister) CreateChallenge(challenge *model.Challenge) error {
	return nil
}

// UpdateChallenge updates a challenge
func (n *NullPersister) UpdateChallenge(challenge *model.Challenge, updatedFields []string) error {
	return nil
}

// PollByPollID gets a poll by pollID
func (n *NullPersister) PollByPollID(pollID int) (*model.Poll, error) {
	return &model.Poll{}, nil
}

// PollsByPollIDs returns a slice of polls in order based on poll IDs
func (n *NullPersister) PollsByPollIDs(pollIDs []int) ([]*model.Poll, error) {
	return []*model.Poll{}, nil
}

// CreatePoll creates a new poll
func (n *NullPersister) CreatePoll(poll *model.Poll) error {
	return nil
}

// UpdatePoll updates a poll
func (n *NullPersister) UpdatePoll(poll *model.Poll, updatedFields []string) error {
	return nil
}

// AppealByChallengeID gets an appeal by challengeID
func (n *NullPersister) AppealByChallengeID(challengeID int) (*model.Appeal, error) {
	return &model.Appeal{}, nil
}

// AppealByAppealChallengeID gets an appeal by appealchallengeID
func (n *NullPersister) AppealByAppealChallengeID(challengeID int) (*model.Appeal, error) {
	return &model.Appeal{}, nil
}

// AppealsByChallengeIDs returns a slice of appeals in order based on challenge IDs
func (n *NullPersister) AppealsByChallengeIDs(challengeIDs []int) ([]*model.Appeal, error) {
	return []*model.Appeal{}, nil
}

// CreateAppeal creates a new appeal
func (n *NullPersister) CreateAppeal(appeal *model.Appeal) error {
	return nil
}

// UpdateAppeal updates an appeal
func (n *NullPersister) UpdateAppeal(appeal *model.Appeal, updatedFields []string) error {
	return nil
}

// TokenTransfersByTxHash gets a list of token transfers by tx hash
func (n *NullPersister) TokenTransfersByTxHash(txHash common.Hash) ([]*model.TokenTransfer, error) {
	return []*model.TokenTransfer{}, nil
}

// TokenTransfersByToAddress gets a list of token transfers by purchaser address
func (n *NullPersister) TokenTransfersByToAddress(addr common.Address) ([]*model.TokenTransfer, error) {
	return []*model.TokenTransfer{}, nil
}

// CreateTokenTransfer creates an token transfer
func (n *NullPersister) CreateTokenTransfer(appeal *model.TokenTransfer) error {
	return nil
}

// ParameterByName gets a parameter from persistence using paramName
func (n *NullPersister) ParameterByName(paramName string) (*model.Parameter, error) {
	return &model.Parameter{}, nil
}

// ParametersByName gets a slice of parameters by name
func (n *NullPersister) ParametersByName(paramName []string) ([]*model.Parameter, error) {
	return []*model.Parameter{}, nil
}

// UpdateParameter updates the value of a parameter in table
func (n *NullPersister) UpdateParameter(parameter *model.Parameter, updatedFields []string) error {
	return nil
}

// CreateDefaultValues creates Parameter default values
func (n *NullPersister) CreateDefaultValues(config *utils.ProcessorConfig) error {
	return nil
}

// GovernmentParameterByName gets a parameter from persistence using paramName
func (n *NullPersister) GovernmentParameterByName(paramName string) (*model.GovernmentParameter, error) {
	return &model.GovernmentParameter{}, nil
}

// GovernmentParametersByName gets a slice of parameters by name
func (n *NullPersister) GovernmentParametersByName(paramName []string) ([]*model.GovernmentParameter, error) {
	return []*model.GovernmentParameter{}, nil
}

// UpdateGovernmentParameter updates the value of a parameter in table
func (n *NullPersister) UpdateGovernmentParameter(parameter *model.GovernmentParameter, updatedFields []string) error {
	return nil
}

// CreateParameterProposal creates a new parameter proposal
func (n *NullPersister) CreateParameterProposal(paramProposal *model.ParameterProposal) error {
	return nil
}

// CreateGovernmentParameterProposal creates a new government parameter proposal
func (n *NullPersister) CreateGovernmentParameterProposal(paramProposal *model.GovernmentParameterProposal) error {
	return nil
}

// ParamProposalByPropID gets a parameter proposal from persistence using propID
func (n *NullPersister) ParamProposalByPropID(propID [32]byte, active bool) (*model.ParameterProposal, error) {
	return &model.ParameterProposal{}, nil
}

// ParamProposalByName gets parameter proposals by name from persistence
func (n *NullPersister) ParamProposalByName(name string, active bool) ([]*model.ParameterProposal, error) {
	return []*model.ParameterProposal{}, nil
}

// UpdateParamProposal updates parameter propsal in table
func (n *NullPersister) UpdateParamProposal(paramProposal *model.ParameterProposal, updatedFields []string) error {
	return nil
}

// GovernmentParamProposalByPropID gets a parameter proposal from persistence using propID
func (n *NullPersister) GovernmentParamProposalByPropID(propID [32]byte, active bool) (*model.GovernmentParameterProposal, error) {
	return &model.GovernmentParameterProposal{}, nil
}

// GovernmentParamProposalByName gets parameter proposals by name from persistence
func (n *NullPersister) GovernmentParamProposalByName(name string, active bool) ([]*model.GovernmentParameterProposal, error) {
	return []*model.GovernmentParameterProposal{}, nil
}

// UpdateGovernmentParamProposal updates parameter propsal in table
func (n *NullPersister) UpdateGovernmentParamProposal(paramProposal *model.GovernmentParameterProposal, updatedFields []string) error {
	return nil
}

// CreateUserChallengeData creates a new UserChallengeData
func (n *NullPersister) CreateUserChallengeData(userChallengeData *model.UserChallengeData) error {
	return nil
}

// UserChallengeDataByCriteria retrieves UserChallengeData based on criteria
func (n *NullPersister) UserChallengeDataByCriteria(criteria *model.UserChallengeDataCriteria) ([]*model.UserChallengeData, error) {
	return []*model.UserChallengeData{}, nil
}

// UpdateUserChallengeData updates UserChallengeData in table.
// user=true updates for user + pollID, user=false updates for pollID
func (n *NullPersister) UpdateUserChallengeData(userChallengeData *model.UserChallengeData, updatedFields []string, updateWithUserAddress bool) error {
	return nil
}

// CreateMultiSig creates a new MultiSig
func (n *NullPersister) CreateMultiSig(multiSig *model.MultiSig) error {
	return nil
}

// UpdateMultiSig updates fields on an existing multi sig
func (n *NullPersister) UpdateMultiSig(multiSig *model.MultiSig, updatedFields []string) error {
	return nil
}

// MultiSigOwners gets the owners of a multi sig
func (n *NullPersister) MultiSigOwners(multiSigAddress common.Address) ([]*model.MultiSigOwner, error) {
	return []*model.MultiSigOwner{}, nil
}

// CreateMultiSigOwner creates a new MultiSigOwner
func (n *NullPersister) CreateMultiSigOwner(multiSigOwner *model.MultiSigOwner) error {
	return nil
}

// DeleteMultiSigOwner deletes a multi sig owner associated with a multi sig
func (n *NullPersister) DeleteMultiSigOwner(multiSigAddress common.Address, ownerAddress common.Address) error {
	return nil
}

// MultiSigOwnersByOwner gets multi sig owners of multi sigs owned by address
func (n *NullPersister) MultiSigOwnersByOwner(ownerAddress common.Address) ([]*model.MultiSigOwner, error) {
	return []*model.MultiSigOwner{}, nil
}

// AllMultiSigAddresses returns all multi sig addresses in persistence
func (n *NullPersister) AllMultiSigAddresses() ([]string, error) {
	return []string{}, nil
}
