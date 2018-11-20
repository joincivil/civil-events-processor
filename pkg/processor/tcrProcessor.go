package processor

import (
	"errors"
	"fmt"
	"math/big"
	"strings"

	log "github.com/golang/glog"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	commongen "github.com/joincivil/civil-events-crawler/pkg/generated/common"
	"github.com/joincivil/civil-events-crawler/pkg/generated/contract"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	crawlerutils "github.com/joincivil/civil-events-crawler/pkg/utils"
	"github.com/joincivil/civil-events-processor/pkg/model"
)

const (
	challengeIDFieldName     = "ChallengeID"
	unstakedDepositFieldName = "UnstakedDeposit"
	whitelistedFieldName     = "Whitelisted"
	lastGovStateFieldName    = "LastGovernanceState"
	rewardPoolFieldName      = "RewardPool"
	stakeFieldName           = "Stake"
	resolvedFieldName        = "Resolved"
	totalTokensFieldName     = "totalTokens"
	appExpiryFieldName       = "AppExpiry"

	appealChallengeIDFieldName           = "AppealChallengeID"
	appealOpenToChallengeExpiryFieldName = "AppealOpenToChallengeExpiry"
	appealGrantedFieldName               = "AppealGranted"
)

// NewTcrEventProcessor is a convenience function to init an EventProcessor
func NewTcrEventProcessor(client bind.ContractBackend, listingPersister model.ListingPersister,
	challengePersister model.ChallengePersister, appealPersister model.AppealPersister,
	govEventPersister model.GovernanceEventPersister) *TcrEventProcessor {
	return &TcrEventProcessor{
		client:             client,
		listingPersister:   listingPersister,
		challengePersister: challengePersister,
		appealPersister:    appealPersister,
		govEventPersister:  govEventPersister,
	}
}

// TcrEventProcessor handles the processing of raw events into aggregated data
// for use via the API.
type TcrEventProcessor struct {
	client             bind.ContractBackend
	listingPersister   model.ListingPersister
	challengePersister model.ChallengePersister
	appealPersister    model.AppealPersister
	govEventPersister  model.GovernanceEventPersister
}

func (t *TcrEventProcessor) isValidCivilTCRContractEventName(name string) bool {
	name = strings.Trim(name, " _")
	eventNames := commongen.EventTypesCivilTCRContract()
	return isStringInSlice(eventNames, name)
}

func (t *TcrEventProcessor) listingAddressFromEvent(event *crawlermodel.Event) (common.Address, error) {
	payload := event.EventPayload()
	listingAddrInterface, ok := payload["ListingAddress"]
	if !ok {
		return common.Address{}, errors.New("Unable to find the listing address in the payload")
	}
	return listingAddrInterface.(common.Address), nil
}

func (t *TcrEventProcessor) challengeIDFromEvent(event *crawlermodel.Event) (*big.Int, error) {
	payload := event.EventPayload()
	challengeIDInterface, ok := payload["ChallengeID"]
	if !ok {
		return nil, errors.New("Unable to find the challenge ID in the payload")
	}
	return challengeIDInterface.(*big.Int), nil
}

// Process processes TcrEvents into aggregated data
func (t *TcrEventProcessor) Process(event *crawlermodel.Event) (bool, error) {
	if !t.isValidCivilTCRContractEventName(event.EventType()) {
		return false, nil
	}

	var err error
	ran := true
	eventName := strings.Trim(event.EventType(), " _")

	// NOTE(IS): RewardClaimed is the only TCR event that doesn't emit a listingAddress
	if eventName == "RewardClaimed" {
		challengeID, errID := t.challengeIDFromEvent(event)
		if err != nil {
			return ran, errID
		}
		log.Infof("Handling Reward Claimed for Challenge %v\n", challengeID)
		err = t.processTCRRewardClaimed(event)
		if err != nil {
			return ran, err
		}
		govErr := t.persistGovernanceEvent(event, eventName)
		return ran, govErr
	}

	listingAddress, listingErr := t.listingAddressFromEvent(event)
	if listingErr != nil {
		log.Infof("Error retrieving listingAddress: err: %v", listingErr)
		ran = false
		return ran, errors.New("Could not get listing address from event")
	}
	tcrAddress := event.ContractAddress()

	switch eventName {
	case "Application":
		log.Infof("Handling Application for %v\n", listingAddress.Hex())
		err = t.processTCRApplication(event, listingAddress)

	case "ApplicationWhitelisted":
		log.Infof("Handling ApplicationWhitelisted for %v\n", listingAddress.Hex())
		err = t.processTCRApplicationWhitelisted(event, listingAddress, tcrAddress)

	case "ApplicationRemoved":
		log.Infof("Handling ApplicationRemoved for %v\n", listingAddress.Hex())
		err = t.processTCRApplicationRemoved(event, listingAddress, tcrAddress)

	case "Deposit":
		log.Infof("Handling Deposit for %v\n", listingAddress.Hex())
		err = t.processTCRDepositWithdrawal(event, model.GovernanceStateDeposit, listingAddress,
			tcrAddress)

	case "Withdrawal":
		log.Infof("Handling Withdrawal for %v\n", listingAddress.Hex())
		err = t.processTCRDepositWithdrawal(event, model.GovernanceStateWithdrawal, listingAddress,
			tcrAddress)

	case "ListingRemoved":
		log.Infof("Handling ListingRemoved for %v\n", listingAddress.Hex())
		err = t.processTCRListingRemoved(event, listingAddress, tcrAddress)

	case "Challenge":
		log.Infof("Handling Challenge for %v\n", listingAddress.Hex())
		err = t.processTCRChallenge(event, listingAddress, tcrAddress)

	case "ChallengeFailed":
		log.Infof("Handling ChallengeFailed for %v\n", listingAddress.Hex())
		err = t.processTCRChallengeFailed(event, listingAddress, tcrAddress)

	case "ChallengeSucceeded":
		log.Infof("Handling ChallengeSucceeded for %v\n", listingAddress.Hex())
		err = t.processTCRChallengeSucceeded(event, listingAddress, tcrAddress)

	case "FailedChallengeOverturned":
		log.Infof("Handling FailedChallengeOverturned for %v\n", listingAddress.Hex())
		err = t.processTCRFailedChallengeOverturned(event, listingAddress, tcrAddress)

	case "SuccessfulChallengeOverturned":
		log.Infof("Handling SuccessfulChallengeOverturned for %v\n", listingAddress.Hex())
		err = t.processTCRSuccessfulChallengeOverturned(event, listingAddress, tcrAddress)

	case "AppealGranted":
		log.Infof("Handling AppealGranted for %v\n", listingAddress.Hex())
		err = t.processTCRAppealGranted(event, listingAddress, tcrAddress)

	case "AppealRequested":
		log.Infof("Handling AppealRequested for %v\n", listingAddress.Hex())
		err = t.processTCRAppealRequested(event, listingAddress, tcrAddress)

	case "GrantedAppealChallenged":
		log.Infof("Handling GrantedAppealChallenged for %v\n", listingAddress.Hex())
		err = t.processTCRGrantedAppealChallenged(event, listingAddress, tcrAddress)

	case "GrantedAppealConfirmed":
		log.Infof("Handling GrantedAppealConfirmed for %v\n", listingAddress.Hex())
		err = t.processTCRGrantedAppealConfirmed(event, listingAddress, tcrAddress)

	case "GrantedAppealOverturned":
		log.Infof("Handling GrantedAppealOverturned for %v\n", listingAddress.Hex())
		err = t.processTCRGrantedAppealOverturned(event, listingAddress, tcrAddress)

	case "TouchAndRemoved":
		log.Infof("Handling TouchAndRemoved for %v\n", listingAddress.Hex())
		err = t.updateListingWithLastGovState(listingAddress, tcrAddress,
			model.GovernanceStateTouchRemoved)

	case "ListingWithdrawn":
		log.Infof("Handling ListingWithdrawn for %v\n", listingAddress.Hex())
		err = t.updateListingWithLastGovState(listingAddress, tcrAddress,
			model.GovernanceStateListingWithdrawn)

	default:
		ran = false
	}

	govErr := t.persistGovernanceEvent(event, eventName)
	if err != nil {
		return ran, govErr
	}
	return ran, err

}

func (t *TcrEventProcessor) persistGovernanceEvent(event *crawlermodel.Event, eventName string) error {
	var listingAddress common.Address
	var err error
	if eventName == "RewardClaimed" {
		challengeID, errID := t.challengeIDFromEvent(event)
		if errID != nil {
			return errID
		}
		tcrAddress := event.ContractAddress()
		listingAddress = common.Address{}
		existingChallenge, errChal := t.getExistingChallenge(challengeID, tcrAddress, listingAddress)
		if errChal != nil {
			return errChal
		}
		// NOTE(IS): If existing challenge is not in persistence, we won't get listingAddress here.
		listingAddress = existingChallenge.ListingAddress()
	} else {
		listingAddress, err = t.listingAddressFromEvent(event)
		if err != nil {
			return err
		}
	}

	logPayload := event.LogPayload()
	govEvent := model.NewGovernanceEvent(
		listingAddress,
		event.EventPayload(),
		event.EventType(),
		event.Timestamp(),
		crawlerutils.CurrentEpochSecsInInt64(),
		event.Hash(),
		logPayload.BlockNumber,
		logPayload.TxHash,
		logPayload.TxIndex,
		logPayload.BlockHash,
		logPayload.Index,
	)
	err = t.govEventPersister.CreateGovernanceEvent(govEvent)
	return err
}

func (t *TcrEventProcessor) processTCRApplication(event *crawlermodel.Event,
	listingAddress common.Address) error {
	return t.newListingFromApplication(event, listingAddress)
}

func (t *TcrEventProcessor) processTCRChallenge(event *crawlermodel.Event,
	listingAddress common.Address, tcrAddress common.Address) error {
	challenge, err := t.newChallengeFromChallenge(event, listingAddress)
	if err != nil {
		return err
	}
	err = t.challengePersister.CreateChallenge(challenge)
	if err != nil {
		return fmt.Errorf("Error persisting new Challenge: %v", err)
	}

	challengeID := challenge.ChallengeID()
	minDeposit := challenge.Stake()

	existingListing, err := t.getExistingListing(tcrAddress, listingAddress)
	if err != nil {
		return err
	}

	existingListing.SetChallengeID(challengeID)
	unstakedDeposit := existingListing.UnstakedDeposit()
	existingListing.SetUnstakedDeposit(unstakedDeposit.Sub(unstakedDeposit, minDeposit))
	existingListing.SetLastGovernanceState(model.GovernanceStateChallenged)
	updatedFields := []string{challengeIDFieldName, unstakedDepositFieldName, lastGovStateFieldName}

	return t.listingPersister.UpdateListing(existingListing, updatedFields)
}

func (t *TcrEventProcessor) processTCRDepositWithdrawal(event *crawlermodel.Event,
	govState model.GovernanceState, listingAddress common.Address, tcrAddress common.Address) error {

	existingListing, err := t.getExistingListing(tcrAddress, listingAddress)
	if err != nil {
		return err
	}

	payload := event.EventPayload()

	if govState == model.GovernanceStateWithdrawal {
		existingListing.SetLastGovernanceState(model.GovernanceStateWithdrawal)
	} else if govState == model.GovernanceStateDeposit {
		existingListing.SetLastGovernanceState(model.GovernanceStateDeposit)
	}
	unstakedDeposit, ok := payload["UnstakedDeposit"]
	if !ok {
		return errors.New("No UnstakedDeposit field found")
	}

	existingListing.SetUnstakedDeposit(unstakedDeposit.(*big.Int))
	updatedFields := []string{unstakedDepositFieldName, lastGovStateFieldName}
	return t.listingPersister.UpdateListing(existingListing, updatedFields)
}

func (t *TcrEventProcessor) processTCRApplicationWhitelisted(event *crawlermodel.Event,
	listingAddress common.Address, tcrAddress common.Address) error {
	// NOTE(IS): The Dapp changes challengeID to 0 here but we keep this as -1 because it hasn't been challenged yet

	whitelisted := true

	existingListing, err := t.getExistingListing(tcrAddress, listingAddress)
	if err != nil {
		return err
	}

	existingListing.SetWhitelisted(whitelisted)
	existingListing.SetLastGovernanceState(model.GovernanceStateAppWhitelisted)
	updatedFields := []string{whitelistedFieldName, lastGovStateFieldName}
	return t.listingPersister.UpdateListing(existingListing, updatedFields)
}

func (t *TcrEventProcessor) processTCRApplicationRemoved(event *crawlermodel.Event,
	listingAddress common.Address, tcrAddress common.Address) error {
	return t.resetListing(event, listingAddress, model.GovernanceStateAppRemoved, tcrAddress)
}

func (t *TcrEventProcessor) processTCRListingRemoved(event *crawlermodel.Event,
	listingAddress common.Address, tcrAddress common.Address) error {
	return t.resetListing(event, listingAddress, model.GovernanceStateRemoved, tcrAddress)
}

func (t *TcrEventProcessor) processTCRChallengeFailed(event *crawlermodel.Event,
	listingAddress common.Address, tcrAddress common.Address) error {

	existingListing, err := t.getExistingListing(tcrAddress, listingAddress)
	if err != nil {
		return err
	}

	challengeID, err := t.challengeIDFromEvent(event)
	if err != nil {
		return err
	}

	reward, err := t.getRewardFromTCRContract(tcrAddress, challengeID)
	if err != nil {
		return err
	}
	unstakedDeposit := existingListing.UnstakedDeposit()
	unstakedDeposit.Add(unstakedDeposit, reward)
	existingListing.SetUnstakedDeposit(unstakedDeposit)
	existingListing.SetLastGovernanceState(model.GovernanceStateChallengeFailed)
	updatedFields := []string{unstakedDepositFieldName, lastGovStateFieldName}
	err = t.listingPersister.UpdateListing(existingListing, updatedFields)
	if err != nil {
		return fmt.Errorf("Error updating listing: %v", err)
	}
	return t.processChallengeResolution(event, tcrAddress, listingAddress)
}

func (t *TcrEventProcessor) processTCRChallengeSucceeded(event *crawlermodel.Event,
	listingAddress common.Address, tcrAddress common.Address) error {
	err := t.updateListingWithLastGovState(listingAddress, tcrAddress,
		model.GovernanceStateChallengeSucceeded)
	if err != nil {
		return err
	}
	return t.processChallengeResolution(event, tcrAddress, listingAddress)
}

func (t *TcrEventProcessor) processTCRRewardClaimed(event *crawlermodel.Event) error {
	challengeID, err := t.challengeIDFromEvent(event)
	if err != nil {
		return err
	}
	tcrAddress := event.ContractAddress()
	// NOTE(IS): This event doesn't emit listingAddress. Put empty address for now
	listingAddress := common.Address{}
	existingChallenge, err := t.getExistingChallenge(challengeID, tcrAddress, listingAddress)
	if err != nil {
		return err
	}
	// NOTE(IS) Have to get totaltokens through contract call, so get all data this way
	challengeRes, err := t.getChallengeFromTCRContract(tcrAddress, challengeID)
	if err != nil {
		return fmt.Errorf("Error getting challenge from contract: %v", err)
	}
	existingChallenge.SetTotalTokens(challengeRes.TotalTokens)
	existingChallenge.SetRewardPool(challengeRes.RewardPool)
	updatedFields := []string{rewardPoolFieldName, totalTokensFieldName}

	return t.challengePersister.UpdateChallenge(existingChallenge, updatedFields)
}

func (t *TcrEventProcessor) processChallengeResolution(event *crawlermodel.Event,
	tcrAddress common.Address, listingAddress common.Address) error {
	payload := event.EventPayload()
	resolved := true
	challengeID, err := t.challengeIDFromEvent(event)
	if err != nil {
		return err
	}
	totalTokens, ok := payload["TotalTokens"]
	if !ok {
		return errors.New("No totalTokens found")
	}

	existingChallenge, err := t.getExistingChallenge(challengeID, tcrAddress, listingAddress)
	if err != nil {
		return err
	}

	existingChallenge.SetResolved(resolved)
	existingChallenge.SetTotalTokens(totalTokens.(*big.Int))
	updatedFields := []string{resolvedFieldName, totalTokensFieldName}

	appealNotGranted, err := t.checkAppealNotGranted(challengeID)
	if err != nil {
		return err
	}
	if appealNotGranted {
		// NOTE(IS) Have to get stake through contract call, so get all data this way
		challenge, err := t.getChallengeFromTCRContract(tcrAddress, challengeID)
		if err != nil {
			return fmt.Errorf("Error getting challenge from contract: %v", err)
		}
		stake := challenge.Stake
		rewardPool := challenge.RewardPool
		existingChallenge.SetRewardPool(rewardPool)
		existingChallenge.SetStake(stake)
		updatedFields = append(updatedFields, rewardPoolFieldName, stakeFieldName)
	}

	return t.challengePersister.UpdateChallenge(existingChallenge, updatedFields)
}

func (t *TcrEventProcessor) processTCRAppealRequested(event *crawlermodel.Event,
	tcrAddress common.Address, listingAddress common.Address) error {
	err := t.newAppealFromAppealRequested(event)
	if err != nil {
		return fmt.Errorf("Error processing AppealRequested: %v", err)
	}
	err = t.updateListingWithLastGovState(listingAddress, tcrAddress,
		model.GovernanceStateAppealRequested)
	return err
}

func (t *TcrEventProcessor) processTCRAppealGranted(event *crawlermodel.Event,
	tcrAddress common.Address, listingAddress common.Address) error {
	challengeID, err := t.challengeIDFromEvent(event)
	if err != nil {
		return err
	}

	tcrContract, err := contract.NewCivilTCRContract(tcrAddress, t.client)
	if err != nil {
		return fmt.Errorf("Error creating TCR contract: err: %v", err)
	}
	challengeRes, err := tcrContract.Appeals(&bind.CallOpts{}, challengeID)
	if err != nil {
		return err
	}
	appealOpenToChallengeExpiry := challengeRes.AppealOpenToChallengeExpiry
	appealGranted := true

	existingAppeal, err := t.getExistingAppeal(challengeID, tcrAddress)
	if err != nil {
		return err
	}

	existingAppeal.SetAppealOpenToChallengeExpiry(appealOpenToChallengeExpiry)
	existingAppeal.SetAppealGranted(appealGranted)
	updatedFields := []string{appealOpenToChallengeExpiryFieldName, appealGrantedFieldName}
	err = t.appealPersister.UpdateAppeal(existingAppeal, updatedFields)
	if err != nil {
		return err
	}
	err = t.updateListingWithLastGovState(listingAddress, tcrAddress,
		model.GovernanceStateAppealGranted)
	return err
}

func (t *TcrEventProcessor) processTCRFailedChallengeOverturned(event *crawlermodel.Event,
	tcrAddress common.Address, listingAddress common.Address) error {
	err := t.updateListingWithLastGovState(listingAddress, tcrAddress,
		model.GovernanceStateFailedChallengeOverturned)
	if err != nil {
		return err
	}
	return t.updateChallengeWithOverturnedData(event, tcrAddress, listingAddress)
}

func (t *TcrEventProcessor) processTCRSuccessfulChallengeOverturned(event *crawlermodel.Event,
	tcrAddress common.Address, listingAddress common.Address) error {

	err := t.updateChallengeWithOverturnedData(event, tcrAddress, listingAddress)
	if err != nil {
		return err
	}
	existingListing, err := t.getExistingListing(tcrAddress, listingAddress)
	if err != nil {
		return err
	}

	challengeID, err := t.challengeIDFromEvent(event)
	if err != nil {
		return err
	}
	unstakedDeposit := existingListing.UnstakedDeposit()
	reward, err := t.getRewardFromTCRContract(tcrAddress, challengeID)
	if err != nil {
		return err
	}
	unstakedDeposit.Add(unstakedDeposit, reward)
	existingListing.SetLastGovernanceState(model.GovernanceStateFailedChallengeOverturned)
	updatedFields := []string{unstakedDepositFieldName, lastGovStateFieldName}
	return t.listingPersister.UpdateListing(existingListing, updatedFields)

}

func (t *TcrEventProcessor) processTCRGrantedAppealChallenged(event *crawlermodel.Event,
	tcrAddress common.Address, listingAddress common.Address) error {
	err := t.updateListingWithLastGovState(listingAddress, tcrAddress,
		model.GovernanceStateGrantedAppealChallenged)
	if err != nil {
		return err
	}
	return t.newAppealChallenge(event, tcrAddress, listingAddress)
}

func (t *TcrEventProcessor) processTCRGrantedAppealOverturned(event *crawlermodel.Event,
	tcrAddress common.Address, listingAddress common.Address) error {
	//NOTE(IS) in sol files, Appeal: overturned = TRUE, we don't have an overturned field.
	err := t.updateListingWithLastGovState(listingAddress, tcrAddress,
		model.GovernanceStateGrantedAppealOverturned)
	if err != nil {
		return err
	}
	return t.updateChallengeWithOverturnedData(event, tcrAddress, listingAddress)
}

func (t *TcrEventProcessor) processTCRGrantedAppealConfirmed(event *crawlermodel.Event,
	tcrAddress common.Address, listingAddress common.Address) error {
	err := t.updateListingWithLastGovState(listingAddress, tcrAddress,
		model.GovernanceStateGrantedAppealConfirmed)
	if err != nil {
		return err
	}
	return t.updateChallengeWithOverturnedData(event, tcrAddress, listingAddress)
}

func (t *TcrEventProcessor) updateChallengeWithOverturnedData(event *crawlermodel.Event,
	tcrAddress common.Address, listingAddress common.Address) error {
	eventPayload := event.EventPayload()
	totalTokens, ok := eventPayload["TotalTokens"]
	if !ok {
		return errors.New("Error getting totalTokens from event payload")
	}
	challengeID, err := t.challengeIDFromEvent(event)
	if err != nil {
		return err
	}
	resolved := true
	existingChallenge, err := t.getExistingChallenge(challengeID, tcrAddress, listingAddress)
	if err != nil {
		return err
	}

	existingChallenge.SetResolved(resolved)
	existingChallenge.SetTotalTokens(totalTokens.(*big.Int))
	updatedFields := []string{resolvedFieldName, totalTokensFieldName}
	return t.challengePersister.UpdateChallenge(existingChallenge, updatedFields)
}

func (t *TcrEventProcessor) newAppealChallenge(event *crawlermodel.Event,
	tcrAddress common.Address, listingAddress common.Address) error {
	payload := event.EventPayload()
	statement, ok := payload["Data"]
	if !ok {
		return errors.New("No data field found")
	}
	appealChallengeID, ok := payload["AppealChallengeID"]
	if !ok {
		return errors.New("No appealChallengeID found")
	}
	challengeID, err := t.challengeIDFromEvent(event)
	if err != nil {
		return err
	}
	tcrContract, err := contract.NewCivilTCRContract(tcrAddress, t.client)
	if err != nil {
		return fmt.Errorf("Error creating TCR contract: err: %v", err)
	}
	challengeRes, err := tcrContract.Challenges(&bind.CallOpts{}, appealChallengeID.(*big.Int))
	if err != nil {
		return fmt.Errorf("Error retrieving challenges: err: %v", err)
	}
	requestAppealExpiry, err := tcrContract.ChallengeRequestAppealExpiries(&bind.CallOpts{}, appealChallengeID.(*big.Int))
	if err != nil {
		return fmt.Errorf("Error retrieving requestAppealExpiries: err: %v", err)
	}
	newAppealChallenge := model.NewChallenge(
		appealChallengeID.(*big.Int),
		listingAddress,
		statement.(string),
		challengeRes.RewardPool,
		challengeRes.Challenger,
		challengeRes.Resolved,
		challengeRes.Stake,
		challengeRes.TotalTokens,
		requestAppealExpiry,
		crawlerutils.CurrentEpochSecsInInt64())

	err = t.challengePersister.CreateChallenge(newAppealChallenge)
	if err != nil {
		return fmt.Errorf("Error persisting new AppealChallenge: %v", err)
	}

	existingAppeal, err := t.getExistingAppeal(challengeID, tcrAddress)
	if err != nil {
		return err
	}

	existingAppeal.SetAppealChallengeID(appealChallengeID.(*big.Int))
	updatedFields := []string{appealChallengeIDFieldName}
	err = t.appealPersister.UpdateAppeal(existingAppeal, updatedFields)
	return err
}

func (t *TcrEventProcessor) checkAppealNotGranted(challengeID *big.Int) (bool, error) {
	appeal, err := t.appealPersister.AppealByChallengeID(int(challengeID.Int64()))
	if err != nil && err != model.ErrPersisterNoResults {
		return false, err
	}
	if appeal == nil {
		return false, err
	}
	if !appeal.AppealGranted() {
		return true, nil
	}
	return false, nil
}

func (t *TcrEventProcessor) getRewardFromTCRContract(tcrAddress common.Address,
	challengeID *big.Int) (*big.Int, error) {
	tcrContract, tcrErr := contract.NewCivilTCRContract(tcrAddress, t.client)
	if tcrErr != nil {
		return nil, fmt.Errorf("Error creating TCR contract: err: %v", tcrErr)
	}
	reward, rewardErr := tcrContract.DetermineReward(&bind.CallOpts{}, challengeID)
	if rewardErr != nil {
		return nil, fmt.Errorf("Error getting reward: err: %v", rewardErr)
	}
	return reward, nil
}

func (t *TcrEventProcessor) getChallengeFromTCRContract(tcrAddress common.Address, challengeID *big.Int) (*struct {
	RewardPool  *big.Int
	Challenger  common.Address
	Resolved    bool
	Stake       *big.Int
	TotalTokens *big.Int
}, error) {
	tcrContract, err := contract.NewCivilTCRContract(tcrAddress, t.client)
	if err != nil {
		return nil, err
	}
	challenge, err := tcrContract.Challenges(&bind.CallOpts{}, challengeID)
	return &challenge, err
}

func (t *TcrEventProcessor) resetListing(event *crawlermodel.Event, listingAddress common.Address,
	govState model.GovernanceState, tcrAddress common.Address) error {
	// This corresponds to delete listings[listingAddress] in the dApp.

	existingListing, err := t.getExistingListing(tcrAddress, listingAddress)
	if err != nil {
		return err
	}
	// NOTE(IS): In dApp, this is delete[listing], check which other fields we match with.
	existingListing.SetUnstakedDeposit(big.NewInt(0))
	existingListing.SetAppExpiry(big.NewInt(0))
	existingListing.SetWhitelisted(false)
	existingListing.SetUnstakedDeposit(big.NewInt(0))
	existingListing.SetChallengeID(big.NewInt(0))
	existingListing.SetLastGovernanceState(govState)
	updatedFields := []string{
		unstakedDepositFieldName,
		appExpiryFieldName,
		whitelistedFieldName,
		unstakedDepositFieldName,
		challengeIDFieldName,
		lastGovStateFieldName}
	return t.listingPersister.UpdateListing(existingListing, updatedFields)
}

func (t *TcrEventProcessor) getExistingChallenge(challengeID *big.Int, tcrAddress common.Address,
	listingAddress common.Address) (*model.Challenge, error) {

	existingChallenge, err := t.challengePersister.ChallengeByChallengeID(int(challengeID.Int64()))
	if err != nil && err != model.ErrPersisterNoResults {
		return nil, err
	}

	if existingChallenge == nil {
		existingChallenge, err = t.persistNewChallengeFromContract(tcrAddress, challengeID, listingAddress)
		if err != nil {
			return nil, fmt.Errorf("Error persisting challenge: %v", err)
		}
	}
	return existingChallenge, nil
}

func (t *TcrEventProcessor) getExistingListing(tcrAddress common.Address,
	listingAddress common.Address) (*model.Listing, error) {

	listing, err := t.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
		return nil, err
	}

	if listing == nil {
		listing, err = t.persistNewListingFromContract(listingAddress, tcrAddress)
		if err != nil {
			return nil, fmt.Errorf("Error persisting listing: %v", err)
		}
	}
	return listing, nil
}

func (t *TcrEventProcessor) getExistingAppeal(challengeID *big.Int,
	tcrAddress common.Address) (*model.Appeal, error) {
	existingAppeal, err := t.appealPersister.AppealByChallengeID(int(challengeID.Int64()))
	if err != nil && err != model.ErrPersisterNoResults {
		return nil, err
	}
	if existingAppeal == nil {
		existingAppeal, err = t.persistNewAppealFromContract(tcrAddress, challengeID)
		if err != nil {
			return nil, fmt.Errorf("Error persisting appeal for id %v", challengeID)
		}
	}
	return existingAppeal, nil
}

func (t *TcrEventProcessor) updateListingWithLastGovState(listingAddress common.Address,
	tcrAddress common.Address, govState model.GovernanceState) error {

	listing, err := t.getExistingListing(tcrAddress, listingAddress)
	if err != nil {
		return err
	}

	listing.SetLastGovernanceState(govState)
	updatedFields := []string{lastGovStateFieldName}
	err = t.listingPersister.UpdateListing(listing, updatedFields)
	if err != nil {
		return fmt.Errorf("Error updating listing: %v", err)
	}
	return nil
}

func (t *TcrEventProcessor) newListingFromApplication(event *crawlermodel.Event,
	listingAddress common.Address) error {
	// TODO(IS): We should make sure an existing listing doesn't already exist
	// which might happen if the events were out of order
	// If events were out of order, we shouldn't update other fields except: (?)
	// CreatedDateTs:     event.Timestamp(),
	// ApplicationDateTs: event.Timestamp(),
	// ApprovalDateTs:    approvalDateEmptyValue,

	newsroom, newsErr := contract.NewNewsroomContract(listingAddress, t.client)
	if newsErr != nil {
		return fmt.Errorf("Error reading from Newsroom contract: %v ", newsErr)
	}
	name, nameErr := newsroom.Name(&bind.CallOpts{})
	if nameErr != nil {
		return fmt.Errorf("Error getting Name from Newsroom contract: %v ", nameErr)
	}

	url := ""

	ownerAddr, err := newsroom.Owner(&bind.CallOpts{})
	if err != nil {
		return err
	}
	ownerAddresses := []common.Address{ownerAddr}

	listing := model.NewListing(&model.NewListingParams{
		Name:              name,
		ContractAddress:   listingAddress,
		Whitelisted:       false,
		LastState:         model.GovernanceStateApplied,
		URL:               url,
		Owner:             ownerAddr,
		OwnerAddresses:    ownerAddresses,
		CreatedDateTs:     event.Timestamp(),
		ApplicationDateTs: event.Timestamp(),
		ApprovalDateTs:    approvalDateEmptyValue,
		LastUpdatedDateTs: crawlerutils.CurrentEpochSecsInInt64(),
	})

	appExpiry := event.EventPayload()["AppEndDate"].(*big.Int)
	unstakedDeposit := event.EventPayload()["Deposit"].(*big.Int)
	listing.SetAppExpiry(appExpiry)
	listing.SetUnstakedDeposit(unstakedDeposit)
	return t.listingPersister.CreateListing(listing)
}

func (t *TcrEventProcessor) newChallengeFromChallenge(event *crawlermodel.Event,
	listingAddress common.Address) (*model.Challenge, error) {
	payload := event.EventPayload()
	statement, ok := payload["Data"]
	if !ok {
		return nil, errors.New("No data field found")
	}
	challengeID, err := t.challengeIDFromEvent(event)
	if err != nil {
		return nil, err
	}
	tcrAddress := event.ContractAddress()
	tcrContract, err := contract.NewCivilTCRContract(tcrAddress, t.client)
	if err != nil {
		return nil, fmt.Errorf("Error creating TCR contract: err: %v", err)
	}
	challengeRes, err := tcrContract.Challenges(&bind.CallOpts{}, challengeID)
	if err != nil {
		return nil, fmt.Errorf("Error calling function in TCR contract: err: %v", err)
	}
	// NOTE(IS): You can get requestAppealExpiry from parameterizer contract as well, this is easier.
	requestAppealExpiry, err := tcrContract.ChallengeRequestAppealExpiries(&bind.CallOpts{}, challengeID)
	if err != nil {
		return nil, fmt.Errorf("Error calling function in TCR contract: err: %v", err)
	}
	challenge := model.NewChallenge(
		challengeID,
		listingAddress,
		statement.(string),
		challengeRes.RewardPool,
		challengeRes.Challenger,
		challengeRes.Resolved,
		challengeRes.Stake,
		challengeRes.TotalTokens,
		requestAppealExpiry,
		crawlerutils.CurrentEpochSecsInInt64())

	return challenge, nil
}

func (t *TcrEventProcessor) newAppealFromAppealRequested(event *crawlermodel.Event) error {
	// This creates a new appeal to an existing challenge (not granted yet)
	payload := event.EventPayload()
	statement, ok := payload["Data"]
	if !ok {
		return errors.New("No data field found")
	}
	challengeID, ok := payload["ChallengeID"]
	if !ok {
		return errors.New("No ChallengeID found")
	}
	appealFeePaid, ok := payload["AppealFeePaid"]
	if !ok {
		return errors.New("No appealFeePaid found")
	}
	appealRequester, ok := payload["Requester"]
	if !ok {
		return errors.New("No appealRequester found")
	}
	tcrAddress := event.ContractAddress()
	tcrContract, err := contract.NewCivilTCRContract(tcrAddress, t.client)
	if err != nil {
		return fmt.Errorf("Error creating TCR contract: err: %v", err)
	}
	challengeRes, err := tcrContract.Appeals(&bind.CallOpts{}, challengeID.(*big.Int))
	if err != nil {
		return fmt.Errorf("Error calling function in TCR contract: err: %v", err)
	}
	appealPhaseExpiry := challengeRes.AppealPhaseExpiry
	appealGranted := false
	appeal := model.NewAppeal(
		challengeID.(*big.Int),
		appealRequester.(common.Address),
		appealFeePaid.(*big.Int),
		appealPhaseExpiry,
		appealGranted,
		statement.(string),
		crawlerutils.CurrentEpochSecsInInt64(),
	)
	err = t.appealPersister.CreateAppeal(appeal)
	return err
}

func (t *TcrEventProcessor) persistNewListingFromContract(listingAddress common.Address,
	tcrAddress common.Address) (*model.Listing, error) {
	// NOTE: In the event that there is no persisted listing, we can create a new listing using data
	// obtained by calling the smart contract

	newsroom, newsErr := contract.NewNewsroomContract(listingAddress, t.client)
	if newsErr != nil {
		return nil, fmt.Errorf("Error reading from Newsroom contract: %v ", newsErr)
	}
	name, nameErr := newsroom.Name(&bind.CallOpts{})
	if nameErr != nil {
		return nil, fmt.Errorf("Error getting Name from Newsroom contract: %v ", nameErr)
	}

	url := ""

	ownerAddr, err := newsroom.Owner(&bind.CallOpts{})
	if err != nil {
		return nil, err
	}
	ownerAddresses := []common.Address{ownerAddr}

	listing := model.NewListing(&model.NewListingParams{
		Name:            name,
		ContractAddress: listingAddress,
		URL:             url,
		Owner:           ownerAddr,
		OwnerAddresses:  ownerAddresses,
		// NOTE(IS): If this isn't from an application event we wouldn't know these dates:
		// CreatedDateTs:     event.Timestamp(),
		// ApplicationDateTs: event.Timestamp(),
		// ApprovalDateTs:    approvalDateEmptyValue,
		LastUpdatedDateTs: crawlerutils.CurrentEpochSecsInInt64(),
	})
	tcrContract, err := contract.NewCivilTCRContract(tcrAddress, t.client)
	if err != nil {
		return nil, fmt.Errorf("Error creating TCR contract: err: %v", err)
	}
	listingFromContract, err := tcrContract.Listings(&bind.CallOpts{}, listingAddress)
	if err != nil {
		return nil, fmt.Errorf("Error calling Listings from TCR contract: err: %v", err)
	}
	listing.SetAppExpiry(listingFromContract.ApplicationExpiry)
	listing.SetUnstakedDeposit(listingFromContract.UnstakedDeposit)
	listing.SetWhitelisted(listingFromContract.Whitelisted)
	listing.SetChallengeID(listingFromContract.ChallengeID)

	err = t.listingPersister.CreateListing(listing)

	return listing, err
}

// In the event that there is no persisted Challenge, we can create a new listing using data
// obtained by calling the smart contract.
func (t *TcrEventProcessor) persistNewChallengeFromContract(tcrAddress common.Address,
	challengeID *big.Int, listingAddress common.Address) (*model.Challenge, error) {
	tcrContract, err := contract.NewCivilTCRContract(tcrAddress, t.client)
	if err != nil {
		return nil, fmt.Errorf("Error creating TCR contract: err: %v", err)
	}
	challengeRes, err := tcrContract.Challenges(&bind.CallOpts{}, challengeID)
	if err != nil {
		return nil, fmt.Errorf("Error retrieving challenges: err: %v", err)
	}
	requestAppealExpiry, err := tcrContract.ChallengeRequestAppealExpiries(&bind.CallOpts{}, challengeID)
	if err != nil {
		return nil, fmt.Errorf("Error retrieving requestAppealExpiries: err: %v", err)
	}
	// TODO(IS): If not getting statement from Challenge event, is there a way to get statement?
	statement := ""
	challenge := model.NewChallenge(
		challengeID,
		listingAddress,
		statement,
		challengeRes.RewardPool,
		challengeRes.Challenger,
		challengeRes.Resolved,
		challengeRes.Stake,
		challengeRes.TotalTokens,
		requestAppealExpiry,
		crawlerutils.CurrentEpochSecsInInt64())

	err = t.challengePersister.CreateChallenge(challenge)
	return challenge, err
}

// In the event that there is no persisted Challenge, we can create a new listing using data
// obtained by calling the smart contract.
func (t *TcrEventProcessor) persistNewAppealFromContract(tcrAddress common.Address,
	challengeID *big.Int) (*model.Appeal, error) {
	tcrContract, err := contract.NewCivilTCRContract(tcrAddress, t.client)
	if err != nil {
		return nil, fmt.Errorf("Error creating TCR contract: err: %v", err)
	}
	statement := ""
	appealRes, err := tcrContract.Appeals(&bind.CallOpts{}, challengeID)
	if err != nil {
		return nil, fmt.Errorf("Error retrieving appeals: err: %v", err)
	}
	appeal := model.NewAppeal(
		challengeID,
		appealRes.Requester,
		appealRes.AppealFeePaid,
		appealRes.AppealPhaseExpiry,
		appealRes.AppealGranted,
		statement,
		crawlerutils.CurrentEpochSecsInInt64(),
	)

	if appealRes.AppealChallengeID.Uint64() != 0 {
		appeal.SetAppealChallengeID(appealRes.AppealChallengeID)
	}
	if appealRes.AppealOpenToChallengeExpiry.Uint64() != 0 {
		appeal.SetAppealOpenToChallengeExpiry(appealRes.AppealOpenToChallengeExpiry)
	}
	err = t.appealPersister.CreateAppeal(appeal)
	return appeal, err
}
