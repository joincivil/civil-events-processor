package processor

import (
	"errors"
	"fmt"
	log "github.com/golang/glog"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/joincivil/civil-events-processor/pkg/model"
	// "github.com/ethereum/go-ethereum/core/types"
	commongen "github.com/joincivil/civil-events-crawler/pkg/generated/common"
	"github.com/joincivil/civil-events-crawler/pkg/generated/contract"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	crawlerutils "github.com/joincivil/civil-events-crawler/pkg/utils"
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

func (t *TcrEventProcessor) process(event *crawlermodel.Event) (bool, error) {
	if !t.isValidCivilTCRContractEventName(event.EventType()) {
		return false, nil
	}

	var err error
	ran := true
	eventName := strings.Trim(event.EventType(), " _")

	// NOTE(IS): RewardClaimed is the only TCR event that doesn't emit a listingAddress
	if eventName == "RewardClaimed" {
		challengeID, challengeErr := t.challengeIDFromEvent(event)
		if challengeErr != nil {
			return ran, challengeErr
		}
		log.Infof("Handling Reward Claimed for Challenge %v\n", challengeID)
		err = t.processTCRRewardClaimed(event)

	}

	listingAddress, listingErr := t.listingAddressFromEvent(event)
	if listingErr != nil {
		log.Infof("Error retrieving listingAddress: err: %v", listingErr)
		ran = false
		return ran, errors.New("Could not get listing address from event")
	}

	// Split this into events based on what they modify
	// For now, just process each event individually

	switch eventName {
	case "Application":
		log.Infof("Handling Application for %v\n", listingAddress.Hex())
		err = t.processTCRApplication(event, listingAddress)

	case "ApplicationWhitelisted":
		log.Infof("Handling ApplicationWhitelisted for %v\n", listingAddress.Hex())
		err = t.processTCRApplicationWhitelisted(event, listingAddress)

	case "ApplicationRemoved":
		log.Infof("Handling ApplicationRemoved for %v\n", listingAddress.Hex())
		err = t.processTCRApplicationRemoved(event, listingAddress)

	case "Deposit":
		log.Infof("Handling Deposit for %v\n", listingAddress.Hex())
		err = t.processTCRDepositWithdrawal(event, model.GovernanceStateDeposit, listingAddress)

	case "Withdrawal":
		log.Infof("Handling Withdrawal for %v\n", listingAddress.Hex())
		err = t.processTCRDepositWithdrawal(event, model.GovernanceStateWithdrawal, listingAddress)

	case "ListingRemoved":
		log.Infof("Handling ListingRemoved for %v\n", listingAddress.Hex())
		err = t.processTCRListingRemoved(event, listingAddress)

	case "Challenge":
		log.Infof("Handling Challenge for %v\n", listingAddress.Hex())
		err = t.processTCRChallenge(event, listingAddress)

	case "ChallengeFailed":
		log.Infof("Handling ChallengeFailed for %v\n", listingAddress.Hex())
		err = t.processTCRChallengeFailed(event, listingAddress)

	case "ChallengeSucceeded":
		log.Infof("Handling ChallengeSucceeded for %v\n", listingAddress.Hex())
		err = t.processTCRChallengeSucceeded(event)

	case "FailedChallengeOverturned":
		log.Infof("Handling FailedChallengeOverturned for %v\n", listingAddress.Hex())
		err = t.processTCRFailedChallengeOverturned(event)

	case "SuccessfulChallengeOverturned":
		log.Infof("Handling SuccessfulChallengeOverturned for %v\n", listingAddress.Hex())
		err = t.processTCRSuccessfulChallengeOverturned(event, listingAddress)

	case "AppealGranted":
		log.Infof("Handling AppealGranted for %v\n", listingAddress.Hex())
		err = t.processTCRAppealGranted(event)

	case "AppealRequested":
		log.Infof("Handling AppealRequested for %v\n", listingAddress.Hex())
		err = t.processTCRAppealRequested(event)

	case "GrantedAppealChallenged":
		log.Infof("Handling GrantedAppealChallenged for %v\n", listingAddress.Hex())
		err = t.processTCRGrantedAppealChallenged(event)

	case "GrantedAppealConfirmed":
		log.Infof("Handling GrantedAppealConfirmed for %v\n", listingAddress.Hex())
		err = t.processTCRGrantedAppealConfirmed(event)

	case "GrantedAppealOverturned":
		log.Infof("Handling GrantedAppealOverturned for %v\n", listingAddress.Hex())
		err = t.processTCRGrantedAppealOverturned(event)
	default:
		ran = false
		// govErr := t.persistGovernanceEvent(event)
		// if err != nil {
		// 	return ran, err
		// }
	}
	return ran, err

}

// func (t *TcrEventProcessor) persistGovernanceEvent(event *crawlermodel.Event) error {
// 	listingAddress, _ := t.listingAddressFromEvent(event) // nolint: gosec
// 	err := t.persistNewGovernanceEvent(
// 		listingAddress,
// 		event.ContractAddress(),
// 		event.EventPayload(),
// 		event.Timestamp(),
// 		event.EventType(),
// 		event.Hash(),
// 		event.LogPayload(),
// 	)
// 	return err
// }

// func (t *TcrEventProcessor) persistNewGovernanceEvent(listingAddr common.Address,
// 	senderAddr common.Address, metadata model.Metadata, creationDate int64, eventType string,
// 	eventHash string, logPayload *types.Log) error {
// 	govEvent := model.NewGovernanceEvent(
// 		listingAddr,
// 		senderAddr,
// 		metadata,
// 		eventType,
// 		creationDate,
// 		crawlerutils.CurrentEpochSecsInInt64(),
// 		eventHash,
// 		logPayload.BlockNumber,
// 		logPayload.TxHash,
// 		logPayload.TxIndex,
// 		logPayload.BlockHash,
// 		logPayload.Index,
// 	)
// 	err := t.govEventPersister.CreateGovernanceEvent(govEvent)
// 	return err
// }

func (t *TcrEventProcessor) processTCRApplication(event *crawlermodel.Event,
	listingAddress common.Address) error {
	return t.persistNewListingFromApplication(event, listingAddress)
}

func (t *TcrEventProcessor) processTCRChallenge(event *crawlermodel.Event,
	listingAddress common.Address) error {
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

	listing, err := t.listingPersister.ListingByAddress(listingAddress)
	if err != nil {
		return err
	}

	if listing == nil {
		// TODO(IS) : persist new listing by getting values to initialize from the contract
		return fmt.Errorf("No listing with listingAddress: %v", listingAddress)
	}

	listing.SetChallengeID(challengeID)
	unstakedDeposit := listing.UnstakedDeposit()
	listing.SetUnstakedDeposit(unstakedDeposit.Sub(unstakedDeposit, minDeposit))
	updatedFields := []string{challengeIDFieldName, unstakedDepositFieldName}

	return t.listingPersister.UpdateListing(listing, updatedFields)
}

func (t *TcrEventProcessor) processTCRDepositWithdrawal(event *crawlermodel.Event,
	govState model.GovernanceState, listingAddress common.Address) error {
	listing, err := t.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
		return err
	}
	if listing == nil {
		// TODO(IS): persist new listing by getting values to initialize from contract
		return fmt.Errorf("No listing with listingAddress: %v", listingAddress)
	}
	unstakedDeposit := listing.UnstakedDeposit()
	payload := event.EventPayload()

	if govState == model.GovernanceStateWithdrawal {
		withdrew, ok := payload["Withdrew"]
		if !ok {
			return errors.New("No withdrew field found")
		}
		unstakedDeposit.Sub(unstakedDeposit, withdrew.(*big.Int))
	} else if govState == model.GovernanceStateDeposit {
		deposit, ok := payload["Deposit"]
		if !ok {
			return errors.New("No deposit field found")
		}
		unstakedDeposit.Add(unstakedDeposit, deposit.(*big.Int))
	}
	updatedFields := []string{unstakedDepositFieldName}
	return t.listingPersister.UpdateListing(listing, updatedFields)
}

func (t *TcrEventProcessor) processTCRApplicationWhitelisted(event *crawlermodel.Event,
	listingAddress common.Address) error {
	// NOTE(IS): The Dapp changes challengeID to 0 here but we keep this as -1 because it hasn't been challenged yet
	listing, err := t.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
		return err
	}
	whitelisted := true
	if listing == nil {
		// TODO(IS) : persist new listing by getting values to initialize from the contract
		return fmt.Errorf("No listing with listingAddress: %v", listingAddress)
	}

	listing.SetWhitelisted(whitelisted)
	updatedFields := []string{whitelistedFieldName}
	return t.listingPersister.UpdateListing(listing, updatedFields)
}

func (t *TcrEventProcessor) processTCRApplicationRemoved(event *crawlermodel.Event, listingAddress common.Address) error {
	return t.resetListing(listingAddress)
}

func (t *TcrEventProcessor) processTCRListingRemoved(event *crawlermodel.Event, listingAddress common.Address) error {
	return t.resetListing(listingAddress)
}

func (t *TcrEventProcessor) processTCRChallengeFailed(event *crawlermodel.Event,
	listingAddress common.Address) error {
	listing, err := t.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
		return err
	}
	if listing == nil {
		// TODO(IS) : persist new listing by getting values to initialize from the contract
		return fmt.Errorf("No listing with listingAddress: %v", listingAddress)
	}
	challengeID, err := t.challengeIDFromEvent(event)
	if err != nil {
		return err
	}
	tcrAddress := event.ContractAddress()
	reward, err := t.getRewardFromTCRContract(tcrAddress, challengeID)
	if err != nil {
		return err
	}
	unstakedDeposit := listing.UnstakedDeposit()
	unstakedDeposit.Add(unstakedDeposit, reward)
	listing.SetUnstakedDeposit(unstakedDeposit)
	listing.SetLastGovernanceState(model.GovernanceStateChallengeFailed)
	updatedFields := []string{unstakedDepositFieldName, lastGovStateFieldName}
	err = t.listingPersister.UpdateListing(listing, updatedFields)
	if err != nil {
		return fmt.Errorf("Error updating listing: %v", err)
	}

	return t.processChallengeResolution(event, tcrAddress)
}

func (t *TcrEventProcessor) processTCRChallengeSucceeded(event *crawlermodel.Event) error {
	tcrAddress := event.ContractAddress()
	return t.processChallengeResolution(event, tcrAddress)
}

func (t *TcrEventProcessor) processTCRRewardClaimed(event *crawlermodel.Event) error {
	tcrAddress := event.ContractAddress()
	challengeID, err := t.challengeIDFromEvent(event)
	if err != nil {
		return err
	}
	existingChallenge, err := t.challengePersister.ChallengeByChallengeID(int(challengeID.Int64()))
	if err != nil {
		return err
	}
	if existingChallenge == nil {
		// TODO(IS): If no challenge returned, create new challenge, for now skip
		return fmt.Errorf("No existing challenge found in persistence for id %v", challengeID)
	}

	// NOTE(IS) Have to get totaltokens through contract call, so get all data this way
	challenge, err := t.getChallengeFromTCRContract(tcrAddress, challengeID)
	if err != nil {
		return fmt.Errorf("Error getting challenge from contract: %v", err)
	}
	totalTokens := challenge.TotalTokens
	existingChallenge.SetTotalTokens(totalTokens)
	rewardPool := challenge.RewardPool
	existingChallenge.SetRewardPool(rewardPool)
	updatedFields := []string{rewardPoolFieldName, totalTokensFieldName}

	return t.challengePersister.UpdateChallenge(existingChallenge, updatedFields)
}

func (t *TcrEventProcessor) processChallengeResolution(event *crawlermodel.Event,
	tcrAddress common.Address) error {
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
	existingChallenge, err := t.challengePersister.ChallengeByChallengeID(int(challengeID.Int64()))
	if err != nil {
		return err
	}
	if existingChallenge == nil {
		// TODO(IS): If no challenge returned, create new challenge, for now skip
		return fmt.Errorf("No existing challenge found in persistence for id %v", challengeID)
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

func (t *TcrEventProcessor) processTCRAppealRequested(event *crawlermodel.Event) error {
	err := t.newAppealFromAppealRequested(event)
	if err != nil {
		return fmt.Errorf("Error processing AppealRequested: %v", err)
	}
	return nil
}

func (t *TcrEventProcessor) processTCRAppealGranted(event *crawlermodel.Event) error {
	challengeID, err := t.challengeIDFromEvent(event)
	if err != nil {
		return err
	}

	tcrAddress := event.ContractAddress()
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

	existingAppeal, err := t.appealPersister.AppealByChallengeID(int(challengeID.Int64()))
	if err != nil {
		return err
	}
	if existingAppeal == nil {
		// TODO(IS): create new appeal
		return fmt.Errorf("No existing appeal found in persistence for id %v", challengeID)
	}
	existingAppeal.SetAppealOpenToChallengeExpiry(appealOpenToChallengeExpiry)
	existingAppeal.SetAppealGranted(appealGranted)
	updatedFields := []string{appealOpenToChallengeExpiryFieldName, appealGrantedFieldName}
	return t.appealPersister.UpdateAppeal(existingAppeal, updatedFields)
}

func (t *TcrEventProcessor) processTCRFailedChallengeOverturned(event *crawlermodel.Event) error {
	return t.updateChallengeWithOverturnedData(event)
}

func (t *TcrEventProcessor) processTCRSuccessfulChallengeOverturned(event *crawlermodel.Event,
	listingAddress common.Address) error {
	tcrAddress := event.ContractAddress()
	err := t.updateChallengeWithOverturnedData(event)
	if err != nil {
		return err
	}
	listing, err := t.listingPersister.ListingByAddress(listingAddress)
	if err != nil {
		return err
	}

	if listing == nil {
		// TODO(IS) : persist new listing by getting values to initialize from the contract
		return fmt.Errorf("No listing with listingAddress: %v", listingAddress)
	}
	challengeID, err := t.challengeIDFromEvent(event)
	if err != nil {
		return err
	}
	unstakedDeposit := listing.UnstakedDeposit()
	reward, err := t.getRewardFromTCRContract(tcrAddress, challengeID)
	if err != nil {
		return err
	}
	unstakedDeposit.Add(unstakedDeposit, reward)
	updatedFields := []string{unstakedDepositFieldName}
	return t.listingPersister.UpdateListing(listing, updatedFields)

}

func (t *TcrEventProcessor) processTCRGrantedAppealChallenged(event *crawlermodel.Event) error {
	// TODO(IS): change this name
	return t.persistNewAppealChallenge(event)
}

func (t *TcrEventProcessor) processTCRGrantedAppealOverturned(event *crawlermodel.Event) error {
	//NOTE(IS) in sol files, Appeal: overturned = TRUE, we don't have an overturned field.
	return t.updateChallengeWithOverturnedData(event)
}

func (t *TcrEventProcessor) processTCRGrantedAppealConfirmed(event *crawlermodel.Event) error {
	return t.updateChallengeWithOverturnedData(event)
}

func (t *TcrEventProcessor) updateChallengeWithOverturnedData(event *crawlermodel.Event) error {
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
	existingChallenge, err := t.challengePersister.ChallengeByChallengeID(int(challengeID.Int64()))
	if err != nil {
		return err
	}
	existingChallenge.SetResolved(resolved)
	existingChallenge.SetTotalTokens(totalTokens.(*big.Int))
	updatedFields := []string{resolvedFieldName, totalTokensFieldName}
	return t.challengePersister.UpdateChallenge(existingChallenge, updatedFields)
}

func (t *TcrEventProcessor) persistNewAppealChallenge(event *crawlermodel.Event) error {
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
	listingAddress := payload["ListingAddress"]
	if !ok {
		return errors.New("No listingAddress found")
	}
	tcrAddress := event.ContractAddress()
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
		listingAddress.(common.Address),
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

	existingAppeal, err := t.appealPersister.AppealByChallengeID(int(challengeID.Int64()))
	if err != nil {
		return err
	}
	if existingAppeal == nil {
		// TODO(IS): create new appeal
		return fmt.Errorf("No existing appeal found in persistence for id %v", challengeID)
	}

	existingAppeal.SetAppealChallengeID(appealChallengeID.(*big.Int))
	updatedFields := []string{appealChallengeIDFieldName}
	err = t.appealPersister.UpdateAppeal(existingAppeal, updatedFields)
	return err
}

func (t *TcrEventProcessor) checkAppealNotGranted(challengeID *big.Int) (bool, error) {
	appeal, err := t.appealPersister.AppealByChallengeID(int(challengeID.Int64()))
	if err != nil {
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

func (t *TcrEventProcessor) resetListing(listingAddress common.Address) error {
	// Based on certain events this resets the listing values.
	// This corresponds to delete listings[listingAddress] in the dApp.
	listing, err := t.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
		return err
	}

	if listing == nil {
		// TODO(IS) : persist new listing by getting values to initialize from the contract
		return fmt.Errorf("No listing with listingAddress: %v", listingAddress)
	}
	// NOTE(IS): In dApp, this is delete[listing], check which other fields we match with.
	listing.SetUnstakedDeposit(big.NewInt(0))
	listing.SetAppExpiry(big.NewInt(0))
	listing.SetWhitelisted(false)
	listing.SetUnstakedDeposit(big.NewInt(0))
	listing.SetChallengeID(big.NewInt(0))
	updatedFields := []string{
		unstakedDepositFieldName,
		appExpiryFieldName,
		whitelistedFieldName,
		unstakedDepositFieldName,
		challengeIDFieldName}
	return t.listingPersister.UpdateListing(listing, updatedFields)
}

func (t *TcrEventProcessor) persistNewListingFromApplication(event *crawlermodel.Event,
	listingAddress common.Address) error {

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
