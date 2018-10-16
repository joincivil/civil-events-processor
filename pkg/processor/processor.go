package processor

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	log "github.com/golang/glog"
	"math/big"
	"strings"

	commongen "github.com/joincivil/civil-events-crawler/pkg/generated/common"
	"github.com/joincivil/civil-events-crawler/pkg/generated/contract"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	crawlerutils "github.com/joincivil/civil-events-crawler/pkg/utils"

	"github.com/joincivil/civil-events-processor/pkg/model"
)

const (
	govStateDBModelName     = "LastGovernanceState"
	whitelistedDBModelName  = "Whitelisted"
	approvalDateDBModelName = "ApprovalDateTs"
	listingNameDBModelName  = "Name"
	ownerAddDBModelName     = "OwnerAddresses"
	challengeIDDBModelName  = "ChallengeID"
	pollDBModelName         = "Poll"
)

type whitelistedStatus int

const (
	whitelistedNoChange whitelistedStatus = iota
	whitelistedTrue
	whitelistedFalse
	whitelistedFlip
)

const (
	approvalDateNoUpdate   = int64(-1)
	approvalDateEmptyValue = int64(0)
)

func isStringInSlice(slice []string, target string) bool {
	for _, str := range slice {
		if target == str {
			return true
		}
	}
	return false
}

func byte32ToHexString(input [32]byte) string {
	return hex.EncodeToString(input[:])
}

// NewEventProcessor is a convenience function to init an EventProcessor
func NewEventProcessor(client bind.ContractBackend, listingPersister model.ListingPersister,
	revisionPersister model.ContentRevisionPersister, govEventPersister model.GovernanceEventPersister,
	challengePersister model.ChallengePersister, contentScraper model.ContentScraper,
	metadataScraper model.MetadataScraper, civilMetadataScraper model.CivilMetadataScraper) *EventProcessor {
	return &EventProcessor{
		client:               client,
		listingPersister:     listingPersister,
		revisionPersister:    revisionPersister,
		govEventPersister:    govEventPersister,
		challengePersister:   challengePersister,
		contentScraper:       contentScraper,
		metadataScraper:      metadataScraper,
		civilMetadataScraper: civilMetadataScraper,
	}
}

// EventProcessor handles the processing of raw events into aggregated data
// for use via the API.
type EventProcessor struct {
	client               bind.ContractBackend
	listingPersister     model.ListingPersister
	revisionPersister    model.ContentRevisionPersister
	govEventPersister    model.GovernanceEventPersister
	challengePersister   model.ChallengePersister
	contentScraper       model.ContentScraper
	metadataScraper      model.MetadataScraper
	civilMetadataScraper model.CivilMetadataScraper
}

// Process runs the processor with the given set of raw CivilEvents
// Returns the last error if one has occurred, saves latest timestamp seen to cron persistence
func (e *EventProcessor) Process(events []*crawlermodel.Event) error {
	var err error
	var ran bool
	for _, event := range events {
		ran, err = e.processNewsroomEvent(event)
		if err != nil {
			log.Errorf("Error processing newsroom event: err: %v\n", err)
		}
		if ran {
			continue
		}
		ran, err = e.processCivilTCREvent(event)
		if err != nil {
			log.Errorf("Error processing civil tcr event: err: %v\n", err)
		}
		if ran {
			continue
		}
		_, err = e.processPLCRVotingEvent(event)
		if err != nil {
			log.Errorf("Error processing plcr event: err: %v\n", err)
		}

	}
	return err
}

func (e *EventProcessor) isValidNewsroomContractEventName(name string) bool {
	name = strings.Trim(name, " _")
	eventNames := commongen.EventTypesNewsroomContract()
	return isStringInSlice(eventNames, name)
}

func (e *EventProcessor) isValidCivilTCRContractEventName(name string) bool {
	name = strings.Trim(name, " _")
	eventNames := commongen.EventTypesCivilTCRContract()
	return isStringInSlice(eventNames, name)
}

func (e *EventProcessor) isValidPLCRContractEventName(name string) bool {
	name = strings.Trim(name, " _")
	eventNames := commongen.EventTypesPLCRVotingContract()
	return isStringInSlice(eventNames, name)
}

func (e *EventProcessor) processNewsroomEvent(event *crawlermodel.Event) (bool, error) {
	if !e.isValidNewsroomContractEventName(event.EventType()) {
		return false, nil
	}

	var err error
	ran := true
	eventName := strings.Trim(event.EventType(), " _")

	// Handling all the actionable events from Newsroom Addressses
	switch eventName {
	// When a listing's name has changed
	case "NameChanged":
		log.Infof("Handling NameChanged for %v\n", event.ContractAddress().Hex())
		err = e.processNewsroomNameChanged(event)

	// When there is a new revision on content
	case "RevisionUpdated":
		log.Infof("Handling RevisionUpdated for %v\n", event.ContractAddress().Hex())
		err = e.processNewsroomRevisionUpdated(event)

	// When there is a new owner
	case "OwnershipTransferred":
		log.Infof("Handling OwnershipTransferred for %v\n", event.ContractAddress().Hex())
		err = e.processNewsroomOwnershipTransferred(event)

	default:
		ran = false
	}
	return ran, err
}

func (e *EventProcessor) processPLCRVotingEvent(event *crawlermodel.Event) (bool, error) {
	if !e.isValidPLCRContractEventName(event.EventType()) {
		return false, nil
	}

	var err error
	ran := true
	eventName := strings.Trim(event.EventType(), " _")

	switch eventName {
	case "PollCreated":
		log.Infof("Handling PollCreated for %v\n", event.ContractAddress().Hex())
		err = e.processPollCreated(event)
	case "VoteRevealed":
		log.Infof("Handling VoteRevealed for %v\n", event.ContractAddress().Hex())
		err = e.processVoteRevealed(event)
	}
	return ran, err
}

func (e *EventProcessor) processPollCreated(event *crawlermodel.Event) error {
	// create poll here
	payload := event.EventPayload()
	voteQuorum, ok := payload["VoteQuorum"]
	if !ok {
		return errors.New("No voteQuorum found in pollCreated event")
	}

	commitEndDate, ok := payload["CommitEndDate"]
	if !ok {
		return errors.New("No commitEndDate found in pollCreated event")
	}

	revealEndDate, ok := payload["RevealEndDate"]
	if !ok {
		return errors.New("No revealEndDate found in pollCreated event")
	}

	pollID, ok := payload["PollID"]
	if !ok {
		return errors.New("No pollID found in pollCreated event")
	}
	votesFor := uint64(0)
	votesAgainst := uint64(0)

	poll := model.NewPoll(commitEndDate.(*big.Int).Int64(), revealEndDate.(*big.Int).Int64(), voteQuorum.(*big.Int).Uint64(),
		votesFor, votesAgainst)
	// fmt.Println("challengeID with", pollID.(*big.Int))
	challenge, err := e.challengePersister.ChallengeByChallengeID(pollID.(*big.Int))
	if err != nil && err != model.ErrPersisterNoResults {
		return err
	}
	// NOTE(IS): model.ErrPersisterNoResults is nil
	if challenge == nil {
		//TODO(IS): If no challenge returned, create new challenge, for now skip
		return err
	}

	challenge.SetPoll(poll)
	// Update challenge with challengeID pollID
	updatedFields := []string{pollDBModelName}

	err = e.challengePersister.UpdateChallenge(challenge, updatedFields)
	return err
}

func (e *EventProcessor) processVoteRevealed(event *crawlermodel.Event) error {
	// NOTE(IS): All challengeIDs come from PollIDs
	payload := event.EventPayload()
	pollID, ok := payload["PollID"]
	if !ok {
		return errors.New("No pollID found in voteRevealed event")
	}
	votesFor, ok := payload["VotesFor"]
	if !ok {
		return errors.New("No votesFor found in voteRevealed event")
	}
	votesAgainst, ok := payload["VotesAgainst"]
	if !ok {
		return errors.New("No votesAgainst found in voteRevealed event")
	}

	challenge, err := e.challengePersister.ChallengeByChallengeID(pollID.(*big.Int))
	if err != nil && err != model.ErrPersisterNoResults {
		return err
	}

	poll := challenge.Poll()
	poll.UpdateVotesFor(votesFor.(*big.Int).Uint64())
	poll.UpdateVotesAgainst(votesAgainst.(*big.Int).Uint64())
	challenge.SetPoll(poll)
	// Update challenge with challengeID pollID
	updatedFields := []string{pollDBModelName}

	err = e.challengePersister.UpdateChallenge(challenge, updatedFields)
	return err
}

func (e *EventProcessor) processCivilTCREvent(event *crawlermodel.Event) (bool, error) {
	if !e.isValidCivilTCRContractEventName(event.EventType()) {
		return false, nil
	}

	var err error
	ran := true
	eventName := strings.Trim(event.EventType(), " _")

	ran, err = e.processCivilTCRApplicationListingEvent(event, eventName)
	if !ran {
		ran, err = e.processCivilTCRChallengeEvent(event, eventName)
	}
	if !ran {
		ran, err = e.processCivilTCRAppealEvent(event, eventName)
	}

	govErr := e.persistGovernanceEvent(event)
	if err != nil {
		return ran, err
	}
	return ran, govErr
}

func (e *EventProcessor) processCivilTCRChallengeEvent(event *crawlermodel.Event,
	eventName string) (bool, error) {
	var err error
	ran := true

	listingAddress, listingErr := e.listingAddressFromEvent(event)

	switch eventName {
	case "Challenge":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling Challenge for %v\n", listingAddress.Hex())
		err = e.processTCRChallenge(event)

	case "ChallengeFailed":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling ChallengeFailed for %v\n", listingAddress.Hex())
		err = e.processTCRChallengeFailed(event)

	case "ChallengeSucceeded":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling ChallengeSucceeded for %v\n", listingAddress.Hex())
		err = e.processTCRChallengeSucceeded(event)

	case "FailedChallengeOverturned":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling FailedChallengeOverturned for %v\n", listingAddress.Hex())
		err = e.processTCRChallengeFailedOverturned(event)

	case "SuccessfulChallengeOverturned":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling SuccessfulChallengeOverturned for %v\n", listingAddress.Hex())
		err = e.processTCRChallengeSuccessfulOverturned(event)

	default:
		ran = false
	}

	return ran, err
}

func (e *EventProcessor) processCivilTCRAppealEvent(event *crawlermodel.Event,
	eventName string) (bool, error) {
	var err error
	ran := true

	listingAddress, listingErr := e.listingAddressFromEvent(event)

	switch eventName {
	case "AppealGranted":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling AppealGranted for %v\n", listingAddress.Hex())
		err = e.processTCRAppealGranted(event)

	case "AppealRequested":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling AppealRequested for %v\n", listingAddress.Hex())
		err = e.processTCRAppealRequested(event)

	case "GrantedAppealChallenged":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling GrantedAppealChallenged for %v\n", listingAddress.Hex())
		err = e.processTCRGrantedAppealChallenged(event)

	case "GrantedAppealConfirmed":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling GrantedAppealConfirmed for %v\n", listingAddress.Hex())
		err = e.processTCRGrantedAppealConfirmed(event)

	case "GrantedAppealOverturned":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling GrantedAppealOverturned for %v\n", listingAddress.Hex())
		err = e.processTCRGrantedAppealOverturned(event)

	default:
		ran = false
	}

	return ran, err
}

func (e *EventProcessor) processCivilTCRApplicationListingEvent(event *crawlermodel.Event,
	eventName string) (bool, error) {
	var err error
	ran := true

	listingAddress, listingErr := e.listingAddressFromEvent(event)

	switch eventName {
	case "Application":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling Application for %v\n", listingAddress.Hex())
		err = e.processTCRApplication(event)

	case "ApplicationWhitelisted":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling ApplicationWhitelisted for %v\n", listingAddress.Hex())
		err = e.processTCRApplicationWhitelisted(event)

	case "ApplicationRemoved":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling ApplicationRemoved for %v\n", listingAddress.Hex())
		err = e.processTCRApplicationRemoved(event)

	case "ListingRemoved":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling ListingRemoved for %v\n", listingAddress.Hex())
		err = e.processTCRListingRemoved(event)

	case "ListingWithdrawn":
		if listingErr != nil {
			log.Infof("Error retrieving listingAddress: err: %v", listingErr)
			break
		}
		log.Infof("Handling ListingWithdrawn for %v\n", listingAddress.Hex())
		err = e.processTCRListingWithdrawn(event)

	default:
		ran = false
	}
	return ran, err
}

func (e *EventProcessor) persistNewGovernanceEvent(listingAddr common.Address,
	senderAddr common.Address, metadata model.Metadata, creationDate int64, eventType string,
	eventHash string, logPayload *types.Log) error {
	govEvent := model.NewGovernanceEvent(
		listingAddr,
		senderAddr,
		metadata,
		eventType,
		creationDate,
		crawlerutils.CurrentEpochSecsInInt64(),
		eventHash,
		logPayload.BlockNumber,
		logPayload.TxHash,
		logPayload.TxIndex,
		logPayload.BlockHash,
		logPayload.Index,
	)
	err := e.govEventPersister.CreateGovernanceEvent(govEvent)
	return err
}

func (e *EventProcessor) persistNewListing(listingAddress common.Address,
	whitelisted bool, lastGovernanceState model.GovernanceState, creationDate int64,
	applicationDate int64, approvalDate int64, tcrAddress common.Address) error {
	// TODO(PN): How do I get the URL of the site?
	url := ""
	newsroom, newsErr := contract.NewNewsroomContract(listingAddress, e.client)
	if newsErr != nil {
		return fmt.Errorf("Error reading from Newsroom contract: %v ", newsErr)
	}
	name, nameErr := newsroom.Name(&bind.CallOpts{})
	if nameErr != nil {
		return fmt.Errorf("Error getting Name from Newsroom contract: %v ", nameErr)
	}
	charterContent, contErr := newsroom.GetContent(&bind.CallOpts{}, big.NewInt(0))
	if contErr != nil {
		return fmt.Errorf("Error getting Content from Newsroom contract: %v ", contErr)
	}

	charterURI := charterContent.Uri
	charterAuthorAddr := charterContent.Author
	ownerAddr, err := newsroom.Owner(&bind.CallOpts{})
	if err != nil {
		return err
	}
	ownerAddresses := []common.Address{ownerAddr}
	contributorAddresses := []common.Address{charterAuthorAddr}

	var appExpiry *big.Int
	var challengeID *big.Int
	var unstakedDeposit *big.Int

	if tcrAddress != (common.Address{}) {
		// NOTE(IS): If this was called for a newsroom event we don't have a TCR address associated
		// TODO(IS): For now, solution is leaving fields blank, FIX THIS

		tcrcontract, tcrErr := contract.NewCivilTCRContract(tcrAddress, e.client)
		if tcrErr != nil {
			return fmt.Errorf("Error reading from CivilTCR Contract: %v", err)
		}
		listingstcr, listingErr := tcrcontract.Listings(&bind.CallOpts{}, listingAddress)
		if listingErr != nil {
			return fmt.Errorf("Error getting Listings from CivilTCRContract: %v", err)
		}

		appExpiry = listingstcr.ApplicationExpiry
		challengeID = listingstcr.ChallengeID
		unstakedDeposit = listingstcr.UnstakedDeposit
	} else {
		log.Errorf("No existing listing in persistence for listing address: %v", listingAddress.Hex())
	}

	listing := model.NewListing(
		name,
		listingAddress,
		whitelisted,
		lastGovernanceState,
		url,
		charterURI,
		ownerAddr,
		ownerAddresses,
		contributorAddresses,
		creationDate,
		applicationDate,
		approvalDate,
		crawlerutils.CurrentEpochSecsInInt64(),
		appExpiry,
		unstakedDeposit,
		challengeID,
	)
	err = e.listingPersister.CreateListing(listing)
	return err
}

func (e *EventProcessor) persistNewChallenge(event *crawlermodel.Event) error {
	// Cannot get all fields from ChallengeEvent so set those to nil values for now
	payload := event.EventPayload()
	statement := ""
	resolved := false
	challengeID, ok := payload["ChallengeID"]
	if !ok {
		return errors.New("No challengeID found in challenge event")
	}
	challenger, ok := payload["Challenger"]
	if !ok {
		return errors.New("No challenger found in challenge event")
	}
	listingAddress := payload["ListingAddress"]
	if !ok {
		return errors.New("No listingAddress found in challenge event")
	}
	// TODO(IS): stake is minDeposit from parametrizer, rewardPool also from parametrizer
	challenge := model.NewChallenge(challengeID.(*big.Int), listingAddress.(common.Address), statement, nil,
		challenger.(common.Address), resolved, nil, nil, nil, nil, crawlerutils.CurrentEpochSecsInInt64())

	err := e.challengePersister.CreateChallenge(challenge)
	if err != nil {
		return fmt.Errorf("Error persisting new challenge: %v", err)
	}
	return nil
}

func (e *EventProcessor) persistNewAppealChallenge(event *crawlermodel.Event) error {
	// NOTE(IS): Creates a new challenge for an appeal challenge
	// TODO(IS): Have to update existing challengeID with appeal data
	payload := event.EventPayload()
	statement := ""
	resolved := false
	appealChallengeID, ok := payload["AppealChallengeID"]
	if !ok {
		return errors.New("No appealChallengeID found in GrantedAppealChallenged event")
	}
	// challengeID, ok := payload["ChallengeID"]
	// if !ok {
	// 	return errors.New("No appealChallengeID found in GrantedAppealChallenged event")
	// }
	listingAddress := payload["ListingAddress"]
	if !ok {
		return errors.New("No listingAddress found in GrantedAppealChallenged event")
	}
	// TODO(IS): Challenger is the messageSender
	challenger := common.HexToAddress("")
	challenge := model.NewChallenge(appealChallengeID.(*big.Int), listingAddress.(common.Address), statement, nil,
		challenger, resolved, nil, nil, nil, nil, crawlerutils.CurrentEpochSecsInInt64())
	err := e.challengePersister.CreateChallenge(challenge)
	if err != nil {
		return fmt.Errorf("Error persisting new challenge: %v", err)
	}
	return nil
}

func (e *EventProcessor) processTCRApplication(event *crawlermodel.Event) error {
	return e.processTCREvent(event, model.GovernanceStateApplied, whitelistedFalse,
		approvalDateEmptyValue)
}

func (e *EventProcessor) processTCRChallenge(event *crawlermodel.Event) error {
	err := e.persistNewChallenge(event)
	if err != nil {
		return fmt.Errorf("Error processing Challenge: %v", err)
	}
	return e.processTCREvent(event, model.GovernanceStateChallenged, whitelistedNoChange,
		approvalDateNoUpdate)
}

func (e *EventProcessor) processTCRChallengeFailed(event *crawlermodel.Event) error {
	return e.processTCREvent(event, model.GovernanceStateChallengeFailed, whitelistedNoChange,
		approvalDateNoUpdate)
}

func (e *EventProcessor) processTCRChallengeSucceeded(event *crawlermodel.Event) error {
	return e.processTCREvent(event, model.GovernanceStateChallengeSucceeded, whitelistedFalse,
		approvalDateEmptyValue)
}

func (e *EventProcessor) processTCRChallengeFailedOverturned(event *crawlermodel.Event) error {
	return e.processTCREvent(event, model.GovernanceStateFailedChallengeOverturned, whitelistedNoChange,
		approvalDateNoUpdate)
}

func (e *EventProcessor) processTCRChallengeSuccessfulOverturned(event *crawlermodel.Event) error {
	return e.processTCREvent(event, model.GovernanceStateSuccessfulChallengeOverturned, whitelistedFalse,
		approvalDateEmptyValue)
}

func (e *EventProcessor) processTCRApplicationWhitelisted(event *crawlermodel.Event) error {
	approvalDate := crawlerutils.CurrentEpochSecsInInt64()
	return e.processTCREvent(event, model.GovernanceStateAppWhitelisted, whitelistedTrue, approvalDate)
}

func (e *EventProcessor) processTCRApplicationRemoved(event *crawlermodel.Event) error {
	return e.processTCREvent(event, model.GovernanceStateAppRemoved, whitelistedFalse,
		approvalDateEmptyValue)
}

func (e *EventProcessor) processTCRListingRemoved(event *crawlermodel.Event) error {
	return e.processTCREvent(event, model.GovernanceStateRemoved, whitelistedFalse,
		approvalDateEmptyValue)
}

func (e *EventProcessor) processTCRListingWithdrawn(event *crawlermodel.Event) error {
	return e.processTCREvent(event, model.GovernanceStateWithdrawn, whitelistedFalse,
		approvalDateEmptyValue)
}

func (e *EventProcessor) processTCRAppealGranted(event *crawlermodel.Event) error {
	return e.processTCREvent(event, model.GovernanceStateAppealGranted, whitelistedNoChange,
		approvalDateNoUpdate)
}

func (e *EventProcessor) processTCRAppealRequested(event *crawlermodel.Event) error {
	return e.processTCREvent(event, model.GovernanceStateAppealRequested, whitelistedNoChange,
		approvalDateNoUpdate)
}

func (e *EventProcessor) processTCRGrantedAppealChallenged(event *crawlermodel.Event) error {
	// Challenge is started here for appeal
	err := e.persistNewAppealChallenge(event)
	if err != nil {
		return fmt.Errorf("Error processesing GrantedAppealChallenged: %v", err)
	}
	return e.processTCREvent(event, model.GovernanceStateGrantedAppealChallenged, whitelistedNoChange,
		approvalDateNoUpdate)
}

func (e *EventProcessor) processTCRGrantedAppealConfirmed(event *crawlermodel.Event) error {
	return e.processTCREvent(event, model.GovernanceStateGrantedAppealConfirmed, whitelistedNoChange,
		approvalDateNoUpdate)
}

func (e *EventProcessor) processTCRGrantedAppealOverturned(event *crawlermodel.Event) error {
	return e.processTCREvent(event, model.GovernanceStateGrantedAppealOverturned, whitelistedNoChange,
		approvalDateNoUpdate)
}

func (e *EventProcessor) processTCREvent(event *crawlermodel.Event, govState model.GovernanceState,
	whitelisted whitelistedStatus, approvalDate int64) error {
	listingAddress, err := e.listingAddressFromEvent(event)
	if err != nil {
		return err
	}
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
		return err
	}
	tcrAddress := event.ContractAddress()
	wlisting := false
	if listing != nil {
		wlisting = listing.Whitelisted()
	}
	if whitelisted == whitelistedTrue {
		wlisting = true
	} else if whitelisted == whitelistedFalse {
		wlisting = false
	} else if whitelisted == whitelistedFlip {
		wlisting = !wlisting
	}
	if listing == nil {
		if approvalDate == approvalDateNoUpdate {
			approvalDate = approvalDateEmptyValue
		}
		err = e.persistNewListing(
			listingAddress,
			wlisting,
			govState,
			event.Timestamp(),
			event.Timestamp(),
			approvalDate,
			tcrAddress,
		)
	} else {
		var updatedFields []string
		listing.SetLastGovernanceState(govState)
		updatedFields = append(updatedFields, govStateDBModelName)
		if whitelisted != whitelistedNoChange {
			listing.SetWhitelisted(wlisting)
			updatedFields = append(updatedFields, whitelistedDBModelName)
		}
		if approvalDate != approvalDateNoUpdate {
			listing.SetApprovalDateTs(approvalDate)
			updatedFields = append(updatedFields, approvalDateDBModelName)
		}
		// Set challengeID if Challenge occurs
		if govState == model.GovernanceStateChallenged {
			challengeID := event.EventPayload()["ChallengeID"].(*big.Int)
			listing.SetChallengeID(challengeID)
			updatedFields = append(updatedFields, challengeIDDBModelName)
		}
		// On `_ApplicationWhitelisted`, `_ListingRemoved`, `_ApplicationRemoved` events, challenge ID goes back to 0
		if govEventInSlice(govState, model.ResetChallengeIDEvents) {
			challengeID := big.NewInt(0)
			listing.SetChallengeID(challengeID)
			updatedFields = append(updatedFields, challengeIDDBModelName)
		}
		err = e.listingPersister.UpdateListing(listing, updatedFields)
	}
	return err
}

func govEventInSlice(govState model.GovernanceState, listGovState []model.GovernanceState) bool {
	for _, g := range listGovState {
		if g == govState {
			return true
		}
	}
	return false
}

func (e *EventProcessor) listingAddressFromEvent(event *crawlermodel.Event) (common.Address, error) {
	payload := event.EventPayload()
	listingAddrInterface, ok := payload["ListingAddress"]
	if !ok {
		return common.Address{}, errors.New("Unable to find the listing address in the payload")
	}
	return listingAddrInterface.(common.Address), nil
}

func (e *EventProcessor) persistGovernanceEvent(event *crawlermodel.Event) error {
	listingAddress, _ := e.listingAddressFromEvent(event) // nolint: gosec
	err := e.persistNewGovernanceEvent(
		listingAddress,
		event.ContractAddress(),
		event.EventPayload(),
		event.Timestamp(),
		event.EventType(),
		event.Hash(),
		event.LogPayload(),
	)
	return err
}

func (e *EventProcessor) retrieveOrCreateListingForNewsroomEvent(event *crawlermodel.Event) (*model.Listing, error) {
	listingAddress := event.ContractAddress()
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
		return nil, err
	}
	if listing != nil {
		return listing, nil
	}
	// NOTE(IS): In this case, we don't have a tcrAddress associated with a newsroom event.
	tcrAddress := common.Address{}
	err = e.persistNewListing(
		listingAddress,
		false,
		model.GovernanceStateNone,
		event.Timestamp(),
		event.Timestamp(),
		approvalDateEmptyValue,
		tcrAddress,
	)
	if err != nil {
		return nil, err
	}
	listing, err = e.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
		return nil, err
	}
	if listing == nil {
		return nil, errors.New("Failed to create a listing")
	}
	return listing, nil
}

func (e *EventProcessor) processNewsroomNameChanged(event *crawlermodel.Event) error {
	var updatedFields []string
	payload := event.EventPayload()
	listing, err := e.retrieveOrCreateListingForNewsroomEvent(event)
	if err != nil && err != model.ErrPersisterNoResults {
		return fmt.Errorf("Error retrieving listing or creating by address: err: %v", err)
	}
	name, ok := payload["NewName"]
	if !ok {
		return errors.New("No NewName field found")
	}
	listing.SetName(name.(string))
	updatedFields = append(updatedFields, listingNameDBModelName)
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processNewsroomRevisionUpdated(event *crawlermodel.Event) error {
	// Create a new listing if none exists for the address in the event
	_, err := e.retrieveOrCreateListingForNewsroomEvent(event)
	if err != nil && err != model.ErrPersisterNoResults {
		return fmt.Errorf("Error retrieving listing or creating by address: err: %v", err)
	}

	payload := event.EventPayload()
	listingAddress := event.ContractAddress()

	editorAddress, ok := payload["Editor"]
	if !ok {
		return errors.New("No editor address found")
	}
	contentID, ok := payload["ContentId"]
	if !ok {
		return errors.New("No content id found")
	}
	revisionID, ok := payload["RevisionId"]
	if !ok {
		return errors.New("No revision id found")
	}
	// Metadata URI
	revisionURI, ok := payload["Uri"]
	if !ok {
		return errors.New("No revision uri found")
	}

	// Pull data from the newsroom contract
	newsroom, err := contract.NewNewsroomContract(listingAddress, e.client)
	if err != nil {
		return fmt.Errorf("Error creating newsroom contract: err: %v", err)
	}
	content, err := newsroom.GetContent(&bind.CallOpts{}, contentID.(*big.Int))
	if err != nil {
		return fmt.Errorf("Error retrieving newsroom content: err: %v", err)
	}
	contentHash := byte32ToHexString(content.ContentHash)

	// Scrape the metadata and content for the revision
	metadata, scraperContent, err := e.scrapeData(revisionURI.(string))
	if err != nil {
		log.Errorf("Error scraping data: err: %v", err)
	}

	articlePayload := model.ArticlePayload{}
	if metadata != nil {
		articlePayload = e.scraperDataToPayload(metadata, scraperContent)
	}

	// Store the new revision
	revision := model.NewContentRevision(
		listingAddress,
		articlePayload,
		contentHash,
		editorAddress.(common.Address),
		contentID.(*big.Int),
		revisionID.(*big.Int),
		revisionURI.(string),
		event.Timestamp(),
	)
	err = e.revisionPersister.CreateContentRevision(revision)
	return err
}

func (e *EventProcessor) processNewsroomOwnershipTransferred(event *crawlermodel.Event) error {
	var updatedFields []string
	payload := event.EventPayload()
	listing, err := e.retrieveOrCreateListingForNewsroomEvent(event)
	if err != nil && err != model.ErrPersisterNoResults {
		return err
	}
	previousOwner, ok := payload["PreviousOwner"]
	if !ok {
		return errors.New("No previous owner found")
	}
	newOwner, ok := payload["NewOwner"]
	if !ok {
		return errors.New("No new owner found")
	}
	listing.RemoveOwnerAddress(previousOwner.(common.Address))
	listing.AddOwnerAddress(newOwner.(common.Address))
	updatedFields = append(updatedFields, ownerAddDBModelName)
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) scrapeData(metadataURL string) (
	*model.ScraperCivilMetadata, *model.ScraperContent, error) {
	var err error
	var civilMetadata *model.ScraperCivilMetadata
	var content *model.ScraperContent

	if metadataURL != "" {
		civilMetadata, err = e.civilMetadataScraper.ScrapeCivilMetadata(metadataURL)
		if err != nil {
			return nil, nil, err
		}
		// TODO(PN): Hack to fix bad URLs received for metadata
		// Remove this later after testing
		if civilMetadata.Title() == "" && civilMetadata.RevisionContentHash() == "" {
			metadataURL = strings.Replace(metadataURL, "/wp-json", "/crawler-pod/wp-json", -1)
			civilMetadata, err = e.civilMetadataScraper.ScrapeCivilMetadata(metadataURL)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	// TODO(PN): Use canonical URL or the revision URL here?
	// TODO(PN): Commenting out the scraping of content until we make a decision on it

	// if civilMetadata != nil && civilMetadata.RevisionContentURL() != "" {
	// 	content, err = e.contentScraper.ScrapeContent(civilMetadata.RevisionContentURL())
	// 	if err != nil {
	// 		err = fmt.Errorf("Error scraping content: err: %v", err)
	// 	}
	// }
	return civilMetadata, content, err
}

func (e *EventProcessor) scraperDataToPayload(metadata *model.ScraperCivilMetadata,
	content *model.ScraperContent) model.ArticlePayload {
	// TODO(PN): ArticlePayload should be a struct rather than a map
	// TODO(PN): Do we need the content here?
	payload := model.ArticlePayload{}
	payload["title"] = metadata.Title()
	payload["revisionContentHash"] = metadata.RevisionContentHash()
	payload["revisionContentURL"] = metadata.RevisionContentURL()
	payload["canonicalURL"] = metadata.CanonicalURL()
	payload["slug"] = metadata.Slug()
	payload["description"] = metadata.Description()
	payload["primaryTag"] = metadata.PrimaryTag()
	payload["revisionDate"] = metadata.RevisionDate()
	payload["originalPublishDate"] = metadata.OriginalPublishDate()
	payload["opinion"] = metadata.Opinion()
	payload["schemaVersion"] = metadata.SchemaVersion()
	payload["authors"] = e.buildAuthors(metadata)
	payload["images"] = e.buildImages(metadata)
	return payload
}

func (e *EventProcessor) buildAuthors(metadata *model.ScraperCivilMetadata) []map[string]interface{} {
	authors := []map[string]interface{}{}
	for _, author := range metadata.Authors() {
		entry := map[string]interface{}{
			"byline": author.Byline(),
		}
		authors = append(authors, entry)
	}
	return authors
}

func (e *EventProcessor) buildImages(metadata *model.ScraperCivilMetadata) []map[string]interface{} {
	images := []map[string]interface{}{}
	for _, image := range metadata.Images() {
		entry := map[string]interface{}{
			"url":  image.URL(),
			"hash": image.Hash(),
			"h":    image.Height(),
			"w":    image.Width(),
		}
		images = append(images, entry)
	}
	return images
}
