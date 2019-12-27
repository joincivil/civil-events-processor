package processor

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/pkg/errors"

	log "github.com/golang/glog"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"

	commongen "github.com/joincivil/civil-events-crawler/pkg/generated/common"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	cerrors "github.com/joincivil/go-common/pkg/errors"
	"github.com/joincivil/go-common/pkg/generated/contract"
	cpersist "github.com/joincivil/go-common/pkg/persistence"
	ctime "github.com/joincivil/go-common/pkg/time"

	"github.com/joincivil/civil-events-processor/pkg/model"
)

const (
	govtProposalAcceptedFieldName = "Accepted"
	govtProposalExpiredFieldName  = "Expired"
	govtPollIsPassedFieldName     = "PollIsPassed"
	govtParameterValueFieldName   = "Value"
	govtProcessByDuration         = 604800
)

// NewGovernmentEventProcessor is a convenience function to init a government parameter processor
func NewGovernmentEventProcessor(client bind.ContractBackend,
	govtParamProposalPersister model.GovernmentParamProposalPersister, governmentParameterPersister model.GovernmentParameterPersister, pollPersister model.PollPersister,
	errRep cerrors.ErrorReporter) *ParameterizerEventProcessor {
	return &ParameterizerEventProcessor{
		client:                     client,
		govtParamProposalPersister: govtParamProposalPersister,
		parameterPersister:         parameterPersister,
		pollPersister:              pollPersister,
		errRep:                     errRep,
	}
}

// GovernmentEventProcessor handles the processing of raw events into aggregated data
type GovernmentEventProcessor struct {
	client                     bind.ContractBackend
	govtParamProposalPersister model.GovernmentParamProposalPersister
	govtParameterPersister     model.GovernmentParameterPersister
	pollPersister              model.PollPersister
	errRep                     cerrors.ErrorReporter
}

func (p *GovernmentParameterEventProcessor) isValidParameterizerContractEventName(name string) bool {
	name = strings.Trim(name, " _")
	eventNames := commongen.EventTypesGovernmentContract()
	return isStringInSlice(eventNames, name)
}

// TODO: Move to go-common?
func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

// Process processes Parameterizer Events into aggregated data
func (p *ParameterizerEventProcessor) Process(event *crawlermodel.Event) (bool, error) {
	if !p.isValidParameterizerContractEventName(event.EventType()) {
		return false, nil
	}

	var err error
	ran := true
	eventName := strings.Trim(event.EventType(), " _")

	var challengeID *big.Int
	if stringInSlice(eventName, paramChallengeEventNames) {
		challengeID, err = p.challengeIDFromEvent(event)
		if err != nil {
			return false, err
		}
	}

	// NOTE(IS): Only tracking challenge related data for Parameterizer contract for now.
	switch eventName {
	case "NewChallenge":
		log.Infof("Handling challenge %v\n", *challengeID)
		err = p.processParameterizerChallenge(event, challengeID)
	case "ChallengeFailed":
		log.Infof("Handling challenge %v\n", *challengeID)
		err = p.processChallengeFailed(event, challengeID)
	case "ChallengeSucceeded":
		log.Infof("Handling challenge %v\n", *challengeID)
		err = p.processChallengeSucceeded(event, challengeID)
	case "ReparameterizationProposal":
		log.Infof("Handling %v\n", eventName)
		err = p.processReparameterizationProposal(event)
	case "ProposalAccepted":
		log.Infof("Handling %v\n", eventName)
		err = p.processProposalAccepted(event)
	case "ProposalExpired":
		log.Infof("Handling %v\n", eventName)
		err = p.processProposalExpired(event)
	default:
		ran = false
	}
	return ran, err
}

func (p *ParameterizerEventProcessor) processReparameterizationProposal(event *crawlermodel.Event) error {
	return p.newParameterizationFromProposal(event)

}

func (p *ParameterizerEventProcessor) getPropIDFromEvent(event *crawlermodel.Event) ([32]byte, error) {
	payload := event.EventPayload()
	propID, ok := payload["PropID"]
	if !ok {
		return [32]byte{}, errors.New("Unable to get PropID in the payload")
	}
	return propID.([32]byte), nil
}

func (p *ParameterizerEventProcessor) processProposalAccepted(event *crawlermodel.Event) error {
	paramProposal, err := p.getExistingParameterProposal(event)
	if err != nil {
		return err
	}
	parameter, err := p.getExistingParameter(event)
	if err != nil {
		return err
	}
	parameter.SetValue(paramProposal.Value())
	paramProposal.SetAccepted(true)
	paramProposal.SetExpired(true)
	err = p.parameterPersister.UpdateParameter(parameter, []string{valueFieldName})
	if err != nil {
		return err
	}

	return p.paramProposalPersister.UpdateParamProposal(paramProposal, []string{proposalAcceptedFieldName, proposalExpiredFieldName})
}

func (p *ParameterizerEventProcessor) processProposalExpired(event *crawlermodel.Event) error {
	paramProposal, err := p.getExistingParameterProposal(event)
	if err != nil {
		return err
	}
	paramProposal.SetExpired(true)
	return p.paramProposalPersister.UpdateParamProposal(paramProposal, []string{proposalExpiredFieldName})
}

func (p *ParameterizerEventProcessor) processChallengeFailed(event *crawlermodel.Event,
	challengeID *big.Int) error {

	pollIsPassed := true
	err := p.setPollIsPassedInPoll(challengeID, pollIsPassed)
	if err != nil {
		return fmt.Errorf("Error setting isPassed field in poll, err: %v", err)
	}
	paramProposal, err := p.getExistingParameterProposal(event)
	if err != nil {
		return err
	}
	processBy := paramProposal.AppExpiry().Int64() + processByDuration
	if event.Timestamp() < processBy {
		parameter, err := p.getExistingParameter(event)
		if err != nil {
			return err
		}
		parameter.SetValue(paramProposal.Value())
		err = p.parameterPersister.UpdateParameter(parameter, []string{valueFieldName})
		if err != nil {
			return err
		}
	}

	paramProposal.SetAccepted(true)
	paramProposal.SetExpired(true)
	err = p.paramProposalPersister.UpdateParamProposal(paramProposal, []string{proposalAcceptedFieldName, proposalExpiredFieldName})
	if err != nil {
		return err
	}
	return p.processChallengeResolution(event, challengeID, pollIsPassed)
}

func (p *ParameterizerEventProcessor) processChallengeSucceeded(event *crawlermodel.Event,
	challengeID *big.Int) error {

	pollIsPassed := false
	err := p.setPollIsPassedInPoll(challengeID, pollIsPassed)
	if err != nil {
		return fmt.Errorf("Error setting isPassed field in poll, err: %v", err)
	}
	paramProposal, err := p.getExistingParameterProposal(event)
	if err != nil {
		return err
	}
	paramProposal.SetExpired(true)
	err = p.paramProposalPersister.UpdateParamProposal(paramProposal, []string{proposalExpiredFieldName})
	if err != nil {
		return err
	}
	return p.processChallengeResolution(event, challengeID, pollIsPassed)
}

func (p *ParameterizerEventProcessor) getExistingParameterProposal(event *crawlermodel.Event) (*model.ParameterProposal, error) {
	propID, err := p.getPropIDFromEvent(event)
	if err != nil {
		return nil, err
	}
	// get parameterization from db
	paramProposal, err := p.paramProposalPersister.ParamProposalByPropID(propID, true)
	if err != nil && err != cpersist.ErrPersisterNoResults {
		return nil, err
	}
	if err == cpersist.ErrPersisterNoResults {
		paramProposal, err = p.newParameterizationFromContract(event)
		if err != nil {
			return nil, err
		}
	}
	return paramProposal, nil
}

func (p *ParameterizerEventProcessor) getExistingParameter(event *crawlermodel.Event) (*model.Parameter, error) {
	propID, err := p.getPropIDFromEvent(event)
	if err != nil {
		return nil, err
	}
	// get parameterization from db, use its `name` value to get parameter
	paramProposal, err := p.paramProposalPersister.ParamProposalByPropID(propID, true)
	if err != nil && err != cpersist.ErrPersisterNoResults {
		return nil, err
	}
	// get parameter from db
	parameter, err := p.parameterPersister.ParameterByName(paramProposal.Name())
	if err != nil && err != cpersist.ErrPersisterNoResults {
		return nil, err
	}

	return parameter, nil
}

func (p *ParameterizerEventProcessor) newParameterizationFromProposal(event *crawlermodel.Event) error {
	payload := event.EventPayload()
	name, ok := payload["Name"]
	if !ok {
		return errors.New("No Name found")
	}
	value, ok := payload["Value"]
	if !ok {
		return errors.New("No Value found")
	}
	propID, ok := payload["PropID"]
	if !ok {
		return errors.New("No PropID found")
	}
	deposit, ok := payload["Deposit"]
	if !ok {
		return errors.New("No Deposit found")
	}
	appExpiry, ok := payload["AppEndDate"]
	if !ok {
		return errors.New("No AppEndDate found")
	}
	proposer, ok := payload["Proposer"]
	if !ok {
		return errors.New("No Proposer found")
	}
	// IF events are out of order this could be true
	accepted := false
	currentTime := ctime.CurrentEpochSecsInInt64()

	nameString := name.(string)
	valueString := (value.(*big.Int)).String()
	appExpiryString := (appExpiry.(*big.Int)).String()

	id := nameString + valueString + appExpiryString

	paramProposal := model.NewParameterProposal(&model.ParameterProposalParams{
		ID:                id,
		Name:              name.(string),
		Value:             value.(*big.Int),
		PropID:            propID.([32]byte),
		Deposit:           deposit.(*big.Int),
		AppExpiry:         appExpiry.(*big.Int),
		ChallengeID:       big.NewInt(0),
		Proposer:          proposer.(common.Address),
		Accepted:          accepted,
		Expired:           false,
		LastUpdatedDateTs: currentTime,
	})

	// newParamProposal
	err := p.paramProposalPersister.CreateParameterProposal(paramProposal)
	return err
}

func (p *ParameterizerEventProcessor) newParameterizationFromContract(event *crawlermodel.Event) (*model.ParameterProposal, error) {
	payload := event.EventPayload()
	propID, ok := payload["PropID"]
	if !ok {
		return nil, errors.New("No PropID field found")
	}
	paramContract, err := contract.NewParameterizerContract(event.ContractAddress(), p.client)
	if err != nil {
		return nil, fmt.Errorf("Error calling parameterizer contract: %v", err)
	}
	prop, err := paramContract.Proposals(&bind.CallOpts{}, propID.([32]byte))
	if err != nil {
		return nil, fmt.Errorf("Error calling parameterizer contract: %v", err)
	}

	currentTime := ctime.CurrentEpochSecsInInt64()

	// calculate if expired
	var expired bool
	if currentTime < prop.AppExpiry.Int64() {
		expired = false
	} else {
		expired = true
	}
	// setting accepted to false for now
	accepted := false

	paramProposal := model.NewParameterProposal(&model.ParameterProposalParams{
		Name:              prop.Name,
		Value:             prop.Value,
		PropID:            propID.([32]byte),
		Deposit:           prop.Deposit,
		AppExpiry:         prop.AppExpiry,
		ChallengeID:       prop.ChallengeID,
		Proposer:          prop.Owner,
		Accepted:          accepted,
		Expired:           expired,
		LastUpdatedDateTs: currentTime,
	})
	return paramProposal, nil
}

func (p *ParameterizerEventProcessor) setPollIsPassedInPoll(pollID *big.Int, isPassed bool) error {
	poll, err := p.pollPersister.PollByPollID(int(pollID.Int64()))
	if err != nil {
		return fmt.Errorf("Error getting poll from persistence: %v", err)
	}
	// TODO(IS): Shouldn't happen if all events are processed and in order, but create new poll if DNE
	poll.SetIsPassed(isPassed)
	updatedFields := []string{isPassedFieldName}

	err = p.pollPersister.UpdatePoll(poll, updatedFields)
	if err != nil {
		return fmt.Errorf("Error updating poll in persistence: %v", err)
	}

	return nil

}
