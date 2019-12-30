package processor

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/pkg/errors"

	log "github.com/golang/glog"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"

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
	govtParamProposalPersister model.GovernmentParamProposalPersister, govtParameterPersister model.GovernmentParameterPersister, pollPersister model.PollPersister,
	errRep cerrors.ErrorReporter) *GovernmentEventProcessor {
	return &GovernmentEventProcessor{
		client:                     client,
		govtParamProposalPersister: govtParamProposalPersister,
		govtParameterPersister:     govtParameterPersister,
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

func (p *GovernmentEventProcessor) isValidParameterizerContractEventName(name string) bool {
	name = strings.Trim(name, " _")
	eventNames := commongen.EventTypesGovernmentContract()
	return isStringInSlice(eventNames, name)
}

// Process processes Parameterizer Events into aggregated data
func (p *GovernmentEventProcessor) Process(event *crawlermodel.Event) (bool, error) {
	if !p.isValidParameterizerContractEventName(event.EventType()) {
		return false, nil
	}

	var err error
	ran := true
	eventName := strings.Trim(event.EventType(), " _")

	switch eventName {
	case "GovtReparameterizationProposal":
		log.Infof("Handling %v\n", eventName)
		err = p.processGovtReparameterizationProposal(event)
	case "ProposalPassed":
		log.Infof("Handling %v\n", eventName)
		err = p.processProposalPassed(event)
	case "ProposalFailed":
		log.Infof("Handling %v\n", eventName)
		err = p.processProposalFailed(event)
	case "ProposalExpired":
		log.Infof("Handling %v\n", eventName)
		err = p.processProposalExpired(event)
	default:
		ran = false
	}
	return ran, err
}

func (p *GovernmentEventProcessor) processGovtReparameterizationProposal(event *crawlermodel.Event) error {
	return p.newGovtParameterizationFromProposal(event)

}

func (p *GovernmentEventProcessor) getPropIDFromEvent(event *crawlermodel.Event) ([32]byte, error) {
	payload := event.EventPayload()
	propID, ok := payload["PropID"]
	if !ok {
		return [32]byte{}, errors.New("Unable to get PropID in the payload")
	}
	return propID.([32]byte), nil
}

func (p *GovernmentEventProcessor) processProposalPassed(event *crawlermodel.Event) error {
	govtParamProposal, err := p.getExistingGovernmentParameterProposal(event)
	if err != nil {
		return err
	}
	govtParameter, err := p.getExistingGovernmentParameter(event)
	if err != nil {
		return err
	}
	govtParameter.SetValue(govtParamProposal.Value())
	govtParamProposal.SetAccepted(true)
	govtParamProposal.SetExpired(true)
	err = p.govtParameterPersister.UpdateGovernmentParameter(govtParameter, []string{valueFieldName})
	if err != nil {
		return err
	}

	return p.govtParamProposalPersister.UpdateGovernmentParamProposal(govtParamProposal, []string{proposalAcceptedFieldName, proposalExpiredFieldName})
}

func (p *GovernmentEventProcessor) processProposalFailed(event *crawlermodel.Event) error {
	govtParamProposal, err := p.getExistingGovernmentParameterProposal(event)
	if err != nil {
		return err
	}
	govtParamProposal.SetExpired(true)
	return p.govtParamProposalPersister.UpdateGovernmentParamProposal(govtParamProposal, []string{proposalExpiredFieldName})
}

func (p *GovernmentEventProcessor) processProposalExpired(event *crawlermodel.Event) error {
	govtParamProposal, err := p.getExistingGovernmentParameterProposal(event)
	if err != nil {
		return err
	}
	govtParamProposal.SetExpired(true)
	return p.govtParamProposalPersister.UpdateGovernmentParamProposal(govtParamProposal, []string{proposalExpiredFieldName})
}

func (p *GovernmentEventProcessor) getExistingGovernmentParameterProposal(event *crawlermodel.Event) (*model.GovernmentParameterProposal, error) {
	propID, err := p.getPropIDFromEvent(event)
	if err != nil {
		return nil, err
	}
	// get parameterization from db
	paramProposal, err := p.govtParamProposalPersister.GovernmentParamProposalByPropID(propID, true)
	if err != nil && err != cpersist.ErrPersisterNoResults {
		return nil, err
	}
	if err == cpersist.ErrPersisterNoResults {
		paramProposal, err = p.newGovtParameterizationFromContract(event)
		if err != nil {
			return nil, err
		}
	}
	return paramProposal, nil
}

func (p *GovernmentEventProcessor) getExistingGovernmentParameter(event *crawlermodel.Event) (*model.GovernmentParameter, error) {
	propID, err := p.getPropIDFromEvent(event)
	if err != nil {
		return nil, err
	}
	// get parameterization from db, use its `name` value to get parameter
	govtParamProposal, err := p.govtParamProposalPersister.GovernmentParamProposalByPropID(propID, true)
	if err != nil && err != cpersist.ErrPersisterNoResults {
		return nil, err
	}
	// get parameter from db
	govtParameter, err := p.govtParameterPersister.GovernmentParameterByName(govtParamProposal.Name())
	if err != nil && err != cpersist.ErrPersisterNoResults {
		return nil, err
	}

	return govtParameter, nil
}

func (p *GovernmentEventProcessor) newGovtParameterizationFromProposal(event *crawlermodel.Event) error {
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
	pollID, ok := payload["PollID"]
	if !ok {
		return errors.New("No Proposer found")
	}
	// IF events are out of order this could be true
	accepted := false
	currentTime := ctime.CurrentEpochSecsInInt64()

	govtPCommitStageLenParam, err := p.govtParameterPersister.GovernmentParameterByName("govtPCommitStageLen")
	if err != nil {
		return err
	}
	govtPCommitStageLen := govtPCommitStageLenParam.Value()

	govtPRevealStageLenParam, err := p.govtParameterPersister.GovernmentParameterByName("govtPRevealStageLen")
	if err != nil {
		return err
	}
	govtPRevealStageLen := govtPRevealStageLenParam.Value()

	appExpiry := *(big.NewInt(0))
	appExpiry.Add(govtPCommitStageLen, govtPRevealStageLen)
	appExpiry.Add(&appExpiry, big.NewInt(event.Timestamp()))
	appExpiry.Add(&appExpiry, big.NewInt(604800))

	nameString := name.(string)
	valueString := (value.(*big.Int)).String()
	appExpiryString := (appExpiry).String()

	id := nameString + valueString + appExpiryString

	govtParamProposal := model.NewGovernmentParameterProposal(&model.GovernmentParameterProposalParams{
		ID:                id,
		Name:              name.(string),
		Value:             value.(*big.Int),
		PropID:            propID.([32]byte),
		AppExpiry:         &appExpiry,
		PollID:            pollID.(*big.Int),
		Accepted:          accepted,
		Expired:           false,
		LastUpdatedDateTs: currentTime,
	})

	// newParamProposal
	err = p.govtParamProposalPersister.CreateGovernmentParameterProposal(govtParamProposal)
	return err
}

func (p *GovernmentEventProcessor) newGovtParameterizationFromContract(event *crawlermodel.Event) (*model.GovernmentParameterProposal, error) {
	payload := event.EventPayload()
	propID, ok := payload["PropID"]
	if !ok {
		return nil, errors.New("No PropID field found")
	}
	paramContract, err := contract.NewGovernmentContract(event.ContractAddress(), p.client)
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
	if currentTime < prop.ProcessBy.Int64() {
		expired = false
	} else {
		expired = true
	}
	// setting accepted to false for now
	accepted := false

	paramProposal := model.NewGovernmentParameterProposal(&model.GovernmentParameterProposalParams{
		Name:              prop.Name,
		Value:             prop.Value,
		PropID:            propID.([32]byte),
		AppExpiry:         prop.ProcessBy,
		PollID:            prop.PollID,
		Accepted:          accepted,
		Expired:           expired,
		LastUpdatedDateTs: currentTime,
	})
	return paramProposal, nil
}

func (p *GovernmentEventProcessor) setPollIsPassedInPoll(pollID *big.Int, isPassed bool) error {
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
