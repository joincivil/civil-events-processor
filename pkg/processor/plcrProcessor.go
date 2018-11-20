package processor

import (
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	log "github.com/golang/glog"
	"math/big"
	"strings"

	commongen "github.com/joincivil/civil-events-crawler/pkg/generated/common"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	crawlerutils "github.com/joincivil/civil-events-crawler/pkg/utils"
	"github.com/joincivil/civil-events-processor/pkg/model"
)

const (
	votesForFieldName     = "VotesFor"
	votesAgainstFieldName = "VotesAgainst"
)

// NewPlcrEventProcessor is a convenience function to init an EventProcessor
func NewPlcrEventProcessor(client bind.ContractBackend, pollPersister model.PollPersister) *PlcrEventProcessor {
	return &PlcrEventProcessor{
		client:        client,
		pollPersister: pollPersister,
	}
}

// PlcrEventProcessor handles the processing of raw events into aggregated data
// for use via the API.
type PlcrEventProcessor struct {
	client        bind.ContractBackend
	pollPersister model.PollPersister
}

func (p *PlcrEventProcessor) isValidPLCRContractEventName(name string) bool {
	name = strings.Trim(name, " _")
	eventNames := commongen.EventTypesCivilPLCRVotingContract()
	return isStringInSlice(eventNames, name)
}

// Process processes Plcr Events into aggregated data
func (p *PlcrEventProcessor) Process(event *crawlermodel.Event) (bool, error) {
	if !p.isValidPLCRContractEventName(event.EventType()) {
		return false, nil
	}

	var err error
	ran := true
	eventName := strings.Trim(event.EventType(), " _")
	// Handling all the actionable events from PLCR Contract
	switch eventName {
	case "PollCreated":
		log.Infof("Handling PollCreated for %v\n", event.ContractAddress().Hex())
		err = p.processPollCreated(event)
	case "VoteRevealed":
		log.Infof("Handling VoteRevealed for %v\n", event.ContractAddress().Hex())
		err = p.processVoteRevealed(event)
	}
	return ran, err
}

func (p *PlcrEventProcessor) processPollCreated(event *crawlermodel.Event) error {
	payload := event.EventPayload()
	voteQuorum, ok := payload["VoteQuorum"]
	if !ok {
		return errors.New("No voteQuorum found")
	}

	commitEndDate, ok := payload["CommitEndDate"]
	if !ok {
		return errors.New("No commitEndDate found")
	}

	revealEndDate, ok := payload["RevealEndDate"]
	if !ok {
		return errors.New("No revealEndDate found")
	}

	pollID, ok := payload["PollID"]
	if !ok {
		return errors.New("No pollID found")
	}
	votesFor := big.NewInt(0)
	votesAgainst := big.NewInt(0)

	poll := model.NewPoll(
		pollID.(*big.Int),
		commitEndDate.(*big.Int),
		revealEndDate.(*big.Int),
		voteQuorum.(*big.Int),
		votesFor,
		votesAgainst,
		crawlerutils.CurrentEpochSecsInInt64(),
	)
	err := p.pollPersister.CreatePoll(poll)
	return err
}

func (p *PlcrEventProcessor) processVoteRevealed(event *crawlermodel.Event) error {
	payload := event.EventPayload()
	pollID, ok := payload["PollID"]
	if !ok {
		return errors.New("No pollID found")
	}
	choice, ok := payload["Choice"]
	if !ok {
		return errors.New("No choice found")
	}
	poll, err := p.pollPersister.PollByPollID(int(pollID.(*big.Int).Int64()))
	if err != nil && err != model.ErrPersisterNoResults {
		return err
	}
	if poll == nil {
		// TODO(IS): create new poll. If getting events in order, this shouldn't happen.
		return fmt.Errorf("No poll with ID: %v", pollID)
	}
	var updatedFields []string
	if choice == 1 {
		votesFor, ok := payload["VotesFor"]
		if !ok {
			return errors.New("No votesFor found")
		}
		poll.UpdateVotesFor(votesFor.(*big.Int))
		updatedFields = []string{votesForFieldName}
	} else {
		votesAgainst, ok := payload["VotesAgainst"]
		if !ok {
			return errors.New("No votesAgainst found")
		}
		poll.UpdateVotesAgainst(votesAgainst.(*big.Int))
		updatedFields = []string{votesAgainstFieldName}
	}

	err = p.pollPersister.UpdatePoll(poll, updatedFields)
	return err
}
