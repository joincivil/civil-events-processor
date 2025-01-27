package processor_test

import (
	"math/big"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/joincivil/civil-events-crawler/pkg/contractutils"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/processor"
	"github.com/joincivil/civil-events-processor/pkg/testutils"

	cerrors "github.com/joincivil/go-common/pkg/errors"
	"github.com/joincivil/go-common/pkg/generated/contract"
	ctime "github.com/joincivil/go-common/pkg/time"
)

var (
	pollID1      = big.NewInt(120)
	testAddress2 = "0xDFe273082089bB7f70Ee36Eebcde64832FE97E58"
)

func createChallengeModel(t *testing.T) *model.Challenge {
	return model.NewChallenge(
		pollID1,
		common.HexToAddress(testAddress2),
		"",
		big.NewInt(0),
		common.HexToAddress(testAddress),
		false,
		big.NewInt(0),
		big.NewInt(0),
		big.NewInt(0),
		model.ChallengePollType,
		ctime.CurrentEpochSecsInInt64())
}

func createAndProcPollCreatedEvent(t *testing.T, contracts *contractutils.AllTestContracts,
	plcrProc *processor.PlcrEventProcessor, persister *testutils.TestPersister) *crawlermodel.Event {
	pollCreated := &contract.CivilPLCRVotingContractPollCreated{
		VoteQuorum:    big.NewInt(100),
		CommitEndDate: big.NewInt(1653860896),
		RevealEndDate: big.NewInt(1653860896),
		PollID:        pollID1,
		Creator:       common.HexToAddress(testAddress),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888890,
			TxHash:      common.Hash{},
			TxIndex:     4,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false,
		}}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_PollCreated",
		"CivilPLCRVotingContract",
		contracts.PlcrAddr,
		pollCreated,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	challengeEvent := createChallengeModel(t)
	_ = persister.CreateChallenge(challengeEvent)
	_, err := plcrProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return event
}

func createAndProcVoteCommittedVotesForEvent(t *testing.T, contracts *contractutils.AllTestContracts,
	plcrProc *processor.PlcrEventProcessor) *crawlermodel.Event {
	voteCommitted := &contract.CivilPLCRVotingContractVoteCommitted{
		PollID:    pollID1,
		NumTokens: big.NewInt(100),
		Voter:     common.HexToAddress(testAddress),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888880,
			TxHash:      common.Hash{},
			TxIndex:     4,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false,
		}}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_VoteCommitted",
		"CivilPLCRVotingContract",
		contracts.PlcrAddr,
		voteCommitted,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	_, err := plcrProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return event
}

func createAndProcVoteRevealedVotesForEvent(t *testing.T, contracts *contractutils.AllTestContracts,
	plcrProc *processor.PlcrEventProcessor) *crawlermodel.Event {
	voteRevealed := &contract.CivilPLCRVotingContractVoteRevealed{
		PollID:       pollID1,
		NumTokens:    big.NewInt(100),
		VotesFor:     big.NewInt(500),
		VotesAgainst: big.NewInt(0),
		Choice:       big.NewInt(1),
		Voter:        common.HexToAddress(testAddress),
		Salt:         big.NewInt(20),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888890,
			TxHash:      common.Hash{},
			TxIndex:     4,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false,
		}}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_VoteRevealed",
		"CivilPLCRVotingContract",
		contracts.PlcrAddr,
		voteRevealed,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	_, err := plcrProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return event
}

func createAndProcVoteCommittedVotesAgstEvent(t *testing.T, contracts *contractutils.AllTestContracts,
	plcrProc *processor.PlcrEventProcessor) *crawlermodel.Event {
	voteCommitted := &contract.CivilPLCRVotingContractVoteCommitted{
		PollID:    pollID1,
		NumTokens: big.NewInt(100),
		Voter:     common.HexToAddress(testAddress2),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888880,
			TxHash:      common.Hash{},
			TxIndex:     4,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false,
		}}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_VoteCommitted",
		"CivilPLCRVotingContract",
		contracts.PlcrAddr,
		voteCommitted,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	_, err := plcrProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return event
}

func createAndProcVoteRevealedVotesAgstEvent(t *testing.T, contracts *contractutils.AllTestContracts,
	plcrProc *processor.PlcrEventProcessor) *crawlermodel.Event {
	voteRevealed := &contract.CivilPLCRVotingContractVoteRevealed{
		PollID:       pollID1,
		NumTokens:    big.NewInt(100),
		VotesFor:     big.NewInt(0),
		VotesAgainst: big.NewInt(500),
		Choice:       big.NewInt(0),
		Voter:        common.HexToAddress(testAddress2),
		Salt:         big.NewInt(20),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888890,
			TxHash:      common.Hash{},
			TxIndex:     4,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false,
		}}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_VoteRevealed",
		"CivilPLCRVotingContract",
		contracts.PlcrAddr,
		voteRevealed,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	_, err := plcrProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return event
}

func setupPlcrProcessor(t *testing.T) (*contractutils.AllTestContracts, *testutils.TestPersister,
	*processor.PlcrEventProcessor) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &testutils.TestPersister{}
	plcrProc := processor.NewPlcrEventProcessor(
		contracts.Client,
		persister,
		persister,
		persister,
		persister,
		&cerrors.NullErrorReporter{},
	)
	return contracts, persister, plcrProc
}

func TestPlcrEventProcessor(t *testing.T) {
	contracts, persister, plcrProc := setupPlcrProcessor(t)
	_ = createAndProcPollCreatedEvent(t, contracts, plcrProc, persister)
	if len(persister.Polls) != 1 {
		t.Error("Should have only 1 poll in persistence")
	}
	memoryCheck(contracts)
}

func TestProcessPollCreated(t *testing.T) {
	contracts, persister, plcrProc := setupPlcrProcessor(t)
	pollEvent := createAndProcPollCreatedEvent(t, contracts, plcrProc, persister)
	pollEventPayload := pollEvent.EventPayload()
	poll, ok := persister.Polls[int(pollID1.Int64())]
	if !ok {
		t.Errorf("Could not get poll from persistence for pollID %v ", pollID1)
	}
	if !reflect.DeepEqual(poll.CommitEndDate(), pollEventPayload["CommitEndDate"].(*big.Int)) {
		t.Error("Poll CommitEndDate is not correct")
	}
	if !reflect.DeepEqual(poll.RevealEndDate(), pollEventPayload["RevealEndDate"].(*big.Int)) {
		t.Error("Poll RevealEndDate is not correct")
	}
	if !reflect.DeepEqual(poll.VotesFor(), big.NewInt(0)) {
		t.Error("Poll VotesFor is not correct")
	}
	if !reflect.DeepEqual(poll.VotesAgainst(), big.NewInt(0)) {
		t.Error("Poll VotesAgainst is not correct")
	}
	if !reflect.DeepEqual(poll.VoteQuorum(), pollEventPayload["VoteQuorum"].(*big.Int)) {
		t.Error("Poll VoteQuorum is not correct")
	}
	memoryCheck(contracts)
}

func TestProcessVoteRevealed(t *testing.T) {
	contracts, persister, plcrProc := setupPlcrProcessor(t)
	_ = createAndProcPollCreatedEvent(t, contracts, plcrProc, persister)
	_ = createAndProcVoteCommittedVotesForEvent(t, contracts, plcrProc)
	voteRevealed := createAndProcVoteRevealedVotesForEvent(t, contracts, plcrProc)
	voteRevealedPayload := voteRevealed.EventPayload()

	poll, ok := persister.Polls[int(pollID1.Int64())]
	if !ok {
		t.Errorf("Could not get poll from persistence for pollID %v ", pollID1)
	}
	if !reflect.DeepEqual(poll.VotesFor(), voteRevealedPayload["VotesFor"].(*big.Int)) {
		t.Error("Poll VotesFor is not correct")
	}
	if !reflect.DeepEqual(poll.VotesAgainst(), voteRevealedPayload["VotesAgainst"].(*big.Int)) {
		t.Error("Poll VotesAgainst is not correct")
	}
	_ = createAndProcVoteCommittedVotesAgstEvent(t, contracts, plcrProc)
	voteRevealedAgainst := createAndProcVoteRevealedVotesAgstEvent(t, contracts, plcrProc)
	voteRevealedAgainstPayload := voteRevealedAgainst.EventPayload()
	poll, ok = persister.Polls[int(pollID1.Int64())]

	if !ok {
		t.Errorf("Could not get poll from persistence for pollID %v ", pollID1)
	}
	totalVotesFor := big.NewInt(0)
	if !reflect.DeepEqual(poll.VotesFor(), totalVotesFor.Add(totalVotesFor, voteRevealedPayload["VotesFor"].(*big.Int))) {
		t.Errorf("Poll VotesFor is not correct %v %v", poll.VotesFor(), voteRevealedAgainstPayload["VotesFor"])
	}
	if !reflect.DeepEqual(poll.VotesAgainst(), voteRevealedAgainstPayload["VotesAgainst"].(*big.Int)) {
		t.Error("Poll VotesAgainst is not correct")
	}
	memoryCheck(contracts)
}
