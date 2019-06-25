package processor_test

import (
	// "fmt"
	"math/big"
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
	testAddress = "0xDFe273082089bB7f70Ee36Eebcde64832FE97E55"
)

func setupParameterizerProcessor(t *testing.T) (*contractutils.AllTestContracts,
	*testutils.TestPersister, *processor.ParameterizerEventProcessor) {

	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &testutils.TestPersister{}
	paramProc := processor.NewParameterizerEventProcessor(
		contracts.Client,
		persister,
		persister,
		persister,
		persister,
		&cerrors.NullErrorReporter{},
	)
	return contracts, persister, paramProc
}

func createNewPollInPersistence(challengeID *big.Int, persister *testutils.TestPersister,
	commitEndDate *big.Int, revealEndDate *big.Int) {
	poll := model.NewPoll(challengeID, commitEndDate, revealEndDate, big.NewInt(1000),
		big.NewInt(100), big.NewInt(100), ctime.CurrentEpochSecsInInt64())
	_ = persister.CreatePoll(poll)
}

func createAndProcNewChallengeEvent(t *testing.T, contracts *contractutils.AllTestContracts,
	paramProc *processor.ParameterizerEventProcessor, persister *testutils.TestPersister) *crawlermodel.Event {

	challengeID := big.NewInt(3)
	commitEndDate := big.NewInt(1653860896)
	revealEndDate := big.NewInt(1663860896)
	challenge := &contract.ParameterizerContractNewChallenge{
		PropID:        [32]byte{0x00, 0x01},
		ChallengeID:   big.NewInt(3),
		CommitEndDate: big.NewInt(1653860896),
		RevealEndDate: big.NewInt(1663860896),
		Challenger:    common.HexToAddress(testAddress),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888890,
			TxHash:      common.Hash{},
			TxIndex:     4,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false},
	}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"NewChallenge",
		"ParameterizerContract",
		contracts.ParamAddr,
		challenge,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)

	createNewPollInPersistence(challengeID, persister, commitEndDate, revealEndDate)

	_, err := paramProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events, err: %v", err)
	}
	return event
}

func createAndProcNewChallengeFailedEvent(t *testing.T, contracts *contractutils.AllTestContracts,
	paramProc *processor.ParameterizerEventProcessor) *crawlermodel.Event {
	challengeFailed := &contract.ParameterizerContractChallengeFailed{
		PropID:      [32]byte{0x00, 0x01},
		ChallengeID: big.NewInt(3),
		RewardPool:  big.NewInt(10000000),
		TotalTokens: big.NewInt(100),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888990,
			TxHash:      common.Hash{},
			TxIndex:     4,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false},
	}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"ChallengeFailed",
		"ParameterizerContract",
		contracts.ParamAddr,
		challengeFailed,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	_, err := paramProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events, err: %v", err)
	}
	return event
}

func createAndProcNewChallengeSucceededEvent(t *testing.T, contracts *contractutils.AllTestContracts,
	paramProc *processor.ParameterizerEventProcessor) *crawlermodel.Event {
	challengeSucceeded := &contract.ParameterizerContractChallengeSucceeded{
		PropID:      [32]byte{0x00, 0x01},
		ChallengeID: big.NewInt(3),
		RewardPool:  big.NewInt(10000000),
		TotalTokens: big.NewInt(100),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888991,
			TxHash:      common.Hash{},
			TxIndex:     4,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false},
	}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"ChallengeSucceeded",
		"ParameterizerContract",
		contracts.ParamAddr,
		challengeSucceeded,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	_, err := paramProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events, err: %v", err)
	}
	return event
}

func createAndProcNewReparameterizationProp(t *testing.T, contracts *contractutils.AllTestContracts,
	paramProc *processor.ParameterizerEventProcessor) *crawlermodel.Event {
	reparameterizationProposal := &contract.ParameterizerContractReparameterizationProposal{
		Name:       "commitStageLen",
		Value:      big.NewInt(1800),
		PropID:     [32]byte{0x00, 0x01},
		Deposit:    big.NewInt(10000000000000000),
		AppEndDate: big.NewInt(1547765493),
		Proposer:   common.HexToAddress("0xcEC56F1D4Dc439E298D5f8B6ff3Aa6be58Cd6Fdf"),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888991,
			TxHash:      common.Hash{},
			TxIndex:     4,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false},
	}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"ReparameterizationProposal",
		"ParameterizerContract",
		contracts.ParamAddr,
		reparameterizationProposal,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	_, err := paramProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events, err: %v", err)
	}
	return event
}

func createAndProcNewProposalAccepted(t *testing.T, contracts *contractutils.AllTestContracts,
	paramProc *processor.ParameterizerEventProcessor) *crawlermodel.Event {
	propAccepted := &contract.ParameterizerContractProposalAccepted{
		Name:   "commitStageLen",
		Value:  big.NewInt(1800),
		PropID: [32]byte{0x00, 0x01},
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888991,
			TxHash:      common.Hash{},
			TxIndex:     4,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false},
	}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"ProposalAccepted",
		"ParameterizerContract",
		contracts.ParamAddr,
		propAccepted,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	_, err := paramProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events, err: %v", err)
	}
	return event
}

func createAndProcNewProposalExpired(t *testing.T, contracts *contractutils.AllTestContracts,
	paramProc *processor.ParameterizerEventProcessor) *crawlermodel.Event {
	propExpired := &contract.ParameterizerContractProposalExpired{
		PropID: [32]byte{0x00, 0x01},
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888991,
			TxHash:      common.Hash{},
			TxIndex:     4,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false},
	}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"ProposalExpired",
		"ParameterizerContract",
		contracts.ParamAddr,
		propExpired,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	_, err := paramProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events, err: %v", err)
	}
	return event
}

func TestParameterizerEventProcessor(t *testing.T) {
	contracts, persister, paramProc := setupParameterizerProcessor(t)
	_ = createAndProcNewReparameterizationProp(t, contracts, paramProc)
	_ = createAndProcNewChallengeEvent(t, contracts, paramProc, persister)
	_ = createAndProcNewProposalExpired(t, contracts, paramProc)
	if len(persister.Challenges) != 1 {
		t.Error("Should have only 1 challenge in persistence")
	}
	if len(persister.ParameterProposal) != 1 {
		t.Error("Should have only 1 parameter proposal in persister")
	}
	memoryCheck(contracts)
}

func TestProcessProposalAccepted(t *testing.T) {
	contracts, persister, paramProc := setupParameterizerProcessor(t)
	reparamProp := createAndProcNewReparameterizationProp(t, contracts, paramProc)
	_ = createAndProcNewProposalAccepted(t, contracts, paramProc)
	if len(persister.ParameterProposal) != 1 {
		t.Error("Should have only 1 parameter proposal in persister")
	}
	payload := reparamProp.EventPayload()
	propID := payload["PropID"]
	persistedProp, _ := persister.ParamProposalByPropID(propID.([32]byte))
	if !persistedProp.Accepted() {
		t.Error("Persisted proposal accepted field should be true")
	}
	memoryCheck(contracts)
}

func TestProcessProposalExpired(t *testing.T) {
	contracts, persister, paramProc := setupParameterizerProcessor(t)
	reparamProp := createAndProcNewReparameterizationProp(t, contracts, paramProc)
	_ = createAndProcNewProposalExpired(t, contracts, paramProc)
	if len(persister.ParameterProposal) != 1 {
		t.Error("Should have only 1 parameter proposal in persister")
	}
	payload := reparamProp.EventPayload()
	propID := payload["PropID"]
	persistedProp, _ := persister.ParamProposalByPropID(propID.([32]byte))
	if !persistedProp.Expired() {
		t.Error("Persisted proposal accepted field should be true")
	}
	memoryCheck(contracts)
}

func TestProcessNewChallenge(t *testing.T) {
	contracts, persister, paramProc := setupParameterizerProcessor(t)
	_ = createAndProcNewReparameterizationProp(t, contracts, paramProc)
	challengeEvent := createAndProcNewChallengeEvent(t, contracts, paramProc, persister)
	if len(persister.Challenges) != 1 {
		t.Error("Should have only 1 challenge in persister")
	}
	payload := challengeEvent.EventPayload()
	challengeID := payload["ChallengeID"]
	persistedChallenge, _ := persister.ChallengeByChallengeID(int(challengeID.(*big.Int).Int64()))
	if persistedChallenge.ListingAddress().Hex() != contracts.ParamAddr.Hex() {
		t.Error("Persisted challenge listingaddress fields do not match")
	}
	memoryCheck(contracts)
}

func TestProcessChallengeFailed(t *testing.T) {
	contracts, persister, paramProc := setupParameterizerProcessor(t)
	_ = createAndProcNewReparameterizationProp(t, contracts, paramProc)
	challengeEvent := createAndProcNewChallengeEvent(t, contracts, paramProc, persister)
	_ = createAndProcNewChallengeFailedEvent(t, contracts, paramProc)
	if len(persister.Challenges) != 1 {
		t.Error("Should have only 1 challenge in persister")
	}
	payload := challengeEvent.EventPayload()
	challengeID := payload["ChallengeID"]
	persistedChallenge, _ := persister.ChallengeByChallengeID(int(challengeID.(*big.Int).Int64()))
	if !persistedChallenge.Resolved() {
		t.Error("Persisted challenge should be resolved")
	}
	memoryCheck(contracts)
}

func TestProcessChallengeSucceeded(t *testing.T) {
	contracts, persister, paramProc := setupParameterizerProcessor(t)
	_ = createAndProcNewReparameterizationProp(t, contracts, paramProc)
	challengeEvent := createAndProcNewChallengeEvent(t, contracts, paramProc, persister)
	_ = createAndProcNewChallengeSucceededEvent(t, contracts, paramProc)
	if len(persister.Challenges) != 1 {
		t.Error("Should have only 1 challenge in persister")
	}
	payload := challengeEvent.EventPayload()
	challengeID := payload["ChallengeID"]
	persistedChallenge, _ := persister.ChallengeByChallengeID(int(challengeID.(*big.Int).Int64()))
	if !persistedChallenge.Resolved() {
		t.Error("Persisted challenge should be resolved")
	}

	memoryCheck(contracts)
}
