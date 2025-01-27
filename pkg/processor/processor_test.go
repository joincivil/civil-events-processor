package processor_test

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/joincivil/civil-events-crawler/pkg/contractutils"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/testutils"

	"github.com/joincivil/civil-events-processor/pkg/processor"

	"github.com/joincivil/go-common/pkg/generated/contract"
	ctime "github.com/joincivil/go-common/pkg/time"
)

func TestEmptyContractAddress(t *testing.T) {
	tcrAddress := common.Address{}
	if tcrAddress != (common.Address{}) {
		t.Error("2 blank common.Address types should be equal")
	}
}

func setupEventList(t *testing.T, contracts *contractutils.AllTestContracts) []*crawlermodel.Event {
	events := []*crawlermodel.Event{}
	application := &contract.CivilTCRContractApplication{
		ListingAddress: contracts.NewsroomAddr,
		Deposit:        big.NewInt(1000),
		AppEndDate:     big.NewInt(1653860896),
		Data:           "DATA",
		Applicant:      common.HexToAddress(testAddress),
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
		"_Application",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		application,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event)
	revision := &contract.NewsroomContractRevisionUpdated{
		Editor:     common.HexToAddress(editorAddress),
		ContentId:  big.NewInt(0),
		RevisionId: big.NewInt(0),
		Uri:        "http://joincivil.com/charter",
		Raw: types.Log{
			Address:     contracts.NewsroomAddr,
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 888889,
			TxHash:      common.Hash{},
			TxIndex:     3,
			BlockHash:   common.Hash{},
			Index:       4,
			Removed:     false,
		},
	}
	event, _ = crawlermodel.NewEventFromContractEvent(
		"RevisionUpdated",
		"NewsroomContract",
		contracts.NewsroomAddr,
		revision,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
	)
	events = append(events, event)
	challenge1 := &contract.CivilTCRContractChallenge{
		ListingAddress: contracts.NewsroomAddr,
		ChallengeID:    challengeID1,
		Data:           "DATA",
		CommitEndDate:  big.NewInt(1653860896),
		RevealEndDate:  big.NewInt(1653860896),
		Challenger:     common.HexToAddress(testAddress),
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
		},
	}
	event, _ = crawlermodel.NewEventFromContractEvent(
		"_Challenge",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		challenge1,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event)
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
	event, _ = crawlermodel.NewEventFromContractEvent(
		"_PollCreated",
		"CivilPLCRVotingContract",
		contracts.PlcrAddr,
		pollCreated,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event)
	appealRequested := &contract.CivilTCRContractAppealRequested{
		ListingAddress: contracts.NewsroomAddr,
		ChallengeID:    challengeID1,
		AppealFeePaid:  big.NewInt(1000),
		Requester:      common.HexToAddress(testAddress),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888888,
			TxHash:      common.Hash{},
			TxIndex:     2,
			BlockHash:   common.Hash{},
			Index:       2,
			Removed:     false,
		},
	}
	event, _ = crawlermodel.NewEventFromContractEvent(
		"_AppealRequested",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		appealRequested,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event)
	tokenTransfer := &contract.CVLTokenContractTransfer{
		From:  common.HexToAddress(testAddress),
		To:    common.HexToAddress(testAddress),
		Value: big.NewInt(100000000),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888888,
			TxHash:      common.Hash{},
			TxIndex:     2,
			BlockHash:   common.Hash{},
			Index:       2,
			Removed:     false,
		},
	}
	event, _ = crawlermodel.NewEventFromContractEvent(
		"Transfer",
		"CVLTokenContract",
		contracts.TokenAddr,
		tokenTransfer,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	events = append(events, event)
	return events
}

func TestProcessor(t *testing.T) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	// _, err = contracts.CivilTcrContract.Challenge(&bind.TransactOpts{}, contracts.NewsroomAddr, "")
	// if err != nil {
	// 	t.Fatalf("Was not able to challenge: err: %v", err)
	// }
	persister := &testutils.TestPersister{}
	processorParams := &processor.NewEventProcessorParams{
		Client:                 contracts.Client,
		ListingPersister:       persister,
		RevisionPersister:      persister,
		GovEventPersister:      persister,
		ChallengePersister:     persister,
		PollPersister:          persister,
		AppealPersister:        persister,
		TokenTransferPersister: persister,
		MultiSigPersister:      persister,
		MultiSigOwnerPersister: persister,
	}
	proc := processor.NewEventProcessor(processorParams)
	events := setupEventList(t, contracts)
	err = proc.Process(events)
	if err != nil {
		t.Fatalf("Error processing events: %v", err)
	}
	if len(persister.Listings) != 1 {
		t.Errorf("Should have only seen 1 listing but saw %v", len(persister.Listings))
	}
	if len(persister.GovEvents[contracts.NewsroomAddr.Hex()]) != 2 {
		t.Errorf("Should have seen 2 govEvents but saw %v", len(persister.GovEvents[contracts.NewsroomAddr.Hex()]))
	}
	// if len(persister.Challenges) != 1 {
	// 	t.Errorf("Should have seen 1 challenge but saw %v", len(persister.Challenges))
	// }
	if len(persister.Revisions[contracts.NewsroomAddr.Hex()]) != 1 {
		t.Errorf("Should have seen 1 revision but saw %v", len(persister.Revisions))
	}
	if len(persister.Polls) != 1 {
		t.Errorf("Should have seen 2 polls but saw %v", len(persister.Polls))
	}
	if len(persister.Appeals) != 1 {
		t.Errorf("Should have seen 1 appeal but saw %v", len(persister.Appeals))
	}
	if len(persister.TokenTransfers) != 1 {
		t.Errorf("Should have seen 1 token transfer but saw %v", len(persister.TokenTransfers))
	}
	memoryCheck(contracts)
}
