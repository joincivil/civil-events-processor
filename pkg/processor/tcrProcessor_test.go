package processor_test

import (
	// "fmt"
	"math/big"
	// "reflect"
	"runtime"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/joincivil/civil-events-crawler/pkg/contractutils"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	// "github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/processor"
	// "github.com/joincivil/civil-events-processor/pkg/testutils"

	"github.com/joincivil/go-common/pkg/generated/contract"
	ctime "github.com/joincivil/go-common/pkg/time"
)

var (
	challengeID1 = big.NewInt(120)
	// appealChallengeID1 = big.NewInt(130)
)

func memoryCheck(contracts *contractutils.AllTestContracts) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// fmt.Printf("alloc = %v, totalalloc = %v, sys = %v, gor = %v\n",
	// 	m.Alloc/1000000,
	// 	m.TotalAlloc/1000000,
	// 	m.Sys/1000000,
	// 	runtime.NumGoroutine())
	// contracts.ClearStruct()
	runtime.GC()
}

func createAndProcAppEvent(t *testing.T, tcrProc *processor.TcrEventProcessor,
	newsroomAddress common.Address, tcrAddress common.Address) *crawlermodel.Event {
	application := &contract.CivilTCRContractApplication{
		ListingAddress: newsroomAddress,
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
		tcrAddress,
		application,
		ctime.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	_, err := tcrProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return event
}

// func createAndProcAppWhitelistedEvent(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	whitelisted1 := &contract.CivilTCRContractApplicationWhitelisted{
// 		ListingAddress: newsroomAddress,
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888895,
// 			TxHash:      common.Hash{},
// 			TxIndex:     8,
// 			BlockHash:   common.Hash{},
// 			Index:       7,
// 			Removed:     false},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_ApplicationWhitelisted",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		whitelisted1,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Watcher,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func createAndProcDepositEvent(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	deposit := &contract.CivilTCRContractDeposit{
// 		ListingAddress: newsroomAddress,
// 		Added:          big.NewInt(100),
// 		NewTotal:       big.NewInt(1100),
// 		Owner:          common.HexToAddress(testAddress),
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888895,
// 			TxHash:      common.Hash{},
// 			TxIndex:     8,
// 			BlockHash:   common.Hash{},
// 			Index:       7,
// 			Removed:     false},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_Deposit",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		deposit,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Watcher,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func createAndProcWithdrawalEvent(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	withdrawal := &contract.CivilTCRContractWithdrawal{
// 		ListingAddress: newsroomAddress,
// 		Withdrew:       big.NewInt(100),
// 		NewTotal:       big.NewInt(900),
// 		Owner:          common.HexToAddress(testAddress),
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888898,
// 			TxHash:      common.Hash{},
// 			TxIndex:     8,
// 			BlockHash:   common.Hash{},
// 			Index:       7,
// 			Removed:     false},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_Withdrawal",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		withdrawal,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Watcher,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func createAndProcAppRemoved(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	appRemoved := &contract.CivilTCRContractApplicationRemoved{
// 		ListingAddress: newsroomAddress,
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888898,
// 			TxHash:      common.Hash{},
// 			TxIndex:     8,
// 			BlockHash:   common.Hash{},
// 			Index:       7,
// 			Removed:     false},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_ApplicationRemoved",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		appRemoved,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Watcher,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func createAndProcListingRemoved(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	listingRemoved := &contract.CivilTCRContractApplicationRemoved{
// 		ListingAddress: newsroomAddress,
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888898,
// 			TxHash:      common.Hash{},
// 			TxIndex:     8,
// 			BlockHash:   common.Hash{},
// 			Index:       7,
// 			Removed:     false},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_ListingRemoved",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		listingRemoved,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Watcher,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func createNewPollInPersistence(challengeID *big.Int, persister *testutils.TestPersister,
// 	commitEndDate *big.Int, revealEndDate *big.Int) {
// 	poll := model.NewPoll(challengeID, commitEndDate, revealEndDate, big.NewInt(1000),
// 		big.NewInt(100), big.NewInt(100), ctime.CurrentEpochSecsInInt64())
// 	_ = persister.CreatePoll(poll)
// }

// func createAndProcChallenge1(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address, persister *testutils.TestPersister) *crawlermodel.Event {
// 	// NOTE: if creating new challenge, also have to create a new poll
// 	commitEndDate := big.NewInt(1653860896)
// 	revealEndDate := big.NewInt(1653860896)
// 	challenge1 := &contract.CivilTCRContractChallenge{
// 		ListingAddress: newsroomAddress,
// 		ChallengeID:    challengeID1,
// 		Data:           "DATA",
// 		CommitEndDate:  commitEndDate,
// 		RevealEndDate:  revealEndDate,
// 		Challenger:     common.HexToAddress(testAddress),
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888890,
// 			TxHash:      common.Hash{},
// 			TxIndex:     4,
// 			BlockHash:   common.Hash{},
// 			Index:       7,
// 			Removed:     false,
// 		},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_Challenge",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		challenge1,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Filterer,
// 	)
// 	createNewPollInPersistence(challengeID1, persister, commitEndDate, revealEndDate)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func createAndProcChallenge1Failed(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	challenge1Failed := &contract.CivilTCRContractChallengeFailed{
// 		ListingAddress: newsroomAddress,
// 		ChallengeID:    challengeID1,
// 		RewardPool:     big.NewInt(100),
// 		TotalTokens:    big.NewInt(100),
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888900,
// 			TxHash:      common.Hash{},
// 			TxIndex:     4,
// 			BlockHash:   common.Hash{},
// 			Index:       7,
// 			Removed:     false,
// 		},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_ChallengeFailed",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		challenge1Failed,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Filterer,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func createAndProcChallenge1Succeeded(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	challenge1Succeeded := &contract.CivilTCRContractChallengeSucceeded{
// 		ListingAddress: newsroomAddress,
// 		ChallengeID:    challengeID1,
// 		RewardPool:     big.NewInt(100),
// 		TotalTokens:    big.NewInt(100),
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888900,
// 			TxHash:      common.Hash{},
// 			TxIndex:     4,
// 			BlockHash:   common.Hash{},
// 			Index:       7,
// 			Removed:     false,
// 		},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_ChallengeSucceeded",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		challenge1Succeeded,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Filterer,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func createAndProcFailedChallenge1Overturned(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	failedChallenge1Overturned := &contract.CivilTCRContractFailedChallengeOverturned{
// 		ListingAddress: newsroomAddress,
// 		ChallengeID:    challengeID1,
// 		RewardPool:     big.NewInt(100),
// 		TotalTokens:    big.NewInt(100),
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888900,
// 			TxHash:      common.Hash{},
// 			TxIndex:     5,
// 			BlockHash:   common.Hash{},
// 			Index:       8,
// 			Removed:     false,
// 		},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_FailedChallengeOverturned",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		failedChallenge1Overturned,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Filterer,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func createAndProcSuccessfulChallenge1Overturned(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	successfulChallenge1Overturned := &contract.CivilTCRContractSuccessfulChallengeOverturned{
// 		ListingAddress: newsroomAddress,
// 		ChallengeID:    challengeID1,
// 		RewardPool:     big.NewInt(100),
// 		TotalTokens:    big.NewInt(100),
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888900,
// 			TxHash:      common.Hash{},
// 			TxIndex:     4,
// 			BlockHash:   common.Hash{},
// 			Index:       7,
// 			Removed:     false,
// 		},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_SuccessfulChallengeOverturned",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		successfulChallenge1Overturned,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Filterer,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func createAndProcAppealGranted(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	appealGranted := &contract.CivilTCRContractAppealGranted{
// 		ListingAddress: newsroomAddress,
// 		ChallengeID:    challengeID1,
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888888,
// 			TxHash:      common.Hash{},
// 			TxIndex:     2,
// 			BlockHash:   common.Hash{},
// 			Index:       3,
// 			Removed:     false,
// 		},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_AppealGranted",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		appealGranted,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Filterer,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event

// }

// func createAndProcAppealRequested(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	appealRequested := &contract.CivilTCRContractAppealRequested{
// 		ListingAddress: newsroomAddress,
// 		ChallengeID:    challengeID1,
// 		AppealFeePaid:  big.NewInt(1000),
// 		Requester:      common.HexToAddress(testAddress),
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888888,
// 			TxHash:      common.Hash{},
// 			TxIndex:     2,
// 			BlockHash:   common.Hash{},
// 			Index:       2,
// 			Removed:     false,
// 		},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_AppealRequested",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		appealRequested,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Filterer,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func createAndProcGrantedAppealChallenged(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	appealChallenged := &contract.CivilTCRContractGrantedAppealChallenged{
// 		ListingAddress:    newsroomAddress,
// 		ChallengeID:       challengeID1,
// 		AppealChallengeID: appealChallengeID1,
// 		Data:              "DATA",
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888889,
// 			TxHash:      common.Hash{},
// 			TxIndex:     2,
// 			BlockHash:   common.Hash{},
// 			Index:       2,
// 			Removed:     false,
// 		},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_GrantedAppealChallenged",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		appealChallenged,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Filterer,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func createAndProcGrantedAppealConfirmed(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	appealConfirmed := &contract.CivilTCRContractGrantedAppealConfirmed{
// 		ListingAddress:    newsroomAddress,
// 		ChallengeID:       challengeID1,
// 		AppealChallengeID: appealChallengeID1,
// 		RewardPool:        big.NewInt(1010101),
// 		TotalTokens:       big.NewInt(110101),
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888889,
// 			TxHash:      common.Hash{},
// 			TxIndex:     2,
// 			BlockHash:   common.Hash{},
// 			Index:       2,
// 			Removed:     false,
// 		},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_GrantedAppealConfirmed",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		appealConfirmed,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Filterer,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func createAndProcGrantedAppealOverturned(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	appealOverturned := &contract.CivilTCRContractGrantedAppealOverturned{
// 		ListingAddress:    newsroomAddress,
// 		ChallengeID:       challengeID1,
// 		AppealChallengeID: appealChallengeID1,
// 		RewardPool:        big.NewInt(1010101),
// 		TotalTokens:       big.NewInt(110101),
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888889,
// 			TxHash:      common.Hash{},
// 			TxIndex:     2,
// 			BlockHash:   common.Hash{},
// 			Index:       2,
// 			Removed:     false,
// 		},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_GrantedAppealOverturned",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		appealOverturned,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Filterer,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func createAndProcTouchAndRemovedEvent(t *testing.T,
// 	tcrProc *processor.TcrEventProcessor, newsroomAddress common.Address,
// 	tcrAddress common.Address) *crawlermodel.Event {
// 	touchAndRemoved := &contract.CivilTCRContractTouchAndRemoved{
// 		ListingAddress: newsroomAddress,
// 		Raw: types.Log{
// 			Address:     common.HexToAddress(testAddress),
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 8888889,
// 			TxHash:      common.Hash{},
// 			TxIndex:     2,
// 			BlockHash:   common.Hash{},
// 			Index:       2,
// 			Removed:     false,
// 		},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"_TouchAndRemoved",
// 		"CivilTCRContract",
// 		tcrAddress,
// 		touchAndRemoved,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Filterer,
// 	)
// 	_, err := tcrProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

// func setupTcrProcessor(t *testing.T) (*contractutils.AllTestContracts, *testutils.TestPersister,
// 	*processor.TcrEventProcessor) {
// 	contracts, err := contractutils.SetupAllTestContracts()
// 	if err != nil {
// 		t.Fatalf("Unable to setup the contracts: %v", err)
// 	}
// 	persister := &testutils.TestPersister{}
// 	tcrProc := processor.NewTcrEventProcessor(
// 		contracts.Client,
// 		persister,
// 		persister,
// 		persister,
// 		persister,
// 		persister,
// 		persister)
// 	return contracts, persister, tcrProc
// }

// func TestTcrEventProcessor(t *testing.T) {
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	_ = createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	_ = createAndProcChallenge1(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr, persister)
// 	_ = createAndProcChallenge1Succeeded(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	_ = createAndProcAppealRequested(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	_ = createAndProcAppealGranted(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)

// 	_ = createAndProcGrantedAppealChallenged(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	_ = createAndProcGrantedAppealOverturned(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)

// 	if len(persister.Listings) != 1 {
// 		t.Error("Should have seen at least 1 listing")
// 	}
// 	if len(persister.GovEvents[listingAddress]) != 7 {
// 		t.Error("Should have seen 7 governance events")
// 	}
// 	if len(persister.Challenges) != 2 {
// 		t.Error("Should have seen 1 challenge")
// 	}
// 	if len(persister.Appeals) != 1 {
// 		t.Error("Should have seen 1 appeal")
// 	}
// 	memoryCheck(contracts)
// }

// func TestProcessTCRApplication(t *testing.T) {
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	event := createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	eventPayload := event.EventPayload()

// 	listing := persister.Listings[listingAddress]
// 	if listing.LastGovernanceState() != model.GovernanceStateApplied {
// 		t.Errorf("Listing should have had governance state of applied")
// 	}
// 	if listing.Whitelisted() {
// 		t.Errorf("Should not be whitelisted")
// 	}
// 	// NOTE(IS): Charter will be set with a content revision event, not with application event
// 	// if listing.Charter().URI() != "newsroom.com/charter" {
// 	// 	t.Errorf("Listing charter URI is not correct")
// 	// }
// 	if listing.ContractAddress().Hex() != listingAddress {
// 		t.Errorf("Should have the correct newsroom address")
// 	}
// 	if len(listing.OwnerAddresses()) <= 0 {
// 		t.Errorf("Should have at least one owner address")
// 	}

// 	if !reflect.DeepEqual(listing.UnstakedDeposit(), eventPayload["Deposit"].(*big.Int)) {
// 		t.Errorf("UnstakedDeposit value is not correct: %v", listing.UnstakedDeposit())
// 	}
// 	if !reflect.DeepEqual(listing.AppExpiry(), eventPayload["AppEndDate"].(*big.Int)) {
// 		t.Errorf("AppExpiry value is not correct: %v", listing.AppExpiry())
// 	}

// 	if listing.ApplicationDateTs() != event.Timestamp() {
// 		t.Errorf("ApplicationDateTs value is not correct: %v", listing.ApplicationDateTs())
// 	}

// 	if listing.ApprovalDateTs() != 0 {
// 		t.Errorf("ApplicationDateTs value is not correct: %v", listing.ApplicationDateTs())
// 	}
// 	memoryCheck(contracts)
// }

// func TestProcessTCRApplicationWhitelisted(t *testing.T) {
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	_ = createAndProcAppWhitelistedEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	listing := persister.Listings[listingAddress]
// 	if listing.LastGovernanceState() != model.GovernanceStateAppWhitelisted {
// 		t.Errorf("Listing should have had governance state of applied")
// 	}
// 	if !listing.Whitelisted() {
// 		t.Errorf("Should be whitelisted")
// 	}
// 	memoryCheck(contracts)
// }

// func TestProcessTCRApplicationRemoved(t *testing.T) {
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcAppRemoved(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	listing := persister.Listings[listingAddress]
// 	if listing.LastGovernanceState() != model.GovernanceStateAppRemoved {
// 		t.Errorf("Listing should have had governance state of appremoved")
// 	}
// 	if !reflect.DeepEqual(listing.UnstakedDeposit(), big.NewInt(0)) {
// 		t.Errorf("UnstakedDeposit value is not correct: %v", listing.UnstakedDeposit())
// 	}
// 	if !reflect.DeepEqual(listing.AppExpiry(), big.NewInt(0)) {
// 		t.Errorf("AppExpiry value is not correct: %v", listing.AppExpiry())
// 	}
// 	if listing.Whitelisted() {
// 		t.Errorf("Whitelisted value is not correct: %v", listing.Whitelisted())
// 	}
// 	if !reflect.DeepEqual(listing.ChallengeID(), big.NewInt(0)) {
// 		t.Errorf("ChallengeID value is not correct: %v", listing.ChallengeID())
// 	}
// 	if !reflect.DeepEqual(listing.OwnerAddresses(), []common.Address{}) {
// 		t.Errorf("OwnerAddresses value is not correct: %v", listing.OwnerAddresses())
// 	}
// 	if !reflect.DeepEqual(listing.Owner(), common.Address{}) {
// 		t.Errorf("OwnerAddress value is not correct: %v", listing.Owner())
// 	}
// 	if !reflect.DeepEqual(listing.ContributorAddresses(), []common.Address{}) {
// 		t.Errorf("ContributorAddresses value is not correct %v", listing.ContributorAddresses())
// 	}
// 	memoryCheck(contracts)
// }

// func TestProcessTCRDepositWithdrawal(t *testing.T) {
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	event := createAndProcWithdrawalEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	eventPayload := event.EventPayload()
// 	withdrawal := eventPayload["NewTotal"]
// 	listing := persister.Listings[listingAddress]
// 	if !reflect.DeepEqual(listing.UnstakedDeposit(), withdrawal.(*big.Int)) {
// 		t.Errorf("UnstakedDeposit value is not correct: %v", listing.UnstakedDeposit())
// 	}
// 	event = createAndProcDepositEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	eventPayload = event.EventPayload()
// 	deposit := eventPayload["NewTotal"]
// 	listing = persister.Listings[listingAddress]
// 	if !reflect.DeepEqual(listing.UnstakedDeposit(), deposit.(*big.Int)) {
// 		t.Errorf("UnstakedDeposit value is not correct: %v", listing.UnstakedDeposit())
// 	}
// 	memoryCheck(contracts)
// }

// func TestProcessTCRListingRemoved(t *testing.T) {
// 	// Same updates as appremoved
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcListingRemoved(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	listing := persister.Listings[listingAddress]
// 	if listing.LastGovernanceState() != model.GovernanceStateRemoved {
// 		t.Errorf("Listing should have had governance state of app removed")
// 	}
// 	if !reflect.DeepEqual(listing.UnstakedDeposit(), big.NewInt(0)) {
// 		t.Errorf("UnstakedDeposit value is not correct: %v", listing.UnstakedDeposit())
// 	}
// 	if !reflect.DeepEqual(listing.AppExpiry(), big.NewInt(0)) {
// 		t.Errorf("AppExpiry value is not correct: %v", listing.AppExpiry())
// 	}
// 	if listing.Whitelisted() {
// 		t.Errorf("Whitelisted value is not correct: %v", listing.Whitelisted())
// 	}
// 	if !reflect.DeepEqual(listing.ChallengeID(), big.NewInt(0)) {
// 		t.Errorf("ChallengeID value is not correct: %v", listing.ChallengeID())
// 	}
// 	if !reflect.DeepEqual(listing.OwnerAddresses(), []common.Address{}) {
// 		t.Errorf("OwnerAddresses value is not correct: %v", listing.OwnerAddresses())
// 	}
// 	if !reflect.DeepEqual(listing.Owner(), common.Address{}) {
// 		t.Errorf("OwnerAddress value is not correct: %v", listing.Owner())
// 	}
// 	if !reflect.DeepEqual(listing.ContributorAddresses(), []common.Address{}) {
// 		t.Errorf("ContributorAddresses value is not correct %v", listing.ContributorAddresses())
// 	}
// 	memoryCheck(contracts)
// }

// func TestProcessTCRChallenge(t *testing.T) {
// 	//Listing: challengeID, lastUpdatedDateTs, unstakedDeposit,
// 	// Challenge: new Challenge _data has statement data

// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	listing := persister.Listings[listingAddress]
// 	unstakedDeposit := listing.UnstakedDeposit()

// 	challengeEvent := createAndProcChallenge1(t, tcrProc, contracts.NewsroomAddr,
// 		contracts.CivilTcrAddr, persister)
// 	eventPayload := challengeEvent.EventPayload()

// 	listing = persister.Listings[listingAddress]
// 	challenge := persister.Challenges[int(challengeID1.Int64())]

// 	if listing.LastGovernanceState() != model.GovernanceStateChallenged {
// 		t.Errorf("Listing should have had governance state of challenged")
// 	}
// 	if listing.ChallengeID() != challengeID1 {
// 		t.Errorf("Listing challenge ID is not correct")
// 	}
// 	if listing.UnstakedDeposit() != unstakedDeposit.Sub(unstakedDeposit, challenge.Stake()) {
// 		t.Errorf("Listing unstaked deposit value is not correct")
// 	}

// 	if challenge.ChallengeID() != challengeID1 {
// 		t.Errorf("Challenge challenge ID is not correct")
// 	}
// 	if challenge.ListingAddress().Hex() != listingAddress {
// 		t.Errorf("Challenge listingAddress is not correct")
// 	}
// 	if challenge.Statement() != eventPayload["Data"] {
// 		t.Errorf("Challenge statement is not correct")
// 	}
// 	// TODO: These values need to be tested using simulated backend bc they are from the contract.
// 	// Currently they are all nil.
// 	// fmt.Println(challenge.Challenger())
// 	// fmt.Println(challenge.RewardPool())
// 	// fmt.Println(challenge.Resolved())
// 	// fmt.Println(challenge.Stake())
// 	// fmt.Println(challenge.TotalTokens())
// 	// fmt.Println(challenge.RequestAppealExpiry())
// 	memoryCheck(contracts)
// }

// func TestProcessTCRChallengeFailed(t *testing.T) {
// 	// Listing: unstakedDeposit,  lastUpdatedDateTs (whitelisted will be changed upon _ApplicationWhitelisted event)
// 	// challengeid is reset to 0
// 	// Challenge: resolved, totalTokens. If appeal is requested and not granted, have to update: rewardPool, stake
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	// listing := persister.listings[listingAddress]
// 	// unstakedDeposit := listing.UnstakedDeposit()

// 	_ = createAndProcChallenge1(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr, persister)
// 	challengeFailedEvent := createAndProcChallenge1Failed(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	challengeFailedEventPayload := challengeFailedEvent.EventPayload()

// 	listing := persister.Listings[listingAddress]
// 	if listing.LastGovernanceState() != model.GovernanceStateChallengeFailed {
// 		t.Errorf("Listing should have had governance state of challengefailed %v", listing.LastGovernanceState())
// 	}
// 	if listing.ChallengeID().Cmp(big.NewInt(0)) != 0 {
// 		t.Errorf("Listing challengeID should have been reset to 0 but it is %v", listing.ChallengeID())
// 	}
// 	challenge := persister.Challenges[int(challengeID1.Int64())]
// 	if challenge.TotalTokens() != challengeFailedEventPayload["TotalTokens"] {
// 		t.Errorf("Challenge TotalTokens is not correct %v, %v", challenge.TotalTokens(), challengeFailedEventPayload["TotalTokens"])
// 	}
// 	// fmt.Println(unstakedDeposit)
// 	// Test for case where appeal is requested and not granted: Need simulated backend to test this
// 	memoryCheck(contracts)
// }

// func TestProcessTCRChallengeSucceeded(t *testing.T) {
// 	// Challenge:* *resolved, totalTokens.  If appeal is requested and not granted, have to update: rewardPool, stake
// 	// Listing: No changes (On resetListing call, listingRemoved will be called, and changes will be made there)
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	// listing := persister.listings[listingAddress]
// 	// unstakedDeposit := listing.UnstakedDeposit()

// 	_ = createAndProcChallenge1(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr, persister)
// 	challengeSucceededEvent := createAndProcChallenge1Succeeded(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	challengeSucceededEventPayload := challengeSucceededEvent.EventPayload()

// 	listing := persister.Listings[listingAddress]
// 	if listing.LastGovernanceState() != model.GovernanceStateChallengeSucceeded {
// 		t.Errorf("Listing should have had governance state of challengesucceeded")
// 	}

// 	challenge := persister.Challenges[int(challengeID1.Int64())]
// 	// Need simulated backend to test
// 	if challenge.TotalTokens() != challengeSucceededEventPayload["TotalTokens"] {
// 		t.Error("Challenge TotalTokens value is not correct")
// 	}
// 	if !challenge.Resolved() {
// 		t.Error("Challenge resolved value is not correct")
// 	}

// 	// Test for case where appeal is requested and not granted
// 	memoryCheck(contracts)
// }

// func TestProcessTCRFailedChallengeOverturned(t *testing.T) {
// 	// Challenge: resolved, totaltokens
// 	// Listing: No changes (On resetListing call, changes will be made depending on conditions)
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	listingAddress := contracts.NewsroomAddr.Hex()

// 	_ = createAndProcChallenge1(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr, persister)
// 	_ = createAndProcChallenge1Failed(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)

// 	challengeOverturnedEvent := createAndProcFailedChallenge1Overturned(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)

// 	challengeOverturnedEventPayload := challengeOverturnedEvent.EventPayload()
// 	listing := persister.Listings[listingAddress]
// 	if listing.LastGovernanceState() != model.GovernanceStateFailedChallengeOverturned {
// 		t.Errorf("Listing should have had governance state of failedchallengeoverturned %v", listing.LastGovernanceState())
// 	}
// 	challenge := persister.Challenges[int(challengeID1.Int64())]
// 	if challenge.TotalTokens() != challengeOverturnedEventPayload["TotalTokens"] {
// 		t.Errorf("Challenge TotalTokens is not correct %v %v", challenge.TotalTokens(), challengeOverturnedEventPayload["TotalTokens"])
// 	}
// 	if !challenge.Resolved() {
// 		t.Error("Challenge Resolved should be true")
// 	}
// 	memoryCheck(contracts)
// }

// func TestProcessTCRSuccessfulChallengeOverturned(t *testing.T) {
// 	// Challenge: resolved, totalTokens
// 	// Listing: unstakedDeposit, set challengeid = 0 other changes made in whitelistApplication() call
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	listing := persister.Listings[listingAddress]
// 	_ = createAndProcChallenge1(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr, persister)
// 	_ = createAndProcChallenge1Succeeded(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)

// 	challengeOverturnedEvent := createAndProcSuccessfulChallenge1Overturned(t, tcrProc,
// 		contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	challengeOverturnedEventPayload := challengeOverturnedEvent.EventPayload()

// 	challenge := persister.Challenges[int(challengeID1.Int64())]
// 	if challenge.TotalTokens() != challengeOverturnedEventPayload["TotalTokens"] {
// 		t.Errorf("Challenge TotalTokens is not correct %v %v", challenge.TotalTokens(), challengeOverturnedEventPayload["TotalTokens"])
// 	}
// 	if !challenge.Resolved() {
// 		t.Error("Challenge Resolved should be true")
// 	}

// 	if listing.LastGovernanceState() != model.GovernanceStateSuccessfulChallengeOverturned {
// 		t.Errorf("Listing should have had governance state of successfulchallengeoverturned %v", listing.LastGovernanceState())
// 	}
// 	if listing.ChallengeID().Cmp(big.NewInt(0)) != 0 {
// 		t.Errorf("ChallengeID should be 0 but it is %v", listing.ChallengeID())
// 	}
// 	//unstaked deposit value check
// 	memoryCheck(contracts)
// }

// func TestProcessTCRAppealRequested(t *testing.T) {
// 	// Appeal: new appeal
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	_ = createAndProcChallenge1(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr, persister)
// 	_ = createAndProcChallenge1Succeeded(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)

// 	appealRequestedEvent := createAndProcAppealRequested(t, tcrProc,
// 		contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	appealRequestedEventPayload := appealRequestedEvent.EventPayload()
// 	appeal := persister.Appeals[int(challengeID1.Int64())]
// 	if appeal.OriginalChallengeID() != appealRequestedEventPayload["ChallengeID"] {
// 		t.Errorf("Appeal challengeID is not correct %v %v", appeal.OriginalChallengeID(), appealRequestedEventPayload["ChallengeID"])
// 	}
// 	if appeal.AppealFeePaid() != appealRequestedEventPayload["AppealFeePaid"] {
// 		t.Errorf("Appeal appealfeepaid is not correct %v %v", appeal.AppealFeePaid(), appealRequestedEventPayload["AppealFeePaid"])
// 	}
// 	if appeal.Requester() != appealRequestedEventPayload["Requester"] {
// 		t.Errorf("Appeal requester is not correct %v %v", appeal.Requester(), appealRequestedEventPayload["Requester"])
// 	}
// 	if appeal.Statement() != appealRequestedEventPayload["Data"] {
// 		t.Errorf("Appeal statement is not correct %v %v", appeal.Statement(), appealRequestedEventPayload["Data"])
// 	}
// 	// TODO: simulated backend needed for this
// 	// fmt.Println(appeal.AppealPhaseExpiry())

// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	listing := persister.Listings[listingAddress]

// 	if listing.LastGovernanceState() != model.GovernanceStateAppealRequested {
// 		t.Errorf("Listing last governance state is not what it should be %v",
// 			listing.LastGovernanceState())
// 	}
// 	memoryCheck(contracts)
// }

// func TestProcessTCRAppealGranted(t *testing.T) {
// 	// Appeal: appealGranted, appealOpenToChallengeExpiry
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	_ = createAndProcChallenge1(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr, persister)
// 	_ = createAndProcChallenge1Succeeded(t, tcrProc, contracts.NewsroomAddr,
// 		contracts.CivilTcrAddr)

// 	_ = createAndProcAppealRequested(t, tcrProc, contracts.NewsroomAddr,
// 		contracts.CivilTcrAddr)

// 	_ = createAndProcAppealGranted(t, tcrProc, contracts.NewsroomAddr,
// 		contracts.CivilTcrAddr)
// 	// appealGrantedEventPayload := appealGrantedEvent.EventPayload()
// 	appeal := persister.Appeals[int(challengeID1.Int64())]

// 	if !appeal.AppealGranted() {
// 		t.Error("Appeal Granted should be true")
// 	}
// 	if appeal.AppealGrantedStatementURI() != "" {
// 		t.Error("Should have set AppealGrantedStatementURI")
// 	}
// 	// need simulated backend for this:
// 	// fmt.Println(appeal.AppealPhaseExpiry())

// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	listing := persister.Listings[listingAddress]

// 	if listing.LastGovernanceState() != model.GovernanceStateAppealGranted {
// 		t.Errorf("Listing last governance state is not what it should be %v",
// 			listing.LastGovernanceState())
// 	}
// 	memoryCheck(contracts)
// }

// func TestProcessTCRGrantedAppealChallenged(t *testing.T) {
// 	// Appeal: appealChallengeID
// 	// Challenge: new challenge
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	_ = createAndProcChallenge1(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr, persister)
// 	_ = createAndProcChallenge1Succeeded(t, tcrProc, contracts.NewsroomAddr,
// 		contracts.CivilTcrAddr)

// 	_ = createAndProcAppealRequested(t, tcrProc, contracts.NewsroomAddr,
// 		contracts.CivilTcrAddr)

// 	_ = createAndProcAppealGranted(t, tcrProc, contracts.NewsroomAddr,
// 		contracts.CivilTcrAddr)
// 	grantedAppealChallengedEvent := createAndProcGrantedAppealChallenged(t, tcrProc,
// 		contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	grantedAppealChallengedEventPayload := grantedAppealChallengedEvent.EventPayload()
// 	// check that all challenge fields are correct for the appeal challenge
// 	appealChallenge, ok := persister.Challenges[int(appealChallengeID1.Int64())]
// 	if !ok {
// 		t.Error("appealChallenge is not in persistence")
// 	}
// 	if appealChallenge.Statement() != grantedAppealChallengedEventPayload["Data"] {
// 		t.Errorf("Challenge Statement data is not correct %v %v", appealChallenge.Statement(),
// 			grantedAppealChallengedEventPayload["Data"])
// 	}
// 	// TODO: use simulated backend to check rest of challenge res

// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	listing := persister.Listings[listingAddress]
// 	if listing.LastGovernanceState() != model.GovernanceStateGrantedAppealChallenged {
// 		t.Errorf("Listing last governance state is not what it should be %v",
// 			listing.LastGovernanceState())
// 	}
// 	memoryCheck(contracts)
// }

// func TestProcessTCRGrantedAppealConfirmed(t *testing.T) {
// 	// Challenge: resolved, totalTokens, changes from call to resolveOverturnedChallenge()
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	_ = createAndProcChallenge1(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr, persister)
// 	_ = createAndProcChallenge1Succeeded(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)

// 	_ = createAndProcAppealRequested(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)

// 	_ = createAndProcAppealGranted(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)

// 	_ = createAndProcGrantedAppealChallenged(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	grantedAppealConfirmedEvent := createAndProcGrantedAppealConfirmed(t, tcrProc,
// 		contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	grantedAppealConfirmedEventPayload := grantedAppealConfirmedEvent.EventPayload()

// 	appealChallenge, ok := persister.Challenges[int(appealChallengeID1.Int64())]
// 	if !ok {
// 		t.Error("appealChallenge is not in persistence")
// 	}
// 	if appealChallenge.TotalTokens() != grantedAppealConfirmedEventPayload["TotalTokens"] {
// 		t.Errorf("Challenge TotalTokens value is not correct %v %v", appealChallenge.TotalTokens(),
// 			grantedAppealConfirmedEventPayload["TotalTokens"])
// 	}
// 	if !appealChallenge.Resolved() {
// 		t.Error("Challenge resolved should be true")
// 	}

// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	listing := persister.Listings[listingAddress]
// 	if listing.LastGovernanceState() != model.GovernanceStateGrantedAppealConfirmed {
// 		t.Errorf("Listing last governance state is not what it should be %v",
// 			listing.LastGovernanceState())
// 	}
// 	memoryCheck(contracts)
// }

// func TestProcessTCRGrantedAppealOverturned(t *testing.T) {
// 	// Challenge: resolved, totalTokens, changes from call to super.resolveChallenge()
// 	// Appeal: overturned -- we don't have an overturned field
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	_ = createAndProcChallenge1(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr, persister)
// 	_ = createAndProcChallenge1Succeeded(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)

// 	_ = createAndProcAppealRequested(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)

// 	_ = createAndProcAppealGranted(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)

// 	_ = createAndProcGrantedAppealChallenged(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	grantedAppealOverturnedEvent := createAndProcGrantedAppealOverturned(t, tcrProc,
// 		contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	grantedAppealOverturnedEventPayload := grantedAppealOverturnedEvent.EventPayload()

// 	appealChallenge, ok := persister.Challenges[int(appealChallengeID1.Int64())]
// 	if !ok {
// 		t.Error("appealChallenge is not in persistence")
// 	}
// 	if appealChallenge.TotalTokens() != grantedAppealOverturnedEventPayload["TotalTokens"] {
// 		t.Errorf("Challenge TotalTokens value is not correct %v %v", appealChallenge.TotalTokens(),
// 			grantedAppealOverturnedEventPayload["TotalTokens"])
// 	}
// 	if !appealChallenge.Resolved() {
// 		t.Error("Challenge resolved should be true")
// 	}

// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	listing := persister.Listings[listingAddress]
// 	if listing.LastGovernanceState() != model.GovernanceStateGrantedAppealOverturned {
// 		t.Errorf("Listing last governance state is not what it should be %v",
// 			listing.LastGovernanceState())
// 	}
// 	memoryCheck(contracts)
// }

// func TestUpdateListingWithLastGovState(t *testing.T) {
// 	contracts, persister, tcrProc := setupTcrProcessor(t)
// 	_ = createAndProcAppEvent(t, tcrProc, contracts.NewsroomAddr, contracts.CivilTcrAddr)
// 	_ = createAndProcTouchAndRemovedEvent(t, tcrProc, contracts.NewsroomAddr,
// 		contracts.CivilTcrAddr)
// 	listingAddress := contracts.NewsroomAddr.Hex()
// 	listing := persister.Listings[listingAddress]
// 	if listing.LastGovernanceState() != model.GovernanceStateTouchRemoved {
// 		t.Errorf("Listing last governance state is not what it should be %v",
// 			listing.LastGovernanceState())
// 	}
// 	memoryCheck(contracts)
// }
