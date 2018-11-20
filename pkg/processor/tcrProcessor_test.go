package processor_test

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/joincivil/civil-events-crawler/pkg/contractutils"
	"github.com/joincivil/civil-events-crawler/pkg/generated/contract"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	"github.com/joincivil/civil-events-crawler/pkg/utils"
	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/processor"
	"math/big"
	"reflect"
	"testing"
)

var (
	testAddress = "0xDFe273082089bB7f70Ee36Eebcde64832FE97E55"
)

func createAndProcAppEvent(t *testing.T, contracts *contractutils.AllTestContracts,
	tcrProc *processor.TcrEventProcessor) *crawlermodel.Event {
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
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Filterer,
	)
	_, err := tcrProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return event
}

func createAndProcAppWhitelistedEvent(t *testing.T, contracts *contractutils.AllTestContracts,
	tcrProc *processor.TcrEventProcessor) *crawlermodel.Event {
	whitelisted1 := &contract.CivilTCRContractApplicationWhitelisted{
		ListingAddress: contracts.NewsroomAddr,
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888895,
			TxHash:      common.Hash{},
			TxIndex:     8,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false},
	}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_ApplicationWhitelisted",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		whitelisted1,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
	)
	_, err := tcrProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return event
}

func createAndProcDepositEvent(t *testing.T, contracts *contractutils.AllTestContracts,
	tcrProc *processor.TcrEventProcessor) *crawlermodel.Event {
	deposit := &contract.CivilTCRContractDeposit{
		ListingAddress: contracts.NewsroomAddr,
		Added:          big.NewInt(100),
		NewTotal:       big.NewInt(1100),
		Owner:          common.HexToAddress(testAddress),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888895,
			TxHash:      common.Hash{},
			TxIndex:     8,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false},
	}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_Deposit",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		deposit,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
	)
	_, err := tcrProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return event
}

func createAndProcWithdrawalEvent(t *testing.T, contracts *contractutils.AllTestContracts,
	tcrProc *processor.TcrEventProcessor) *crawlermodel.Event {
	withdrawal := &contract.CivilTCRContractWithdrawal{
		ListingAddress: contracts.NewsroomAddr,
		Withdrew:       big.NewInt(100),
		NewTotal:       big.NewInt(900),
		Owner:          common.HexToAddress(testAddress),
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888898,
			TxHash:      common.Hash{},
			TxIndex:     8,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false},
	}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_Withdrawal",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		withdrawal,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
	)
	_, err := tcrProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return event
}

func createAndProcAppRemoved(t *testing.T, contracts *contractutils.AllTestContracts,
	tcrProc *processor.TcrEventProcessor) *crawlermodel.Event {
	appRemoved := &contract.CivilTCRContractApplicationRemoved{
		ListingAddress: contracts.NewsroomAddr,
		Raw: types.Log{
			Address:     common.HexToAddress(testAddress),
			Topics:      []common.Hash{},
			Data:        []byte{},
			BlockNumber: 8888898,
			TxHash:      common.Hash{},
			TxIndex:     8,
			BlockHash:   common.Hash{},
			Index:       7,
			Removed:     false},
	}
	event, _ := crawlermodel.NewEventFromContractEvent(
		"_ApplicationRemoved",
		"CivilTCRContract",
		contracts.CivilTcrAddr,
		appRemoved,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
	)
	_, err := tcrProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return event
}

func setupTcrProcessor(t *testing.T) (*contractutils.AllTestContracts, *TestPersister,
	*processor.TcrEventProcessor) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &TestPersister{}
	tcrProc := processor.NewTcrEventProcessor(
		contracts.Client,
		persister,
		persister,
		persister,
		persister)
	return contracts, persister, tcrProc
}

func TestTcrEventProcessor(t *testing.T) {
	// here should test all events and then count that the numbers in persistence adds up.
	// In individual functions, test if the data is right.
	contracts, persister, tcrProc := setupTcrProcessor(t)
	events := []*crawlermodel.Event{}
	event1 := createAndProcAppEvent(t, contracts, tcrProc)
	listingAddress := contracts.NewsroomAddr.Hex()
	events = append(events, event1)

	if len(persister.listings) != 1 {
		t.Error("Should have seen at least 1 listing")
	}
	fmt.Println(persister.govEvents[listingAddress])
	if len(persister.govEvents[listingAddress]) != 1 {
		t.Error("Should have seen only 1 governance events")
	}

}

func TestProcessTCRApplication(t *testing.T) {
	contracts, persister, tcrProc := setupTcrProcessor(t)
	listingAddress := contracts.NewsroomAddr.Hex()
	event := createAndProcAppEvent(t, contracts, tcrProc)
	eventPayload := event.EventPayload()

	listing := persister.listings[listingAddress]
	if listing.LastGovernanceState() != model.GovernanceStateApplied {
		t.Errorf("Listing should have had governance state of applied")
	}
	if listing.Whitelisted() {
		t.Errorf("Should not be whitelisted")
	}
	// NOTE(IS): Charter will be set with a content revision event, not with application event
	// if listing.Charter().URI() != "newsroom.com/charter" {
	// 	t.Errorf("Listing charter URI is not correct")
	// }
	if listing.ContractAddress().Hex() != listingAddress {
		t.Errorf("Should have the correct newsroom address")
	}
	if len(listing.OwnerAddresses()) <= 0 {
		t.Errorf("Should have at least one owner address")
	}

	if !reflect.DeepEqual(listing.UnstakedDeposit(), eventPayload["Deposit"].(*big.Int)) {
		t.Errorf("UnstakedDeposit value is not correct: %v", listing.UnstakedDeposit())
	}
	if !reflect.DeepEqual(listing.AppExpiry(), eventPayload["AppEndDate"].(*big.Int)) {
		t.Errorf("AppExpiry value is not correct: %v", listing.AppExpiry())
	}

	if listing.ApplicationDateTs() != event.Timestamp() {
		t.Errorf("ApplicationDateTs value is not correct: %v", listing.ApplicationDateTs())
	}

	if listing.ApprovalDateTs() != 0 {
		t.Errorf("ApplicationDateTs value is not correct: %v", listing.ApplicationDateTs())
	}

	if listing.LastUpdatedDateTs() != event.Timestamp() {
		t.Errorf("LastUpdatedDateTs value is not correct: %v", listing.LastUpdatedDateTs())
	}
}

func TestProcessTCRApplicationWhitelisted(t *testing.T) {
	contracts, persister, tcrProc := setupTcrProcessor(t)
	_ = createAndProcAppEvent(t, contracts, tcrProc)
	listingAddress := contracts.NewsroomAddr.Hex()
	_ = createAndProcAppWhitelistedEvent(t, contracts, tcrProc)
	listing := persister.listings[listingAddress]
	if listing.LastGovernanceState() != model.GovernanceStateAppWhitelisted {
		t.Errorf("Listing should have had governance state of applied")
	}
	if !listing.Whitelisted() {
		t.Errorf("Should be whitelisted")
	}
}

func TestProcessTCRApplicationRemoved(t *testing.T) {
	contracts, persister, tcrProc := setupTcrProcessor(t)
	_ = createAndProcAppRemoved(t, contracts, tcrProc)
	listingAddress := contracts.NewsroomAddr.Hex()
	listing := persister.listings[listingAddress]
	if listing.LastGovernanceState() != model.GovernanceStateAppRemoved {
		t.Errorf("Listing should have had governance state of appremoved")
	}
	if !reflect.DeepEqual(listing.UnstakedDeposit(), big.NewInt(0)) {
		t.Errorf("UnstakedDeposit value is not correct: %v", listing.UnstakedDeposit())
	}
	if !reflect.DeepEqual(listing.AppExpiry(), big.NewInt(0)) {
		t.Errorf("AppExpiry value is not correct: %v", listing.AppExpiry())
	}
	if listing.Whitelisted() {
		t.Errorf("Whitelisted value is not correct: %v", listing.Whitelisted())
	}
	if !reflect.DeepEqual(listing.ChallengeID(), big.NewInt(0)) {
		t.Errorf("ChallengeID value is not correct: %v", listing.ChallengeID())
	}
	return

}

func TestProcessTCRDepositWithdrawal(t *testing.T) {
	contracts, persister, tcrProc := setupTcrProcessor(t)
	_ = createAndProcAppEvent(t, contracts, tcrProc)
	listingAddress := contracts.NewsroomAddr.Hex()
	event := createAndProcWithdrawalEvent(t, contracts, tcrProc)
	eventPayload := event.EventPayload()
	withdrawal, _ := eventPayload["NewTotal"]
	listing := persister.listings[listingAddress]
	if reflect.DeepEqual(listing.UnstakedDeposit(), withdrawal.(*big.Int)) {
		t.Errorf("UnstakedDeposit value is not correct: %v", listing.UnstakedDeposit())
	}
	event = createAndProcDepositEvent(t, contracts, tcrProc)
	eventPayload = event.EventPayload()
	deposit, _ := eventPayload["NewTotal"]
	listing = persister.listings[listingAddress]
	if reflect.DeepEqual(listing.UnstakedDeposit(), deposit.(*big.Int)) {
		t.Errorf("UnstakedDeposit value is not correct: %v", listing.UnstakedDeposit())
	}

}

func TestProcessTCRListingRemoved(t *testing.T) {

}

func TestProcessTCRChallenge(t *testing.T) {

}

func TestProcessTCRChallengeFailed(t *testing.T) {

}

func TestProcessTCRChallengeSucceeded(t *testing.T) {

}

func TestProcessTCRFailedChallengeOverturned(t *testing.T) {

}

func TestProcessTCRSuccessfulChallengeOverturned(t *testing.T) {

}

func TestProcessTCRAppealGranted(t *testing.T) {

}

func TestProcessTCRAppealRequested(t *testing.T) {

}

func TestProcessTCRGrantedAppealChallenged(t *testing.T) {

}

func TestProcessTCRGrantedAppealConfirmed(t *testing.T) {

}

func TestProcessTCRGrantedAppealOverturned(t *testing.T) {

}

func TestUpdateListingWithLastGovState(t *testing.T) {

}

// Test some situations of events being out of order
