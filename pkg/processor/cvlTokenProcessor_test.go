package processor_test

// import (
// 	"fmt"
// 	"math/big"
// 	"runtime"
// 	"testing"

// 	"github.com/ethereum/go-ethereum/common"
// 	"github.com/ethereum/go-ethereum/core/types"
// 	"github.com/joincivil/civil-events-crawler/pkg/contractutils"
// 	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

// 	"github.com/joincivil/go-common/pkg/generated/contract"
// 	cstrings "github.com/joincivil/go-common/pkg/strings"
// 	ctime "github.com/joincivil/go-common/pkg/time"

// 	"github.com/joincivil/civil-events-processor/pkg/processor"
// 	"github.com/joincivil/civil-events-processor/pkg/testutils"
// )

// func setupCvlTokenEventProcessor(t *testing.T) (*contractutils.AllTestContracts,
// 	*testutils.TestPersister, *processor.CvlTokenEventProcessor) {
// 	contracts, err := contractutils.SetupAllTestContracts()
// 	if err != nil {
// 		t.Fatalf("Unable to setup the contracts: %v", err)
// 	}
// 	persister := &testutils.TestPersister{}
// 	proc := processor.NewCvlTokenEventProcessor(
// 		contracts.Client,
// 		persister,
// 	)
// 	return contracts, persister, proc
// }

// func createAndProcCvlTokenTransferEvent(t *testing.T, contracts *contractutils.AllTestContracts,
// 	proc *processor.CvlTokenEventProcessor) *crawlermodel.Event {
// 	tokens := big.NewInt(int64(1000))
// 	// In gwei
// 	tokens = tokens.Mul(tokens, big.NewInt(1e18))

// 	addr1, _ := cstrings.RandomHexStr(32)
// 	addr2, _ := cstrings.RandomHexStr(32)

// 	newPurchase := &contract.CVLTokenContractTransfer{
// 		From:  common.HexToAddress(addr1),
// 		To:    common.HexToAddress(addr2),
// 		Value: tokens,
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
// 		"Transfer",
// 		"CVLTokenContract",
// 		contracts.TokenAddr,
// 		newPurchase,
// 		ctime.CurrentEpochSecsInInt64(),
// 		crawlermodel.Filterer,
// 	)
// 	_, err := proc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}

// 	return event
// }
// func TestProcessTransfer(t *testing.T) {
// 	contracts, persister, proc := setupCvlTokenEventProcessor(t)
// 	event := createAndProcCvlTokenTransferEvent(t, contracts, proc)

// 	eventPayload := event.EventPayload()
// 	toAddr, ok := eventPayload["To"]
// 	if !ok {
// 		t.Fatalf("Should have added to address")
// 	}
// 	purchases, err := persister.TokenTransfersByToAddress(toAddr.(common.Address))
// 	if err != nil {
// 		t.Fatalf("Should have not gotten error when retrieving purchases")
// 	}
// 	purchase := purchases[0]

// 	fromAddr, ok := eventPayload["From"]
// 	if !ok {
// 		t.Fatalf("Should have added from address")
// 	}
// 	value, ok := eventPayload["Value"]
// 	if !ok {
// 		t.Fatalf("Should have added value")
// 	}

// 	if purchase.ToAddress().Hex() != toAddr.(common.Address).Hex() {
// 		t.Errorf("ToAddress should have been the same")
// 	}
// 	if purchase.FromAddress().Hex() != fromAddr.(common.Address).Hex() {
// 		t.Errorf("FromAddress should have been the same")
// 	}
// 	if purchase.Amount() != value.(*big.Int) {
// 		t.Errorf("Purchase amoutn should have been the same")
// 	}
// 	memoryCheck()
// 	runtime.GC()
// 	var m runtime.MemStats
// 	runtime.ReadMemStats(&m)
// 	fmt.Printf("after clear: alloc = %v, totalalloc = %v, sys = %v, gor = %v\n",
// 		m.Alloc/1000000,
// 		m.TotalAlloc/1000000,
// 		m.Sys/1000000,
// 		runtime.NumGoroutine())
// }
