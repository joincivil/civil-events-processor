package processormain_test

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/processormain"
	"github.com/joincivil/civil-events-processor/pkg/testutils"
	"github.com/joincivil/go-common/pkg/generated/contract"
	cstring "github.com/joincivil/go-common/pkg/strings"
	ctime "github.com/joincivil/go-common/pkg/time"
	"math/big"
	"testing"
	"time"
)

const (
	ContractAddress = "0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d"
)

func ReturnRandomTestApplicationEvent(t *testing.T) *contract.CivilTCRContractApplication {
	testAddress, _ := cstring.RandomHexStr(20)
	return &contract.CivilTCRContractApplication{
		ListingAddress: common.HexToAddress(testAddress),
		Deposit:        big.NewInt(1000),
		AppEndDate:     big.NewInt(1653860896),
		Data:           "DATA",
		Applicant:      common.HexToAddress(testAddress),
		Raw: types.Log{
			Address: common.HexToAddress(ContractAddress),
			Topics: []common.Hash{
				common.HexToHash("0x09cd8dcaf170a50a26316b5fe0727dd9fb9581a688d65e758b16a1650da65c0b"),
				common.HexToHash("0x0000000000000000000000002652c60cf04bbf6bb6cc8a5e6f1c18143729d440"),
				common.HexToHash("0x00000000000000000000000025bf9a1595d6f6c70e6848b60cba2063e4d9e552"),
			},
			Data:        []byte("thisisadatastring"),
			BlockNumber: 8888888,
			TxHash:      common.Hash{},
			TxIndex:     2,
			BlockHash:   common.Hash{},
			Index:       2,
			Removed:     false,
		},
	}
}

func ReturnTestEventsSameTimestamp(t *testing.T, numEvents int) []*crawlermodel.Event {
	appEvents := make([]*crawlermodel.Event, numEvents)
	ts := ctime.CurrentEpochSecsInInt64()
	for i := 0; i < numEvents; i++ {
		appEvent := ReturnRandomTestApplicationEvent(t)
		event, err := crawlermodel.NewEventFromContractEvent(
			"Application",
			"CivilTCRContract",
			common.HexToAddress(ContractAddress),
			appEvent,
			ts,
			crawlermodel.Watcher,
		)
		if err != nil {
			t.Errorf("Error creating new event %v", err)
		}
		appEvents[i] = event
	}
	return appEvents
}

func TestSaveLastEventInformation(t *testing.T) {
	testCronPersister := &testutils.TestPersister{}
	events := ReturnTestEventsSameTimestamp(t, 3)
	time.Sleep(1 * time.Second)
	events = append(events, ReturnTestEventsSameTimestamp(t, 4)...)
	err := processormain.SaveLastEventInformation(testCronPersister, events, 0)
	if err != nil {
		t.Errorf("Error saving last event info, err: %v", err)
	}

	hashes, _ := testCronPersister.EventHashesOfLastTimestampForCron()
	if len(hashes) != 4 {
		t.Errorf("Number of hashes returned for timestamp should be %v but is %v", 4, len(hashes))
	}

}
