package processor_test

import (
    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/core/types"
    "github.com/joincivil/civil-events-crawler/pkg/contractutils"
    "github.com/joincivil/civil-events-crawler/pkg/generated/contract"
    crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
    "github.com/joincivil/civil-events-crawler/pkg/utils"
    "github.com/joincivil/civil-events-processor/pkg/processor"
    "math/big"
    "testing"
)

var (
    editorAddress = "0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d"

    prevOwnertestAddress = "0xDFe273082089bB7f70Ee36Eebcde64832FE97E55"
    newOwnertestAddress  = "0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d"
)

func createAndProcNameChangedEvent(t *testing.T, contracts *contractutils.AllTestContracts,
    nwsrmProc *processor.NewsroomEventProcessor) *crawlermodel.Event {
    newName := "ThisIsANewName"
    namechanged := &contract.NewsroomContractNameChanged{
        NewName: newName,
        Raw: types.Log{
            Address:     contracts.NewsroomAddr,
            Topics:      []common.Hash{},
            Data:        []byte{},
            BlockNumber: 8888891,
            TxHash:      common.Hash{},
            TxIndex:     1,
            BlockHash:   common.Hash{},
            Index:       10,
            Removed:     false,
        },
    }

    event, _ := crawlermodel.NewEventFromContractEvent(
        "NameChanged",
        "NewsroomContract",
        contracts.NewsroomAddr,
        namechanged,
        utils.CurrentEpochSecsInInt64(),
        crawlermodel.Filterer,
    )
    return event
}

func createAndProcRevisionUpdatedEvent(t *testing.T, contracts *contractutils.AllTestContracts,
    nwsrmProc *processor.NewsroomEventProcessor) *crawlermodel.Event {
    revision := &contract.NewsroomContractRevisionUpdated{
        Editor:     common.HexToAddress(editorAddress),
        ContentId:  big.NewInt(0),
        RevisionId: big.NewInt(0),
        Uri:        "http://joincivil.com/1",
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
    event, _ := crawlermodel.NewEventFromContractEvent(
        "RevisionUpdated",
        "NewsroomContract",
        contracts.NewsroomAddr,
        revision,
        utils.CurrentEpochSecsInInt64(),
        crawlermodel.Watcher,
    )
    return event
}

func createAndProcOwnershipTransferredEvent(t *testing.T, contracts *contractutils.AllTestContracts,
    nwsrmProc *processor.NewsroomEventProcessor) *crawlermodel.Event {
    ownership := &contract.NewsroomContractOwnershipTransferred{
        PreviousOwner: common.HexToAddress(prevOwnertestAddress),
        NewOwner:      common.HexToAddress(newOwnertestAddress),
        Raw: types.Log{
            Address:     contracts.NewsroomAddr,
            Topics:      []common.Hash{},
            Data:        []byte{},
            BlockNumber: 8888891,
            TxHash:      common.Hash{},
            TxIndex:     1,
            BlockHash:   common.Hash{},
            Index:       10,
            Removed:     false,
        },
    }
    event, _ := crawlermodel.NewEventFromContractEvent(
        "OwnershipTransferred",
        "NewsroomContract",
        contracts.NewsroomAddr,
        ownership,
        utils.CurrentEpochSecsInInt64(),
        crawlermodel.Watcher,
    )

    return event
}
