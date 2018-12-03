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
	_, err := nwsrmProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return event
}

func createAndProcRevisionUpdatedEventCharter(t *testing.T, contracts *contractutils.AllTestContracts,
	nwsrmProc *processor.NewsroomEventProcessor) *crawlermodel.Event {
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
	event, _ := crawlermodel.NewEventFromContractEvent(
		"RevisionUpdated",
		"NewsroomContract",
		contracts.NewsroomAddr,
		revision,
		utils.CurrentEpochSecsInInt64(),
		crawlermodel.Watcher,
	)
	_, err := nwsrmProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return event
}

// NOTE(IS): Commenting this out bc not working
// func createAndProcRevisionUpdatedEvent(t *testing.T, contracts *contractutils.AllTestContracts,
// 	nwsrmProc *processor.NewsroomEventProcessor) *crawlermodel.Event {
// 	revision := &contract.NewsroomContractRevisionUpdated{
// 		Editor:     common.HexToAddress(editorAddress),
// 		ContentId:  big.NewInt(1),
// 		RevisionId: big.NewInt(0),
// 		Uri:        "http://joincivil.com/content",
// 		Raw: types.Log{
// 			Address:     contracts.NewsroomAddr,
// 			Topics:      []common.Hash{},
// 			Data:        []byte{},
// 			BlockNumber: 888889,
// 			TxHash:      common.Hash{},
// 			TxIndex:     3,
// 			BlockHash:   common.Hash{},
// 			Index:       4,
// 			Removed:     false,
// 		},
// 	}
// 	event, _ := crawlermodel.NewEventFromContractEvent(
// 		"RevisionUpdated",
// 		"NewsroomContract",
// 		contracts.NewsroomAddr,
// 		revision,
// 		utils.CurrentEpochSecsInInt64(),
// 		crawlermodel.Watcher,
// 	)
// 	_, err := nwsrmProc.Process(event)
// 	if err != nil {
// 		t.Errorf("Should not have failed processing events: err: %v", err)
// 	}
// 	return event
// }

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
	_, err := nwsrmProc.Process(event)
	if err != nil {
		t.Errorf("Should not have failed processing events: err: %v", err)
	}
	return event
}

func setupApplicationAndNewsroomProcessor(t *testing.T) (*contractutils.AllTestContracts, *TestPersister,
	*processor.NewsroomEventProcessor) {
	contracts, err := contractutils.SetupAllTestContracts()
	if err != nil {
		t.Fatalf("Unable to setup the contracts: %v", err)
	}
	persister := &TestPersister{}
	scraper := &TestScraper{}
	tcrProc := processor.NewTcrEventProcessor(
		contracts.Client,
		persister,
		persister,
		persister,
		persister)
	_ = createAndProcAppEvent(t, contracts, tcrProc)
	newsroomProc := processor.NewNewsroomEventProcessor(
		contracts.Client,
		persister,
		persister,
		scraper,
		scraper,
		scraper)
	return contracts, persister, newsroomProc
}

func TestNewsroomProcessor(t *testing.T) {
	contracts, persister, nwsrmProc := setupApplicationAndNewsroomProcessor(t)
	_ = createAndProcRevisionUpdatedEventCharter(t, contracts, nwsrmProc)
	_ = createAndProcOwnershipTransferredEvent(t, contracts, nwsrmProc)
	_ = createAndProcNameChangedEvent(t, contracts, nwsrmProc)

	if len(persister.listings) != 1 {
		t.Error("Should be only 1 listing")
	}
	if len(persister.revisions) != 1 {
		t.Error("Should be one revision")
	}

}

func TestProcessNameChanged(t *testing.T) {
	contracts, persister, nwsrmProc := setupApplicationAndNewsroomProcessor(t)
	listingAddress := contracts.NewsroomAddr.Hex()
	event := createAndProcNameChangedEvent(t, contracts, nwsrmProc)
	eventPayload := event.EventPayload()
	listing := persister.listings[listingAddress]

	if listing.Name() != eventPayload["NewName"] {
		t.Errorf("Listing name is not correct %v %v", listing.Name(), eventPayload["NewName"])
	}
}

func TestCreateAndProcRevisionUpdatedEvent(t *testing.T) {
	contracts, persister, nwsrmProc := setupApplicationAndNewsroomProcessor(t)
	listingAddress := contracts.NewsroomAddr.Hex()

	event := createAndProcRevisionUpdatedEventCharter(t, contracts, nwsrmProc)
	eventPayload := event.EventPayload()

	listing := persister.listings[listingAddress]

	charter := listing.Charter()
	// NOTE(IS): These are the fields that get set through revision.
	// Cannot check fields of charter that are set through contract calls without simulated backend.

	if eventPayload["ContentId"].(*big.Int).Cmp(charter.ContentID()) != 0 {
		t.Errorf("Charter contentID is not correct %v %v", eventPayload["ContentId"], charter.ContentID())
	}

	if eventPayload["RevisionId"].(*big.Int).Cmp(charter.ContentID()) != 0 {
		t.Errorf("Charter contentID is not correct %v %v", eventPayload["ContentId"], charter.ContentID())
	}

	if eventPayload["Uri"].(string) != charter.URI() {
		t.Errorf("Charter Uri is not correct %v %v", eventPayload["ContentId"], charter.ContentID())
	}
	// can also test scrape data

	// test content revision
	revisionCharter := persister.revisions[listingAddress][0]
	if revisionCharter.ContractContentID() != eventPayload["ContentId"] {
		t.Error("ContentRevision contentID not correct")
	}
	if revisionCharter.EditorAddress().Hex() != eventPayload["Editor"].(common.Address).Hex() {
		t.Error("Editor Address not correct")
	}
	if revisionCharter.ContractRevisionID() != eventPayload["RevisionId"] {
		t.Error("RevisionID not correct")
	}
	if revisionCharter.RevisionURI() != eventPayload["Uri"] {
		t.Error("Revision URi not correct")
	}

	// NOTE: Getting errors with contract calls for RevisionUpdated event that's not a charter

}

func TestCreateAndProcOwnershipTransferredEvent(t *testing.T) {
	contracts, persister, nwsrmProc := setupApplicationAndNewsroomProcessor(t)
	listingAddress := contracts.NewsroomAddr.Hex()
	event := createAndProcOwnershipTransferredEvent(t, contracts, nwsrmProc)
	eventPayload := event.EventPayload()
	listing := persister.listings[listingAddress]
	if len(listing.OwnerAddresses()) != 1 {
		t.Errorf("Should still only have 1 owner for the listing")
	}
	if listing.OwnerAddresses()[0].Hex() != eventPayload["NewOwner"].(common.Address).Hex() {
		t.Errorf("Should have updated the listing with new owner")
	}
}