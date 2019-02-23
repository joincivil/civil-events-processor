package processor

import (
	"fmt"
	"math/big"
	"strings"

	log "github.com/golang/glog"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"

	commongen "github.com/joincivil/civil-events-crawler/pkg/generated/common"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	cpersist "github.com/joincivil/go-common/pkg/persistence"

	"github.com/joincivil/civil-events-processor/pkg/model"
)

// NewCvlTokenEventProcessor is a convenience function to init an Event processor
func NewCvlTokenEventProcessor(client bind.ContractBackend,
	purchasePersister model.TokenPurchasePersister) *CvlTokenEventProcessor {
	return &CvlTokenEventProcessor{
		client:            client,
		purchasePersister: purchasePersister,
	}
}

// CvlTokenEventProcessor handles the processing of raw CvlToken events into aggregated data
// for use via the API.
type CvlTokenEventProcessor struct {
	client            bind.ContractBackend
	purchasePersister model.TokenPurchasePersister
}

// Process processes Newsroom Events into aggregated data
func (c *CvlTokenEventProcessor) Process(event *crawlermodel.Event) (bool, error) {
	if !c.isValidCvlTokenEventName(event.EventType()) {
		return false, nil
	}

	var err error
	ran := true
	eventName := strings.Trim(event.EventType(), " _")

	// Handling all the actionable events from Newsroom Addressses
	switch eventName {
	// When a token transfer has occurred
	case "Transfer":
		log.Infof("Handling Token Transfer for %v\n", event.ContractAddress().Hex())
		err = c.processCvlTokenTransfer(event)

	default:
		ran = false
	}
	return ran, err
}

func (c *CvlTokenEventProcessor) isValidCvlTokenEventName(name string) bool {
	name = strings.Trim(name, " _")
	eventNames := commongen.EventTypesCVLTokenContract()
	return isStringInSlice(eventNames, name)
}

func (c *CvlTokenEventProcessor) processCvlTokenTransfer(event *crawlermodel.Event) error {
	payload := event.EventPayload()

	purchaserAddress, ok := payload["To"]
	if !ok {
		return fmt.Errorf("No purchaser address found")
	}
	sourceAddress, ok := payload["From"]
	if !ok {
		return fmt.Errorf("No source address found")
	}
	amount, ok := payload["Value"]
	if !ok {
		return fmt.Errorf("No amount found")
	}
	purchaseDate := event.Timestamp()

	paddr := purchaserAddress.(common.Address)
	caddr := sourceAddress.(common.Address)

	params := &model.TokenPurchaseParams{
		PurchaserAddress: paddr,
		SourceAddress:    caddr,
		Amount:           amount.(*big.Int),
		PurchaseDate:     purchaseDate,
		BlockNumber:      event.BlockNumber(),
		TxHash:           event.TxHash(),
		TxIndex:          event.TxIndex(),
		BlockHash:        event.BlockHash(),
		Index:            event.LogIndex(),
	}
	newPurchase := model.NewTokenPurchase(params)

	purchases, err := c.purchasePersister.TokenPurchasesByPurchaserAddress(paddr)
	if err != nil {
		if err != cpersist.ErrPersisterNoResults {
			return fmt.Errorf("Error retrieving token purchase: err: %v", err)
		}
	}
	if len(purchases) > 0 {
		for _, purchase := range purchases {
			if purchase.Equals(newPurchase) {
				return fmt.Errorf(
					"Token purchase already exists: %v, %v, %v, %v",
					purchase.PurchaserAddress().Hex(),
					purchase.SourceAddress().Hex(),
					purchase.Amount().Int64(),
					purchase.PurchaseDate(),
				)
			}
		}
	}

	return c.purchasePersister.CreateTokenPurchase(newPurchase)
}
