package processor

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	log "github.com/golang/glog"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/shurcooL/graphql"

	commongen "github.com/joincivil/civil-events-crawler/pkg/generated/common"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	cpersist "github.com/joincivil/go-common/pkg/persistence"

	"github.com/joincivil/civil-events-processor/pkg/model"
)

// NewCvlTokenEventProcessor is a convenience function to init an Event processor
func NewCvlTokenEventProcessor(client bind.ContractBackend, graphqlClient *graphql.Client,
	transferPersister model.TokenTransferPersister) *CvlTokenEventProcessor {
	return &CvlTokenEventProcessor{
		client:            client,
		graphqlClient:     graphqlClient,
		transferPersister: transferPersister,
	}
}

// CvlTokenEventProcessor handles the processing of raw CvlToken events into aggregated data
// for use via the API.
type CvlTokenEventProcessor struct {
	client            bind.ContractBackend
	graphqlClient     *graphql.Client
	transferPersister model.TokenTransferPersister
}

// Process processes Newsroom Events into aggregated data
func (c *CvlTokenEventProcessor) Process(event *crawlermodel.Event) (bool, error) {
	if !c.isValidCvlTokenEventName(event.EventType()) {
		return false, nil
	}

	var err error
	ran := true
	eventName := strings.Trim(event.EventType(), " _")

	// Handling all the actionable events from the cvl token addressses
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

	toAddress, ok := payload["To"]
	if !ok {
		return fmt.Errorf("No purchaser address found")
	}
	fromAddress, ok := payload["From"]
	if !ok {
		return fmt.Errorf("No source address found")
	}
	amount, ok := payload["Value"]
	if !ok {
		return fmt.Errorf("No amount found")
	}
	transferDate := event.Timestamp()

	toaddr := toAddress.(common.Address)
	fromaddr := fromAddress.(common.Address)

	// Figure out the token/cvl values
	cvlPrice := big.NewFloat(0.0)
	ethPrice := big.NewFloat(0.0)

	cprice, err := c.fetchCvlPrice()
	if err != nil {
		log.Errorf("Error fetching cvl price: err: %v", err)
	} else {
		cvlPrice = cprice
	}
	eprice, err := c.fetchEthPrice()
	if err != nil {
		log.Errorf("Error fetching eth price: err: %v", err)
	} else {
		ethPrice = eprice
	}

	params := &model.TokenTransferParams{
		ToAddress:    toaddr,
		FromAddress:  fromaddr,
		Amount:       amount.(*big.Int),
		TransferDate: transferDate,
		CvlPrice:     cvlPrice,
		EthPrice:     ethPrice,
		EventHash:    event.Hash(),
		BlockNumber:  event.BlockNumber(),
		TxHash:       event.TxHash(),
		TxIndex:      event.TxIndex(),
		BlockHash:    event.BlockHash(),
		Index:        event.LogIndex(),
	}
	newPurchase := model.NewTokenTransfer(params)

	purchases, err := c.transferPersister.TokenTransfersByToAddress(toaddr)
	if err != nil {
		if err != cpersist.ErrPersisterNoResults {
			return fmt.Errorf("Error retrieving token transfer: err: %v", err)
		}
	}
	if len(purchases) > 0 {
		for _, purchase := range purchases {
			if purchase.Equals(newPurchase) {
				return fmt.Errorf(
					"Token transfer already exists: %v, %v, %v, %v",
					purchase.ToAddress().Hex(),
					purchase.FromAddress().Hex(),
					purchase.Amount().Int64(),
					purchase.TransferDate(),
				)
			}
		}
	}

	return c.transferPersister.CreateTokenTransfer(newPurchase)
}

func (c *CvlTokenEventProcessor) fetchCvlPrice() (*big.Float, error) {
	if c.graphqlClient == nil {
		log.Infof("No graphql client init to fetch cvl price")
		return nil, nil
	}

	var priceQuery struct {
		Price graphql.Float `graphql:"storefrontCvlPrice"`
	}

	err := c.graphqlClient.Query(context.Background(), &priceQuery, nil)
	if err != nil {
		return nil, err
	}

	return big.NewFloat(float64(priceQuery.Price)), nil
}

func (c *CvlTokenEventProcessor) fetchEthPrice() (*big.Float, error) {
	if c.graphqlClient == nil {
		return nil, fmt.Errorf("No graphql client init to fetch eth price")
	}

	var priceQuery struct {
		Price graphql.Float `graphql:"storefrontEthPrice"`
	}

	err := c.graphqlClient.Query(context.Background(), &priceQuery, nil)
	if err != nil {
		return nil, err
	}

	return big.NewFloat(float64(priceQuery.Price)), nil
}
