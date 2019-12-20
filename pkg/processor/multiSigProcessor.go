package processor

import (
	"strings"

	log "github.com/golang/glog"
	"github.com/pkg/errors"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/joincivil/go-common/pkg/generated/contract"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	"github.com/joincivil/civil-events-processor/pkg/model"
)

// NewMultiSigEventProcessor is a convenience function to init an Event processor
func NewMultiSigEventProcessor(client bind.ContractBackend,
	multiSigPersister model.MultiSigPersister, multiSigOwnerPersister model.MultiSigOwnerPersister) *MultiSigEventProcessor {
	return &MultiSigEventProcessor{
		client:                 client,
		multiSigPersister:      multiSigPersister,
		multiSigOwnerPersister: multiSigOwnerPersister,
	}
}

// MultiSigEventProcessor handles the processing of raw Multi Sig events into aggregated data
// for use via the API.
type MultiSigEventProcessor struct {
	client                 bind.ContractBackend
	multiSigPersister      model.MultiSigPersister
	multiSigOwnerPersister model.MultiSigOwnerPersister
}

// Process processes Newsroom Events into aggregated data
func (c *MultiSigEventProcessor) Process(event *crawlermodel.Event) (bool, error) {
	if event.ContractName() != "MultiSigWalletContract" && event.ContractName() != "MultiSigWalletFactoryContract" {
		return false, nil
	}

	var err error
	ran := true
	eventName := strings.Trim(event.EventType(), " _")

	// Handling all the actionable events from the cvl token addressses
	switch eventName {
	// When a new contract has been instantiated
	case "ContractInstantiation":
		log.Infof("Handling Multi Sig Wallet Contract Instantiation for %v\n", event.ContractAddress().Hex())
		err = c.processMultiSigWalletContractInstantiation(event)
	case "OwnerAddition":
		log.Infof("Handling Multi Sig Wallet Owner Addition for %v\n", event.ContractAddress().Hex())
		err = c.processMultiSigWalletOwnerAddedOrRemoved(event)
	case "OwnerRemoval":
		log.Infof("Handling Multi Sig Wallet Owner Removal for %v\n", event.ContractAddress().Hex())
		err = c.processMultiSigWalletOwnerAddedOrRemoved(event)
	default:
		ran = false
	}
	return ran, err
}

func (c *MultiSigEventProcessor) processMultiSigWalletContractInstantiation(event *crawlermodel.Event) error {
	payload := event.EventPayload()
	multiSigAddr, ok := payload["Instantiation"]
	if !ok {
		return errors.New("could not get instantiated address from event payload")
	}
	multiSigAddress := (multiSigAddr).(common.Address)

	multiSigWalletContract, err := contract.NewMultiSigWalletContract(multiSigAddress, c.client)
	if err != nil {
		return errors.WithMessage(err, "error getting multi sig wallet contract")
	}

	owners, err := multiSigWalletContract.GetOwners(&bind.CallOpts{})
	if err != nil {
		return errors.WithMessage(err, "error getting owners from multi sig wallet contract")
	}

	multiSig := model.NewMultiSig(&model.NewMultiSigParams{
		ContractAddress: multiSigAddress,
		OwnerAddresses:  owners,
	})

	log.Infof("multiSigPersister %v: ", c.multiSigPersister)
	err = c.multiSigPersister.CreateMultiSig(multiSig)
	if err != nil {
		return errors.WithMessage(err, "error creating multi sig")
	}
	return c.updateMultiSigOwners(multiSigAddress)
}

func (c *MultiSigEventProcessor) processMultiSigWalletOwnerAddedOrRemoved(event *crawlermodel.Event) error {
	multiSigAddr := event.ContractAddress()

	err := c.multiSigOwnerPersister.DeleteMultiSigOwners(multiSigAddr)
	if err != nil {
		return errors.WithMessage(err, "error deleting multi sig owner")
	}

	return c.updateMultiSigOwners(multiSigAddr)
}

func (c *MultiSigEventProcessor) updateMultiSigOwners(multiSigAddress common.Address) error {
	multiSigWalletContract, err := contract.NewMultiSigWalletContract(multiSigAddress, c.client)
	if err != nil {
		return errors.WithMessage(err, "error getting multi sig wallet contract")
	}

	owners, err := multiSigWalletContract.GetOwners(&bind.CallOpts{})
	if err != nil {
		return errors.WithMessage(err, "error getting owners from multi sig wallet contract")
	}

	for _, owner := range owners {
		ownerKey := owner.String()
		multiSigAddressKey := multiSigAddress.String()
		multiSigOwner := model.NewMultiSigOwner(&model.NewMultiSigOwnerParams{
			Key:             ownerKey + "-" + multiSigAddressKey,
			OwnerAddress:    owner,
			MultiSigAddress: multiSigAddress,
		})
		err = c.multiSigOwnerPersister.CreateMultiSigOwner(multiSigOwner)
		if err != nil {
			return errors.WithMessage(err, "error creating multi sig owner")
		}
	}
	return nil
}
