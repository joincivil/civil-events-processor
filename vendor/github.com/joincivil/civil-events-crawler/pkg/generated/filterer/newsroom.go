// Code generated by 'gen/eventhandlergen.go'  DO NOT EDIT.
// IT SHOULD NOT BE EDITED BY HAND AS ANY CHANGES MAY BE OVERWRITTEN
// Please reference 'gen/filterergen_template.go' for more details
// File was generated at 2018-08-08 20:43:50.422553327 +0000 UTC
package filterer

import (
	"fmt"
	log "github.com/golang/glog"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"

	"github.com/joincivil/civil-events-crawler/pkg/generated/contract"
	"github.com/joincivil/civil-events-crawler/pkg/model"
	"github.com/joincivil/civil-events-crawler/pkg/utils"

	"math/big"
)

// TODO(IS): Need to move this to a central place, use it outside the package
var eventTypesNewsroomContract = []string{
	"ContentPublished",
	"NameChanged",
	"OwnershipTransferred",
	"RevisionSigned",
	"RevisionUpdated",
	"RoleAdded",
	"RoleRemoved",
}

// TODO(IS): Need to move this to a central place, use it outside the package
func EventTypesNewsroomContract() []string {
	tmp := make([]string, len(eventTypesNewsroomContract))
	copy(tmp, eventTypesNewsroomContract)
	return tmp
}

func NewNewsroomContractFilterers(contractAddress common.Address) *NewsroomContractFilterers {
	contractFilterers := &NewsroomContractFilterers{
		contractAddress:   contractAddress,
		eventTypes:        eventTypesNewsroomContract,
		eventToStartBlock: make(map[string]uint64),
		lastEvents:        make([]*model.Event, 0),
	}
	for _, eventType := range contractFilterers.eventTypes {
		contractFilterers.eventToStartBlock[eventType] = 0
	}
	return contractFilterers
}

type NewsroomContractFilterers struct {
	contractAddress   common.Address
	contract          *contract.NewsroomContract
	eventTypes        []string
	eventToStartBlock map[string]uint64
	lastEvents        []*model.Event
}

func (f *NewsroomContractFilterers) ContractName() string {
	return "NewsroomContract"
}

func (f *NewsroomContractFilterers) ContractAddress() common.Address {
	return f.contractAddress
}

func (f *NewsroomContractFilterers) StartFilterers(client bind.ContractBackend, pastEvents []*model.Event) (error, []*model.Event) {
	return f.StartNewsroomContractFilterers(client, pastEvents)
}

func (f *NewsroomContractFilterers) EventTypes() []string {
	return f.eventTypes
}

func (f *NewsroomContractFilterers) UpdateStartBlock(eventType string, startBlock uint64) {
	f.eventToStartBlock[eventType] = startBlock
}

func (f *NewsroomContractFilterers) LastEvents() []*model.Event {
	return f.lastEvents
}

// StartNewsroomContractFilterers retrieves events for NewsroomContract
func (f *NewsroomContractFilterers) StartNewsroomContractFilterers(client bind.ContractBackend, pastEvents []*model.Event) (error, []*model.Event) {
	contract, err := contract.NewNewsroomContract(f.contractAddress, client)
	if err != nil {
		log.Errorf("Error initializing StartNewsroomContract: err: %v", err)
		return err, pastEvents
	}
	f.contract = contract
	var startBlock uint64
	prevEventsLength := len(pastEvents)

	startBlock = f.eventToStartBlock["ContentPublished"]
	err, pastEvents = f.startFilterContentPublished(startBlock, pastEvents)
	if err != nil {
		return fmt.Errorf("Error retrieving ContentPublished: err: %v", err), pastEvents
	}
	if len(pastEvents) > prevEventsLength {
		f.lastEvents = append(f.lastEvents, pastEvents[len(pastEvents)-1])
		prevEventsLength = len(pastEvents)
	}

	startBlock = f.eventToStartBlock["NameChanged"]
	err, pastEvents = f.startFilterNameChanged(startBlock, pastEvents)
	if err != nil {
		return fmt.Errorf("Error retrieving NameChanged: err: %v", err), pastEvents
	}
	if len(pastEvents) > prevEventsLength {
		f.lastEvents = append(f.lastEvents, pastEvents[len(pastEvents)-1])
		prevEventsLength = len(pastEvents)
	}

	startBlock = f.eventToStartBlock["OwnershipTransferred"]
	err, pastEvents = f.startFilterOwnershipTransferred(startBlock, pastEvents)
	if err != nil {
		return fmt.Errorf("Error retrieving OwnershipTransferred: err: %v", err), pastEvents
	}
	if len(pastEvents) > prevEventsLength {
		f.lastEvents = append(f.lastEvents, pastEvents[len(pastEvents)-1])
		prevEventsLength = len(pastEvents)
	}

	startBlock = f.eventToStartBlock["RevisionSigned"]
	err, pastEvents = f.startFilterRevisionSigned(startBlock, pastEvents)
	if err != nil {
		return fmt.Errorf("Error retrieving RevisionSigned: err: %v", err), pastEvents
	}
	if len(pastEvents) > prevEventsLength {
		f.lastEvents = append(f.lastEvents, pastEvents[len(pastEvents)-1])
		prevEventsLength = len(pastEvents)
	}

	startBlock = f.eventToStartBlock["RevisionUpdated"]
	err, pastEvents = f.startFilterRevisionUpdated(startBlock, pastEvents)
	if err != nil {
		return fmt.Errorf("Error retrieving RevisionUpdated: err: %v", err), pastEvents
	}
	if len(pastEvents) > prevEventsLength {
		f.lastEvents = append(f.lastEvents, pastEvents[len(pastEvents)-1])
		prevEventsLength = len(pastEvents)
	}

	startBlock = f.eventToStartBlock["RoleAdded"]
	err, pastEvents = f.startFilterRoleAdded(startBlock, pastEvents)
	if err != nil {
		return fmt.Errorf("Error retrieving RoleAdded: err: %v", err), pastEvents
	}
	if len(pastEvents) > prevEventsLength {
		f.lastEvents = append(f.lastEvents, pastEvents[len(pastEvents)-1])
		prevEventsLength = len(pastEvents)
	}

	startBlock = f.eventToStartBlock["RoleRemoved"]
	err, pastEvents = f.startFilterRoleRemoved(startBlock, pastEvents)
	if err != nil {
		return fmt.Errorf("Error retrieving RoleRemoved: err: %v", err), pastEvents
	}
	if len(pastEvents) > prevEventsLength {
		f.lastEvents = append(f.lastEvents, pastEvents[len(pastEvents)-1])
		prevEventsLength = len(pastEvents)
	}

	return nil, pastEvents
}

func (f *NewsroomContractFilterers) startFilterContentPublished(startBlock uint64, pastEvents []*model.Event) (error, []*model.Event) {
	var opts = &bind.FilterOpts{
		Start: startBlock,
	}

	log.Infof("Filtering events for ContentPublished for contract %v", f.contractAddress.Hex())
	itr, err := f.contract.FilterContentPublished(
		opts,
		[]common.Address{},
		[]*big.Int{},
	)
	if err != nil {
		log.Errorf("Error getting event ContentPublished: %v", err)
		return err, pastEvents
	}
	nextEvent := itr.Next()
	for nextEvent {
		modelEvent, err := model.NewEventFromContractEvent("ContentPublished", f.ContractName(), f.contractAddress, itr.Event, utils.CurrentEpochNanoSecsInInt64(), model.Filterer)
		if err != nil {
			log.Errorf("Error creating new event: event: %v, err: %v", itr.Event, err)
			continue
		}
		pastEvents = append(pastEvents, modelEvent)
		nextEvent = itr.Next()
	}
	return nil, pastEvents
}

func (f *NewsroomContractFilterers) startFilterNameChanged(startBlock uint64, pastEvents []*model.Event) (error, []*model.Event) {
	var opts = &bind.FilterOpts{
		Start: startBlock,
	}

	log.Infof("Filtering events for NameChanged for contract %v", f.contractAddress.Hex())
	itr, err := f.contract.FilterNameChanged(
		opts,
	)
	if err != nil {
		log.Errorf("Error getting event NameChanged: %v", err)
		return err, pastEvents
	}
	nextEvent := itr.Next()
	for nextEvent {
		modelEvent, err := model.NewEventFromContractEvent("NameChanged", f.ContractName(), f.contractAddress, itr.Event, utils.CurrentEpochNanoSecsInInt64(), model.Filterer)
		if err != nil {
			log.Errorf("Error creating new event: event: %v, err: %v", itr.Event, err)
			continue
		}
		pastEvents = append(pastEvents, modelEvent)
		nextEvent = itr.Next()
	}
	return nil, pastEvents
}

func (f *NewsroomContractFilterers) startFilterOwnershipTransferred(startBlock uint64, pastEvents []*model.Event) (error, []*model.Event) {
	var opts = &bind.FilterOpts{
		Start: startBlock,
	}

	log.Infof("Filtering events for OwnershipTransferred for contract %v", f.contractAddress.Hex())
	itr, err := f.contract.FilterOwnershipTransferred(
		opts,
		[]common.Address{},
		[]common.Address{},
	)
	if err != nil {
		log.Errorf("Error getting event OwnershipTransferred: %v", err)
		return err, pastEvents
	}
	nextEvent := itr.Next()
	for nextEvent {
		modelEvent, err := model.NewEventFromContractEvent("OwnershipTransferred", f.ContractName(), f.contractAddress, itr.Event, utils.CurrentEpochNanoSecsInInt64(), model.Filterer)
		if err != nil {
			log.Errorf("Error creating new event: event: %v, err: %v", itr.Event, err)
			continue
		}
		pastEvents = append(pastEvents, modelEvent)
		nextEvent = itr.Next()
	}
	return nil, pastEvents
}

func (f *NewsroomContractFilterers) startFilterRevisionSigned(startBlock uint64, pastEvents []*model.Event) (error, []*model.Event) {
	var opts = &bind.FilterOpts{
		Start: startBlock,
	}

	log.Infof("Filtering events for RevisionSigned for contract %v", f.contractAddress.Hex())
	itr, err := f.contract.FilterRevisionSigned(
		opts,
		[]*big.Int{},
		[]*big.Int{},
		[]common.Address{},
	)
	if err != nil {
		log.Errorf("Error getting event RevisionSigned: %v", err)
		return err, pastEvents
	}
	nextEvent := itr.Next()
	for nextEvent {
		modelEvent, err := model.NewEventFromContractEvent("RevisionSigned", f.ContractName(), f.contractAddress, itr.Event, utils.CurrentEpochNanoSecsInInt64(), model.Filterer)
		if err != nil {
			log.Errorf("Error creating new event: event: %v, err: %v", itr.Event, err)
			continue
		}
		pastEvents = append(pastEvents, modelEvent)
		nextEvent = itr.Next()
	}
	return nil, pastEvents
}

func (f *NewsroomContractFilterers) startFilterRevisionUpdated(startBlock uint64, pastEvents []*model.Event) (error, []*model.Event) {
	var opts = &bind.FilterOpts{
		Start: startBlock,
	}

	log.Infof("Filtering events for RevisionUpdated for contract %v", f.contractAddress.Hex())
	itr, err := f.contract.FilterRevisionUpdated(
		opts,
		[]common.Address{},
		[]*big.Int{},
		[]*big.Int{},
	)
	if err != nil {
		log.Errorf("Error getting event RevisionUpdated: %v", err)
		return err, pastEvents
	}
	nextEvent := itr.Next()
	for nextEvent {
		modelEvent, err := model.NewEventFromContractEvent("RevisionUpdated", f.ContractName(), f.contractAddress, itr.Event, utils.CurrentEpochNanoSecsInInt64(), model.Filterer)
		if err != nil {
			log.Errorf("Error creating new event: event: %v, err: %v", itr.Event, err)
			continue
		}
		pastEvents = append(pastEvents, modelEvent)
		nextEvent = itr.Next()
	}
	return nil, pastEvents
}

func (f *NewsroomContractFilterers) startFilterRoleAdded(startBlock uint64, pastEvents []*model.Event) (error, []*model.Event) {
	var opts = &bind.FilterOpts{
		Start: startBlock,
	}

	log.Infof("Filtering events for RoleAdded for contract %v", f.contractAddress.Hex())
	itr, err := f.contract.FilterRoleAdded(
		opts,
		[]common.Address{},
		[]common.Address{},
	)
	if err != nil {
		log.Errorf("Error getting event RoleAdded: %v", err)
		return err, pastEvents
	}
	nextEvent := itr.Next()
	for nextEvent {
		modelEvent, err := model.NewEventFromContractEvent("RoleAdded", f.ContractName(), f.contractAddress, itr.Event, utils.CurrentEpochNanoSecsInInt64(), model.Filterer)
		if err != nil {
			log.Errorf("Error creating new event: event: %v, err: %v", itr.Event, err)
			continue
		}
		pastEvents = append(pastEvents, modelEvent)
		nextEvent = itr.Next()
	}
	return nil, pastEvents
}

func (f *NewsroomContractFilterers) startFilterRoleRemoved(startBlock uint64, pastEvents []*model.Event) (error, []*model.Event) {
	var opts = &bind.FilterOpts{
		Start: startBlock,
	}

	log.Infof("Filtering events for RoleRemoved for contract %v", f.contractAddress.Hex())
	itr, err := f.contract.FilterRoleRemoved(
		opts,
		[]common.Address{},
		[]common.Address{},
	)
	if err != nil {
		log.Errorf("Error getting event RoleRemoved: %v", err)
		return err, pastEvents
	}
	nextEvent := itr.Next()
	for nextEvent {
		modelEvent, err := model.NewEventFromContractEvent("RoleRemoved", f.ContractName(), f.contractAddress, itr.Event, utils.CurrentEpochNanoSecsInInt64(), model.Filterer)
		if err != nil {
			log.Errorf("Error creating new event: event: %v, err: %v", itr.Event, err)
			continue
		}
		pastEvents = append(pastEvents, modelEvent)
		nextEvent = itr.Next()
	}
	return nil, pastEvents
}
