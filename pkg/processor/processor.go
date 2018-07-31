package processor

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	log "github.com/golang/glog"
	"math/big"
	"strings"

	"github.com/joincivil/civil-events-crawler/pkg/generated/contract"
	"github.com/joincivil/civil-events-crawler/pkg/generated/filterer"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	crawlerutils "github.com/joincivil/civil-events-crawler/pkg/utils"

	"github.com/joincivil/civil-events-processor/pkg/model"
)

func isStringInSlice(slice []string, target string) bool {
	for _, str := range slice {
		if target == str {
			return true
		}
	}
	return false
}

func byte32ToHexString(input [32]byte) string {
	return hex.EncodeToString(input[:])
}

// NewEventProcessor is a convenience function to init an EventProcessor
func NewEventProcessor(client bind.ContractBackend, listingPersister model.ListingPersister,
	revisionPersister model.ContentRevisionPersister,
	govEventPersister model.GovernanceEventPersister, contentScraper model.ContentScraper,
	metadataScraper model.MetadataScraper) *EventProcessor {
	return &EventProcessor{
		client:            client,
		listingPersister:  listingPersister,
		revisionPersister: revisionPersister,
		govEventPersister: govEventPersister,
		contentScraper:    contentScraper,
		metadataScraper:   metadataScraper,
	}
}

// EventProcessor handles the processing of raw events into aggregated data
// for use via the API.
type EventProcessor struct {
	client            bind.ContractBackend
	listingPersister  model.ListingPersister
	revisionPersister model.ContentRevisionPersister
	govEventPersister model.GovernanceEventPersister
	contentScraper    model.ContentScraper
	metadataScraper   model.MetadataScraper
}

// Process runs the processor with the given set of raw CivilEvents
// Returns the last error if one has occurred
func (e *EventProcessor) Process(events []*crawlermodel.CivilEvent) error {
	var err error
	var ran bool
	for _, event := range events {
		ran, err = e.processNewsroomEvent(event)
		if err != nil {
			log.Errorf("Error processing newsroom event: err: %v\n", err)
		}
		if ran {
			continue
		}
		_, err = e.processCivilTCREvent(event)
		if err != nil {
			log.Errorf("Error processing civil tcr event: err: %v\n", err)
		}
	}
	return err
}

func (e *EventProcessor) isValidNewsroomContractEventName(name string) bool {
	name = strings.Trim(name, " _")
	eventNames := filterer.EventTypesNewsroomContract()
	return isStringInSlice(eventNames, name)
}

func (e *EventProcessor) isValidCivilTCRContractEventName(name string) bool {
	name = strings.Trim(name, " _")
	eventNames := filterer.EventTypesCivilTCRContract()
	return isStringInSlice(eventNames, name)
}

func (e *EventProcessor) processNewsroomEvent(event *crawlermodel.CivilEvent) (bool, error) {
	if !e.isValidNewsroomContractEventName(event.EventType()) {
		return false, nil
	}
	var err error
	// Handling all the actionable events from Newsroom Addressses
	switch event.EventType() {
	// When a listing's name has changed
	case "NameChanged":
		log.Info("Handling NameChanged")
		err = e.processNewsroomNameChanged(event)
	// When there is a new revision on content
	case "RevisionUpdated":
		log.Info("Handling RevisionUpdated")
		err = e.processNewsroomRevisionUpdated(event)
	// When there is a new owner
	case "OwnershipTransferred":
		log.Info("Handling OwnershipTransferred")
		err = e.processNewsroomOwnershipTransferred(event)

	}
	return true, err
}

func (e *EventProcessor) processCivilTCREvent(event *crawlermodel.CivilEvent) (bool, error) {
	if !e.isValidCivilTCRContractEventName(event.EventType()) {
		return false, nil
	}
	var err error
	// Handling all the actionable events from the TCR
	switch event.EventType() {
	// When a listing has applied
	case "_Application":
		log.Info("Handling _Application")
		err = e.processTCRApplication(event)

	// When a listing has been challenged at any point
	case "_Challenge":
		log.Info("Handling _Challenge")
		err = e.processTCRChallenge(event)

	// When a listing gets whitelisted
	case "_ApplicationWhitelisted":
		log.Info("Handling _ApplicationWhitelisted")
		err = e.processTCRApplicationWhitelisted(event)

	// When an application for a listing has been removed
	case "_ApplicationRemoved":
		log.Info("Handling _ApplicationRemoved")
		err = e.processTCRApplicationRemoved(event)

	// When a listing is de-listed
	case "_ListingRemoved":
		log.Info("Handling _ListingRemoved")
		err = e.processTCRListingRemoved(event)

	// When a listing applicaiton has been withdrawn
	case "_ListingWithdrawn":
		log.Info("Handling _ListingWithdrawn")
		err = e.processTCRListingWithdrawn(event)
	}

	return true, err
}

func (e *EventProcessor) persistNewGovernanceEvent(listingAddr common.Address,
	senderAddr common.Address, metadata model.Metadata, eventType string, eventHash string) error {
	govEvent := model.NewGovernanceEvent(
		listingAddr,
		senderAddr,
		metadata,
		eventType,
		crawlerutils.CurrentEpochSecsInInt64(),
		crawlerutils.CurrentEpochSecsInInt64(),
		eventHash,
	)
	err := e.govEventPersister.CreateGovernanceEvent(govEvent)
	return err
}

func (e *EventProcessor) persistNewListing(listingAddress common.Address,
	whitelisted bool, lastGovernanceState model.GovernanceState) error {
	// TODO(PN): How do I get the URL of the site?
	url := ""
	newsroom, err := contract.NewNewsroomContract(listingAddress, e.client)
	if err != nil {
		return err
	}
	name, err := newsroom.Name(&bind.CallOpts{})
	if err != nil {
		return err
	}
	charterContent, err := newsroom.GetContent(&bind.CallOpts{}, big.NewInt(0))
	if err != nil {
		return err
	}
	charterURI := charterContent.Uri
	charterAuthorAddr := charterContent.Author
	ownerAddr, err := newsroom.Owner(&bind.CallOpts{})
	if err != nil {
		return err
	}
	ownerAddresses := []common.Address{ownerAddr}
	contributorAddresses := []common.Address{charterAuthorAddr}
	listing := model.NewListing(
		name,
		listingAddress,
		whitelisted,
		lastGovernanceState,
		url,
		charterURI,
		ownerAddresses,
		contributorAddresses,
		crawlerutils.CurrentEpochSecsInInt64(),
		crawlerutils.CurrentEpochSecsInInt64(),
		int64(0),
		crawlerutils.CurrentEpochSecsInInt64(),
	)
	err = e.listingPersister.CreateListing(listing)
	return err
}

func (e *EventProcessor) processTCRApplication(event *crawlermodel.CivilEvent) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddrInterface, ok := payload["ListingAddress"]
	if !ok {
		return errors.New("Unable to find the listing address in the payload")
	}
	listingAddress := listingAddrInterface.(common.Address)
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil {
		return err
	}
	lastGovState := model.GovernanceStateApplied
	whitelisted := false
	if listing == nil {
		err = e.persistNewListing(listingAddress, whitelisted, lastGovState)
		if err != nil {
			return err
		}
		metadata := map[string]interface{}{}
		err = e.persistNewGovernanceEvent(
			listingAddress,
			event.ContractAddress(),
			metadata,
			event.EventType(),
			event.Hash(),
		)
		return err
	}
	listing.SetLastGovernanceState(lastGovState)
	updatedFields = append(updatedFields, "LastGovernanceState")
	listing.SetWhitelisted(whitelisted)
	updatedFields = append(updatedFields, "whitelisted")
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processTCRChallenge(event *crawlermodel.CivilEvent) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddrInterface, ok := payload["ListingAddress"]
	if !ok {
		return errors.New("Unable to find the listing address in the payload")
	}
	listingAddress := listingAddrInterface.(common.Address)
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil {
		return err
	}
	lastGovState := model.GovernanceStateChallenged
	whitelisted := false
	if listing == nil {
		err = e.persistNewListing(listingAddress, whitelisted, lastGovState)
		if err != nil {
			return err
		}
		metadata := map[string]interface{}{}
		err = e.persistNewGovernanceEvent(
			listingAddress,
			event.ContractAddress(),
			metadata,
			event.EventType(),
			event.Hash(),
		)
		return err
	}
	listing.SetLastGovernanceState(lastGovState)
	updatedFields = append(updatedFields, "LastGovernanceState")
	listing.SetWhitelisted(whitelisted)
	updatedFields = append(updatedFields, "whitelisted")
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processTCRApplicationWhitelisted(event *crawlermodel.CivilEvent) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddrInterface, ok := payload["ListingAddress"]
	if !ok {
		return errors.New("Unable to find the listing address in the payload")
	}
	listingAddress := listingAddrInterface.(common.Address)
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil {
		return err
	}
	lastGovState := model.GovernanceStateAppWhitelisted
	whitelisted := true
	if listing == nil {
		err = e.persistNewListing(listingAddress, whitelisted, lastGovState)
		if err != nil {
			return err
		}
		metadata := map[string]interface{}{}
		err = e.persistNewGovernanceEvent(
			listingAddress,
			event.ContractAddress(),
			metadata,
			event.EventType(),
			event.Hash(),
		)
		return err
	}
	listing.SetLastGovernanceState(lastGovState)
	updatedFields = append(updatedFields, "LastGovernanceState")
	listing.SetWhitelisted(whitelisted)
	updatedFields = append(updatedFields, "whitelisted")
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processTCRApplicationRemoved(event *crawlermodel.CivilEvent) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddrInterface, ok := payload["ListingAddress"]
	if !ok {
		return errors.New("Unable to find the listing address in the payload")
	}
	listingAddress := listingAddrInterface.(common.Address)
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil {
		return err
	}
	lastGovState := model.GovernanceStateAppRemoved
	whitelisted := false
	if listing == nil {
		err = e.persistNewListing(listingAddress, whitelisted, lastGovState)
		if err != nil {
			return err
		}
		metadata := map[string]interface{}{}
		err = e.persistNewGovernanceEvent(
			listingAddress,
			event.ContractAddress(),
			metadata,
			event.EventType(),
			event.Hash(),
		)
		return err
	}
	listing.SetLastGovernanceState(lastGovState)
	updatedFields = append(updatedFields, "LastGovernanceState")
	listing.SetWhitelisted(whitelisted)
	updatedFields = append(updatedFields, "whitelisted")
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processTCRListingRemoved(event *crawlermodel.CivilEvent) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddrInterface, ok := payload["ListingAddress"]
	if !ok {
		return errors.New("Unable to find the listing address in the payload")
	}
	listingAddress := listingAddrInterface.(common.Address)
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil {
		return err
	}
	lastGovState := model.GovernanceStateRemoved
	whitelisted := false
	if listing == nil {
		err = e.persistNewListing(listingAddress, whitelisted, lastGovState)
		if err != nil {
			return err
		}
		metadata := map[string]interface{}{}
		err = e.persistNewGovernanceEvent(
			listingAddress,
			event.ContractAddress(),
			metadata,
			event.EventType(),
			event.Hash(),
		)
		return err
	}
	listing.SetLastGovernanceState(lastGovState)
	updatedFields = append(updatedFields, "LastGovernanceState")
	listing.SetWhitelisted(whitelisted)
	updatedFields = append(updatedFields, "whitelisted")
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processTCRListingWithdrawn(event *crawlermodel.CivilEvent) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddrInterface, ok := payload["ListingAddress"]
	if !ok {
		return errors.New("Unable to find the listing address in the payload")
	}
	listingAddress := listingAddrInterface.(common.Address)
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil {
		return fmt.Errorf("Error retrieving listing by address: err: %v", err)
	}
	lastGovState := model.GovernanceStateWithdrawn
	whitelisted := false
	if listing == nil {
		err = e.persistNewListing(listingAddress, whitelisted, lastGovState)
		if err != nil {
			return fmt.Errorf("Error persisting new listing: err: %v", err)
		}
		metadata := map[string]interface{}{}
		err = e.persistNewGovernanceEvent(
			listingAddress,
			event.ContractAddress(),
			metadata,
			event.EventType(),
			event.Hash(),
		)
		return err
	}
	listing.SetLastGovernanceState(lastGovState)
	updatedFields = append(updatedFields, "LastGovernanceState")
	listing.SetWhitelisted(whitelisted)
	updatedFields = append(updatedFields, "whitelisted")
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processNewsroomNameChanged(event *crawlermodel.CivilEvent) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddress := event.ContractAddress()
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil {
		return fmt.Errorf("Error retrieving listing by address: err: %v", err)
	}
	if listing == nil {
		return errors.New("No listing found to update with new name")
	}
	name, ok := payload["NewName"]
	if !ok {
		return errors.New("No NewName field found")
	}
	listing.SetName(name.(string))
	updatedFields = append(updatedFields, "Name")
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processNewsroomRevisionUpdated(event *crawlermodel.CivilEvent) error {
	payload := event.EventPayload()
	listingAddress := event.ContractAddress()

	editorAddress, ok := payload["Editor"]
	if !ok {
		return errors.New("No editor address found")
	}
	contentID, ok := payload["ContentId"]
	if !ok {
		return errors.New("No content id found")
	}
	revisionID, ok := payload["RevisionId"]
	if !ok {
		return errors.New("No revision id found")
	}
	revisionURI, ok := payload["Uri"]
	if !ok {
		return errors.New("No revision uri found")
	}

	newsroom, err := contract.NewNewsroomContract(listingAddress, e.client)
	if err != nil {
		return fmt.Errorf("Error creating newsroom contract: err: %v", err)
	}
	content, err := newsroom.GetContent(&bind.CallOpts{}, contentID.(*big.Int))
	if err != nil {
		return fmt.Errorf("Error retrieving newsroom content: err: %v", err)
	}
	contentHash := byte32ToHexString(content.ContentHash)

	scraperContent, err := e.contentScraper.ScrapeContent(revisionURI.(string))
	if err != nil {
		log.Errorf("Error scraping content: err: %v", err)
	}

	articlePayload := e.scraperContentToPayload(scraperContent)
	revision := model.NewContentRevision(
		listingAddress,
		articlePayload,
		contentHash,
		editorAddress.(common.Address),
		contentID.(*big.Int),
		revisionID.(*big.Int),
		revisionURI.(string),
		crawlerutils.CurrentEpochSecsInInt64(),
	)
	if err != nil {
		return err
	}
	err = e.revisionPersister.CreateContentRevision(revision)
	return err
}

func (e *EventProcessor) processNewsroomOwnershipTransferred(event *crawlermodel.CivilEvent) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddress := event.ContractAddress()
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil {
		return err
	}
	if listing == nil {
		return errors.New("No listing found to update owners")
	}
	previousOwner, ok := payload["PreviousOwner"]
	if !ok {
		return errors.New("No previous owner found")
	}
	newOwner, ok := payload["NewOwner"]
	if !ok {
		return errors.New("No new owner found")
	}
	listing.RemoveOwnerAddress(previousOwner.(common.Address))
	listing.AddOwnerAddress(newOwner.(common.Address))
	updatedFields = append(updatedFields, "OwnerAddresses")
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) scraperContentToPayload(content *model.ScraperContent) model.ArticlePayload {
	payload := model.ArticlePayload{}
	payload["text"] = content.Text()
	payload["html"] = content.HTML()
	payload["uri"] = content.URI()
	payload["data"] = content.Data()
	return payload
}
