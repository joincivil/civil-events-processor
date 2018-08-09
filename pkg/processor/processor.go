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

const (
	govStateDBModelName    = "LastGovernanceState"
	whitelistedDBModelName = "Whitelisted"
	listingNameDBModelName = "Name"
	ownerAddDBModelName    = "OwnerAddresses"
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
	revisionPersister model.ContentRevisionPersister, govEventPersister model.GovernanceEventPersister,
	cronPersister model.CronPersister, contentScraper model.ContentScraper, metadataScraper model.MetadataScraper,
	civilMetadataScraper model.CivilMetadataScraper) *EventProcessor {
	return &EventProcessor{
		client:               client,
		listingPersister:     listingPersister,
		revisionPersister:    revisionPersister,
		govEventPersister:    govEventPersister,
		cronPersister:        cronPersister,
		contentScraper:       contentScraper,
		metadataScraper:      metadataScraper,
		civilMetadataScraper: civilMetadataScraper,
	}
}

// EventProcessor handles the processing of raw events into aggregated data
// for use via the API.
type EventProcessor struct {
	client               bind.ContractBackend
	listingPersister     model.ListingPersister
	revisionPersister    model.ContentRevisionPersister
	govEventPersister    model.GovernanceEventPersister
	cronPersister        model.CronPersister
	contentScraper       model.ContentScraper
	metadataScraper      model.MetadataScraper
	civilMetadataScraper model.CivilMetadataScraper
}

// Process runs the processor with the given set of raw CivilEvents
// Returns the last error if one has occurred, saves latest timestamp seen to cron persistence
func (e *EventProcessor) Process(events []*crawlermodel.Event) error {
	var err error
	var ran bool
	for _, event := range events {
		timestamp := int64(event.Timestamp())
		if timestamp > e.cronPersister.TimestampOfLastEvent() {
			e.cronPersister.SaveTimestamp(timestamp)
		}

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
		// if ran {
		// 	continue
		// }
		// log.Infof("Unhandled event: %v", event.EventType())
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

func (e *EventProcessor) processNewsroomEvent(event *crawlermodel.Event) (bool, error) {
	if !e.isValidNewsroomContractEventName(event.EventType()) {
		return false, nil
	}

	var err error
	ran := true
	eventName := strings.Trim(event.EventType(), " _")

	// Handling all the actionable events from Newsroom Addressses
	switch eventName {
	// When a listing's name has changed
	case "NameChanged":
		log.Infof("Handling NameChanged for %v\n", event.ContractAddress().Hex())
		err = e.processNewsroomNameChanged(event)

	// When there is a new revision on content
	case "RevisionUpdated":
		log.Infof("Handling RevisionUpdated for %v\n", event.ContractAddress().Hex())
		err = e.processNewsroomRevisionUpdated(event)

	// When there is a new owner
	case "OwnershipTransferred":
		log.Infof("Handling OwnershipTransferred for %v\n", event.ContractAddress().Hex())
		err = e.processNewsroomOwnershipTransferred(event)

	default:
		ran = false
	}
	return ran, err
}

func (e *EventProcessor) processCivilTCREvent(event *crawlermodel.Event) (bool, error) {
	if !e.isValidCivilTCRContractEventName(event.EventType()) {
		return false, nil
	}

	var err error
	ran := true
	eventName := strings.Trim(event.EventType(), " _")

	var listingAddress common.Address
	addr := event.EventPayload()["ListingAddress"]
	if addr != nil {
		listingAddress = addr.(common.Address)
	}

	// Handling all the actionable events from the TCR
	switch eventName {
	// When a listing has applied
	case "Application":
		log.Infof("Handling Application for %v\n", listingAddress.Hex())
		err = e.processTCRApplication(event)

	// When a listing has been challenged at any point
	case "Challenge":
		log.Infof("Handling Challenge for %v\n", listingAddress.Hex())
		err = e.processTCRChallenge(event)

	// When a listing gets whitelisted
	case "ApplicationWhitelisted":
		log.Infof("Handling ApplicationWhitelisted for %v\n", listingAddress.Hex())
		err = e.processTCRApplicationWhitelisted(event)

	// When an application for a listing has been removed
	case "ApplicationRemoved":
		log.Infof("Handling ApplicationRemoved for %v\n", listingAddress.Hex())
		err = e.processTCRApplicationRemoved(event)

	// When a listing is de-listed
	case "ListingRemoved":
		log.Infof("Handling ListingRemoved for %v\n", listingAddress.Hex())
		err = e.processTCRListingRemoved(event)

	// When a listing applicaiton has been withdrawn
	case "ListingWithdrawn":
		log.Infof("Handling ListingWithdrawn for %v\n", listingAddress.Hex())
		err = e.processTCRListingWithdrawn(event)

	default:
		ran = false
	}

	return ran, err
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

func (e *EventProcessor) processTCRApplication(event *crawlermodel.Event) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddrInterface, ok := payload["ListingAddress"]
	if !ok {
		return errors.New("Unable to find the listing address in the payload")
	}
	listingAddress := listingAddrInterface.(common.Address)
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
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
	updatedFields = append(updatedFields, govStateDBModelName)
	listing.SetWhitelisted(whitelisted)
	updatedFields = append(updatedFields, whitelistedDBModelName)
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processTCRChallenge(event *crawlermodel.Event) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddrInterface, ok := payload["ListingAddress"]
	if !ok {
		return errors.New("Unable to find the listing address in the payload")
	}
	listingAddress := listingAddrInterface.(common.Address)
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
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
	updatedFields = append(updatedFields, govStateDBModelName)
	listing.SetWhitelisted(whitelisted)
	updatedFields = append(updatedFields, whitelistedDBModelName)
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processTCRApplicationWhitelisted(event *crawlermodel.Event) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddrInterface, ok := payload["ListingAddress"]
	if !ok {
		return errors.New("Unable to find the listing address in the payload")
	}
	listingAddress := listingAddrInterface.(common.Address)
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
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
	updatedFields = append(updatedFields, govStateDBModelName)
	listing.SetWhitelisted(whitelisted)
	updatedFields = append(updatedFields, whitelistedDBModelName)
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processTCRApplicationRemoved(event *crawlermodel.Event) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddrInterface, ok := payload["ListingAddress"]
	if !ok {
		return errors.New("Unable to find the listing address in the payload")
	}
	listingAddress := listingAddrInterface.(common.Address)
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
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
	updatedFields = append(updatedFields, govStateDBModelName)
	listing.SetWhitelisted(whitelisted)
	updatedFields = append(updatedFields, whitelistedDBModelName)
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processTCRListingRemoved(event *crawlermodel.Event) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddrInterface, ok := payload["ListingAddress"]
	if !ok {
		return errors.New("Unable to find the listing address in the payload")
	}
	listingAddress := listingAddrInterface.(common.Address)
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
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
	updatedFields = append(updatedFields, govStateDBModelName)
	listing.SetWhitelisted(whitelisted)
	updatedFields = append(updatedFields, whitelistedDBModelName)
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processTCRListingWithdrawn(event *crawlermodel.Event) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddrInterface, ok := payload["ListingAddress"]
	if !ok {
		return errors.New("Unable to find the listing address in the payload")
	}
	listingAddress := listingAddrInterface.(common.Address)
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
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
	updatedFields = append(updatedFields, govStateDBModelName)
	listing.SetWhitelisted(whitelisted)
	updatedFields = append(updatedFields, whitelistedDBModelName)
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processNewsroomNameChanged(event *crawlermodel.Event) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddress := event.ContractAddress()
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
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
	updatedFields = append(updatedFields, listingNameDBModelName)
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) processNewsroomRevisionUpdated(event *crawlermodel.Event) error {
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
	// Metadata URI
	revisionURI, ok := payload["Uri"]
	if !ok {
		return errors.New("No revision uri found")
	}

	// Pull data from the newsroom contract
	newsroom, err := contract.NewNewsroomContract(listingAddress, e.client)
	if err != nil {
		return fmt.Errorf("Error creating newsroom contract: err: %v", err)
	}
	content, err := newsroom.GetContent(&bind.CallOpts{}, contentID.(*big.Int))
	if err != nil {
		return fmt.Errorf("Error retrieving newsroom content: err: %v", err)
	}
	contentHash := byte32ToHexString(content.ContentHash)

	// Scrape the metadata and content for the revision
	metadata, scraperContent, err := e.scrapeData(revisionURI.(string))
	if err != nil {
		log.Errorf("Error scraping data: err: %v", err)
	}

	var articlePayload model.ArticlePayload
	if metadata != nil {
		articlePayload = e.scraperDataToPayload(metadata, scraperContent)
	}

	// Store the new revision
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
	err = e.revisionPersister.CreateContentRevision(revision)
	return err
}

func (e *EventProcessor) processNewsroomOwnershipTransferred(event *crawlermodel.Event) error {
	var updatedFields []string
	payload := event.EventPayload()
	listingAddress := event.ContractAddress()
	listing, err := e.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
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
	updatedFields = append(updatedFields, ownerAddDBModelName)
	err = e.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (e *EventProcessor) scrapeData(metadataURL string) (
	*model.ScraperCivilMetadata, *model.ScraperContent, error) {
	var err error
	var civilMetadata *model.ScraperCivilMetadata
	var content *model.ScraperContent

	if metadataURL != "" {
		civilMetadata, err = e.civilMetadataScraper.ScrapeCivilMetadata(metadataURL)
		if err != nil {
			return nil, nil, err
		}
	}

	// TODO(PN): Use canonical URL or the revision URL here?
	// TODO(PN): Commenting out the scraping of content until we make a decision on it

	// if civilMetadata != nil && civilMetadata.RevisionContentURL() != "" {
	// 	content, err = e.contentScraper.ScrapeContent(civilMetadata.RevisionContentURL())
	// 	if err != nil {
	// 		err = fmt.Errorf("Error scraping content: err: %v", err)
	// 	}
	// }
	return civilMetadata, content, err
}

func (e *EventProcessor) scraperDataToPayload(metadata *model.ScraperCivilMetadata,
	content *model.ScraperContent) model.ArticlePayload {
	// TODO(PN): ArticlePayload should be a struct rather than a map
	// TODO(PN): Do we need the content here?
	payload := model.ArticlePayload{}
	payload["title"] = metadata.Title()
	payload["revisionContentHash"] = metadata.RevisionContentHash()
	payload["revisionContentURL"] = metadata.RevisionContentURL()
	payload["canonicalURL"] = metadata.CanonicalURL()
	payload["slug"] = metadata.Slug()
	payload["description"] = metadata.Description()
	payload["primaryTag"] = metadata.PrimaryTag()
	payload["revisionDate"] = metadata.RevisionDate()
	payload["originalPublishDate"] = metadata.OriginalPublishDate()
	payload["opinion"] = metadata.Opinion()
	payload["schemaVersion"] = metadata.SchemaVersion()
	payload["authors"] = e.buildAuthors(metadata)
	payload["images"] = e.buildImages(metadata)
	return payload
}

func (e *EventProcessor) buildAuthors(metadata *model.ScraperCivilMetadata) []map[string]interface{} {
	authors := []map[string]interface{}{}
	for _, author := range metadata.Authors() {
		entry := map[string]interface{}{
			"byline": author.Byline(),
		}
		authors = append(authors, entry)
	}
	return authors
}

func (e *EventProcessor) buildImages(metadata *model.ScraperCivilMetadata) []map[string]interface{} {
	images := []map[string]interface{}{}
	for _, image := range metadata.Images() {
		entry := map[string]interface{}{
			"url":  image.URL(),
			"hash": image.Hash(),
			"h":    image.Height(),
			"w":    image.Width(),
		}
		images = append(images, entry)
	}
	return images
}
