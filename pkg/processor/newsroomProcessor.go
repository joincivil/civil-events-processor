package processor

import (
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	log "github.com/golang/glog"
	"math/big"
	"strings"

	commongen "github.com/joincivil/civil-events-crawler/pkg/generated/common"
	"github.com/joincivil/civil-events-crawler/pkg/generated/contract"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	crawlerutils "github.com/joincivil/civil-events-crawler/pkg/utils"

	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/utils"
)

const (
	listingNameFieldName = "Name"
	ownerFieldName       = "OwnerAddresses"

	defaultCharterContentID = 0
	// approvalDateNoUpdate    = int64(-1)
	approvalDateEmptyValue = int64(0)
)

// NewNewsroomEventProcessor is a convenience function to init an EventProcessor
func NewNewsroomEventProcessor(client bind.ContractBackend, listingPersister model.ListingPersister,
	revisionPersister model.ContentRevisionPersister,
	contentScraper model.ContentScraper, metadataScraper model.MetadataScraper,
	civilMetadataScraper model.CivilMetadataScraper) *NewsroomEventProcessor {
	return &NewsroomEventProcessor{
		client:               client,
		listingPersister:     listingPersister,
		revisionPersister:    revisionPersister,
		contentScraper:       contentScraper,
		metadataScraper:      metadataScraper,
		civilMetadataScraper: civilMetadataScraper,
	}
}

// NewsroomEventProcessor handles the processing of raw events into aggregated data
// for use via the API.
type NewsroomEventProcessor struct {
	client               bind.ContractBackend
	listingPersister     model.ListingPersister
	revisionPersister    model.ContentRevisionPersister
	contentScraper       model.ContentScraper
	metadataScraper      model.MetadataScraper
	civilMetadataScraper model.CivilMetadataScraper
}

func (n *NewsroomEventProcessor) process(event *crawlermodel.Event) (bool, error) {
	if !n.isValidNewsroomContractEventName(event.EventType()) {
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
		err = n.processNewsroomNameChanged(event)

	// When there is a new revision on content
	case "RevisionUpdated":
		log.Infof("Handling RevisionUpdated for %v\n", event.ContractAddress().Hex())
		err = n.processNewsroomRevisionUpdated(event)

	// When there is a new owner
	case "OwnershipTransferred":
		log.Infof("Handling OwnershipTransferred for %v\n", event.ContractAddress().Hex())
		err = n.processNewsroomOwnershipTransferred(event)

	default:
		ran = false
	}
	return ran, err
}

func (n *NewsroomEventProcessor) isValidNewsroomContractEventName(name string) bool {
	name = strings.Trim(name, " _")
	eventNames := commongen.EventTypesNewsroomContract()
	return isStringInSlice(eventNames, name)
}

func (n *NewsroomEventProcessor) processNewsroomNameChanged(event *crawlermodel.Event) error {
	var updatedFields []string
	payload := event.EventPayload()
	listing, err := n.retrieveOrCreateListingForNewsroomEvent(event)
	if err != nil && err != model.ErrPersisterNoResults {
		return fmt.Errorf("Error retrieving listing or creating by address: err: %v", err)
	}
	name, ok := payload["NewName"]
	if !ok {
		return errors.New("No NewName field found")
	}
	listing.SetName(name.(string))
	updatedFields = append(updatedFields, listingNameFieldName)
	err = n.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (n *NewsroomEventProcessor) processNewsroomRevisionUpdated(event *crawlermodel.Event) error {
	// Create a new listing if none exists for the address in the event
	_, err := n.retrieveOrCreateListingForNewsroomEvent(event)
	if err != nil && err != model.ErrPersisterNoResults {
		return fmt.Errorf("Error retrieving listing or creating by address: err: %v", err)
	}

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
	newsroom, err := contract.NewNewsroomContract(listingAddress, n.client)
	if err != nil {
		return fmt.Errorf("Error creating newsroom contract: err: %v", err)
	}
	content, err := newsroom.GetContent(&bind.CallOpts{}, contentID.(*big.Int))
	if err != nil {
		return fmt.Errorf("Error retrieving newsroom content: err: %v", err)
	}
	contentHash := utils.Byte32ToHexString(content.ContentHash)

	// Scrape the metadata and content for the revision
	metadata, scraperContent, err := n.scrapeData(revisionURI.(string))
	if err != nil {
		log.Errorf("Error scraping data: err: %v", err)
	}

	articlePayload := model.ArticlePayload{}
	if metadata != nil {
		articlePayload = n.scraperDataToPayload(metadata, scraperContent)
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
		event.Timestamp(),
	)

	err = n.revisionPersister.CreateContentRevision(revision)
	if err != nil {
		return err
	}

	// If the revision is for the charter, need to update the data in the
	// listing.
	if contentID.(*big.Int).Int64() == defaultCharterContentID {
		err = n.updateListingCharterRevision(revision)
	}
	return err
}

func (n *NewsroomEventProcessor) processNewsroomOwnershipTransferred(event *crawlermodel.Event) error {
	var updatedFields []string
	payload := event.EventPayload()
	listing, err := n.retrieveOrCreateListingForNewsroomEvent(event)
	if err != nil && err != model.ErrPersisterNoResults {
		return err
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
	updatedFields = append(updatedFields, ownerFieldName)
	err = n.listingPersister.UpdateListing(listing, updatedFields)
	return err
}

func (n *NewsroomEventProcessor) updateListingCharterRevision(revision *model.ContentRevision) error {
	listing, err := n.listingPersister.ListingByAddress(revision.ListingAddress())
	if err != nil {
		return err
	}

	if revision.ContractRevisionID().Cmp(listing.Charter().RevisionID()) == 0 {
		return fmt.Errorf("Not updating listing charter, revision ids are the same")
	}

	newsroom, newsErr := contract.NewNewsroomContract(revision.ListingAddress(), n.client)
	if newsErr != nil {
		return fmt.Errorf("Error reading from Newsroom contract: %v ", newsErr)
	}

	charterContent, contErr := newsroom.GetRevision(
		&bind.CallOpts{},
		revision.ContractContentID(),
		revision.ContractRevisionID(),
	)
	if contErr != nil {
		return fmt.Errorf("Error getting charter revision from Newsroom contract: %v ", contErr)
	}

	updatedFields := []string{"Charter"}
	updatedCharter := model.NewCharter(&model.CharterParams{
		URI:         revision.RevisionURI(),
		ContentID:   listing.Charter().ContentID(),
		RevisionID:  revision.ContractRevisionID(),
		Signature:   charterContent.Signature,
		Author:      charterContent.Author,
		ContentHash: charterContent.ContentHash,
		Timestamp:   charterContent.Timestamp,
	})
	listing.SetCharter(updatedCharter)

	return n.listingPersister.UpdateListing(listing, updatedFields)
}

func (n *NewsroomEventProcessor) retrieveOrCreateListingForNewsroomEvent(event *crawlermodel.Event) (*model.Listing, error) {
	listingAddress := event.ContractAddress()
	listing, err := n.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
		return nil, err
	}
	if listing != nil {
		return listing, nil
	}
	// If a listing doesn't exist, create a new one. This shouldn't happen if events are ordered
	err = n.persistNewListing(
		listingAddress,
		false,
		model.GovernanceStateNone,
		event.Timestamp(),
		event.Timestamp(),
		approvalDateEmptyValue,
	)
	if err != nil {
		return nil, err
	}
	listing, err = n.listingPersister.ListingByAddress(listingAddress)
	if err != nil && err != model.ErrPersisterNoResults {
		return nil, err
	}
	if listing == nil {
		return nil, errors.New("Failed to create a listing")
	}
	return listing, nil
}

// NOTE: This should be the function that is called to get data from contract
// in the case events are out of order.
func (n *NewsroomEventProcessor) persistNewListing(listingAddress common.Address,
	whitelisted bool, lastGovernanceState model.GovernanceState, creationDate int64,
	applicationDate int64, approvalDate int64) error {
	// TODO(PN): How do I get the URL of the site?
	url := ""
	// charter is the first content item in the newsroom contract
	charterContentID := big.NewInt(defaultCharterContentID)
	newsroom, newsErr := contract.NewNewsroomContract(listingAddress, n.client)
	if newsErr != nil {
		return fmt.Errorf("Error reading from Newsroom contract: %v ", newsErr)
	}
	name, nameErr := newsroom.Name(&bind.CallOpts{})
	if nameErr != nil {
		return fmt.Errorf("Error getting Name from Newsroom contract: %v ", nameErr)
	}
	// NOTE: We should get this revision as an event, and only use this if events are not in order
	revisionCount, countErr := newsroom.RevisionCount(&bind.CallOpts{}, charterContentID)
	if countErr != nil {
		return fmt.Errorf("Error getting RevisionCount from Newsroom contract: %v ", countErr)
	}
	if revisionCount.Int64() <= 0 {
		return fmt.Errorf("Error there are no revisions for the charter: addr: %v", listingAddress)
	}

	// latest revision should be total revisions - 1 for index
	latestRevisionID := big.NewInt(revisionCount.Int64() - 1)
	charterContent, contErr := newsroom.GetRevision(&bind.CallOpts{}, charterContentID, latestRevisionID)
	if contErr != nil {
		return fmt.Errorf("Error getting charter revision from Newsroom contract: %v ", contErr)
	}

	charter := model.NewCharter(&model.CharterParams{
		URI:         charterContent.Uri,
		ContentID:   charterContentID,
		RevisionID:  latestRevisionID,
		Signature:   charterContent.Signature,
		Author:      charterContent.Author,
		ContentHash: charterContent.ContentHash,
		Timestamp:   charterContent.Timestamp,
	})

	charterAuthorAddr := charterContent.Author
	ownerAddr, err := newsroom.Owner(&bind.CallOpts{})
	if err != nil {
		return err
	}
	ownerAddresses := []common.Address{ownerAddr}
	contributorAddresses := []common.Address{charterAuthorAddr}

	listing := model.NewListing(&model.NewListingParams{
		Name:                 name,
		ContractAddress:      listingAddress,
		Whitelisted:          whitelisted,
		LastState:            lastGovernanceState,
		URL:                  url,
		Charter:              charter,
		Owner:                ownerAddr,
		OwnerAddresses:       ownerAddresses,
		ContributorAddresses: contributorAddresses,
		CreatedDateTs:        creationDate,
		ApplicationDateTs:    applicationDate,
		ApprovalDateTs:       approvalDate,
		LastUpdatedDateTs:    crawlerutils.CurrentEpochSecsInInt64(),
	})
	err = n.listingPersister.CreateListing(listing)
	return err
}

func (n *NewsroomEventProcessor) scrapeData(metadataURL string) (
	*model.ScraperCivilMetadata, *model.ScraperContent, error) {
	var err error
	var civilMetadata *model.ScraperCivilMetadata
	var content *model.ScraperContent

	if metadataURL != "" {
		civilMetadata, err = n.civilMetadataScraper.ScrapeCivilMetadata(metadataURL)
		if err != nil {
			return nil, nil, err
		}
		// TODO(PN): Hack to fix bad URLs received for metadata
		// Remove this later after testing
		if civilMetadata.Title() == "" && civilMetadata.RevisionContentHash() == "" {
			metadataURL = strings.Replace(metadataURL, "/wp-json", "/crawler-pod/wp-json", -1)
			civilMetadata, err = n.civilMetadataScraper.ScrapeCivilMetadata(metadataURL)
			if err != nil {
				return nil, nil, err
			}
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

func (n *NewsroomEventProcessor) scraperDataToPayload(metadata *model.ScraperCivilMetadata,
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
	payload["authors"] = n.buildAuthors(metadata)
	payload["images"] = n.buildImages(metadata)
	return payload
}

func (n *NewsroomEventProcessor) buildAuthors(metadata *model.ScraperCivilMetadata) []map[string]interface{} {
	authors := []map[string]interface{}{}
	for _, author := range metadata.Authors() {
		entry := map[string]interface{}{
			"byline": author.Byline(),
		}
		authors = append(authors, entry)
	}
	return authors
}

func (n *NewsroomEventProcessor) buildImages(metadata *model.ScraperCivilMetadata) []map[string]interface{} {
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
