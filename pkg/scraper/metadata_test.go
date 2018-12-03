package scraper_test

// import (
// 	"strings"
// 	"testing"

// 	"github.com/joincivil/civil-events-processor/pkg/scraper"
// )

// const (
// 	// This may go away at some point, so see if this fails
// 	civilTestAddress = "https://civil-develop.go-vip.co/crawler-pod/wp-json/civil-newsroom-protocol/v1/revisions/11"
// )

// func TestCivilMetadataScraper(t *testing.T) {
// 	_scraper := &scraper.CivilMetadataScraper{}
// 	metadata, err := _scraper.ScrapeCivilMetadata(civilTestAddress)

// 	if err != nil {
// 		if strings.Contains(err.Error(), "Error retrieving metadata") {
// 			t.Logf("Looks like issue retrieving remote data: err: %v", err)
// 		} else {
// 			t.Errorf("Should not have been an error scraping metadata: err: %v", err)
// 		}
// 	}

// 	if metadata.Title() == "" {
// 		t.Errorf("Should not have a empty title")
// 	}
// 	if metadata.Slug() == "" {
// 		t.Errorf("Should not have a empty slug")
// 	}
// 	if metadata.CanonicalURL() == "" {
// 		t.Errorf("Should not have a empty canonical URL")
// 	}
// }
