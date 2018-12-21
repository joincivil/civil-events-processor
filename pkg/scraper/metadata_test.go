// +build integration

package scraper_test

// const (
// 	// This may go away at some point, so see if this fails
// 	// Updated on 12/18/2018
// 	civilTestAddress = "https://vip.civil.co/nicole-test/wp-json/civil-publisher/v1/revisions/11"
// )

// func TestCivilMetadataScraper(t *testing.T) {
// 	_scraper := &scraper.CivilMetadataScraper{}
// 	metadata, err := _scraper.ScrapeCivilMetadata(civilTestAddress)

// 	if err != nil {
// 		if strings.Contains(err.Error(), "Error retrieving metadata") {
// 			t.Fatalf("Looks like issue retrieving remote data: err: %v", err)
// 		} else {
// 			t.Fatalf("Should not have been an error scraping metadata: err: %v", err)
// 		}
// 	}

// 	checkMetadataValues(t, metadata)
// }

// func checkMetadataValues(t *testing.T, metadata *model.ScraperCivilMetadata) {
// 	if metadata.Title() == "" {
// 		t.Errorf("Did not have correct title: %v", metadata.Title())
// 	}

// 	if metadata.RevisionContentHash() == "" {
// 		t.Errorf("Did not have correct revision content hash: %v", metadata.RevisionContentHash())
// 	}

// 	if metadata.RevisionContentURL() == "" {
// 		t.Errorf("Did not have correct revision content URL: %v", metadata.RevisionContentURL())
// 	}

// 	if metadata.CanonicalURL() == "" {
// 		t.Errorf("Did not have correct canonical URL: %v", metadata.CanonicalURL())
// 	}

// 	if metadata.Slug() == "" {
// 		t.Errorf("Did not have correct slug: %v", metadata.Slug())
// 	}

// 	if metadata.RevisionDate() == "" {
// 		t.Errorf("Did not have correct revision date: %v", metadata.RevisionDate())
// 	}

// 	if metadata.OriginalPublishDate() == "" {
// 		t.Errorf("Did not have correct revision date: %v", metadata.OriginalPublishDate())
// 	}

// 	if metadata.Opinion() {
// 		t.Errorf("Did not have correct opinion flag: %v", metadata.Opinion())
// 	}

// 	if metadata.SchemaVersion() != "0.0.1" {
// 		t.Errorf("Did not have correct schema version: %v", metadata.SchemaVersion())
// 	}

// 	if len(metadata.Contributors()) != 1 {
// 		t.Errorf("Did not find any contributors: %v", metadata.Contributors())
// 	} else {
// 		if metadata.Contributors()[0].Name() != "nicole" {
// 			t.Errorf("Did not find correct name: %v", metadata.Contributors()[0].Name())
// 		}
// 		if metadata.Contributors()[0].Role() != "author" {
// 			t.Errorf("Did not find correct role: %v", metadata.Contributors()[0].Role())
// 		}
// 	}
// }
