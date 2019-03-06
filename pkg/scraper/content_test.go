package scraper_test

import (
	"testing"

	"github.com/joincivil/civil-events-processor/pkg/scraper"
)

const (
	testIPFSLink = "ipfs://zb34W52j4ctZtqo99ko7D64TWbsaF5DzFuw1A7gntSJfFfEwV"
)

func TestIPFSScraper(t *testing.T) {
	ipfs := &scraper.CharterIPFSScraper{}
	content, err := ipfs.ScrapeContent(testIPFSLink)
	if err != nil {
		t.Fatalf("Should not have gotten error scraping IPFS data: err: %v", err)
	}

	uri := content.URI()
	if uri != testIPFSLink {
		t.Errorf("Should have gotten the same link in the content")
	}

	d := content.Data()
	url, ok := d["newsroomUrl"]
	if !ok {
		t.Errorf("Should have found newsroomUrl in data")
	}

	if url != "https://coloradosun.com" {
		t.Errorf("Should have matched the newsroom URLs")
	}
}
