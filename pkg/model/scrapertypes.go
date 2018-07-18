// Package model contains the general data models and interfaces for the Civil processor.
package model // import "github.com/joincivil/civil-events-processor/pkg/model"

// ScraperContentMetadata represents metadata for the scraped content
// Potentially retrieved from a different location than the content and generally
// in JSON format
type ScraperContentMetadata map[string]interface{}

// NewScraperContent is a convenience function to init a new ScraperContent struct
func NewScraperContent(text string, html string, uri string, author string,
	data map[string]interface{}) *ScraperContent {
	return &ScraperContent{
		text:   text,
		html:   html,
		uri:    uri,
		author: author,
		data:   data,
	}
}

// ScraperContent represents the scraped content data
type ScraperContent struct {
	text   string
	html   string
	uri    string
	author string
	data   map[string]interface{}
}

// URI returns the URI to the content
func (c *ScraperContent) URI() string {
	return c.uri
}

// HTML returns the raw HTML of the content
func (c *ScraperContent) HTML() string {
	return c.html
}

// Text returns the plain text of the content
func (c *ScraperContent) Text() string {
	return c.text
}

// Author returns the plain text name of the author if found.
func (c *ScraperContent) Author() string {
	return c.author
}

// Data returns any additional data for the content
func (c *ScraperContent) Data() map[string]interface{} {
	return c.data
}

// MetadataScraper is the interface for implementations of metadata scraper
type MetadataScraper interface {
	ScrapeMetadata(uri string) (*ScraperContentMetadata, error)
}

// ContentScraper is the interface for implementations of content scraper
type ContentScraper interface {
	ScrapeContent(uri string) (*ScraperContent, error)
}
