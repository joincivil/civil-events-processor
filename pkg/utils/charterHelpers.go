// Package utils contains various common utils separate by utility types
package utils

import (
	"github.com/pkg/errors"
	"net/url"
	"strings"
)

// CleanURL takes in a url and returns a string cleaned of non-essential url info (e.g. "http", "https", "www.")
func CleanURL(charterURL string) (string, error) {
	parsedURL, err := url.Parse(charterURL)
	if err != nil {
		return "", errors.Errorf("Could not parse URL")
	}

	cleanURL := strings.TrimPrefix(strings.ToLower(parsedURL.Host), "www.")

	return cleanURL, nil
}
