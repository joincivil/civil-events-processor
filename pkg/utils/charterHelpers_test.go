// Package time_test contains tests for the config utils
package utils_test

import (
	"testing"

	"github.com/joincivil/civil-events-processor/pkg/utils"
)

func TestCleanURL1(t *testing.T) {
	cleanedURL, err := utils.CleanURL("http://fivethirtyeight.com")
	if err != nil {
		t.Fatalf("not expecting error")
	}
	if cleanedURL != "fivethirtyeight.com" {
		t.Fatalf("url not cleaned properly")
	}
}

func TestCleanURL2(t *testing.T) {
	cleanedURL, err := utils.CleanURL("http://www.fivethirtyeight.com")
	if err != nil {
		t.Fatalf("not expecting error")
	}
	if cleanedURL != "fivethirtyeight.com" {
		t.Fatalf("url not cleaned properly")
	}
}

func TestCleanURL3(t *testing.T) {
	cleanedURL, err := utils.CleanURL("http://www.fivethirtyeight.com/some-article")
	if err != nil {
		t.Fatalf("not expecting error")
	}
	if cleanedURL != "fivethirtyeight.com" {
		t.Fatalf("url not cleaned properly")
	}
}
