package model_test

import (
	"encoding/json"
	"testing"

	"github.com/joincivil/civil-events-processor/pkg/model"
)

const (
	// https://civil-develop.go-vip.co/crawler-pod/wp-json/civil-newsroom-protocol/v1/revisions/11
	testCivilMetadata = `{"title":"This is a test post","revisionContentHash":"0x9e4acfe532c8458abfc1f1d30c4eaf986fee52cf1f65c9548f1dc437fb6dfd38","revisionContentUrl":"https:\/\/civil-develop.go-vip.co\/crawler-pod\/wp-json\/civil-newsroom-protocol\/v1\/revisions-content\/0x9e4acfe532c8458abfc1f1d30c4eaf986fee52cf1f65c9548f1dc437fb6dfd38\/","canonicalUrl":"https:\/\/civil-develop.go-vip.co\/crawler-pod\/2018\/07\/25\/this-is-a-test-post\/","slug":"this-is-a-test-post","description":"I'm being described","authors":[{"byline":"Walker Flynn"}],"images":[{"url":"https:\/\/civil-develop.go-vip.co\/crawler-pod\/wp-content\/uploads\/sites\/20\/2018\/07\/Messages-Image3453599984.png","hash":"0x72ca80ed96a2b1ca20bf758a2142a678c0bc316e597161d0572af378e52b2e80","h":960,"w":697}],"tags":["news"],"primaryTag":"news","revisionDate":"2018-07-25 17:17:20","originalPublishDate":"2018-07-25 17:17:07","credibilityIndicators":{"original_reporting":"1","on_the_ground":false,"sources_cited":"1","subject_specialist":false},"opinion":false,"civilSchemaVersion":"1.0.0"}`
)

func checkMetadataValues(t *testing.T, metadata *model.ScraperCivilMetadata) {
	if metadata.Title() != "This is a test post" {
		t.Errorf("Did not have correct title: %v", metadata.Title())
	}

	if metadata.RevisionContentHash() != "0x9e4acfe532c8458abfc1f1d30c4eaf986fee52cf1f65c9548f1dc437fb6dfd38" {
		t.Errorf("Did not have correct revision content hash: %v", metadata.RevisionContentHash())
	}

	if metadata.RevisionContentURL() != "https://civil-develop.go-vip.co/crawler-pod/wp-json/civil-newsroom-protocol/v1/revisions-content/0x9e4acfe532c8458abfc1f1d30c4eaf986fee52cf1f65c9548f1dc437fb6dfd38/" {
		t.Errorf("Did not have correct revision content URL: %v", metadata.RevisionContentURL())
	}

	if metadata.CanonicalURL() != "https://civil-develop.go-vip.co/crawler-pod/2018/07/25/this-is-a-test-post/" {
		t.Errorf("Did not have correct canonical URL: %v", metadata.CanonicalURL())
	}

	if metadata.Slug() != "this-is-a-test-post" {
		t.Errorf("Did not have correct slug: %v", metadata.Slug())
	}

	if metadata.PrimaryTag() != "news" {
		t.Errorf("Did not have correct primary tag: %v", metadata.PrimaryTag())
	}

	if metadata.RevisionDate() != "2018-07-25 17:17:20" {
		t.Errorf("Did not have correct revision date: %v", metadata.RevisionDate())
	}

	if metadata.OriginalPublishDate() != "2018-07-25 17:17:07" {
		t.Errorf("Did not have correct revision date: %v", metadata.OriginalPublishDate())
	}

	if metadata.Opinion() {
		t.Errorf("Did not have correct opinion flag: %v", metadata.Opinion())
	}

	if metadata.SchemaVersion() != "1.0.0" {
		t.Errorf("Did not have correct schema version: %v", metadata.SchemaVersion())
	}

	if metadata.Description() != "I'm being described" {
		t.Errorf("Did not have correct description: %v", metadata.Description())
	}

	if len(metadata.Authors()) != 1 {
		t.Errorf("Did not find any authors: %v", metadata.Authors())
	} else if metadata.Authors()[0].Byline() != "Walker Flynn" {
		t.Errorf("Did not find correct author: %v", metadata.Authors()[0].Byline())
	}

	if len(metadata.Images()) <= 0 {
		t.Errorf("Did not find any images: %v", metadata.Images())
	} else {
		image := metadata.Images()[0]
		if image.URL() != "https://civil-develop.go-vip.co/crawler-pod/wp-content/uploads/sites/20/2018/07/Messages-Image3453599984.png" {
			t.Errorf("Did not find correct image URL: %v", image.URL())
		}
		if image.Hash() != "0x72ca80ed96a2b1ca20bf758a2142a678c0bc316e597161d0572af378e52b2e80" {
			t.Errorf("Did not find correct image hash: %v", image.Hash())
		}
		if image.Height() != 960 {
			t.Errorf("Did not find correct image height: %v", image.Height())
		}
		if image.Width() != 697 {
			t.Errorf("Did not find correct image width: %v", image.Width())
		}
	}

	if metadata.CredibilityIndicators() == nil {
		t.Error("Did not find credibility indicators")
	} else {
		creds := metadata.CredibilityIndicators()
		if creds.OriginalReporting() != "1" {
			t.Errorf("Did not find correct original reporting: %v", creds.OriginalReporting())
		}
		if creds.OnTheGround() {
			t.Errorf("Did not find correct on the ground value: %v", creds.OnTheGround())
		}
		if creds.SourcesCited() != "1" {
			t.Errorf("Did not find correct sources cited value: %v", creds.SourcesCited())
		}
		if creds.SubjectSpecialist() {
			t.Errorf("Did not find correct subject specialist value: %v", creds.SubjectSpecialist())
		}
	}
}

func TestScraperCivilMetadataUnmarshal(t *testing.T) {
	metadata := model.NewScraperCivilMetadata()
	err := json.Unmarshal([]byte(testCivilMetadata), metadata)
	if err != nil {
		t.Fatalf("Should not have failed to unmarshal JSON: err: %v", err)
	}
	checkMetadataValues(t, metadata)
}

func TestScraperCivilMetadataMarshal(t *testing.T) {
	metadata := model.NewScraperCivilMetadata()
	err := json.Unmarshal([]byte(testCivilMetadata), metadata)
	if err != nil {
		t.Fatalf("Should not have failed to unmarshal JSON: err: %v", err)
	}

	bytes, err := json.Marshal(metadata)
	if err != nil {
		t.Errorf("Should not have failed to marshal: err: %v", err)
	}

	metadata = model.NewScraperCivilMetadata()
	err = json.Unmarshal(bytes, metadata)
	if err != nil {
		t.Fatalf("Should not have failed to unmarshal JSON: err: %v", err)
	}

	checkMetadataValues(t, metadata)
}
