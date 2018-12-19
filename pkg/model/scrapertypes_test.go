package model_test

import (
	"encoding/json"
	"testing"

	"github.com/joincivil/civil-events-processor/pkg/model"
)

const (
	testCivilMetadata = `{"title":"This is the first test","revisionContentHash":"0x574b0d5e64c30dc27d5728d5725fdecd35fddd9cfa8af522709deaa77421aa5f","revisionContentUrl":"https://vip.civil.co/nicole-test/wp-json/civil-publisher/v1/revisions-content/0x574b0d5e64c30dc27d5728d5725fdecd35fddd9cfa8af522709deaa77421aa5f/","canonicalUrl":"https://vip.civil.co/nicole-test/2018/12/17/this-is-the-first-test/","slug":"this-is-the-first-test","description":"I am being described","contributors":[{"role":"author","name":"nicole","address":"0xbc772037676252833e2b273adc82d0608ea366ab","signature":"0x97cdd0ad9da0e68732c817fefa046c72117f735445b14d8bc5418cb5aa2f45fc6865b872b3d8e891a3c49dfa1db348763c293ae2e1481d724496ece8c42709121b"}],"images":[{"url":"https://civil-develop.go-vip.co/crawler-pod/wp-content/uploads/sites/20/2018/07/Messages-Image3453599984.png","hash":"0x72ca80ed96a2b1ca20bf758a2142a678c0bc316e597161d0572af378e52b2e80","h":960,"w":697}],"tags":["news"],"primaryTag":"news","revisionDate":"2018-12-17 16:17:26","originalPublishDate":"2018-12-17 16:16:29","credibilityIndicators":{"original_reporting":true,"on_the_ground":false,"sources_cited":true,"subject_specialist":false},"opinion":false,"civilSchemaVersion":"0.0.1","content":"<!-- wp:paragraph -->\n<p>Adding this to the blockchain. Test.</p>\n<!-- /wp:paragraph -->"}`
)

func checkMetadataValues(t *testing.T, metadata *model.ScraperCivilMetadata) {
	if metadata.Title() != "This is the first test" {
		t.Errorf("Did not have correct title: %v", metadata.Title())
	}

	if metadata.RevisionContentHash() != "0x574b0d5e64c30dc27d5728d5725fdecd35fddd9cfa8af522709deaa77421aa5f" {
		t.Errorf("Did not have correct revision content hash: %v", metadata.RevisionContentHash())
	}

	if metadata.RevisionContentURL() != "https://vip.civil.co/nicole-test/wp-json/civil-publisher/v1/revisions-content/0x574b0d5e64c30dc27d5728d5725fdecd35fddd9cfa8af522709deaa77421aa5f/" {
		t.Errorf("Did not have correct revision content URL: %v", metadata.RevisionContentURL())
	}

	if metadata.CanonicalURL() != "https://vip.civil.co/nicole-test/2018/12/17/this-is-the-first-test/" {
		t.Errorf("Did not have correct canonical URL: %v", metadata.CanonicalURL())
	}

	if metadata.Slug() != "this-is-the-first-test" {
		t.Errorf("Did not have correct slug: %v", metadata.Slug())
	}

	if metadata.PrimaryTag() != "news" {
		t.Errorf("Did not have correct primary tag: %v", metadata.PrimaryTag())
	}

	if metadata.RevisionDate() != "2018-12-17 16:17:26" {
		t.Errorf("Did not have correct revision date: %v", metadata.RevisionDate())
	}

	if metadata.OriginalPublishDate() != "2018-12-17 16:16:29" {
		t.Errorf("Did not have correct revision date: %v", metadata.OriginalPublishDate())
	}

	if metadata.Opinion() {
		t.Errorf("Did not have correct opinion flag: %v", metadata.Opinion())
	}

	if metadata.SchemaVersion() != "0.0.1" {
		t.Errorf("Did not have correct schema version: %v", metadata.SchemaVersion())
	}

	if metadata.Description() != "I am being described" {
		t.Errorf("Did not have correct description: %v", metadata.Description())
	}

	if len(metadata.Contributors()) != 1 {
		t.Errorf("Did not find any contributors: %v", metadata.Contributors())
	} else {
		if metadata.Contributors()[0].Name() != "nicole" {
			t.Errorf("Did not find correct name: %v", metadata.Contributors()[0].Name())
		}
		if metadata.Contributors()[0].Role() != "author" {
			t.Errorf("Did not find correct role: %v", metadata.Contributors()[0].Role())
		}
		if metadata.Contributors()[0].Address() != "0xbc772037676252833e2b273adc82d0608ea366ab" {
			t.Errorf("Did not find correct address: %v", metadata.Contributors()[0].Address())
		}
		if metadata.Contributors()[0].Signature() != "0x97cdd0ad9da0e68732c817fefa046c72117f735445b14d8bc5418cb5aa2f45fc6865b872b3d8e891a3c49dfa1db348763c293ae2e1481d724496ece8c42709121b" {
			t.Errorf("Did not find correct signature: %v", metadata.Contributors()[0].Signature())
		}
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
		if !creds.OriginalReporting() {
			t.Errorf("Did not find correct original reporting: %v", creds.OriginalReporting())
		}
		if creds.OnTheGround() {
			t.Errorf("Did not find correct on the ground value: %v", creds.OnTheGround())
		}
		if !creds.SourcesCited() {
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
