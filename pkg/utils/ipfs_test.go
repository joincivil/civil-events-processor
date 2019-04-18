package utils_test

// import (
// 	"testing"

// 	"github.com/joincivil/civil-events-processor/pkg/utils"
// )

// const (
// 	testIPFSLink = "ipfs://zb34W52j4ctZtqo99ko7D64TWbsaF5DzFuw1A7gntSJfFfEwV"
// )

// func TestRetrieveIPFSLink(t *testing.T) {
// 	bys, err := utils.RetrieveIPFSLink(testIPFSLink)
// 	if err != nil {
// 		t.Errorf("Should not have gotten error retrieving IPFS link: err: %v", err)
// 	}

// 	if len(bys) == 0 {
// 		t.Errorf("Should not have gotten empty value from link")
// 	}
// }

// func TestRetrieveIPFSLinkInvalidLink(t *testing.T) {
// 	bys, err := utils.RetrieveIPFSLink("https://civil.co")
// 	if err == nil {
// 		t.Errorf("Should not have gotten error retrieving IPFS link: err: %v", err)
// 	}

// 	if len(bys) > 0 {
// 		t.Errorf("Should have gotten empty value from link")
// 	}
// }
