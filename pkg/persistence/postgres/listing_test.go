package postgres_test

import (
	"crypto/rand"
	"encoding/hex"
	// "fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/persistence/postgres"
	"math/big"
	"reflect"
	"testing"
)

// random hex string generation
func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func setupSampleListing() (*model.Listing, common.Address) {
	address1, _ := randomHex(32)
	address2, _ := randomHex(32)
	address3, _ := randomHex(32)
	contractAddress := common.HexToAddress(address1)
	ownerAddr := common.HexToAddress(address2)
	ownerAddresses := []common.Address{common.HexToAddress(address2), common.HexToAddress(address3)}
	contributorAddresses := ownerAddresses
	appExpiry := big.NewInt(232424242)
	unstakedDeposit := new(big.Int)
	unstakedDeposit.SetString("100000000000000000000", 10)
	challengeID := big.NewInt(10)

	hash := "33333333333333333333333333333333"
	contentHash := [32]byte{}
	copy(contentHash[:], hash)

	charter := model.NewCharter(&model.CharterParams{
		URI:         "charterURI",
		ContentID:   big.NewInt(0),
		RevisionID:  big.NewInt(3),
		Signature:   []byte("signature"),
		Author:      common.HexToAddress("0x98C8CF45BD844627E84E1C506Ca87cC9436317D0"),
		ContentHash: contentHash,
		Timestamp:   big.NewInt(1234567),
	})
	testListingParams := &model.NewListingParams{
		Name:                 "test_listing",
		ContractAddress:      contractAddress,
		Whitelisted:          true,
		LastState:            model.GovernanceStateAppWhitelisted,
		URL:                  "url_string",
		Charter:              charter,
		Owner:                ownerAddr,
		OwnerAddresses:       ownerAddresses,
		ContributorAddresses: contributorAddresses,
		CreatedDateTs:        1257894000,
		ApplicationDateTs:    1257894000,
		ApprovalDateTs:       1257894000,
		LastUpdatedDateTs:    1257894000,
		AppExpiry:            appExpiry,
		UnstakedDeposit:      unstakedDeposit,
		ChallengeID:          challengeID,
	}
	testListing := model.NewListing(testListingParams)
	return testListing, contractAddress
}

func TestNewDBListing(t *testing.T) {
	modelListing, _ := setupSampleListing()
	dbListing := postgres.NewListing(modelListing)
	modelListingCheck := dbListing.DbToListingData()
	reflect.DeepEqual(modelListing, modelListingCheck)
}
