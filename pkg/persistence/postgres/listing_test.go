package postgres_test

import (
	"crypto/rand"
	"encoding/hex"
	// "fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/persistence/postgres"
	// "math/big"
	// "reflect"
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
	// appExpiry := big.NewInt(232424242)
	// unstakedDeposit := new(big.Int)
	// unstakedDeposit.SetString("100000000000000000000", 10)
	// challengeID := big.NewInt(10)
	testListing := model.NewListing("test_listing", contractAddress, true,
		model.GovernanceStateAppWhitelisted, "url_string", "charterURI", ownerAddr, ownerAddresses,
		contributorAddresses, 1257894000, 1257894000, 1257894000, 1257894000)
	return testListing, contractAddress
}

func TestNewDBListing(t *testing.T) {
	modelListing, _ := setupSampleListing()
	dbListing := postgres.NewListing(modelListing)
	_ = dbListing.DbToListingData()
	// TODO(IS): Check all fields other than the nil ones which are appexpiry, unstakeddeposit, challengeID
	// when you first save a listing
}
