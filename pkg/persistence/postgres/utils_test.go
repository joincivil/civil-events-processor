package postgres_test

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/joincivil/civil-events-processor/pkg/persistence/postgres"
	"math/big"
	"reflect"
	"testing"
)

var (
	// addressesString    = []string{"0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d", "0xDFe273082089bB7f70Ee36Eebcde64832FE97E55"}
	addressesCommon    = []common.Address{common.HexToAddress("0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d"), common.HexToAddress("0xDFe273082089bB7f70Ee36Eebcde64832FE97E55")}
	addressesOneString = "0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d,0xDFe273082089bB7f70Ee36Eebcde64832FE97E55"
)

func TestListCommonAddressesToString(t *testing.T) {
	stringConverted := postgres.ListCommonAddressesToString(addressesCommon)
	if stringConverted != addressesOneString {
		t.Errorf("string is not what it should be, %v", stringConverted)
	}
}

func TestStringToCommonAddressesList(t *testing.T) {
	commonAddressConverted := postgres.StringToCommonAddressesList(addressesOneString)
	if !reflect.DeepEqual(commonAddressConverted, addressesCommon) {
		t.Errorf("common.Address slice is not what it should be, %v", commonAddressConverted)
	}
}

func TestDbFieldNameFromModelName(t *testing.T) {
	listingNameMapping := map[string]string{
		"Name":                 "name",
		"ContractAddress":      "contract_address",
		"Whitelisted":          "whitelisted",
		"LastGovernanceState":  "last_governance_state",
		"URL":                  "url",
		"Charter":              "charter",
		"OwnerAddresses":       "owner_addresses",
		"Owner":                "owner",
		"ContributorAddresses": "contributor_addresses",
		"CreatedDateTs":        "creation_timestamp",
		"ApplicationDateTs":    "application_timestamp",
		"ApprovalDateTs":       "approval_timestamp",
		"LastUpdatedDateTs":    "last_updated_timestamp",
		"AppExpiry":            "app_expiry",
		"ChallengeID":          "challenge_id",
		"UnstakedDeposit":      "unstaked_deposit",
	}
	for modelName, dbName := range listingNameMapping {
		dbNameCheck, err := postgres.DbFieldNameFromModelName(postgres.Listing{}, modelName)
		if err != nil {
			t.Errorf("Error getting db struct name: %v", err)
		}
		if dbName != dbNameCheck {
			t.Errorf("Struct tag names do not match for: %v, %v", dbName, dbNameCheck)
		}
	}
}

func TestGetAllStructFieldsForQuery(t *testing.T) {
	listing := postgres.Listing{}
	structFieldsString, structFieldsString2 := postgres.StructFieldsForQuery(listing, false)
	if structFieldsString != "name, contract_address, whitelisted, last_governance_state, url, charter, owner_addresses, owner, contributor_addresses, creation_timestamp, application_timestamp, approval_timestamp, last_updated_timestamp, app_expiry, unstaked_deposit, challenge_id" {
		t.Errorf("Generated structFieldString is not what it should be.")
	}
	if structFieldsString2 != "" {
		t.Errorf("Structfield must be empty but it isn't")
	}
	structFieldsString3, structFieldsString4 := postgres.StructFieldsForQuery(listing, true)
	if structFieldsString3 != "name, contract_address, whitelisted, last_governance_state, url, charter, owner_addresses, owner, contributor_addresses, creation_timestamp, application_timestamp, approval_timestamp, last_updated_timestamp, app_expiry, unstaked_deposit, challenge_id" {
		t.Errorf("Generated structFieldString is not what it should be: %v", structFieldsString3)
	}
	if structFieldsString4 != ":name, :contract_address, :whitelisted, :last_governance_state, :url, :charter, :owner_addresses, :owner, :contributor_addresses, :creation_timestamp, :application_timestamp, :approval_timestamp, :last_updated_timestamp, :app_expiry, :unstaked_deposit, :challenge_id" {
		t.Errorf("Generated structFieldString with colon is not what it should be: %v", structFieldsString4)
	}
}

func TestBigIntToFloat64(t *testing.T) {
	floatVal := float64(3)
	bigInt := big.NewInt(3)
	floatNum := postgres.BigIntToFloat64(bigInt)
	if floatVal != floatNum {
		t.Errorf("Bigint to Float64 conversion failed")
	}
}

func TestFloat64ToBigInt(t *testing.T) {
	bigIntVal := big.NewInt(34)
	float := float64(34)
	bigInt := postgres.Float64ToBigInt(float)
	if bigInt == bigIntVal {
		t.Errorf("Float64 to Bigint conversion failed")
	}
}
