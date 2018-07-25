package postgres_test

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/joincivil/civil-events-processor/pkg/persistence/postgres"
	"reflect"
	"testing"
)

var (
	// addressesString    = []string{"0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d", "0xDFe273082089bB7f70Ee36Eebcde64832FE97E55"}
	addressesCommon    = []common.Address{common.HexToAddress("0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d"), common.HexToAddress("0xDFe273082089bB7f70Ee36Eebcde64832FE97E55")}
	addressesOneString = "0x77e5aaBddb760FBa989A1C4B2CDd4aA8Fa3d311d,0xDFe273082089bB7f70Ee36Eebcde64832FE97E55"
)

func TestListCommonAddressToListString(t *testing.T) {

}

func TestListStringToListCommonAddress(t *testing.T) {
}

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
		"Name":                "name",
		"ContractAddress":     "contract_address",
		"Whitelisted":         "whitelisted",
		"LastGovernanceState": "last_governance_state",
		"URL":                  "url",
		"CharterURI":           "charter_uri",
		"OwnerAddresses":       "owner_addresses",
		"ContributorAddresses": "contributor_addresses",
		"CreatedDateTs":        "creation_timestamp",
		"ApplicationDateTs":    "application_timestamp",
		"ApprovalDateTs":       "approval_timestamp",
		"LastUpdatedDateTs":    "last_updated_timestamp",
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
	structFieldsString, structFieldsString2 := postgres.GetAllStructFieldsForQuery(listing, false)
	if structFieldsString != "name, contract_address, whitelisted, last_governance_state, url, charter_uri, owner_addresses, contributor_addresses, creation_timestamp, application_timestamp, approval_timestamp, last_updated_timestamp" {
		t.Errorf("Generated structFieldString is not what it should be.")
	}
	if structFieldsString2 != "" {
		t.Errorf("Structfield must be empty but it isn't")
	}
	structFieldsString3, structFieldsString4 := postgres.GetAllStructFieldsForQuery(listing, true)
	if structFieldsString3 != "name, contract_address, whitelisted, last_governance_state, url, charter_uri, owner_addresses, contributor_addresses, creation_timestamp, application_timestamp, approval_timestamp, last_updated_timestamp" {
		t.Errorf("Generated structFieldString is not what it should be: %v", structFieldsString3)
	}
	if structFieldsString4 != ":name, :contract_address, :whitelisted, :last_governance_state, :url, :charter_uri, :owner_addresses, :contributor_addresses, :creation_timestamp, :application_timestamp, :approval_timestamp, :last_updated_timestamp" {
		t.Errorf("Generated structFieldString with colon is not what it should be: %v", structFieldsString4)
	}
}
