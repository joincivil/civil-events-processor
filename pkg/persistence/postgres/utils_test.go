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
