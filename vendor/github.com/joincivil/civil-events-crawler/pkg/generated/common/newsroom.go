// Code generated by 'gen/eventhandlergen.go'  DO NOT EDIT.
// IT SHOULD NOT BE EDITED BY HAND AS ANY CHANGES MAY BE OVERWRITTEN
// Please reference 'gen/filterergen_template.go' for more details
// File was generated at 2019-03-20 17:29:17.265647 +0000 UTC
package common

var eventTypesNewsroomContract = []string{
	"ContentPublished",
	"NameChanged",
	"OwnershipRenounced",
	"OwnershipTransferred",
	"RevisionSigned",
	"RevisionUpdated",
	"RoleAdded",
	"RoleRemoved",
}

func EventTypesNewsroomContract() []string {
	tmp := make([]string, len(eventTypesNewsroomContract))
	copy(tmp, eventTypesNewsroomContract)
	return tmp
}
