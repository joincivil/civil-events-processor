// Code generated by 'gen/eventhandlergen.go'  DO NOT EDIT.
// IT SHOULD NOT BE EDITED BY HAND AS ANY CHANGES MAY BE OVERWRITTEN
// Please reference 'gen/filterergen_template.go' for more details
// File was generated at 2018-10-31 19:32:13.575845609 +0000 UTC
package common

var eventTypesCivilPLCRVotingContract = []string{
	"PollCreated",
	"TokensRescued",
	"VoteCommitted",
	"VoteRevealed",
	"VotingRightsGranted",
	"VotingRightsWithdrawn",
}

func EventTypesCivilPLCRVotingContract() []string {
	tmp := make([]string, len(eventTypesCivilPLCRVotingContract))
	copy(tmp, eventTypesCivilPLCRVotingContract)
	return tmp
}