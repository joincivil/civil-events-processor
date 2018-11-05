package persistence_test

import (
	"testing"

	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/persistence"
)

func testListingPersister(p model.ListingPersister) {
}

func testContentRevisionPersister(p model.ContentRevisionPersister) {
}

func testGovernanceEventPersister(p model.GovernanceEventPersister) {
}

func testCronPersister(p model.CronPersister) {
}

func testPollPersister(p model.PollPersister) {
}

func testChallengePersister(p model.ChallengePersister) {
}

func testAppealPersister(p model.AppealPersister) {
}

func TestNullInterface(t *testing.T) {
	p := &persistence.NullPersister{}

	testListingPersister(p)
	testContentRevisionPersister(p)
	testGovernanceEventPersister(p)
	testCronPersister(p)
	testPollPersister(p)
	testChallengePersister(p)
	testAppealPersister(p)
}
