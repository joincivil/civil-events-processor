// Package persistence contains components to interact with the DB
package persistence // import "github.com/joincivil/civil-events-processor/pkg/persistence"

// CronPersister stores information about the cron job
type CronPersister struct {
	lastTimestamp int64
}

// NewCronPersister creates a cron persister
func NewCronPersister() *CronPersister {
	return &CronPersister{}
}

// TimestampOfLastEvent returns the timestamp of the last event seen
func (cp *CronPersister) TimestampOfLastEvent() int64 {
	return cp.lastTimestamp
}

// SaveTimestamp saves the timestamp to cron persistence
func (cp *CronPersister) SaveTimestamp(timestamp int64) {
	cp.lastTimestamp = timestamp
}
