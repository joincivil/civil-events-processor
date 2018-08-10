package postgres // import "github.com/joincivil/civil-events-processor/pkg/persistence/postgres"

import (
	"fmt"
)

// CreateCronTableQuery returns the query to create the cron table
func CreateCronTableQuery() string {
	return CreateCronTableQueryString("cron")
}

// CreateCronTableQueryString returns the query to create this table
// NOTE: This table only is allowed to ever have 1 row, so insert a row with nil value
func CreateCronTableQueryString(tableName string) string {
	queryString := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s(timestamp BIGINT NOT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s((timestamp IS NOT NULL));
    `, tableName, tableName+"_one_row", tableName)
	return queryString
}

// CronData contains all the information related to cronjob that needs to be persisted in cron DB.
type CronData struct {
	Timestamp int64 `db:"timestamp"`
}

// NewCron creates a CronData model for DB from a timestamp to save
func NewCron(timestamp int64) *CronData {
	return &CronData{Timestamp: timestamp}
}
