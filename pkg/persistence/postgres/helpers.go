package postgres

import (
	"fmt"
)

// CheckTableCount returns the query to check the count of the table
func CheckTableCount(tableName string) string {
	queryString := fmt.Sprintf(`SELECT COUNT(*) FROM %v`, tableName) // nolint: gosec
	return queryString
}
