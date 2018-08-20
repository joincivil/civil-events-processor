// Package utils contains various common utils separate by utility types
package utils

import (
	"time"
)

// SecsToTime converts an int64 of seconds from epoch to Time struct
func SecsToTime(ts int64) time.Time {
	return time.Unix(ts, 0)
}

// NanoSecsToTime converts an int64 of nanoseconds from epoch to Time struct
func NanoSecsToTime(ts int64) time.Time {
	return time.Unix(0, ts)
}
