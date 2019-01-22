package timeutil

import (
	"time"
)

// TimeMillis convert time.Time to int64 millisecond
func TimeMillis(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}
