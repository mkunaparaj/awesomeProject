package metrics

import (
	"time"
)

// MetricLogger sends metrics places
type MetricLogger interface {
	// PutTiming sends difference between end and start in milliseconds
	PutTiming(metric string, start time.Time, end time.Time)
	// PutTimingWithMetadata sends difference between end and start in milliseconds
	PutTimingWithMetadata(metric string, metadata map[string]string, start time.Time, end time.Time)
	// PutCount sends a counter
	PutCount(metric string, count int64)
	// PutGauge sends a value
	PutGauge(metric string, value float64)
}
