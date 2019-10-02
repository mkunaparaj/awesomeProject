package metrics

import (
	"encoding/json"
	"fmt"
	"time"
)

// These stats functions are loosely derived from here:
// https://github.com/statsd/statsd/blob/master/docs/metric_types.md

// StatsdMetrics implements the mcauto metrics Interface
// https://github.com/sendgrid/mcauto/blob/master/metrics/metrics.go
type StatsdMetrics struct{}

// compile-time check to make sure statsd implements interface
var _ MetricLogger = (*StatsdMetrics)(nil)

// PutTiming records timing metrics
func (l *StatsdMetrics) PutTiming(metric string, start time.Time, end time.Time) {
	timing(metric, end.Sub(start))
}

// from go 1.13: https://github.com/golang/go/pull/30819/files
func milliseconds(duration time.Duration) int64 {
	return int64(duration) / 1e6
}

// PutTimingWithMetadata records timing metrics and extra data
func (l *StatsdMetrics) PutTimingWithMetadata(metric string, dimensions map[string]string, start time.Time, end time.Time) {
	metadata := make(map[string]interface{}, 2)
	metadata["metric"] = metric
	metadata["time"] = milliseconds(end.Sub(start))
	for key, value := range dimensions {
		metadata[key] = value
	}
	put(metadata)
}

// PutCount records counters
func (l *StatsdMetrics) PutCount(metric string, value int64) {
	counter(metric, value)
}

// PutGauge emits a log entry that can be used for implementing a gauge
// {"metric": "${name}", "gauge": 25}
// "stats min(`gauge`) by `metric`, bin(600s)"
func (l *StatsdMetrics) PutGauge(metricName string, gauge float64) {
	metadata := make(map[string]interface{}, 2)
	metadata["metric"] = metricName
	metadata["gauge"] = gauge
	put(metadata)
}

// put generates the output
func put(met map[string]interface{}) {
	d, err := json.Marshal(met)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
	} else {
		fmt.Printf("%s\n", string(d))
	}
}

// counter emits a log entry that can be used for implementing a counter
// {"metric": "${name}", "incr": 1}
// visualize with this Cloudwatch Logs Insights query:
// "stats sum(`incr`) by `metric`, bin(600s)"
func counter(metricName string, value int64) {
	metadata := make(map[string]interface{}, 2)
	metadata["metric"] = metricName
	metadata["incr"] = value
	put(metadata)
}

// timing emits a log entry that can be used for implementing timing in
// milliseconds
// {"metric": "${name}", "time": "148s"}
// "stats max(`time`) by `metric`, bin(600s)"
// "stats pct(`time`, 95) by `metric`, bin(600s)"
func timing(metricName string, interval time.Duration) {
	metadata := make(map[string]interface{}, 2)
	metadata["metric"] = metricName
	metadata["time"] = milliseconds(interval)
	put(metadata)
}
