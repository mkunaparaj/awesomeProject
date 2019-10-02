package metrics

// Deprecated: This should NOT be used, use StatsdMetrics
//
// This implementation is frozen and no new functionality will be added.
import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	log "github.com/sirupsen/logrus"
)

const (
	defaultFlushDuration = time.Second * 5
	// These are aws limits, do not increase
	maxValuesPerMetric = 150
	maxMetricsPerBatch = 20
)

// compile-time check to make sure aws implements interface
var _ MetricLogger = (*deprecatedAWSMetricLogger)(nil)

type deprecatedAWSMetricLogger struct {
	client        cloudwatchiface.CloudWatchAPI
	namespace     string
	dimensions    []*cloudwatch.Dimension
	queueItems    []*cloudwatch.MetricDatum
	flushDuration time.Duration
	locker        *sync.Mutex
}

// NewAWSMetricLogger creates an instance
func NewAWSMetricLogger(configuration AWSConfig, options ...func(*deprecatedAWSMetricLogger)) MetricLogger {
	log.SetLevel(configuration.LogLevel)

	m := &deprecatedAWSMetricLogger{
		namespace:     configuration.Namespace,
		locker:        &sync.Mutex{},
		flushDuration: defaultFlushDuration,
		queueItems:    make([]*cloudwatch.MetricDatum, 0),
	}

	for key, value := range configuration.Dimensions {
		m.dimensions = append(m.dimensions, &cloudwatch.Dimension{
			Name:  aws.String(key),
			Value: aws.String(value),
		})
	}

	options = append([]func(*deprecatedAWSMetricLogger){ApplyClient(DefaultClient(configuration.Region))}, options...)

	for _, applyOptionTo := range options {
		applyOptionTo(m)
	}

	go m.flusher()

	return m
}

// DefaultClient is the AWS implementation of cloudwatchiface.CloudWatchAPI
func DefaultClient(region string) cloudwatchiface.CloudWatchAPI {
	return cloudwatch.New(session.New(&aws.Config{
		Region: aws.String(region),
	}))
}

// ApplyClient is an option for NewMetriccLogger for dependency injection
func ApplyClient(client cloudwatchiface.CloudWatchAPI) func(*deprecatedAWSMetricLogger) {
	return func(m *deprecatedAWSMetricLogger) {
		m.client = client
	}
}

// SetFlushDuration is an option to set the batch window for metrics
func SetFlushDuration(d time.Duration) func(*deprecatedAWSMetricLogger) {
	return func(m *deprecatedAWSMetricLogger) {
		m.flushDuration = d
	}
}

// PutGauge is a noop
func (m *deprecatedAWSMetricLogger) PutGauge(metricName string, gauge float64) {
	log.Println("PutGauge() is not implemented by deprecatedAWSMetricLogger")
}

// PutTiming sends timing metric difference in milliseconds
func (m *deprecatedAWSMetricLogger) PutTiming(name string, start time.Time, end time.Time) {
	m.PutTimingWithMetadata(name, map[string]string{}, start, end)
}

// PutTimingWithMetadata adds cloudwatch metadata to timing
func (m *deprecatedAWSMetricLogger) PutTimingWithMetadata(name string, dimensions map[string]string, start time.Time, end time.Time) {
	var metadata []*cloudwatch.Dimension
	for key, value := range dimensions {
		metadata = append(metadata, &cloudwatch.Dimension{
			Name:  aws.String(key),
			Value: aws.String(value),
		})
	}

	datum := &cloudwatch.MetricDatum{
		MetricName: aws.String(name),
		Unit:       aws.String(cloudwatch.StandardUnitMilliseconds),
		Values:     []*float64{aws.Float64(float64(end.Sub(start) / time.Millisecond))},
		Dimensions: append(m.dimensions, metadata...),
	}

	m.queue(datum)
}

// PutCount sends count metric (per minute, the default collection window)
func (m *deprecatedAWSMetricLogger) PutCount(name string, value int64) {
	datum := &cloudwatch.MetricDatum{
		MetricName: aws.String(name),
		Unit:       aws.String(cloudwatch.StandardUnitCount),
		Values:     []*float64{aws.Float64(float64(value))},
		Dimensions: m.dimensions,
	}
	m.queue(datum)
}

func (m *deprecatedAWSMetricLogger) flusher() {
	ticker := time.NewTicker(m.flushDuration)
	for range ticker.C {
		m.locker.Lock()
		if len(m.queueItems) > 0 {
			batch := m.queueItems
			m.queueItems = make([]*cloudwatch.MetricDatum, 0)
			m.locker.Unlock()
			m.handleBatch(batch)
		} else {
			m.locker.Unlock()
		}
	}
}

func (m *deprecatedAWSMetricLogger) handleBatch(batch []*cloudwatch.MetricDatum) {
	metrics := make(map[string]*cloudwatch.MetricDatum)
	for _, newDatum := range batch {
		if oldDatum, ok := metrics[*newDatum.MetricName]; ok {
			if len(oldDatum.Values) < maxValuesPerMetric {
				oldDatum.Values = append(oldDatum.Values, newDatum.Values...)
			} else {
				//send
				m.put(metrics)
				metrics = make(map[string]*cloudwatch.MetricDatum)
				metrics[*newDatum.MetricName] = newDatum
			}
		} else if len(metrics) < maxMetricsPerBatch {
			metrics[*newDatum.MetricName] = newDatum
		} else {
			//send
			m.put(metrics)
			metrics = make(map[string]*cloudwatch.MetricDatum)
			metrics[*newDatum.MetricName] = newDatum
		}
	}
	if len(metrics) > 0 {
		m.put(metrics)
	}
}

func (m *deprecatedAWSMetricLogger) queue(input *cloudwatch.MetricDatum) {
	m.locker.Lock()
	m.queueItems = append(m.queueItems, input)
	m.locker.Unlock()
}

func (m *deprecatedAWSMetricLogger) put(input map[string]*cloudwatch.MetricDatum) {
	// TODO: verify we want to log and not return errors on metric "misses"
	metricData := make([]*cloudwatch.MetricDatum, 0)
	for _, v := range input {
		metricData = append(metricData, v)
	}

	data := &cloudwatch.PutMetricDataInput{
		Namespace:  aws.String(m.namespace),
		MetricData: metricData,
	}
	_, err := m.client.PutMetricData(data)
	if err != nil {
		log.Errorf("error when publishing custom metric: %s", err)
	}

}
