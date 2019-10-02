package listsample

import (
	"errors"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc" // clustering client
	"github.com/sendgrid/mcauto/metrics"
	"github.com/sendgrid/mclogger/lib/logger"
)

const (
	listEntryPutMetricName = "list.sample.put.latency"
	listEntryGetMetricName = "list.sample.get.latency"
	maxRedisValue          = int64(9007199254740992) //see https://redis.io/commands/zadd#range-of-integer-scores-that-can-be-expressed-precisely for more detail. This is the max we must substract timestamps from in order to get "descending" order in the zset

	defaultMaxActiveConnections = 100
	defaultMinIdleConnections   = 50
	defaultIdleTimeout          = 1 * time.Minute
	defaultMaxSortedSetBuffer   = 100
)

//DAL the DAL for performing list sample IO
type DAL interface {
	//Put the userID listID and contactID
	Put(batch *PutBatch) error

	//Get the most recent contacts for the user.  Slice may contain less than the requested maxSize
	Get(userID, listID string, maxSize int) ([]string, error)
}

//PutBatch a struct used for creating batches for the PUT
type PutBatch struct {
	deletes []contactDeleteMutation
	updates []contactWriteMutation
}

//internal mutation struct.
type contactDeleteMutation struct {
	userID    string
	listID    string
	contactID string
}

//internal mutation struct.
type contactWriteMutation struct {
	contactDeleteMutation
	updatedAt time.Time
}

//ClusterOpts opts for the cluster connection.  use NewCusterOpts() to return options with sensible defaults
type ClusterOpts struct {
	MaxActiveConnections  int
	MinIdleConnections    int
	ConnectionIdleTimeout time.Duration
	BoostrapHost          string
}

type redisDAL struct {
	metricsLogger metrics.MetricLogger
	maxSetSize    int
	cluster       *redisc.Cluster
	clusterOpts   *ClusterOpts
}

//NewDAL create a new DAL with the configuratio and options
func NewDAL(options ...func(*redisDAL)) (DAL, error) {
	r := &redisDAL{}

	//apply all user options ane ensure the opts and host was specified
	for _, opt := range options {
		opt(r)
	}

	if r.clusterOpts == nil {
		return nil, errors.New("You must specify clusterOptions via WithClusterOptions")
	}

	if r.clusterOpts.BoostrapHost == "" {
		return nil, errors.New("You must specify the 'BoostrapHost' in the cluster options")
	}

	//set defaults if not overridden
	if r.metricsLogger == nil {
		r.metricsLogger = &metrics.StatsdMetrics{}
	}

	if r.maxSetSize == 0 {
		r.maxSetSize = defaultMaxSortedSetBuffer
	}

	//Create our pooled connection that will track connections to each host
	metricsNodePoolConnection := &metricsNodePoolConnection{
		metricsLogger: r.metricsLogger,
		maxIdle:       r.clusterOpts.MinIdleConnections,
		idleTimeout:   r.clusterOpts.ConnectionIdleTimeout,
		maxActive:     r.clusterOpts.MaxActiveConnections,
	}

	r.cluster = &redisc.Cluster{
		StartupNodes: []string{r.clusterOpts.BoostrapHost},
		DialOptions:  []redis.DialOption{redis.DialConnectTimeout(5 * time.Second)},
		CreatePool:   metricsNodePoolConnection.createPoolConnection,
	}

	logger.NewEntry().Info("Initializing Redis cluster state for shard -> node mapping")

	// initialize its mapping
	if err := r.cluster.Refresh(); err != nil {
		logger.NewEntry().SetError(err).Errorf("Refresh failed.  Unable to get cluster shard mapping:")
		return nil, err
	}

	return r, nil
}

// NewClusterOptions A factory to generate options with a sensible defaults
func NewClusterOptions() *ClusterOpts {
	return &ClusterOpts{
		MinIdleConnections:    defaultMinIdleConnections,
		ConnectionIdleTimeout: defaultIdleTimeout,
		MaxActiveConnections:  defaultMaxActiveConnections,
	}
}

// WithClusterOptions Set the redis cluster options
func WithClusterOptions(clusterOptions *ClusterOpts) func(*redisDAL) {
	return func(r *redisDAL) {
		r.clusterOpts = clusterOptions
	}
}

// WithMaxSortedBuffer set the max buffer size.  Default is 100
func WithMaxSortedBuffer(maxBufferSize int) func(*redisDAL) {
	return func(r *redisDAL) {
		r.maxSetSize = maxBufferSize
	}
}

// WithMetricsLogger Set the metrics logger
func WithMetricsLogger(metricsLogger metrics.MetricLogger) func(*redisDAL) {
	return func(r *redisDAL) {
		r.metricsLogger = metricsLogger
	}
}

// Put the userID listID and contactID
func (r *redisDAL) Put(batch *PutBatch) error {
	//get metrics
	start := time.Now()
	defer func() {
		r.metricsLogger.PutTiming(listEntryPutMetricName, start, time.Now())
	}()

	//get connection and close the connection
	conn := r.cluster.Get()
	defer conn.Close()

	//used to keep track of every key that we're written to trucate based on score later
	writtenKeys := map[string]bool{}

	//write all entries
	for _, write := range batch.updates {
		key := createKey(write.userID, write.listID)

		//calculateScore calculates a score by taking the max value redis can support and substracting the user's epoch time.
		//This is because we want newer entries to be highest timestamp first bu rank, and therefore closer to the root of the tree.
		//This allows ZREMRANGEBYRANK truncation to the cfg.MaxSize to operate without the need to invoke Count before truncation, which is O(log(N)) runtime for each key.
		//Thereby increasing write speed, and also removes the need for locking on trunctation
		insertScore := maxRedisValue - write.updatedAt.Unix()

		entry := logger.NewEntry().
			SetField("key", key).
			SetField("contactID", write.contactID).
			SetField("listID", write.listID).
			SetField("updatedAt", write.updatedAt).
			SetField("contactID", write.contactID).
			SetField("insertScore", insertScore)

		_, err := conn.Do("zadd", key, insertScore, write.contactID)

		if err != nil {
			entry.SetError(err).Error("Unable to write entry to Redis")
			return err
		}

		entry.Debug("Entry written to Redis")

		writtenKeys[key] = true
	}

	//write all deletes  Delete deliberately takes precendence in a "last write wins" scenario if both and add and delete are in the same batch
	for _, delete := range batch.deletes {
		key := createKey(delete.userID, delete.listID)

		entry := logger.NewEntry().
			SetField("key", key).
			SetField("contactID", delete.contactID).
			SetField("listID", delete.listID).
			SetField("contactID", delete.contactID)

		_, err := conn.Do("zrem", key, delete.contactID)

		if err != nil {
			entry.SetError(err).Error("Unable to remove entry from Redis")
			return err
		}

		entry.Debug("Entry deleted from Redis")

		writtenKeys[key] = true
	}

	//now truncate every written key to our max set size by rank
	for writtenKey := range writtenKeys {
		entry := logger.NewEntry().
			SetField("key", writtenKey).
			SetField("maxSize", r.maxSetSize)

		_, err := conn.Do("ZREMRANGEBYRANK", writtenKey, r.maxSetSize, -1)

		if err != nil {
			entry.SetError(err).Error("Unable to truncate entries to size")
			return err
		}

		entry.Debug("Entry truncated")
	}

	return nil
}

// Get the last N contacts for the user
func (r *redisDAL) Get(userID, listID string, maxSize int) ([]string, error) {
	//get metrics
	start := time.Now()
	defer func() {
		r.metricsLogger.PutTiming(listEntryGetMetricName, start, time.Now())
	}()

	//get connection and close the connection
	conn := r.cluster.Get()
	defer conn.Close()

	key := createKey(userID, listID)

	return redis.Strings(conn.Do("ZRANGE", key, 0, maxSize))
}

func createKey(userID, listID string) string {
	return fmt.Sprintf("%s_%s", userID, listID)
}

// metricsNodePoolConnection This is simply a holder for a metrics pointer to adhere to the createPoolConnection func signature below.
type metricsNodePoolConnection struct {
	// pointer to our metrics logger
	metricsLogger metrics.MetricLogger
	maxIdle       int
	idleTimeout   time.Duration
	maxActive     int
}

// createPoolConnection This function creates
func (m *metricsNodePoolConnection) createPoolConnection(host string, options ...redis.DialOption) (*redis.Pool, error) {
	logger.NewEntry().SetField("host", host).Infof("Creating a pool for address")

	pool := &redis.Pool{

		Dial: func() (redis.Conn, error) {
			logger.NewEntry().SetField("host", host).Infof("Connecting to Redis")
			c, err := redis.Dial("tcp", host, options...)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			logger.NewEntry().SetField("host", host).Infof("Pinging Redis")
			_, err := c.Do("PING")
			return err
		},
	}

	pool.MaxIdle = m.maxIdle
	pool.IdleTimeout = m.idleTimeout
	pool.MaxActive = m.maxActive
	pool.Wait = true

	go func(p *redis.Pool, host string) {
		// does not have a shutdown channel as it is expected to run for the life of the process
		updateTick := time.NewTicker(5 * time.Second)
		defer updateTick.Stop()

		for range updateTick.C {
			m.metricsLogger.PutCount(fmt.Sprintf("list.sample.redis.%s.active", host), int64(p.Stats().ActiveCount))
			m.metricsLogger.PutCount(fmt.Sprintf("list.sample.redis.%s.idle", host), int64(p.Stats().IdleCount))
		}
	}(pool, host)

	return pool, nil
}
