package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	influxdb "github.com/influxdata/influxdb/v2/v1"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	// ErrTimeout is returned when a write times out.
	ErrTimeout = errors.New("timeout")

	// ErrWriteFailed is returned when no writes succeeded.
	ErrWriteFailed = errors.New("write failed")
)

// handle writes across multiple local and remote data nodes.
type PointsWriter struct {
	mu           sync.RWMutex
	closing      chan struct{}
	WriteTimeout time.Duration // 对应 storage-write-timeout
	Logger       *zap.Logger

	Node *influxdb.Node

	MetaClient interface {
		Database(name string) (di *meta.DatabaseInfo)
		RetentionPolicy(database, policy string) (*meta.RetentionPolicyInfo, error)
		CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	}

	TSDBStore interface { // tsdb.Store 是global的
		CreateShard(ctx context.Context, database, retentionPolicy string, shardID uint64, enabled bool) error
		WriteToShard(ctx context.Context, shardID uint64, points []models.Point) error
	}

	stats *engineWriteMetrics
}

// WritePointsRequest represents a request to write point data to the cluster.
type WritePointsRequest struct {
	BucketIdStr         string
	RetentionPolicyName string
	Points              []models.Point
}

// AddPoint adds a point to the WritePointRequest with field key 'value'
func (w *WritePointsRequest) AddPoint(name string, value interface{}, timestamp time.Time, tags map[string]string) {
	pt, err := models.NewPoint(
		name, models.NewTags(tags), map[string]interface{}{"value": value}, timestamp,
	)
	if err != nil {
		return
	}
	w.Points = append(w.Points, pt)
}

// returns a new instance of PointsWriter for a node.
func NewPointsWriter(writeTimeout time.Duration, path string) *PointsWriter {
	return &PointsWriter{
		closing:      make(chan struct{}),
		WriteTimeout: writeTimeout,
		Logger:       zap.NewNop(),
		stats:        newEngineWriteMetrics(path),
	}
}

// contains a mapping of shards to points.
type ShardMapping struct {
	pointCount        int
	ShardId2Points    map[uint64][]models.Point  // The points associated with a shard ID
	ShardId2ShardInfo map[uint64]*meta.ShardInfo // The shards that have been mapped, keyed by shard ID
	Dropped           []models.Point             // Points that were dropped
}

// create an empty ShardMapping.
func NewShardMapping(pointCount int) *ShardMapping {
	return &ShardMapping{
		pointCount:        pointCount,
		ShardId2Points:    map[uint64][]models.Point{},
		ShardId2ShardInfo: map[uint64]*meta.ShardInfo{},
	}
}

// add the point to the ShardMapping, associated with the given shardInfo.
func (s *ShardMapping) MapPoint(shardInfo *meta.ShardInfo, point models.Point) {
	if cap(s.ShardId2Points[shardInfo.ID]) < s.pointCount {
		s.ShardId2Points[shardInfo.ID] = make([]models.Point, 0, s.pointCount)
	}
	s.ShardId2Points[shardInfo.ID] = append(s.ShardId2Points[shardInfo.ID], point)
	s.ShardId2ShardInfo[shardInfo.ID] = shardInfo
}

// Open opens the communication channel with the point writer.
func (pointsWriter *PointsWriter) Open() error {
	pointsWriter.mu.Lock()
	defer pointsWriter.mu.Unlock()
	pointsWriter.closing = make(chan struct{})
	return nil
}

// Close closes the communication channel with the point writer.
func (pointsWriter *PointsWriter) Close() error {
	pointsWriter.mu.Lock()
	defer pointsWriter.mu.Unlock()
	if pointsWriter.closing != nil {
		close(pointsWriter.closing)
	}
	return nil
}

// WithLogger sets the Logger on w.
func (pointsWriter *PointsWriter) WithLogger(log *zap.Logger) {
	pointsWriter.Logger = log.With(zap.String("service", "write"))
}

var globalPointsWriteMetrics *writeMetrics = newWriteMetrics()

type writeMetrics struct {
	// labels: type: requested,ok,dropped,err
	pointsWriteRequested *prometheus.HistogramVec
	pointsWriteOk        *prometheus.HistogramVec
	pointsWriteDropped   *prometheus.HistogramVec
	pointsWriteErr       *prometheus.HistogramVec
	timeout              *prometheus.CounterVec
}

// PrometheusCollectors returns all prometheus metrics for the tsm1 package.
func PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		globalPointsWriteMetrics.pointsWriteRequested,
		globalPointsWriteMetrics.pointsWriteOk,
		globalPointsWriteMetrics.pointsWriteDropped,
		globalPointsWriteMetrics.pointsWriteErr,
		globalPointsWriteMetrics.timeout,
	}
}

const namespace = "storage"
const writerSubsystem = "writer"

func newWriteMetrics() *writeMetrics {
	labels := []string{"path"}
	writeBuckets := []float64{10, 100, 1000, 10000, 100000}
	return &writeMetrics{
		pointsWriteRequested: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: writerSubsystem,
			Name:      "req_points",
			Help:      "Histogram of number of points requested to be written",
			Buckets:   writeBuckets,
		}, labels),
		pointsWriteOk: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: writerSubsystem,
			Name:      "ok_points",
			Help:      "Histogram of number of points in successful shard write requests",
			Buckets:   writeBuckets,
		}, labels),
		pointsWriteDropped: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: writerSubsystem,
			Name:      "dropped_points",
			Help:      "Histogram of number of points dropped due to partial writes",
			Buckets:   writeBuckets,
		}, labels),
		pointsWriteErr: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: writerSubsystem,
			Name:      "err_points",
			Help:      "Histogram of number of points in errored shard write requests",
			Buckets:   writeBuckets,
		}, labels),
		timeout: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   writerSubsystem,
			Name:        "timeouts",
			Help:        "Number of shard write request timeouts",
			ConstLabels: nil,
		}, labels),
	}
}

type engineWriteMetrics struct {
	pointsWriteRequested prometheus.Observer
	pointsWriteOk        prometheus.Observer
	pointsWriteDropped   prometheus.Observer
	pointsWriteErr       prometheus.Observer
	timeout              prometheus.Counter
}

func newEngineWriteMetrics(path string) *engineWriteMetrics {
	return &engineWriteMetrics{
		pointsWriteRequested: globalPointsWriteMetrics.pointsWriteRequested.With(prometheus.Labels{"path": path}),
		pointsWriteOk:        globalPointsWriteMetrics.pointsWriteOk.With(prometheus.Labels{"path": path}),
		pointsWriteDropped:   globalPointsWriteMetrics.pointsWriteDropped.With(prometheus.Labels{"path": path}),
		pointsWriteErr:       globalPointsWriteMetrics.pointsWriteErr.With(prometheus.Labels{"path": path}),
		timeout:              globalPointsWriteMetrics.timeout.With(prometheus.Labels{"path": path}),
	}
}

// maps the points contained in wp to a ShardMapping.  If a point
// maps to a shard group or shard that does not currently exist, it will be
// created before returning the mapping. 点位会落地到多个shardGroup的shard上
func (pointsWriter *PointsWriter) Map2Shards(writePointsRequest *WritePointsRequest) (*ShardMapping, error) {
	retentionPolicyInfo, err := pointsWriter.MetaClient.RetentionPolicy(writePointsRequest.BucketIdStr, writePointsRequest.RetentionPolicyName)
	if err != nil {
		return nil, err
	} else if retentionPolicyInfo == nil {
		return nil, influxdb.ErrRetentionPolicyNotFound(writePointsRequest.RetentionPolicyName)
	}

	// 这些的要写入的point对应的多个shardGroupInfo
	shardGroupInfoList := sgList{shardGroupInfos: make(meta.ShardGroupInfos, 0, 8)}
	min := time.Unix(0, models.MinNanoTime)
	if retentionPolicyInfo.Duration > 0 {
		min = time.Now().Add(-retentionPolicyInfo.Duration)
	}

	for _, point := range writePointsRequest.Points { // 创建shardGroup
		// Either the point is outside the scope of the RP, or we already have
		// a suitable shard group for the point.
		if point.Time().Before(min) || shardGroupInfoList.Covers(point.Time()) {
			continue
		}

		// No shard groups overlap with the point's time, so we will create
		// a new shard group for this point.
		shardGroupInfo, err := pointsWriter.MetaClient.CreateShardGroup(writePointsRequest.BucketIdStr, writePointsRequest.RetentionPolicyName, point.Time())
		if err != nil {
			return nil, err
		}

		if shardGroupInfo == nil {
			return nil, errors.New("nil shard group")
		}
		shardGroupInfoList.Add(*shardGroupInfo)
	}

	shardMapping := NewShardMapping(len(writePointsRequest.Points))
	for _, point := range writePointsRequest.Points {
		shardGroupInfo := shardGroupInfoList.ShardGroupAt(point.Time()) // 定位到shardGroupInfo使用的是点位的time
		if shardGroupInfo == nil {
			// We didn't create a shard group because the point was outside the
			// scope of the RP.
			shardMapping.Dropped = append(shardMapping.Dropped, point)
			continue
		}

		shardInfo := shardGroupInfo.ShardFor(point) // 定位到shard使用的是点位的key(其实是measurement加上tags)
		shardMapping.MapPoint(&shardInfo, point)
	}

	return shardMapping, nil
}

// a wrapper around a meta.ShardGroupInfos where we can also check
// if a given time is covered by any of the shard groups in the list.
type sgList struct {
	shardGroupInfos meta.ShardGroupInfos

	// needsSort indicates if items has been modified without a sort operation.
	needsSort bool

	// earliest is the last begin time of any item in items.
	earliest time.Time

	// latest is the greatest end time of any item in items.
	latest time.Time
}

func (l sgList) Covers(t time.Time) bool {
	if len(l.shardGroupInfos) == 0 {
		return false
	}
	return l.ShardGroupAt(t) != nil
}

// ShardGroupAt attempts to find a shard group that could contain a point
// at the given time.
//
// Shard groups are sorted first according to end time, and then according
// to start time. Therefore, if there are multiple shard groups that match
// this point's time they will be preferred in this order:
//
//   - a shard group with the earliest end time;
//   - (assuming identical end times) the shard group with the earliest start time.
func (l sgList) ShardGroupAt(t time.Time) *meta.ShardGroupInfo {
	if l.shardGroupInfos.Len() == 0 {
		return nil
	}

	// find the earliest shardgroup that could contain this point using binary search.
	if l.needsSort {
		sort.Sort(l.shardGroupInfos)
		l.needsSort = false
	}
	idx := sort.Search(l.shardGroupInfos.Len(), func(i int) bool { return l.shardGroupInfos[i].EndTime.After(t) })

	// Check if sort.Search actually found the proper shard. It feels like we should also
	// be checking l.items[idx].EndTime, but sort.Search was looking at that field for us.
	if idx == l.shardGroupInfos.Len() || t.Before(l.shardGroupInfos[idx].StartTime) {
		// This could mean we are looking for a time not in the list, or we have
		// overlaping shards. Overlapping shards do not work with binary searches
		// on 1d arrays. You have to use an interval tree, but that's a lot of
		// work for what is hopefully a rare event. Instead, we'll check if t
		// should be in l, and perform a linear search if it is. This way we'll
		// do the correct thing, it may just take a little longer. If we don't
		// do this, then we may non-silently drop writes we should have accepted.

		if t.Before(l.earliest) || t.After(l.latest) {
			// t is not in range, we can avoid going through the linear search.
			return nil
		}

		// Oh no, we've probably got overlapping shards. Perform a linear search.
		for idx = 0; idx < l.shardGroupInfos.Len(); idx++ {
			if l.shardGroupInfos[idx].Contains(t) {
				// Found it!
				break
			}
		}
		if idx == l.shardGroupInfos.Len() {
			// We did not find a shard which contained t. This is very strange.
			return nil
		}
	}

	return &l.shardGroupInfos[idx]
}

// Add appends a shard group to the list, updating the earliest/latest times of the list if needed.
func (l *sgList) Add(sgi meta.ShardGroupInfo) {
	l.shardGroupInfos = append(l.shardGroupInfos, sgi)
	l.needsSort = true

	// Update our earliest and latest times for l.items
	if l.earliest.IsZero() || l.earliest.After(sgi.StartTime) {
		l.earliest = sgi.StartTime
	}
	if l.latest.IsZero() || l.latest.Before(sgi.EndTime) {
		l.latest = sgi.EndTime
	}
}

// writes the data to the underlying storage. consistencyLevel and user are only used for clustered scenarios
func (pointsWriter *PointsWriter) WritePoints(
	ctx context.Context,
	bucketIdStr, retentionPolicy string,
	consistencyLevel models.ConsistencyLevel,
	user meta.User,
	points []models.Point,
) error {
	return pointsWriter.WritePointsPrivileged(ctx, bucketIdStr, retentionPolicy, consistencyLevel, points)
}

// write the data to the underlying storage, consistencyLevel is only used for clustered scenarios
func (pointsWriter *PointsWriter) WritePointsPrivileged(
	ctx context.Context,
	bucketIdStr, retentionPolicy string,
	consistencyLevel models.ConsistencyLevel, // 单机是无用
	points []models.Point,
) error {
	pointsWriter.stats.pointsWriteRequested.Observe(float64(len(points)))

	if retentionPolicy == "" {
		db := pointsWriter.MetaClient.Database(bucketIdStr)
		if db == nil {
			return influxdb.ErrDatabaseNotFound(bucketIdStr)
		}
		retentionPolicy = db.DefaultRetentionPolicy
	}
	// 点位落地到多个shardGroup的多个shard上
	shardMapping, err := pointsWriter.Map2Shards(&WritePointsRequest{BucketIdStr: bucketIdStr, RetentionPolicyName: retentionPolicy, Points: points})
	if err != nil {
		return err
	}

	// Write each shard in it's own goroutine and return as soon as one fails.
	errorChan := make(chan error, len(shardMapping.ShardId2Points))
	for shardId, points := range shardMapping.ShardId2Points {
		go func(shardInfo *meta.ShardInfo, bucketIdStr, retentionPolicy string, points []models.Point) {
			err := pointsWriter.writeToShard(ctx, shardInfo, bucketIdStr, retentionPolicy, points)
			if err == nil {
				pointsWriter.stats.pointsWriteOk.Observe(float64(len(points)))
			} else {
				pointsWriter.stats.pointsWriteErr.Observe(float64(len(points)))
			}
			if err == tsdb.ErrShardDeletion {
				err = tsdb.PartialWriteError{Reason: fmt.Sprintf("shard %d is pending deletion", shardInfo.ID), Dropped: len(points)}
			}
			errorChan <- err
		}(shardMapping.ShardId2ShardInfo[shardId], bucketIdStr, retentionPolicy, points)
	}

	if len(shardMapping.Dropped) > 0 {
		pointsWriter.stats.pointsWriteDropped.Observe(float64(len(shardMapping.Dropped)))
		err = tsdb.PartialWriteError{Reason: "points beyond retention policy", Dropped: len(shardMapping.Dropped)}
	}
	timeout := time.NewTimer(pointsWriter.WriteTimeout)
	defer timeout.Stop()
	for range shardMapping.ShardId2Points {
		select {
		case <-pointsWriter.closing:
			return ErrWriteFailed
		case <-timeout.C:
			pointsWriter.stats.timeout.Inc()
			// return timeout error to caller
			return ErrTimeout
		case err := <-errorChan:
			if err != nil {
				return err
			}
		}
	}
	return err
}

// write points to a shard
func (pointsWriter *PointsWriter) writeToShard(ctx context.Context, shardInfo *meta.ShardInfo, database, retentionPolicy string, points []models.Point) error {
	err := pointsWriter.TSDBStore.WriteToShard(ctx, shardInfo.ID, points)
	if err == nil {
		return nil
	}

	// Except tsdb.ErrShardNotFound no error can be handled here
	if err != tsdb.ErrShardNotFound {
		return err
	}

	// If we've written to shard that should exist on the current node, but the store has
	// not actually created this shard, tell it to create it and retry the write
	if err = pointsWriter.TSDBStore.CreateShard(ctx, database, retentionPolicy, shardInfo.ID, true); err != nil {
		pointsWriter.Logger.Warn("Write failed creating shard", zap.Uint64("shard", shardInfo.ID), zap.Error(err))
		return err
	}

	if err = pointsWriter.TSDBStore.WriteToShard(ctx, shardInfo.ID, points); err != nil {
		pointsWriter.Logger.Info("Write failed", zap.Uint64("shard", shardInfo.ID), zap.Error(err))
		return err
	}

	return nil
}
