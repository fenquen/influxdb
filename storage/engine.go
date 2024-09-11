package storage

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	_ "github.com/influxdata/influxdb/v2/tsdb/engine"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	_ "github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"github.com/influxdata/influxdb/v2/v1/coordinator"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/influxdata/influxdb/v2/v1/services/precreator"
	"github.com/influxdata/influxdb/v2/v1/services/retention"
	"github.com/influxdata/influxql"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

var (
	// ErrEngineClosed is returned when a caller attempts to use the engine while
	// it's closed.
	ErrEngineClosed = errors.New("engine is closed")

	// ErrNotImplemented is returned for APIs that are temporarily not implemented.
	ErrNotImplemented = errors.New("not implemented")
)

type Engine struct {
	config Config
	path   string

	mu           sync.RWMutex
	closing      chan struct{} // closing returns the zero value when the engine is shutting down.
	tsdbStore    *tsdb.Store
	metaClient   MetaClient
	pointsWriter interface {
		WritePoints(ctx context.Context, database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, user meta.User, points []models.Point) error
		Close() error
	}

	retentionService  *retention.Service
	precreatorService *precreator.Service

	writePointsValidationEnabled bool

	logger          *zap.Logger
	metricsDisabled bool
}

// Option provides a set
type Option func(*Engine)

func WithMetaClient(c MetaClient) Option {
	return func(e *Engine) {
		e.metaClient = c
	}
}

func WithMetricsDisabled(m bool) Option {
	return func(e *Engine) {
		e.metricsDisabled = m
	}
}

type MetaClient interface {
	CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error)
	DropDatabase(name string) error
	CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	Database(name string) (di *meta.DatabaseInfo)
	Databases() []meta.DatabaseInfo
	DeleteShardGroup(database, policy string, id uint64) error
	PrecreateShardGroups(now, cutoff time.Time) error
	PruneShardGroups() error
	RetentionPolicy(database, policy string) (*meta.RetentionPolicyInfo, error)
	ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error
	RLock()
	RUnlock()
	Backup(ctx context.Context, w io.Writer) error
	Restore(ctx context.Context, r io.Reader) error
	Data() meta.Data
	SetData(data *meta.Data) error
}

type TSDBStore interface {
	DeleteMeasurement(ctx context.Context, database, name string) error
	DeleteSeries(ctx context.Context, database string, sources []influxql.Source, condition influxql.Expr) error
	MeasurementNames(ctx context.Context, auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error)
	ShardGroup(ids []uint64) tsdb.ShardGroup
	Shards(ids []uint64) []*tsdb.Shard
	TagKeys(ctx context.Context, auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error)
	TagValues(ctx context.Context, auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error)
	SeriesCardinality(ctx context.Context, database string) (int64, error)
	SeriesCardinalityFromShards(ctx context.Context, shards []*tsdb.Shard) (*tsdb.SeriesIDSet, error)
	SeriesFile(database string) *tsdb.SeriesFile
}

// NewEngine initialises a new storage engine, including a series file, index and
// TSM engine.
func NewEngine(path string, c Config, options ...Option) *Engine {
	c.Data.Dir = filepath.Join(path, "data")
	c.Data.WALDir = filepath.Join(path, "wal")

	e := &Engine{
		config:    c,
		path:      path,
		tsdbStore: tsdb.NewStore(c.Data.Dir),
		logger:    zap.NewNop(),

		writePointsValidationEnabled: true,
	}

	for _, opt := range options {
		opt(e)
	}

	e.tsdbStore.EngineOptions.Config = c.Data

	// Copy TSDB configuration.
	e.tsdbStore.EngineOptions.EngineVersion = c.Data.Engine
	e.tsdbStore.EngineOptions.IndexVersion = c.Data.Index
	e.tsdbStore.EngineOptions.MetricsDisabled = e.metricsDisabled

	pointsWriter := coordinator.NewPointsWriter(c.WriteTimeout, path)
	pointsWriter.TSDBStore = e.tsdbStore
	pointsWriter.MetaClient = e.metaClient
	e.pointsWriter = pointsWriter

	e.retentionService = retention.NewService(c.RetentionService)
	e.retentionService.TSDBStore = e.tsdbStore
	e.retentionService.MetaClient = e.metaClient

	e.precreatorService = precreator.NewService(c.PrecreatorConfig)
	e.precreatorService.MetaClient = e.metaClient

	return e
}

// WithLogger sets the logger on the Store. It must be called before Open.
func (engine *Engine) WithLogger(log *zap.Logger) {
	engine.logger = log.With(zap.String("service", "storage-engine"))

	engine.tsdbStore.WithLogger(engine.logger)
	if pw, ok := engine.pointsWriter.(*coordinator.PointsWriter); ok {
		pw.WithLogger(engine.logger)
	}

	if engine.retentionService != nil {
		engine.retentionService.WithLogger(log)
	}

	if engine.precreatorService != nil {
		engine.precreatorService.WithLogger(log)
	}
}

// PrometheusCollectors returns all the prometheus collectors associated with
// the engine and its components.
func (engine *Engine) PrometheusCollectors() []prometheus.Collector {
	var metrics []prometheus.Collector
	metrics = append(metrics, tsm1.PrometheusCollectors()...)
	metrics = append(metrics, coordinator.PrometheusCollectors()...)
	metrics = append(metrics, tsdb.ShardCollectors()...)
	metrics = append(metrics, tsdb.BucketCollectors()...)
	metrics = append(metrics, retention.PrometheusCollectors()...)
	return metrics
}

// Open opens the store and all underlying resources. It returns an error if
// any of the underlying systems fail to open.
func (engine *Engine) Open(ctx context.Context) (err error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	if engine.closing != nil {
		return nil // Already open
	}

	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := engine.tsdbStore.Open(ctx); err != nil {
		return err
	}

	if err := engine.retentionService.Open(ctx); err != nil {
		return err
	}

	if err := engine.precreatorService.Open(ctx); err != nil {
		return err
	}

	engine.closing = make(chan struct{})

	return nil
}

// EnableCompactions allows the series file, index, & underlying engine to compact.
func (engine *Engine) EnableCompactions() {
}

// DisableCompactions disables compactions in the series file, index, & engine.
func (engine *Engine) DisableCompactions() {
}

// Close closes the store and all underlying resources. It returns an error if
// any of the underlying systems fail to close.
func (engine *Engine) Close() error {
	engine.mu.RLock()
	if engine.closing == nil {
		engine.mu.RUnlock()
		// Unusual if an engine is closed more than once, so note it.
		engine.logger.Info("Close() called on already-closed engine")
		return nil // Already closed
	}

	close(engine.closing)
	engine.mu.RUnlock()

	engine.mu.Lock()
	defer engine.mu.Unlock()
	engine.closing = nil

	var retErr error
	if err := engine.precreatorService.Close(); err != nil {
		retErr = multierr.Append(retErr, fmt.Errorf("error closing shard precreator service: %w", err))
	}

	if err := engine.retentionService.Close(); err != nil {
		retErr = multierr.Append(retErr, fmt.Errorf("error closing retention service: %w", err))
	}

	if err := engine.tsdbStore.Close(); err != nil {
		retErr = multierr.Append(retErr, fmt.Errorf("error closing TSDB store: %w", err))
	}

	if err := engine.pointsWriter.Close(); err != nil {
		retErr = multierr.Append(retErr, fmt.Errorf("error closing points writer: %w", err))
	}
	return retErr
}

// WritePoints writes the provided points to the engine.
//
// The Engine expects all points to have been correctly validated by the caller.
// However, WritePoints will determine if any tag key-pairs are missing, or if
// there are any field type conflicts.
// Rosalie was here lockdown 2020
//
// Appropriate errors are returned in those cases.
func (engine *Engine) WritePoints(ctx context.Context, orgID platform.ID, bucketID platform.ID, points []models.Point) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	//TODO - remember to add back unicode validation...

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if engine.closing == nil {
		return ErrEngineClosed
	}

	return engine.pointsWriter.WritePoints(ctx, bucketID.String(), meta.DefaultRetentionPolicyName, models.ConsistencyLevelAll, &meta.UserInfo{}, points)
}

func (engine *Engine) CreateBucket(ctx context.Context, b *influxdb.Bucket) (err error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	spec := meta.RetentionPolicySpec{
		Name:               meta.DefaultRetentionPolicyName,
		Duration:           &b.RetentionPeriod,
		ShardGroupDuration: b.ShardGroupDuration,
	}

	if _, err = engine.metaClient.CreateDatabaseWithRetentionPolicy(b.ID.String(), &spec); err != nil {
		return err
	}

	return nil
}

func (engine *Engine) UpdateBucketRetentionPolicy(ctx context.Context, bucketID platform.ID, upd *influxdb.BucketUpdate) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	rpu := meta.RetentionPolicyUpdate{
		Duration:           upd.RetentionPeriod,
		ShardGroupDuration: upd.ShardGroupDuration,
	}

	err := engine.metaClient.UpdateRetentionPolicy(bucketID.String(), meta.DefaultRetentionPolicyName, &rpu, true)
	if err == meta.ErrIncompatibleDurations {
		err = &errors2.Error{
			Code: errors2.EUnprocessableEntity,
			Msg:  "shard-group duration must also be updated to be smaller than new retention duration",
		}
	}
	return err
}

// DeleteBucket deletes an entire bucket from the storage engine.
func (engine *Engine) DeleteBucket(ctx context.Context, orgID, bucketID platform.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	err := engine.tsdbStore.DeleteDatabase(bucketID.String())
	if err != nil {
		return err
	}
	return engine.metaClient.DropDatabase(bucketID.String())
}

// DeleteBucketRangePredicate deletes data within a bucket from the storage engine. Any data
// deleted must be in [min, max], and the key must match the predicate if provided.
func (engine *Engine) DeleteBucketRangePredicate(ctx context.Context, orgID, bucketID platform.ID, min, max int64, pred influxdb.Predicate, measurement influxql.Expr) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	engine.mu.RLock()
	defer engine.mu.RUnlock()
	if engine.closing == nil {
		return ErrEngineClosed
	}
	return engine.tsdbStore.DeleteSeriesWithPredicate(ctx, bucketID.String(), min, max, pred, measurement)
}

// RLockKVStore locks the KV store as well as the engine in preparation for doing a backup.
func (engine *Engine) RLockKVStore() {
	engine.mu.RLock()
	engine.metaClient.RLock()
}

// RUnlockKVStore unlocks the KV store & engine, intended to be used after a backup is complete.
func (engine *Engine) RUnlockKVStore() {
	engine.mu.RUnlock()
	engine.metaClient.RUnlock()
}

func (engine *Engine) BackupKVStore(ctx context.Context, w io.Writer) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if engine.closing == nil {
		return ErrEngineClosed
	}

	return engine.metaClient.Backup(ctx, w)
}

func (engine *Engine) BackupShard(ctx context.Context, w io.Writer, shardID uint64, since time.Time) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if engine.closing == nil {
		return ErrEngineClosed
	}

	return engine.tsdbStore.BackupShard(shardID, since, w)
}

func (engine *Engine) RestoreKVStore(ctx context.Context, r io.Reader) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if engine.closing == nil {
		return ErrEngineClosed
	}

	// Replace KV store data and remove all existing shard data.
	if err := engine.metaClient.Restore(ctx, r); err != nil {
		return err
	} else if err := engine.tsdbStore.DeleteShards(); err != nil {
		return err
	}

	// Create new shards based on the restored KV data.
	data := engine.metaClient.Data()
	for _, dbi := range data.Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				if sgi.Deleted() {
					continue
				}

				for _, sh := range sgi.Shards {
					if err := engine.tsdbStore.CreateShard(ctx, dbi.Name, rpi.Name, sh.ID, true); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (engine *Engine) RestoreBucket(ctx context.Context, id platform.ID, buf []byte) (map[uint64]uint64, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if engine.closing == nil {
		return nil, ErrEngineClosed
	}

	var newDBI meta.DatabaseInfo
	if err := newDBI.UnmarshalBinary(buf); err != nil {
		return nil, err
	}

	data := engine.metaClient.Data()
	dbi := data.Database(id.String())
	if dbi == nil {
		return nil, fmt.Errorf("bucket dbi for %q not found during restore", newDBI.Name)
	} else if len(newDBI.RetentionPolicies) != 1 {
		return nil, fmt.Errorf("bucket must have 1 retention policy; attempting to restore %d retention policies", len(newDBI.RetentionPolicies))
	}

	dbi.RetentionPolicies = newDBI.RetentionPolicies
	dbi.ContinuousQueries = newDBI.ContinuousQueries

	// Generate shard ID mapping.
	shardIDMap := make(map[uint64]uint64)
	rpi := newDBI.RetentionPolicies[0]
	for j, sgi := range rpi.ShardGroups {
		data.MaxShardGroupID++
		rpi.ShardGroups[j].ID = data.MaxShardGroupID

		for k := range sgi.Shards {
			data.MaxShardID++
			shardIDMap[sgi.Shards[k].ID] = data.MaxShardID
			sgi.Shards[k].ID = data.MaxShardID
			sgi.Shards[k].Owners = []meta.ShardOwner{}
		}
	}

	// Update data.
	if err := engine.metaClient.SetData(&data); err != nil {
		return nil, err
	}

	// Create shards.
	for _, sgi := range rpi.ShardGroups {
		if sgi.Deleted() {
			continue
		}

		for _, sh := range sgi.Shards {
			if err := engine.tsdbStore.CreateShard(ctx, dbi.Name, rpi.Name, sh.ID, true); err != nil {
				return nil, err
			}
		}
	}

	return shardIDMap, nil
}

func (engine *Engine) RestoreShard(ctx context.Context, shardID uint64, r io.Reader) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if engine.closing == nil {
		return ErrEngineClosed
	}

	return engine.tsdbStore.RestoreShard(ctx, shardID, r)
}

// SeriesCardinality returns the number of series in the engine.
func (engine *Engine) SeriesCardinality(ctx context.Context, bucketID platform.ID) int64 {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	if engine.closing == nil {
		return 0
	}

	n, err := engine.tsdbStore.SeriesCardinality(ctx, bucketID.String())
	if err != nil {
		return 0
	}
	return n
}

// Path returns the path of the engine's base directory.
func (engine *Engine) Path() string {
	return engine.path
}

func (engine *Engine) TSDBStore() TSDBStore {
	return engine.tsdbStore
}

func (engine *Engine) MetaClient() MetaClient {
	return engine.metaClient
}
