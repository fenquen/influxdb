// Package tsm1 provides a TSDB in the Time Structured Merge tree format.
package tsm1 // import "github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/bytesutil"
	"github.com/influxdata/influxdb/v2/pkg/estimator"
	"github.com/influxdata/influxdb/v2/pkg/file"
	"github.com/influxdata/influxdb/v2/pkg/limiter"
	"github.com/influxdata/influxdb/v2/pkg/metrics"
	"github.com/influxdata/influxdb/v2/pkg/radix"
	intar "github.com/influxdata/influxdb/v2/pkg/tar"
	"github.com/influxdata/influxdb/v2/pkg/tracing"
	"github.com/influxdata/influxdb/v2/tsdb"
	_ "github.com/influxdata/influxdb/v2/tsdb/index"
	"github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

//go:generate -command tmpl go run github.com/benbjohnson/tmpl
//go:generate tmpl -data=@iterator.gen.go.tmpldata iterator.gen.go.tmpl engine.gen.go.tmpl array_cursor.gen.go.tmpl array_cursor_iterator.gen.go.tmpl
// The file store generate uses a custom modified tmpl
// to support adding templated data from the command line.
// This can probably be worked into the upstream tmpl
// but isn't at the moment.
//go:generate go run ../../../tools/tmpl -data=file_store.gen.go.tmpldata file_store.gen.go.tmpl=file_store.gen.go
//go:generate go run ../../../tools/tmpl -d isArray=y -data=file_store.gen.go.tmpldata file_store.gen.go.tmpl=file_store_array.gen.go
//go:generate tmpl -data=@encoding.gen.go.tmpldata encoding.gen.go.tmpl
//go:generate tmpl -data=@compact.gen.go.tmpldata compact.gen.go.tmpl
//go:generate tmpl -data=@reader.gen.go.tmpldata reader.gen.go.tmpl

func init() {
	tsdb.RegisterEngine("tsm1", NewEngine)
}

var (
	// Ensure Engine implements the interface.
	_ tsdb.Engine = &Engine{}
	// Static objects to prevent small allocs.
	timeBytes              = []byte("time")
	keyFieldSeparatorBytes = []byte(keyFieldSeparator)
	emptyBytes             = []byte{}
)

var (
	tsmGroup                   = metrics.MustRegisterGroup("tsm1")
	numberOfRefCursorsCounter  = metrics.MustRegisterCounter("cursors_ref", metrics.WithGroup(tsmGroup))
	numberOfAuxCursorsCounter  = metrics.MustRegisterCounter("cursors_aux", metrics.WithGroup(tsmGroup))
	numberOfCondCursorsCounter = metrics.MustRegisterCounter("cursors_cond", metrics.WithGroup(tsmGroup))
	planningTimer              = metrics.MustRegisterTimer("planning_time", metrics.WithGroup(tsmGroup))
)

const (
	// keyFieldSeparator separates the series key from the field name in the composite key
	// that identifies a specific field in series
	keyFieldSeparator = "#!~#"

	// deleteFlushThreshold is the size in bytes of a batch of series keys to delete.
	deleteFlushThreshold = 50 * 1024 * 1024
)

// represents a storage engine with compressed blocks.
type Engine struct {
	mu sync.RWMutex

	index tsdb.Index

	// The following group of fields is used to track the state of level compactions within the
	// Engine. The WaitGroup is used to monitor the compaction goroutines, the 'done' channel is
	// used to signal those goroutines to shutdown. Every request to disable level compactions will
	// call 'Wait' on 'wg', with the first goroutine to arrive (levelWorkers == 0 while holding the
	// lock) will close the done channel and re-assign 'nil' to the variable. Re-enabling will
	// decrease 'levelWorkers', and when it decreases to zero, level compactions will be started
	// back up again.

	wg           *sync.WaitGroup // waitgroup for active level compaction goroutines
	done         chan struct{}   // channel to signal level compactions to stop
	levelWorkers int             // Number of "workers" that expect compactions to be in a disabled state

	snapDone chan struct{}   // channel to signal snapshot compactions to stop
	snapWG   *sync.WaitGroup // waitgroup for running snapshot compactions

	id           uint64
	path         string
	sfile        *tsdb.SeriesFile
	logger       *zap.Logger // Logger to be used for important messages
	traceLogger  *zap.Logger // Logger to be used when trace-logging is on.
	traceLogging bool

	fieldset *tsdb.MeasurementFieldSet

	WAL            *WAL
	Cache          *Cache
	Compactor      *Compactor
	CompactionPlan CompactionPlanner
	FileStore      *FileStore

	MaxPointsPerBlock int

	// CacheFlushMemorySizeThreshold specifies the minimum size threshold for
	// the cache when the engine should write a snapshot to a TSM file
	CacheFlushMemorySizeThreshold uint64 // 对应 storage-cache-snapshot-memory-size

	// CacheFlushWriteColdDuration specifies the length of time after which if
	// no writes have been committed to the WAL, the engine will write
	// a snapshot of the cache to a TSM file
	CacheFlushWriteColdDuration time.Duration // 对应 storage-cache-snapshot-write-cold-duration

	// WALEnabled determines whether writes to the WAL are enabled.  If this is false,
	// writes will only exist in the cache and can be lost if a snapshot has not occurred.
	WALEnabled bool // 写死的是true

	// Invoked when creating a backup file "as new".
	formatFileName FormatFileNameFunc

	// Controls whether to enabled compactions when the engine is open
	enableCompactionsOnOpen bool

	stats *compactionMetrics

	activeCompactions *compactionCounter

	// Limiter for concurrent compactions.
	compactionLimiter limiter.Fixed

	scheduler *scheduler

	// provides access to the total set of series IDs
	seriesIDSets tsdb.SeriesIDSets

	// seriesTypeMap maps a series key to field type
	seriesTypeMap *radix.Tree

	// muDigest ensures only one goroutine can generate a digest at a time.
	muDigest sync.RWMutex
}

// returns a new instance of Engine.
func NewEngine(id uint64, idx tsdb.Index, path string, walPath string, sfile *tsdb.SeriesFile, engineOptions tsdb.EngineOptions) tsdb.Engine {
	etags := tsdb.EngineTags{
		Path:          path,
		WalPath:       walPath,
		Id:            fmt.Sprintf("%d", id),
		Bucket:        filepath.Base(filepath.Dir(filepath.Dir(path))), // discard shard & rp, take db
		EngineVersion: engineOptions.EngineVersion,
	}

	var wal *WAL
	if engineOptions.WALEnabled {
		wal = NewWAL(walPath, engineOptions.Config.WALMaxConcurrentWrites, engineOptions.Config.WALMaxWriteDelay, etags)
		wal.syncDelay = time.Duration(engineOptions.Config.WALFsyncDelay)
	}

	fs := NewFileStore(path, etags)
	fs.openLimiter = engineOptions.OpenLimiter
	if engineOptions.FileStoreObserver != nil {
		fs.WithObserver(engineOptions.FileStoreObserver)
	}
	fs.tsmMMAPWillNeed = engineOptions.Config.TSMWillNeed

	cache := NewCache(uint64(engineOptions.Config.CacheMaxMemorySize), etags)

	c := NewCompactor()
	c.Dir = path
	c.FileStore = fs
	c.RateLimit = engineOptions.CompactionThroughputLimiter

	var compactionPlanner CompactionPlanner = NewDefaultPlanner(fs, time.Duration(engineOptions.Config.CompactFullWriteColdDuration))
	if engineOptions.CompactionPlannerCreator != nil {
		compactionPlanner = engineOptions.CompactionPlannerCreator(engineOptions.Config).(CompactionPlanner)
		compactionPlanner.SetFileStore(fs)
	}

	stats := newEngineMetrics(etags)
	activeCompactions := &compactionCounter{}
	e := &Engine{
		id:           id,
		path:         path,
		index:        idx,
		sfile:        sfile,
		logger:       zap.NewNop(),
		traceLogger:  zap.NewNop(),
		traceLogging: engineOptions.Config.TraceLoggingEnabled,

		WAL:   wal,
		Cache: cache,

		FileStore:      fs,
		Compactor:      c,
		CompactionPlan: compactionPlanner,

		activeCompactions: activeCompactions,
		scheduler:         newScheduler(activeCompactions, engineOptions.CompactionLimiter.Capacity()),

		CacheFlushMemorySizeThreshold: uint64(engineOptions.Config.CacheSnapshotMemorySize),
		CacheFlushWriteColdDuration:   time.Duration(engineOptions.Config.CacheSnapshotWriteColdDuration),
		enableCompactionsOnOpen:       true,
		WALEnabled:                    engineOptions.WALEnabled,
		formatFileName:                DefaultFormatFileName,
		stats:                         stats,
		compactionLimiter:             engineOptions.CompactionLimiter,
		seriesIDSets:                  engineOptions.SeriesIDSets,
	}

	// Feature flag to enable per-series type checking, by default this is off and
	// e.seriesTypeMap will be nil.
	if os.Getenv("INFLUXDB_SERIES_TYPE_CHECK_ENABLED") != "" {
		e.seriesTypeMap = radix.New()
	}

	if e.traceLogging {
		fs.enableTraceLogging(true)
		if e.WALEnabled {
			e.WAL.enableTraceLogging(true)
		}
	}

	return e
}

func (engine *Engine) WithFormatFileNameFunc(formatFileNameFunc FormatFileNameFunc) {
	engine.Compactor.WithFormatFileNameFunc(formatFileNameFunc)
	engine.formatFileName = formatFileNameFunc
}

func (engine *Engine) WithParseFileNameFunc(parseFileNameFunc ParseFileNameFunc) {
	engine.FileStore.WithParseFileNameFunc(parseFileNameFunc)
	engine.Compactor.WithParseFileNameFunc(parseFileNameFunc)
}

// Digest returns a reader for the shard's digest.
func (engine *Engine) Digest() (io.ReadCloser, int64, error) {
	engine.muDigest.Lock()
	defer engine.muDigest.Unlock()

	log, logEnd := logger.NewOperation(context.TODO(), engine.logger, "Engine digest", "tsm1_digest")
	defer logEnd()

	log.Info("Starting digest", zap.String("tsm1_path", engine.path))

	digestPath := filepath.Join(engine.path, DigestFilename)

	// Get a list of tsm file paths from the FileStore.
	files := engine.FileStore.Files()
	tsmfiles := make([]string, 0, len(files))
	for _, f := range files {
		tsmfiles = append(tsmfiles, f.Path())
	}

	// See if there's a fresh digest cached on disk.
	fresh, reason := DigestFresh(engine.path, tsmfiles, engine.LastModified())
	if fresh {
		f, err := os.Open(digestPath)
		if err == nil {
			fi, err := f.Stat()
			if err != nil {
				log.Info("Digest aborted, couldn't stat digest file", logger.Shard(engine.id), zap.Error(err))
				return nil, 0, err
			}

			log.Info("Digest is fresh", logger.Shard(engine.id), zap.String("path", digestPath))

			// Return the cached digest.
			return f, fi.Size(), nil
		}
	}

	log.Info("Digest stale", logger.Shard(engine.id), zap.String("reason", reason))

	// Either no digest existed or the existing one was stale
	// so generate a new digest.

	// Make sure the directory exists, in case it was deleted for some reason.
	if err := os.MkdirAll(engine.path, 0777); err != nil {
		log.Info("Digest aborted, problem creating shard directory path", zap.Error(err))
		return nil, 0, err
	}

	// Create a tmp file to write the digest to.
	tf, err := os.Create(digestPath + ".tmp")
	if err != nil {
		log.Info("Digest aborted, problem creating tmp digest", zap.Error(err))
		return nil, 0, err
	}

	// Write the new digest to the tmp file.
	if err := Digest(engine.path, tsmfiles, tf); err != nil {
		log.Info("Digest aborted, problem writing tmp digest", zap.Error(err))
		tf.Close()
		os.Remove(tf.Name())
		return nil, 0, err
	}

	// Rename the temporary digest file to the actual digest file.
	if err := file.RenameFile(tf.Name(), digestPath); err != nil {
		log.Info("Digest aborted, problem renaming tmp digest", zap.Error(err))
		return nil, 0, err
	}

	// Create and return a reader for the new digest file.
	f, err := os.Open(digestPath)
	if err != nil {
		log.Info("Digest aborted, opening new digest", zap.Error(err))
		return nil, 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		log.Info("Digest aborted, can't stat new digest", zap.Error(err))
		f.Close()
		return nil, 0, err
	}

	log.Info("Digest written", zap.String("tsm1_digest_path", digestPath), zap.Int64("size", fi.Size()))

	return f, fi.Size(), nil
}

// SetEnabled sets whether the engine is enabled.
func (engine *Engine) SetEnabled(enabled bool) {
	engine.enableCompactionsOnOpen = enabled
	engine.SetCompactionsEnabled(enabled)
}

// SetCompactionsEnabled enables compactions on the engine.  When disabled
// all running compactions are aborted and new compactions stop running.
func (engine *Engine) SetCompactionsEnabled(enabled bool) {
	if enabled {
		engine.enableSnapshotCompactions()
		engine.enableLevelCompactions(false)
	} else {
		engine.disableSnapshotCompactions()
		engine.disableLevelCompactions(false)
	}
}

// enableLevelCompactions will request that level compactions start back up again
//
// 'wait' signifies that a corresponding call to disableLevelCompactions(true) was made at some
// point, and the associated task that required disabled compactions is now complete
func (engine *Engine) enableLevelCompactions(wait bool) {
	// If we don't need to wait, see if we're already enabled
	if !wait {
		engine.mu.RLock()
		if engine.done != nil {
			engine.mu.RUnlock()
			return
		}
		engine.mu.RUnlock()
	}

	engine.mu.Lock()
	if wait {
		engine.levelWorkers -= 1
	}
	if engine.levelWorkers != 0 || engine.done != nil {
		// still waiting on more workers or already enabled
		engine.mu.Unlock()
		return
	}

	// last one to enable, start things back up
	engine.Compactor.EnableCompactions()
	engine.done = make(chan struct{})
	wg := new(sync.WaitGroup)
	wg.Add(1)
	engine.wg = wg
	engine.mu.Unlock()

	go func() { defer wg.Done(); engine.compact(wg) }()
}

// disableLevelCompactions will stop level compactions before returning.
//
// If 'wait' is set to true, then a corresponding call to enableLevelCompactions(true) will be
// required before level compactions will start back up again.
func (engine *Engine) disableLevelCompactions(wait bool) {
	engine.mu.Lock()
	old := engine.levelWorkers
	if wait {
		engine.levelWorkers += 1
	}

	// Hold onto the current done channel so we can wait on it if necessary
	waitCh := engine.done
	wg := engine.wg

	if old == 0 && engine.done != nil {
		// It's possible we have closed the done channel and released the lock and another
		// goroutine has attempted to disable compactions.  We're current in the process of
		// disabling them so check for this and wait until the original completes.
		select {
		case <-engine.done:
			engine.mu.Unlock()
			return
		default:
		}

		// Prevent new compactions from starting
		engine.Compactor.DisableCompactions()

		// Stop all background compaction goroutines
		close(engine.done)
		engine.mu.Unlock()
		wg.Wait()

		// Signal that all goroutines have exited.
		engine.mu.Lock()
		engine.done = nil
		engine.mu.Unlock()
		return
	}
	engine.mu.Unlock()

	// Compaction were already disabled.
	if waitCh == nil {
		return
	}

	// We were not the first caller to disable compactions and they were in the process
	// of being disabled.  Wait for them to complete before returning.
	<-waitCh
	wg.Wait()
}

func (engine *Engine) enableSnapshotCompactions() {
	// Check if already enabled under read lock
	engine.mu.RLock()
	if engine.snapDone != nil {
		engine.mu.RUnlock()
		return
	}
	engine.mu.RUnlock()

	// Check again under write lock
	engine.mu.Lock()
	if engine.snapDone != nil {
		engine.mu.Unlock()
		return
	}

	engine.Compactor.EnableSnapshots()
	engine.snapDone = make(chan struct{})
	wg := new(sync.WaitGroup)
	wg.Add(1)
	engine.snapWG = wg
	engine.mu.Unlock()

	go func() { defer wg.Done(); engine.compactCache() }()
}

func (engine *Engine) disableSnapshotCompactions() {
	engine.mu.Lock()
	if engine.snapDone == nil {
		engine.mu.Unlock()
		return
	}

	// We may be in the process of stopping snapshots.  See if the channel
	// was closed.
	select {
	case <-engine.snapDone:
		engine.mu.Unlock()
		return
	default:
	}

	// first one here, disable and wait for completion
	close(engine.snapDone)
	engine.Compactor.DisableSnapshots()
	wg := engine.snapWG
	engine.mu.Unlock()

	// Wait for the snapshot goroutine to exit.
	wg.Wait()

	// Signal that the goroutines are exit and everything is stopped by setting
	// snapDone to nil.
	engine.mu.Lock()
	engine.snapDone = nil
	engine.mu.Unlock()

	// If the cache is empty, free up its resources as well.
	if engine.Cache.Size() == 0 {
		engine.Cache.Free()
	}
}

// ScheduleFullCompaction will force the engine to fully compact all data stored.
// This will cancel and running compactions and snapshot any data in the cache to
// TSM files.  This is an expensive operation.
func (engine *Engine) ScheduleFullCompaction() error {
	// Snapshot any data in the cache
	if err := engine.WriteSnapshot(); err != nil {
		return err
	}

	// Cancel running compactions
	engine.SetCompactionsEnabled(false)

	// Ensure compactions are restarted
	defer engine.SetCompactionsEnabled(true)

	// Force the planner to only create a full plan.
	engine.CompactionPlan.ForceFull()
	return nil
}

// Path returns the path the engine was opened with.
func (engine *Engine) Path() string { return engine.path }

func (engine *Engine) MeasurementExists(name []byte) (bool, error) {
	return engine.index.MeasurementExists(name)
}

func (engine *Engine) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	return engine.index.MeasurementNamesByRegex(re)
}

// MeasurementFieldSet returns the measurement field set.
func (engine *Engine) MeasurementFieldSet() *tsdb.MeasurementFieldSet {
	return engine.fieldset
}

// MeasurementFields returns the measurement fields for a measurement.
func (engine *Engine) MeasurementFields(measurement []byte) *tsdb.MeasurementFields {
	return engine.fieldset.CreateFieldsIfNotExists(measurement)
}

func (engine *Engine) HasTagKey(name, key []byte) (bool, error) {
	return engine.index.HasTagKey(name, key)
}

func (engine *Engine) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	return engine.index.MeasurementTagKeysByExpr(name, expr)
}

func (engine *Engine) TagKeyCardinality(name, key []byte) int {
	return engine.index.TagKeyCardinality(name, key)
}

// SeriesN returns the unique number of series in the index.
func (engine *Engine) SeriesN() int64 {
	return engine.index.SeriesN()
}

// MeasurementsSketches returns sketches that describe the cardinality of the
// measurements in this shard and measurements that were in this shard, but have
// been tombstoned.
func (engine *Engine) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	return engine.index.MeasurementsSketches()
}

// SeriesSketches returns sketches that describe the cardinality of the
// series in this shard and series that were in this shard, but have
// been tombstoned.
func (engine *Engine) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	return engine.index.SeriesSketches()
}

// LastModified returns the time when this shard was last modified.
func (engine *Engine) LastModified() time.Time {
	fsTime := engine.FileStore.LastModified()

	if engine.WALEnabled && engine.WAL.LastWriteTime().After(fsTime) {
		return engine.WAL.LastWriteTime()
	}

	return fsTime
}

var globalCompactionMetrics *compactionMetrics = newAllCompactionMetrics(tsdb.EngineLabelNames())

// PrometheusCollectors returns all prometheus metrics for the tsm1 package.
func PrometheusCollectors() []prometheus.Collector {
	collectors := []prometheus.Collector{
		globalCompactionMetrics.Duration,
		globalCompactionMetrics.Active,
		globalCompactionMetrics.Failed,
		globalCompactionMetrics.Queued,
	}
	collectors = append(collectors, FileStoreCollectors()...)
	collectors = append(collectors, CacheCollectors()...)
	collectors = append(collectors, WALCollectors()...)
	return collectors
}

const (
	storageNamespace = "storage"
	engineSubsystem  = "compactions"
	level1           = "1"
	level2           = "2"
	level3           = "3"
	levelOpt         = "opt"
	levelFull        = "full"
	levelKey         = "level"
	levelCache       = "cache"
)

func labelForLevel(l int) prometheus.Labels {
	switch l {
	case 1:
		return prometheus.Labels{levelKey: level1}
	case 2:
		return prometheus.Labels{levelKey: level2}
	case 3:
		return prometheus.Labels{levelKey: level3}
	}
	panic(fmt.Sprintf("labelForLevel: level out of range %d", l))
}

func newAllCompactionMetrics(labelNames []string) *compactionMetrics {
	labelNamesWithLevel := append(labelNames, levelKey)
	return &compactionMetrics{
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: storageNamespace,
			Subsystem: engineSubsystem,
			Name:      "duration_seconds",
			Help:      "Histogram of compactions by level since startup",
			// 10 minute compactions seem normal, 1h40min is high
			Buckets: []float64{60, 600, 6000},
		}, labelNamesWithLevel),
		Active: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: engineSubsystem,
			Name:      "active",
			Help:      "Gauge of compactions (by level) currently running",
		}, labelNamesWithLevel),
		Failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: engineSubsystem,
			Name:      "failed",
			Help:      "Counter of TSM compactions (by level) that have failed due to error",
		}, labelNamesWithLevel),
		Queued: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: engineSubsystem,
			Name:      "queued",
			Help:      "Counter of TSM compactions (by level) that are currently queued",
		}, labelNamesWithLevel),
	}
}

type compactionCounter struct {
	l1       int64
	l2       int64
	l3       int64
	full     int64
	optimize int64
}

func (c *compactionCounter) countForLevel(l int) *int64 {
	switch l {
	case 1:
		return &c.l1
	case 2:
		return &c.l2
	case 3:
		return &c.l3
	}
	return nil
}

// engineMetrics holds statistics across all instantiated engines
type compactionMetrics struct {
	Duration prometheus.ObserverVec
	Active   *prometheus.GaugeVec
	Queued   *prometheus.GaugeVec
	Failed   *prometheus.CounterVec
}

func newEngineMetrics(tags tsdb.EngineTags) *compactionMetrics {
	engineLabels := tags.GetLabels()
	return &compactionMetrics{
		Duration: globalCompactionMetrics.Duration.MustCurryWith(engineLabels),
		Active:   globalCompactionMetrics.Active.MustCurryWith(engineLabels),
		Failed:   globalCompactionMetrics.Failed.MustCurryWith(engineLabels),
		Queued:   globalCompactionMetrics.Queued.MustCurryWith(engineLabels),
	}
}

// DiskSize returns the total size in bytes of all TSM and WAL segments on disk.
func (engine *Engine) DiskSize() int64 {
	var walDiskSizeBytes int64
	if engine.WALEnabled {
		walDiskSizeBytes = engine.WAL.DiskSizeBytes()
	}
	return engine.FileStore.DiskSizeBytes() + walDiskSizeBytes
}

// Open opens and initializes the engine.
func (engine *Engine) Open(ctx context.Context) error {
	if err := os.MkdirAll(engine.path, 0777); err != nil {
		return err
	}

	if err := engine.cleanup(); err != nil {
		return err
	}

	fields, err := tsdb.NewMeasurementFieldSet(filepath.Join(engine.path, "fields.idx"), engine.logger)
	if err != nil {
		engine.logger.Warn(fmt.Sprintf("error opening fields.idx: %v.  Rebuilding.", err))
	}

	engine.mu.Lock()
	engine.fieldset = fields
	engine.mu.Unlock()

	engine.index.SetFieldSet(fields)

	if engine.WALEnabled {
		if err := engine.WAL.Open(); err != nil {
			return err
		}
	}

	if err := engine.FileStore.Open(ctx); err != nil {
		return err
	}

	if engine.WALEnabled {
		if err := engine.reloadCache(); err != nil {
			return err
		}
	}

	engine.Compactor.Open()

	if engine.enableCompactionsOnOpen {
		engine.SetCompactionsEnabled(true)
	}

	return nil
}

// Close closes the engine. Subsequent calls to Close are a nop.
func (engine *Engine) Close() error {
	engine.SetCompactionsEnabled(false)

	// Lock now and close everything else down.
	engine.mu.Lock()
	defer engine.mu.Unlock()
	engine.done = nil // Ensures that the channel will not be closed again.

	var err error = nil
	err = engine.fieldset.Close()
	if err2 := engine.FileStore.Close(); err2 != nil && err == nil {
		err = err2
	}
	if engine.WALEnabled {
		if err2 := engine.WAL.Close(); err2 != nil && err == nil {
			err = err2
		}
	}
	return err
}

// WithLogger sets the logger for the engine.
func (engine *Engine) WithLogger(log *zap.Logger) {
	engine.logger = log.With(zap.String("engine", "tsm1"))

	if engine.traceLogging {
		engine.traceLogger = engine.logger
	}

	if engine.WALEnabled {
		engine.WAL.WithLogger(engine.logger)
	}
	engine.FileStore.WithLogger(engine.logger)
}

// LoadMetadataIndex loads the shard metadata into memory.
//
// Note, it not safe to call LoadMetadataIndex concurrently. LoadMetadataIndex
// should only be called when initialising a new Engine.
func (engine *Engine) LoadMetadataIndex(shardID uint64, index tsdb.Index) error {
	now := time.Now()

	// Save reference to index for iterator creation.
	engine.index = index

	// If we have the cached fields index on disk, we can skip scanning all the TSM files.
	if !engine.fieldset.IsEmpty() {
		return nil
	}

	keys := make([][]byte, 0, 10000)
	fieldTypes := make([]influxql.DataType, 0, 10000)

	if err := engine.FileStore.WalkKeys(nil, func(key []byte, typ byte) error {
		fieldType := BlockTypeToInfluxQLDataType(typ)
		if fieldType == influxql.Unknown {
			return fmt.Errorf("unknown block type: %v", typ)
		}

		keys = append(keys, key)
		fieldTypes = append(fieldTypes, fieldType)
		if len(keys) == cap(keys) {
			// Send batch of keys to the index.
			if err := engine.addToIndexFromKey(keys, fieldTypes); err != nil {
				return err
			}

			// Reset buffers.
			keys, fieldTypes = keys[:0], fieldTypes[:0]
		}

		return nil
	}); err != nil {
		return err
	}

	if len(keys) > 0 {
		// Add remaining partial batch from FileStore.
		if err := engine.addToIndexFromKey(keys, fieldTypes); err != nil {
			return err
		}
		keys, fieldTypes = keys[:0], fieldTypes[:0]
	}

	// load metadata from the Cache
	if err := engine.Cache.ApplyEntryFn(func(key []byte, entry *entry) error {
		fieldType, err := entry.values.InfluxQLType()
		if err != nil {
			engine.logger.Info("Error getting the data type of values for key", zap.ByteString("key", key), zap.Error(err))
		}

		keys = append(keys, key)
		fieldTypes = append(fieldTypes, fieldType)
		if len(keys) == cap(keys) {
			// Send batch of keys to the index.
			if err := engine.addToIndexFromKey(keys, fieldTypes); err != nil {
				return err
			}

			// Reset buffers.
			keys, fieldTypes = keys[:0], fieldTypes[:0]
		}
		return nil
	}); err != nil {
		return err
	}

	if len(keys) > 0 {
		// Add remaining partial batch from FileStore.
		if err := engine.addToIndexFromKey(keys, fieldTypes); err != nil {
			return err
		}
	}

	// Save the field set index so we don't have to rebuild it next time
	if err := engine.fieldset.WriteToFile(); err != nil {
		return err
	}

	engine.traceLogger.Info("Meta data index for shard loaded", zap.Uint64("id", shardID), zap.Duration("duration", time.Since(now)))
	return nil
}

// IsIdle returns true if the cache is empty, there are no running compactions and the
// shard is fully compacted.
func (engine *Engine) IsIdle() (state bool, reason string) {
	c := []struct {
		ActiveCompactions *int64
		LogMessage        string
	}{
		// We don't actually track cache compactions: {&e.status.CacheCompactionsActive, "not idle because of active Cache compactions"},
		{&engine.activeCompactions.l1, "not idle because of active Level1 compactions"},
		{&engine.activeCompactions.l2, "not idle because of active Level2 compactions"},
		{&engine.activeCompactions.l3, "not idle because of active Level3 compactions"},
		{&engine.activeCompactions.full, "not idle because of active Full compactions"},
		{&engine.activeCompactions.optimize, "not idle because of active TSM Optimization compactions"},
	}

	for _, compactionState := range c {
		count := atomic.LoadInt64(compactionState.ActiveCompactions)
		if count > 0 {
			return false, compactionState.LogMessage
		}
	}

	if cacheSize := engine.Cache.Size(); cacheSize > 0 {
		return false, "not idle because cache size is nonzero"
	} else if c, r := engine.CompactionPlan.FullyCompacted(); !c {
		return false, r
	} else {
		return true, ""
	}
}

// Free releases any resources held by the engine to free up memory or CPU.
func (engine *Engine) Free() error {
	engine.Cache.Free()
	return engine.FileStore.Free()
}

// Backup writes a tar archive of any TSM files modified since the passed
// in time to the passed in writer. The basePath will be prepended to the names
// of the files in the archive. It will force a snapshot of the WAL first
// then perform the backup with a read lock against the file store. This means
// that new TSM files will not be able to be created in this shard while the
// backup is running. For shards that are still actively getting writes, this
// could cause the WAL to backup, increasing memory usage and eventually rejecting writes.
func (engine *Engine) Backup(w io.Writer, basePath string, since time.Time) error {
	var err error
	var path string
	path, err = engine.CreateSnapshot(true)
	if err != nil {
		return err
	}
	// Remove the temporary snapshot dir
	defer os.RemoveAll(path)

	return intar.Stream(w, path, basePath, intar.SinceFilterTarFile(since))
}

func (engine *Engine) timeStampFilterTarFile(start, end time.Time) func(f os.FileInfo, shardRelativePath, fullPath string, tw *tar.Writer) error {
	return func(fi os.FileInfo, shardRelativePath, fullPath string, tw *tar.Writer) error {
		if !strings.HasSuffix(fi.Name(), ".tsm") {
			return intar.StreamFile(fi, shardRelativePath, fullPath, tw)
		}

		f, err := os.Open(fullPath)
		if err != nil {
			return err
		}
		r, err := NewTSMReader(f)
		if err != nil {
			return err
		}

		// Grab the tombstone file if one exists.
		if ts := r.TombstoneStats(); ts.TombstoneExists {
			return intar.StreamFile(fi, shardRelativePath, filepath.Base(ts.Path), tw)
		}

		min, max := r.TimeRange()
		stun := start.UnixNano()
		eun := end.UnixNano()

		// We overlap time ranges, we need to filter the file
		if min >= stun && min <= eun && max > eun || // overlap to the right
			max >= stun && max <= eun && min < stun || // overlap to the left
			min <= stun && max >= eun { // TSM file has a range LARGER than the boundary
			err := engine.filterFileToBackup(r, fi, shardRelativePath, fullPath, start.UnixNano(), end.UnixNano(), tw)
			if err != nil {
				if err := r.Close(); err != nil {
					return err
				}
				return err
			}

		}

		// above is the only case where we need to keep the reader open.
		if err := r.Close(); err != nil {
			return err
		}

		// the TSM file is 100% inside the range, so we can just write it without scanning each block
		if min >= start.UnixNano() && max <= end.UnixNano() {
			if err := intar.StreamFile(fi, shardRelativePath, fullPath, tw); err != nil {
				return err
			}
		}
		return nil
	}
}

func (engine *Engine) Export(w io.Writer, basePath string, start time.Time, end time.Time) error {
	path, err := engine.CreateSnapshot(false)
	if err != nil {
		return err
	}
	// Remove the temporary snapshot dir
	defer os.RemoveAll(path)

	return intar.Stream(w, path, basePath, engine.timeStampFilterTarFile(start, end))
}

func (engine *Engine) filterFileToBackup(r *TSMReader, fi os.FileInfo, shardRelativePath, fullPath string, start, end int64, tw *tar.Writer) error {
	path := fullPath + ".tmp"
	out, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer os.Remove(path)

	w, err := NewTSMWriter(out)
	if err != nil {
		return err
	}
	defer w.Close()

	// implicit else: here we iterate over the blocks and only keep the ones we really want.
	bi := r.BlockIterator()

	for bi.Next() {
		// not concerned with typ or checksum since we are just blindly writing back, with no decoding
		key, minTime, maxTime, _, _, buf, err := bi.Read()
		if err != nil {
			return err
		}
		if minTime >= start && minTime <= end ||
			maxTime >= start && maxTime <= end ||
			minTime <= start && maxTime >= end {
			err := w.WriteBlock(key, minTime, maxTime, buf)
			if err != nil {
				return err
			}
		}
	}

	if err := bi.Err(); err != nil {
		return err
	}

	err = w.WriteIndex()
	if err != nil {
		return err
	}

	// make sure the whole file is out to disk
	if err := w.Flush(); err != nil {
		return err
	}

	tmpFi, err := os.Stat(path)
	if err != nil {
		return err
	}

	return intar.StreamRenameFile(tmpFi, fi.Name(), shardRelativePath, path, tw)
}

// Restore reads a tar archive generated by Backup().
// Only files that match basePath will be copied into the directory. This obtains
// a write lock so no operations can be performed while restoring.
func (engine *Engine) Restore(r io.Reader, basePath string) error {
	return engine.overlay(r, basePath, false)
}

// Import reads a tar archive generated by Backup() and adds each
// file matching basePath as a new TSM file.  This obtains
// a write lock so no operations can be performed while Importing.
// If the import is successful, a full compaction is scheduled.
func (engine *Engine) Import(r io.Reader, basePath string) error {
	if err := engine.overlay(r, basePath, true); err != nil {
		return err
	}
	return engine.ScheduleFullCompaction()
}

// overlay reads a tar archive generated by Backup() and adds each file
// from the archive matching basePath to the shard.
// If asNew is true, each file will be installed as a new TSM file even if an
// existing file with the same name in the backup exists.
func (engine *Engine) overlay(r io.Reader, basePath string, asNew bool) error {
	// Copy files from archive while under lock to prevent reopening.
	newFiles, err := func() ([]string, error) {
		engine.mu.Lock()
		defer engine.mu.Unlock()

		var newFiles []string
		tr := tar.NewReader(r)
		for {
			if fileName, err := engine.readFileFromBackup(tr, basePath, asNew); err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			} else if fileName != "" {
				newFiles = append(newFiles, fileName)
			}
		}

		if err := file.SyncDir(engine.path); err != nil {
			return nil, err
		}

		// The filestore will only handle tsm files. Other file types will be ignored.
		if err := engine.FileStore.Replace(nil, newFiles); err != nil {
			return nil, err
		}
		return newFiles, nil
	}()

	if err != nil {
		return err
	}

	// Load any new series keys to the index
	tsmFiles := make([]TSMFile, 0, len(newFiles))
	defer func() {
		for _, r := range tsmFiles {
			r.Close()
		}
	}()

	ext := fmt.Sprintf(".%s", TmpTSMFileExtension)
	for _, f := range newFiles {
		// If asNew is true, the files created from readFileFromBackup will be new ones
		// having a temp extension.
		f = strings.TrimSuffix(f, ext)
		if !strings.HasSuffix(f, TSMFileExtension) {
			// This isn't a .tsm file.
			continue
		}

		fd, err := os.Open(f)
		if err != nil {
			return err
		}

		r, err := NewTSMReader(fd)
		if err != nil {
			return err
		}
		tsmFiles = append(tsmFiles, r)
	}

	// Merge and dedup all the series keys across each reader to reduce
	// lock contention on the index.
	keys := make([][]byte, 0, 10000)
	fieldTypes := make([]influxql.DataType, 0, 10000)

	ki := newMergeKeyIterator(tsmFiles, nil)
	for ki.Next() {
		key, typ := ki.Read()
		fieldType := BlockTypeToInfluxQLDataType(typ)
		if fieldType == influxql.Unknown {
			return fmt.Errorf("unknown block type: %v", typ)
		}

		keys = append(keys, key)
		fieldTypes = append(fieldTypes, fieldType)

		if len(keys) == cap(keys) {
			// Send batch of keys to the index.
			if err := engine.addToIndexFromKey(keys, fieldTypes); err != nil {
				return err
			}

			// Reset buffers.
			keys, fieldTypes = keys[:0], fieldTypes[:0]
		}
	}

	if len(keys) > 0 {
		// Add remaining partial batch.
		if err := engine.addToIndexFromKey(keys, fieldTypes); err != nil {
			return err
		}
	}
	return engine.MeasurementFieldSet().WriteToFile()
}

// readFileFromBackup copies the next file from the archive into the shard.
// The file is skipped if it does not have a matching shardRelativePath prefix.
// If asNew is true, each file will be installed as a new TSM file even if an
// existing file with the same name in the backup exists.
func (engine *Engine) readFileFromBackup(tr *tar.Reader, shardRelativePath string, asNew bool) (string, error) {
	// Read next archive file.
	hdr, err := tr.Next()
	if err != nil {
		return "", err
	}

	if !strings.HasSuffix(hdr.Name, TSMFileExtension) {
		// This isn't a .tsm file.
		return "", nil
	}

	filename := filepath.Base(filepath.FromSlash(hdr.Name))

	// If this is a directory entry (usually just `index` for tsi), create it an move on.
	if hdr.Typeflag == tar.TypeDir {
		if err := os.MkdirAll(filepath.Join(engine.path, filename), os.FileMode(hdr.Mode).Perm()); err != nil {
			return "", err
		}
		return "", nil
	}

	if asNew {
		filename = engine.formatFileName(engine.FileStore.NextGeneration(), 1) + "." + TSMFileExtension
	}

	tmp := fmt.Sprintf("%s.%s", filepath.Join(engine.path, filename), TmpTSMFileExtension)
	// Create new file on disk.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// Copy from archive to the file.
	if _, err := io.CopyN(f, tr, hdr.Size); err != nil {
		return "", err
	}

	// Sync to disk & close.
	if err := f.Sync(); err != nil {
		return "", err
	}

	return tmp, nil
}

// addToIndexFromKey will pull the measurement names, series keys, and field
// names from composite keys, and add them to the database index and measurement
// fields.
func (engine *Engine) addToIndexFromKey(keys [][]byte, fieldTypes []influxql.DataType) error {
	var field []byte
	names := make([][]byte, 0, len(keys))
	tags := make([]models.Tags, 0, len(keys))

	for i := 0; i < len(keys); i++ {
		// Replace tsm key format with index key format.
		keys[i], field = SeriesAndFieldFromCompositeKey(keys[i])
		name := models.ParseName(keys[i])
		mf := engine.fieldset.CreateFieldsIfNotExists(name)
		if err := mf.CreateFieldIfNotExists(field, fieldTypes[i]); err != nil {
			return err
		}

		names = append(names, name)
		tags = append(tags, models.ParseTags(keys[i]))
	}

	return engine.index.CreateSeriesListIfNotExists(keys, names, tags)
}

// write metadata and point data into the engine.
// It returns an error if new points are added to an existing key.
func (engine *Engine) WritePoints(ctx context.Context, points []models.Point) error {
	values := make(map[string][]Value, len(points))
	var (
		keyBuf    []byte
		baseLen   int
		seriesErr error
	)

	for _, p := range points {
		keyBuf = append(keyBuf[:0], p.Key()...)
		keyBuf = append(keyBuf, keyFieldSeparator...)
		baseLen = len(keyBuf)
		iter := p.FieldIterator()
		t := p.Time().UnixNano()
		for iter.Next() {
			// Skip fields name "time", they are illegal
			if bytes.Equal(iter.FieldKey(), timeBytes) {
				continue
			}

			keyBuf = append(keyBuf[:baseLen], iter.FieldKey()...)

			if engine.seriesTypeMap != nil {
				// Fast-path check to see if the field for the series already exists.
				if v, ok := engine.seriesTypeMap.Get(keyBuf); !ok {
					if typ, err := engine.Type(keyBuf); err != nil {
						// Field type is unknown, we can try to add it.
					} else if typ != iter.Type() {
						// Existing type is different from what was passed in, we need to drop
						// this write and refresh the series type map.
						seriesErr = tsdb.ErrFieldTypeConflict
						engine.seriesTypeMap.Insert(keyBuf, int(typ))
						continue
					}

					// Doesn't exist, so try to insert
					vv, ok := engine.seriesTypeMap.Insert(keyBuf, int(iter.Type()))

					// We didn't insert and the type that exists isn't what we tried to insert, so
					// we have a conflict and must drop this field/series.
					if !ok || vv != int(iter.Type()) {
						seriesErr = tsdb.ErrFieldTypeConflict
						continue
					}
				} else if v != int(iter.Type()) {
					// The series already exists, but with a different type.  This is also a type conflict
					// and we need to drop this field/series.
					seriesErr = tsdb.ErrFieldTypeConflict
					continue
				}
			}

			var v Value
			switch iter.Type() {
			case models.Float:
				fv, err := iter.FloatValue()
				if err != nil {
					return err
				}
				v = NewFloatValue(t, fv)
			case models.Integer:
				iv, err := iter.IntegerValue()
				if err != nil {
					return err
				}
				v = NewIntegerValue(t, iv)
			case models.Unsigned:
				iv, err := iter.UnsignedValue()
				if err != nil {
					return err
				}
				v = NewUnsignedValue(t, iv)
			case models.String:
				v = NewStringValue(t, iter.StringValue())
			case models.Boolean:
				bv, err := iter.BooleanValue()
				if err != nil {
					return err
				}
				v = NewBooleanValue(t, bv)
			default:
				return fmt.Errorf("unknown field type for %s: %s", string(iter.FieldKey()), p.String())
			}
			values[string(keyBuf)] = append(values[string(keyBuf)], v)
		}
	}

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	// first try to write to the cache
	if err := engine.Cache.WriteMulti(values); err != nil {
		return err
	}

	if engine.WALEnabled {
		if _, err := engine.WAL.WriteMulti(ctx, values); err != nil {
			return err
		}
	}
	return seriesErr
}

// removes the values between min and max (inclusive) from all series
func (engine *Engine) DeleteSeriesRange(ctx context.Context, itr tsdb.SeriesIterator, min, max int64) error {
	return engine.DeleteSeriesRangeWithPredicate(ctx, itr, func(name []byte, tags models.Tags) (int64, int64, bool) {
		return min, max, true
	})
}

// DeleteSeriesRangeWithPredicate removes the values between min and max (inclusive) from all series
// for which predicate() returns true. If predicate() is nil, then all values in range are removed.
func (engine *Engine) DeleteSeriesRangeWithPredicate(
	ctx context.Context,
	itr tsdb.SeriesIterator,
	predicate func(name []byte, tags models.Tags) (int64, int64, bool),
) error {
	var disableOnce bool

	// Ensure that the index does not compact away the measurement or series we're
	// going to delete before we're done with them.
	if tsiIndex, ok := engine.index.(*tsi1.Index); ok {
		tsiIndex.DisableCompactions()
		defer tsiIndex.EnableCompactions()
		tsiIndex.Wait()

		fs, err := tsiIndex.RetainFileSet()
		if err != nil {
			return err
		}
		defer fs.Release()
	}

	var (
		sz       int
		min, max int64 = math.MinInt64, math.MaxInt64

		// Indicator that the min/max time for the current batch has changed and
		// we need to flush the current batch before appending to it.
		flushBatch bool
	)

	// These are reversed from min/max to ensure they are different the first time through.
	newMin, newMax := int64(math.MaxInt64), int64(math.MinInt64)

	// There is no predicate, so setup newMin/newMax to delete the full time range.
	if predicate == nil {
		newMin = min
		newMax = max
	}

	batch := make([][]byte, 0, 10000)
	for {
		elem, err := itr.Next()
		if err != nil {
			return err
		} else if elem == nil {
			break
		}

		// See if the series should be deleted and if so, what range of time.
		if predicate != nil {
			var shouldDelete bool
			newMin, newMax, shouldDelete = predicate(elem.Name(), elem.Tags())
			if !shouldDelete {
				continue
			}

			// If the min/max happens to change for the batch, we need to flush
			// the current batch and start a new one.
			flushBatch = (min != newMin || max != newMax) && len(batch) > 0
		}

		if elem.Expr() != nil {
			if v, ok := elem.Expr().(*influxql.BooleanLiteral); !ok || !v.Val {
				return errors.New("fields not supported in WHERE clause during deletion")
			}
		}

		if !disableOnce {
			// Disable and abort running compactions so that tombstones added existing tsm
			// files don't get removed.  This would cause deleted measurements/series to
			// re-appear once the compaction completed.  We only disable the level compactions
			// so that snapshotting does not stop while writing out tombstones.  If it is stopped,
			// and writing tombstones takes a long time, writes can get rejected due to the cache
			// filling up.
			engine.disableLevelCompactions(true)
			defer engine.enableLevelCompactions(true)

			engine.sfile.DisableCompactions()
			defer engine.sfile.EnableCompactions()
			engine.sfile.Wait()

			disableOnce = true
		}

		if sz >= deleteFlushThreshold || flushBatch {
			// Delete all matching batch.
			if err := engine.deleteSeriesRange(ctx, batch, min, max); err != nil {
				return err
			}
			batch = batch[:0]
			sz = 0
			flushBatch = false
		}

		// Use the new min/max time for the next iteration
		min = newMin
		max = newMax

		key := models.MakeKey(elem.Name(), elem.Tags())
		sz += len(key)
		batch = append(batch, key)
	}

	if len(batch) > 0 {
		// Delete all matching batch.
		if err := engine.deleteSeriesRange(ctx, batch, min, max); err != nil {
			return err
		}
	}

	return nil
}

// removes the values between min and max (inclusive) from all series.  This
// does not update the index or disable compactions.  This should mainly be called by DeleteSeriesRange
// and not directly.
func (engine *Engine) deleteSeriesRange(ctx context.Context, seriesKeys [][]byte, min, max int64) error {
	if len(seriesKeys) == 0 {
		return nil
	}

	// Min and max time in the engine are slightly different from the query language values.
	if min == influxql.MinTime {
		min = math.MinInt64
	}
	if max == influxql.MaxTime {
		max = math.MaxInt64
	}

	var overlapsTimeRangeMinMax bool
	var overlapsTimeRangeMinMaxLock sync.Mutex
	engine.FileStore.Apply(ctx, func(r TSMFile) error {
		if r.OverlapsTimeRange(min, max) {
			overlapsTimeRangeMinMaxLock.Lock()
			overlapsTimeRangeMinMax = true
			overlapsTimeRangeMinMaxLock.Unlock()
		}
		return nil
	})

	if !overlapsTimeRangeMinMax && engine.Cache.store.count() > 0 {
		overlapsTimeRangeMinMax = true
	}

	if !overlapsTimeRangeMinMax {
		return nil
	}

	// Ensure keys are sorted since lower layers require them to be.
	if !bytesutil.IsSorted(seriesKeys) {
		bytesutil.Sort(seriesKeys)
	}

	// Run the delete on each TSM file in parallel
	if err := engine.FileStore.Apply(ctx, func(r TSMFile) error {
		// See if this TSM file contains the keys and time range
		minKey, maxKey := seriesKeys[0], seriesKeys[len(seriesKeys)-1]
		tsmMin, tsmMax := r.KeyRange()

		tsmMin, _ = SeriesAndFieldFromCompositeKey(tsmMin)
		tsmMax, _ = SeriesAndFieldFromCompositeKey(tsmMax)

		overlaps := bytes.Compare(tsmMin, maxKey) <= 0 && bytes.Compare(tsmMax, minKey) >= 0
		if !overlaps || !r.OverlapsTimeRange(min, max) {
			return nil
		}

		// Delete each key we find in the file.  We seek to the min key and walk from there.
		batch := r.BatchDelete()
		n := r.KeyCount()
		var j int
		for i := r.Seek(minKey); i < n; i++ {
			indexKey, _ := r.KeyAt(i)
			seriesKey, _ := SeriesAndFieldFromCompositeKey(indexKey)

			for j < len(seriesKeys) && bytes.Compare(seriesKeys[j], seriesKey) < 0 {
				j++
			}

			if j >= len(seriesKeys) {
				break
			}
			if bytes.Equal(seriesKeys[j], seriesKey) {
				if err := batch.DeleteRange([][]byte{indexKey}, min, max); err != nil {
					batch.Rollback()
					return err
				}
			}
		}

		return batch.Commit()
	}); err != nil {
		return err
	}

	// find the keys in the cache and remove them
	deleteKeys := make([][]byte, 0, len(seriesKeys))

	// ApplySerialEntryFn cannot return an error in this invocation.
	_ = engine.Cache.ApplyEntryFn(func(k []byte, _ *entry) error {
		seriesKey, _ := SeriesAndFieldFromCompositeKey([]byte(k))

		// Cache does not walk keys in sorted order, so search the sorted
		// series we need to delete to see if any of the cache keys match.
		i := bytesutil.SearchBytes(seriesKeys, seriesKey)
		if i < len(seriesKeys) && bytes.Equal(seriesKey, seriesKeys[i]) {
			// k is the measurement + tags + sep + field
			deleteKeys = append(deleteKeys, k)
		}
		return nil
	})

	// Sort the series keys because ApplyEntryFn iterates over the keys randomly.
	bytesutil.Sort(deleteKeys)

	engine.Cache.DeleteRange(deleteKeys, min, max)

	// delete from the WAL
	if engine.WALEnabled {
		if _, err := engine.WAL.DeleteRange(ctx, deleteKeys, min, max); err != nil {
			return err
		}
	}

	// The series are deleted on disk, but the index may still say they exist.
	// Depending on the min,max time passed in, the series may or not actually
	// exists now.  To reconcile the index, we walk the series keys that still exists
	// on disk and cross out any keys that match the passed in series.  Any series
	// left in the slice at the end do not exist and can be deleted from the index.
	// Note: this is inherently racy if writes are occurring to the same measurement/series are
	// being removed.  A write could occur and exist in the cache at this point, but we
	// would delete it from the index.
	minKey := seriesKeys[0]

	// Apply runs this func concurrently.  The seriesKeys slice is mutated concurrently
	// by different goroutines setting positions to nil.
	if err := engine.FileStore.Apply(ctx, func(r TSMFile) error {
		n := r.KeyCount()
		var j int

		// Start from the min deleted key that exists in this file.
		for i := r.Seek(minKey); i < n; i++ {
			if j >= len(seriesKeys) {
				return nil
			}

			indexKey, _ := r.KeyAt(i)
			seriesKey, _ := SeriesAndFieldFromCompositeKey(indexKey)

			// Skip over any deleted keys that are less than our tsm key
			cmp := bytes.Compare(seriesKeys[j], seriesKey)
			for j < len(seriesKeys) && cmp < 0 {
				j++
				if j >= len(seriesKeys) {
					return nil
				}
				cmp = bytes.Compare(seriesKeys[j], seriesKey)
			}

			// We've found a matching key, cross it out so we do not remove it from the index.
			if j < len(seriesKeys) && cmp == 0 {
				seriesKeys[j] = emptyBytes
				j++
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// The seriesKeys slice is mutated if they are still found in the cache.
	cacheKeys := engine.Cache.Keys()
	for i := 0; i < len(seriesKeys); i++ {
		seriesKey := seriesKeys[i]
		// Already crossed out
		if len(seriesKey) == 0 {
			continue
		}

		j := bytesutil.SearchBytes(cacheKeys, seriesKey)
		if j < len(cacheKeys) {
			cacheSeriesKey, _ := SeriesAndFieldFromCompositeKey(cacheKeys[j])
			if bytes.Equal(seriesKey, cacheSeriesKey) {
				seriesKeys[i] = emptyBytes
			}
		}
	}

	// Have we deleted all values for the series? If so, we need to remove
	// the series from the index.
	hasDeleted := false
	for _, k := range seriesKeys {
		if len(k) > 0 {
			hasDeleted = true
			break
		}
	}
	if hasDeleted {
		buf := make([]byte, 1024) // For use when accessing series file.
		ids := tsdb.NewSeriesIDSet()
		measurements := make(map[string]struct{}, 1)

		for _, k := range seriesKeys {
			if len(k) == 0 {
				continue // This key was wiped because it shouldn't be removed from index.
			}

			name, tags := models.ParseKeyBytes(k)
			sid := engine.sfile.SeriesID(name, tags, buf)
			if sid == 0 {
				continue
			}

			// See if this series was found in the cache earlier
			i := bytesutil.SearchBytes(deleteKeys, k)

			var hasCacheValues bool
			// If there are multiple fields, they will have the same prefix.  If any field
			// has values, then we can't delete it from the index.
			for i < len(deleteKeys) && bytes.HasPrefix(deleteKeys[i], k) {
				if engine.Cache.Values(deleteKeys[i]).Len() > 0 {
					hasCacheValues = true
					break
				}
				i++
			}

			if hasCacheValues {
				continue
			}

			measurements[string(name)] = struct{}{}
			// Remove the series from the local index.
			if err := engine.index.DropSeries(sid, k, false); err != nil {
				return err
			}

			// Add the id to the set of delete ids.
			ids.Add(sid)
		}

		actuallyDeleted := make([]string, 0, len(measurements))
		for k := range measurements {
			if dropped, err := engine.index.DropMeasurementIfSeriesNotExist([]byte(k)); err != nil {
				return err
			} else if dropped {
				if deleted, err := engine.cleanupMeasurement([]byte(k)); err != nil {
					return err
				} else if deleted {
					actuallyDeleted = append(actuallyDeleted, k)
				}
			}
		}
		if len(actuallyDeleted) > 0 {
			if err := engine.fieldset.Save(tsdb.MeasurementsToFieldChangeDeletions(actuallyDeleted)); err != nil {
				return err
			}
		}

		// Remove any series IDs for our set that still exist in other shards.
		// We cannot remove these from the series file yet.
		if err := engine.seriesIDSets.ForEach(func(s *tsdb.SeriesIDSet) {
			ids = ids.AndNot(s)
		}); err != nil {
			return err
		}

		// Remove the remaining ids from the series file as they no longer exist
		// in any shard.
		var err error
		ids.ForEach(func(id uint64) {
			if err1 := engine.sfile.DeleteSeriesID(id); err1 != nil {
				err = err1
				return
			}
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (engine *Engine) cleanupMeasurement(name []byte) (deleted bool, err error) {
	// A sentinel error message to cause DeleteWithLock to not delete the measurement
	abortErr := fmt.Errorf("measurements still exist")

	// Under write lock, delete the measurement if we no longer have any data stored for
	// the measurement.  If data exists, we can't delete the field set yet as there
	// were writes to the measurement while we are deleting it.
	if err := engine.fieldset.DeleteWithLock(string(name), func() error {
		encodedName := models.EscapeMeasurement(name)
		sep := len(encodedName)

		// First scan the cache to see if any series exists for this measurement.
		if err := engine.Cache.ApplyEntryFn(func(k []byte, _ *entry) error {
			if bytes.HasPrefix(k, encodedName) && (k[sep] == ',' || k[sep] == keyFieldSeparator[0]) {
				return abortErr
			}
			return nil
		}); err != nil {
			return err
		}

		// Check the filestore.
		return engine.FileStore.WalkKeys(name, func(k []byte, _ byte) error {
			if bytes.HasPrefix(k, encodedName) && (k[sep] == ',' || k[sep] == keyFieldSeparator[0]) {
				return abortErr
			}
			return nil
		})

	}); err != nil && err != abortErr {
		// Something else failed, return it
		return false, err
	}

	return err != abortErr, nil
}

// DeleteMeasurement deletes a measurement and all related series.
func (engine *Engine) DeleteMeasurement(ctx context.Context, name []byte) error {
	// Attempt to find the series keys.
	indexSet := tsdb.IndexSet{Indexes: []tsdb.Index{engine.index}, SeriesFile: engine.sfile}
	itr, err := indexSet.MeasurementSeriesByExprIterator(name, nil)
	if err != nil {
		return err
	} else if itr == nil {
		return nil
	}
	defer itr.Close()
	return engine.DeleteSeriesRange(ctx, tsdb.NewSeriesIteratorAdapter(engine.sfile, itr), math.MinInt64, math.MaxInt64)
}

// ForEachMeasurementName iterates over each measurement name in the engine.
func (engine *Engine) ForEachMeasurementName(fn func(name []byte) error) error {
	return engine.index.ForEachMeasurementName(fn)
}

func (engine *Engine) CreateSeriesListIfNotExists(keys, names [][]byte, tagsSlice []models.Tags) error {
	return engine.index.CreateSeriesListIfNotExists(keys, names, tagsSlice)
}

func (engine *Engine) CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error {
	return engine.index.CreateSeriesIfNotExists(key, name, tags)
}

// WriteTo is not implemented.
func (engine *Engine) WriteTo(w io.Writer) (n int64, err error) { panic("not implemented") }

// will snapshot the cache and write into a new TSM file with its contents, releasing the snapshot when done.
func (engine *Engine) WriteSnapshot() (err error) {
	// Lock and grab the cache snapshot along with all the closed WAL
	// filenames associated with the snapshot

	started := time.Now()
	log, logEnd := logger.NewOperation(context.TODO(), engine.logger, "Cache snapshot", "tsm1_cache_snapshot")
	defer func() {
		elapsed := time.Since(started)
		if err != nil && err != errCompactionsDisabled {
			engine.stats.Failed.With(prometheus.Labels{levelKey: levelCache}).Inc()
		}
		engine.stats.Duration.With(prometheus.Labels{levelKey: levelCache}).Observe(elapsed.Seconds())
		if err == nil {
			log.Info("Snapshot for path written", zap.String("path", engine.path), zap.Duration("duration", elapsed))
		}
		logEnd()
	}()

	closedFiles, snapshot, err := func() (segments []string, snapshot *Cache, err error) {
		engine.mu.Lock()
		defer engine.mu.Unlock()

		if engine.WALEnabled {
			if err = engine.WAL.CloseSegment(); err != nil {
				return
			}

			segments, err = engine.WAL.ClosedSegments()
			if err != nil {
				return
			}
		}

		snapshot, err = engine.Cache.Snapshot()
		if err != nil {
			return
		}

		return
	}()

	if err != nil {
		return err
	}

	if snapshot.Size() == 0 {
		engine.Cache.ClearSnapshot(true)
		return nil
	}

	// The snapshotted cache may have duplicate points and unsorted data.  We need to deduplicate
	// it before writing the snapshot.  This can be very expensive so it's done while we are not
	// holding the engine write lock.
	dedup := time.Now()
	snapshot.Deduplicate()
	engine.traceLogger.Info("Snapshot for path deduplicated",
		zap.String("path", engine.path),
		zap.Duration("duration", time.Since(dedup)))

	return engine.writeSnapshotAndCommit(log, closedFiles, snapshot)
}

// CreateSnapshot will create a temp directory that holds
// temporary hardlinks to the underlying shard files.
// skipCacheOk controls whether it is permissible to fail writing out
// in-memory cache data when a previous snapshot is in progress.
func (engine *Engine) CreateSnapshot(skipCacheOk bool) (string, error) {
	err := engine.WriteSnapshot()
	for i := 0; i < 3 && err == ErrSnapshotInProgress; i += 1 {
		backoff := time.Duration(math.Pow(32, float64(i))) * time.Millisecond
		time.Sleep(backoff)
		err = engine.WriteSnapshot()
	}
	if err == ErrSnapshotInProgress && skipCacheOk {
		engine.logger.Warn("Snapshotter busy: proceeding without cache contents")
	} else if err != nil {
		return "", err
	}

	engine.mu.RLock()
	defer engine.mu.RUnlock()
	path, err := engine.FileStore.CreateSnapshot()
	if err != nil {
		return "", err
	}

	// Generate a snapshot of the index.
	return path, nil
}

// will write the passed cache to a new TSM file and remove the closed WAL segments.
func (engine *Engine) writeSnapshotAndCommit(log *zap.Logger, closedFiles []string, snapshot *Cache) (err error) {
	defer func() {
		if err != nil {
			engine.Cache.ClearSnapshot(false)
		}
	}()

	// write the new snapshot files
	newFiles, err := engine.Compactor.WriteSnapshot(snapshot, engine.logger)
	if err != nil {
		log.Info("Error writing snapshot from compactor", zap.Error(err))
		return err
	}

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	// update the file store with these new files
	if err := engine.FileStore.Replace(nil, newFiles); err != nil {
		log.Info("Error adding new TSM files from snapshot. Removing temp files.", zap.Error(err))

		// Remove the new snapshot files. We will try again.
		for _, file := range newFiles {
			if err := os.Remove(file); err != nil {
				log.Info("Unable to remove file", zap.String("path", file), zap.Error(err))
			}
		}
		return err
	}

	// clear the snapshot from the in-memory cache, then the old WAL files
	engine.Cache.ClearSnapshot(true)

	if engine.WALEnabled {
		if err := engine.WAL.Remove(closedFiles); err != nil {
			log.Info("Error removing closed WAL segments", zap.Error(err))
		}
	}

	return nil
}

// continually checks if the WAL cache should be written to disk
func (engine *Engine) compactCache() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		engine.mu.RLock()
		quit := engine.snapDone
		engine.mu.RUnlock()

		select {
		case <-quit:
			return

		case <-t.C:
			if engine.ShouldCompactCache(time.Now()) {
				engine.traceLogger.Info("Compacting cache", zap.String("path", engine.path))
				err := engine.WriteSnapshot()
				if err != nil && err != errCompactionsDisabled {
					engine.logger.Info("Error writing snapshot", zap.Error(err))
				}
			}
		}
	}
}

// returns true if the Cache is over its flush threshold
// or if the passed in lastWriteTime is older than the write cold threshold.
func (engine *Engine) ShouldCompactCache(t time.Time) bool {
	sz := engine.Cache.Size()

	if sz == 0 {
		return false
	}

	if sz > engine.CacheFlushMemorySizeThreshold {
		return true
	}

	return t.Sub(engine.Cache.LastWriteTime()) > engine.CacheFlushWriteColdDuration
}

func (engine *Engine) compact(wg *sync.WaitGroup) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		engine.mu.RLock()
		quit := engine.done
		engine.mu.RUnlock()

		select {
		case <-quit:
			return

		case <-t.C:

			// Find our compaction plans
			level1Groups, len1 := engine.CompactionPlan.PlanLevel(1)
			level2Groups, len2 := engine.CompactionPlan.PlanLevel(2)
			level3Groups, len3 := engine.CompactionPlan.PlanLevel(3)
			level4Groups, len4 := engine.CompactionPlan.Plan(engine.LastModified())

			engine.stats.Queued.With(prometheus.Labels{levelKey: levelFull}).Set(float64(len4))

			// If no full compactions are need, see if an optimize is needed
			if len(level4Groups) == 0 {
				level4Groups, len4 = engine.CompactionPlan.PlanOptimize()
				engine.stats.Queued.With(prometheus.Labels{levelKey: levelOpt}).Set(float64(len4))
			}

			// Update the level plan queue stats
			// For stats, use the length needed, even if the lock was
			// not acquired
			engine.stats.Queued.With(prometheus.Labels{levelKey: level1}).Set(float64(len1))
			engine.stats.Queued.With(prometheus.Labels{levelKey: level2}).Set(float64(len2))
			engine.stats.Queued.With(prometheus.Labels{levelKey: level3}).Set(float64(len3))

			// Set the queue depths on the scheduler
			// Use the real queue depth, dependent on acquiring
			// the file locks.
			engine.scheduler.setDepth(1, len(level1Groups))
			engine.scheduler.setDepth(2, len(level2Groups))
			engine.scheduler.setDepth(3, len(level3Groups))
			engine.scheduler.setDepth(4, len(level4Groups))

			// Find the next compaction that can run and try to kick it off
			if level, runnable := engine.scheduler.next(); runnable {
				switch level {
				case 1:
					if engine.compactLevel(level1Groups[0], 1, false, wg) {
						level1Groups = level1Groups[1:]
					}
				case 2:
					if engine.compactLevel(level2Groups[0], 2, false, wg) {
						level2Groups = level2Groups[1:]
					}
				case 3:
					if engine.compactLevel(level3Groups[0], 3, true, wg) {
						level3Groups = level3Groups[1:]
					}
				case 4:
					if engine.compactFull(level4Groups[0], wg) {
						level4Groups = level4Groups[1:]
					}
				}
			}

			// Release all the plans we didn't start.
			engine.CompactionPlan.Release(level1Groups)
			engine.CompactionPlan.Release(level2Groups)
			engine.CompactionPlan.Release(level3Groups)
			engine.CompactionPlan.Release(level4Groups)
		}
	}
}

// compactLevel kicks off compactions using the level strategy. It returns
// true if the compaction was started
func (engine *Engine) compactLevel(grp CompactionGroup, level int, fast bool, wg *sync.WaitGroup) bool {
	s := engine.levelCompactionStrategy(grp, fast, level)
	if s == nil {
		return false
	}

	if engine.compactionLimiter.TryTake() {
		{
			val := atomic.AddInt64(engine.activeCompactions.countForLevel(level), 1)
			engine.stats.Active.With(labelForLevel(level)).Set(float64(val))
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				val := atomic.AddInt64(engine.activeCompactions.countForLevel(level), -1)
				engine.stats.Active.With(labelForLevel(level)).Set(float64(val))
			}()

			defer engine.compactionLimiter.Release()
			s.Apply()
			// Release the files in the compaction plan
			engine.CompactionPlan.Release([]CompactionGroup{s.group})
		}()
		return true
	}

	// Return the unused plans
	return false
}

// compactFull kicks off full and optimize compactions using the lo priority policy. It returns
// the plans that were not able to be started.
func (engine *Engine) compactFull(grp CompactionGroup, wg *sync.WaitGroup) bool {
	compactionStrategy := engine.fullCompactionStrategy(grp, false)
	if compactionStrategy == nil {
		return false
	}

	// Try the lo priority limiter, otherwise steal a little from the high priority if we can.
	if engine.compactionLimiter.TryTake() {
		{
			val := atomic.AddInt64(&engine.activeCompactions.full, 1)
			engine.stats.Active.With(prometheus.Labels{levelKey: levelFull}).Set(float64(val))
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				val := atomic.AddInt64(&engine.activeCompactions.full, -1)
				engine.stats.Active.With(prometheus.Labels{levelKey: levelFull}).Set(float64(val))
			}()
			defer engine.compactionLimiter.Release()
			compactionStrategy.Apply()
			// Release the files in the compaction plan
			engine.CompactionPlan.Release([]CompactionGroup{compactionStrategy.group})
		}()
		return true
	}
	return false
}

// compactionStrategy holds the details of what to do in a compaction.
type compactionStrategy struct {
	group CompactionGroup

	fast  bool
	level int

	durationSecondsStat prometheus.Observer
	errorStat           prometheus.Counter

	logger    *zap.Logger
	compactor *Compactor
	fileStore *FileStore

	engine *Engine
}

// Apply concurrently compacts all the groups in a compaction strategy.
func (s *compactionStrategy) Apply() {
	start := time.Now()
	s.compactGroup()
	s.durationSecondsStat.Observe(time.Since(start).Seconds())
}

// compactGroup executes the compaction strategy against a single CompactionGroup.
func (s *compactionStrategy) compactGroup() {
	group := s.group
	log, logEnd := logger.NewOperation(context.TODO(), s.logger, "TSM compaction", "tsm1_compact_group")
	defer logEnd()

	log.Info("Beginning compaction", zap.Int("tsm1_files_n", len(group)))
	for i, f := range group {
		log.Info("Compacting file", zap.Int("tsm1_index", i), zap.String("tsm1_file", f))
	}

	var (
		err   error
		files []string
	)
	if s.fast {
		files, err = s.compactor.CompactFast(group, log)
	} else {
		files, err = s.compactor.CompactFull(group, log)
	}

	if err != nil {
		_, inProgress := err.(errCompactionInProgress)
		if err == errCompactionsDisabled || inProgress {
			log.Info("Aborted compaction", zap.Error(err))

			if _, ok := err.(errCompactionInProgress); ok {
				time.Sleep(time.Second)
			}
			return
		}

		log.Warn("Error compacting TSM files", zap.Error(err))

		// We hit a bad TSM file - rename so the next compaction can proceed.
		if _, ok := err.(errBlockRead); ok {
			path := err.(errBlockRead).file
			log.Info("Renaming a corrupt TSM file due to compaction error", zap.Error(err))
			if err := s.fileStore.ReplaceWithCallback([]string{path}, nil, nil); err != nil {
				log.Info("Error removing bad TSM file", zap.Error(err))
			} else if e := os.Rename(path, path+"."+BadTSMFileExtension); e != nil {
				log.Info("Error renaming corrupt TSM file", zap.Error((err)))
			}
		}

		s.errorStat.Inc()
		time.Sleep(time.Second)
		return
	}

	if err := s.fileStore.ReplaceWithCallback(group, files, nil); err != nil {
		log.Info("Error replacing new TSM files", zap.Error(err))
		s.errorStat.Inc()
		time.Sleep(time.Second)

		// Remove the new snapshot files. We will try again.
		for _, file := range files {
			if err := os.Remove(file); err != nil {
				log.Error("Unable to remove file", zap.String("path", file), zap.Error(err))
			}
		}
		return
	}

	for i, f := range files {
		log.Info("Compacted file", zap.Int("tsm1_index", i), zap.String("tsm1_file", f))
	}
	log.Info("Finished compacting files",
		zap.Int("tsm1_files_n", len(files)))
}

// levelCompactionStrategy returns a compactionStrategy for the given level.
// It returns nil if there are no TSM files to compact.
func (engine *Engine) levelCompactionStrategy(group CompactionGroup, fast bool, level int) *compactionStrategy {
	label := labelForLevel(level)
	return &compactionStrategy{
		group:     group,
		logger:    engine.logger.With(zap.Int("tsm1_level", level), zap.String("tsm1_strategy", "level")),
		fileStore: engine.FileStore,
		compactor: engine.Compactor,
		fast:      fast,
		engine:    engine,
		level:     level,

		errorStat:           engine.stats.Failed.With(label),
		durationSecondsStat: engine.stats.Duration.With(label),
	}
}

// fullCompactionStrategy returns a compactionStrategy for higher level generations of TSM files.
// It returns nil if there are no TSM files to compact.
func (engine *Engine) fullCompactionStrategy(group CompactionGroup, optimize bool) *compactionStrategy {
	s := &compactionStrategy{
		group:     group,
		logger:    engine.logger.With(zap.String("tsm1_strategy", "full"), zap.Bool("tsm1_optimize", optimize)),
		fileStore: engine.FileStore,
		compactor: engine.Compactor,
		fast:      optimize,
		engine:    engine,
		level:     4,
	}

	plabel := prometheus.Labels{levelKey: levelFull}
	if optimize {
		plabel = prometheus.Labels{levelKey: levelOpt}
	}
	s.errorStat = engine.stats.Failed.With(plabel)
	s.durationSecondsStat = engine.stats.Duration.With(plabel)
	return s
}

// reads the WAL segment files and loads them into the cache.
func (engine *Engine) reloadCache() error {
	now := time.Now()
	files, err := segmentFileNames(engine.WAL.Path())
	if err != nil {
		return err
	}

	limit := engine.Cache.MaxSize()
	defer func() {
		engine.Cache.SetMaxSize(limit)
	}()

	// Disable the max size during loading
	engine.Cache.SetMaxSize(0)

	cacheLoader := NewCacheLoader(files)
	cacheLoader.WithLogger(engine.logger)
	if err := cacheLoader.Load(engine.Cache); err != nil {
		return err
	}

	engine.traceLogger.Info("Reloaded WAL cache",
		zap.String("path", engine.WAL.Path()), zap.Duration("duration", time.Since(now)))
	return nil
}

// cleanup removes all temp files and dirs that exist on disk.  This is should only be run at startup to avoid
// removing tmp files that are still in use.
func (engine *Engine) cleanup() error {
	allfiles, err := os.ReadDir(engine.path)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	ext := fmt.Sprintf(".%s", TmpTSMFileExtension)
	for _, f := range allfiles {
		// Check to see if there are any `.tmp` directories that were left over from failed shard snapshots
		if f.IsDir() && strings.HasSuffix(f.Name(), ext) {
			if err := os.RemoveAll(filepath.Join(engine.path, f.Name())); err != nil {
				return fmt.Errorf("error removing tmp snapshot directory %q: %s", f.Name(), err)
			}
		}
	}

	return engine.cleanupTempTSMFiles()
}

func (engine *Engine) cleanupTempTSMFiles() error {
	files, err := filepath.Glob(filepath.Join(engine.path, fmt.Sprintf("*.%s", CompactionTempExtension)))
	if err != nil {
		return fmt.Errorf("error getting compaction temp files: %s", err.Error())
	}

	for _, f := range files {
		if err := os.Remove(f); err != nil {
			return fmt.Errorf("error removing temp compaction files: %v", err)
		}
	}
	return nil
}

// KeyCursor returns a KeyCursor for the given key starting at time t.
func (engine *Engine) KeyCursor(ctx context.Context, key []byte, t int64, ascending bool) *KeyCursor {
	return engine.FileStore.KeyCursor(ctx, key, t, ascending)
}

// CreateIterator returns an iterator for the measurement based on opt.
func (engine *Engine) CreateIterator(ctx context.Context, measurement string, opt query.IteratorOptions) (query.Iterator, error) {
	if span := tracing.SpanFromContext(ctx); span != nil {
		labels := []string{"shard_id", strconv.Itoa(int(engine.id)), "measurement", measurement}
		if opt.Condition != nil {
			labels = append(labels, "cond", opt.Condition.String())
		}

		span = span.StartSpan("create_iterator")
		span.SetLabels(labels...)
		ctx = tracing.NewContextWithSpan(ctx, span)

		group := metrics.NewGroup(tsmGroup)
		ctx = metrics.NewContextWithGroup(ctx, group)
		start := time.Now()

		defer group.GetTimer(planningTimer).UpdateSince(start)
	}

	if call, ok := opt.Expr.(*influxql.Call); ok {
		if opt.Interval.IsZero() {
			if call.Name == "first" || call.Name == "last" {
				refOpt := opt
				refOpt.Limit = 1
				refOpt.Ascending = call.Name == "first"
				refOpt.Ordered = true
				refOpt.Expr = call.Args[0]

				itrs, err := engine.createVarRefIterator(ctx, measurement, refOpt)
				if err != nil {
					return nil, err
				}
				return newMergeFinalizerIterator(ctx, itrs, opt, engine.logger)
			}
		}

		inputs, err := engine.createCallIterator(ctx, measurement, call, opt)
		if err != nil {
			return nil, err
		} else if len(inputs) == 0 {
			return nil, nil
		}
		return newMergeFinalizerIterator(ctx, inputs, opt, engine.logger)
	}

	itrs, err := engine.createVarRefIterator(ctx, measurement, opt)
	if err != nil {
		return nil, err
	}
	return newMergeFinalizerIterator(ctx, itrs, opt, engine.logger)
}

// createSeriesIterator creates an optimized series iterator if possible.
// We exclude less-common cases for now as not worth implementing.
func (engine *Engine) createSeriesIterator(measurement string, ref *influxql.VarRef, is tsdb.IndexSet, opt query.IteratorOptions) (query.Iterator, error) {
	// Main check to see if we are trying to create a seriesKey iterator
	if ref == nil || ref.Val != "_seriesKey" || len(opt.Aux) != 0 {
		return nil, nil
	}
	// Check some other cases that we could maybe handle, but don't
	if len(opt.Dimensions) > 0 {
		return nil, nil
	}
	if opt.SLimit != 0 || opt.SOffset != 0 {
		return nil, nil
	}
	if opt.StripName {
		return nil, nil
	}
	if opt.Ordered {
		return nil, nil
	}
	// Actual creation of the iterator
	seriesCursor, err := is.MeasurementSeriesKeyByExprIterator([]byte(measurement), opt.Condition, opt.Authorizer)
	if err != nil {
		seriesCursor.Close()
		return nil, err
	}
	var seriesIterator query.Iterator
	seriesIterator = newSeriesIterator(measurement, seriesCursor)
	if opt.InterruptCh != nil {
		seriesIterator = query.NewInterruptIterator(seriesIterator, opt.InterruptCh)
	}
	return seriesIterator, nil
}

func (engine *Engine) createCallIterator(ctx context.Context, measurement string, call *influxql.Call, opt query.IteratorOptions) ([]query.Iterator, error) {
	ref, _ := call.Args[0].(*influxql.VarRef)

	if exists, err := engine.index.MeasurementExists([]byte(measurement)); err != nil {
		return nil, err
	} else if !exists {
		return nil, nil
	}

	// check for optimized series iteration for tsi index
	if engine.index.Type() == tsdb.TSI1IndexName {
		indexSet := tsdb.IndexSet{Indexes: []tsdb.Index{engine.index}, SeriesFile: engine.sfile}
		seriesOpt := opt
		if len(opt.Dimensions) == 0 && (call.Name == "count" || call.Name == "sum_hll") {
			// no point ordering the series if we are just counting all of them
			seriesOpt.Ordered = false
		}
		seriesIterator, err := engine.createSeriesIterator(measurement, ref, indexSet, seriesOpt)
		if err != nil {
			return nil, err
		}
		if seriesIterator != nil {
			callIterator, err := query.NewCallIterator(seriesIterator, opt)
			if err != nil {
				seriesIterator.Close()
				return nil, err
			}
			return []query.Iterator{callIterator}, nil
		}
	}

	// Determine tagsets for this measurement based on dimensions and filters.
	var (
		tagSets []*query.TagSet
		err     error
	)
	indexSet := tsdb.IndexSet{Indexes: []tsdb.Index{engine.index}, SeriesFile: engine.sfile}
	tagSets, err = indexSet.TagSets(engine.sfile, []byte(measurement), opt)

	if err != nil {
		return nil, err
	}

	// Reverse the tag sets if we are ordering by descending.
	if !opt.Ascending {
		for _, t := range tagSets {
			t.Reverse()
		}
	}

	// Calculate tag sets and apply SLIMIT/SOFFSET.
	tagSets = query.LimitTagSets(tagSets, opt.SLimit, opt.SOffset)

	itrs := make([]query.Iterator, 0, len(tagSets))
	if err := func() error {
		for _, t := range tagSets {
			// Abort if the query was killed
			select {
			case <-opt.InterruptCh:
				query.Iterators(itrs).Close()
				return query.ErrQueryInterrupted
			default:
			}

			inputs, err := engine.createTagSetIterators(ctx, ref, measurement, t, opt)
			if err != nil {
				return err
			} else if len(inputs) == 0 {
				continue
			}

			// Wrap each series in a call iterator.
			for i, input := range inputs {
				if opt.InterruptCh != nil {
					input = query.NewInterruptIterator(input, opt.InterruptCh)
				}

				itr, err := query.NewCallIterator(input, opt)
				if err != nil {
					query.Iterators(inputs).Close()
					return err
				}
				inputs[i] = itr
			}

			itr := query.NewParallelMergeIterator(inputs, opt, runtime.GOMAXPROCS(0))
			itrs = append(itrs, itr)
		}
		return nil
	}(); err != nil {
		query.Iterators(itrs).Close()
		return nil, err
	}

	return itrs, nil
}

// createVarRefIterator creates an iterator for a variable reference.
func (engine *Engine) createVarRefIterator(ctx context.Context, measurement string, opt query.IteratorOptions) ([]query.Iterator, error) {
	ref, _ := opt.Expr.(*influxql.VarRef)

	if exists, err := engine.index.MeasurementExists([]byte(measurement)); err != nil {
		return nil, err
	} else if !exists {
		return nil, nil
	}

	var (
		tagSets []*query.TagSet
		err     error
	)
	indexSet := tsdb.IndexSet{Indexes: []tsdb.Index{engine.index}, SeriesFile: engine.sfile}
	tagSets, err = indexSet.TagSets(engine.sfile, []byte(measurement), opt)

	if err != nil {
		return nil, err
	}

	// Reverse the tag sets if we are ordering by descending.
	if !opt.Ascending {
		for _, t := range tagSets {
			t.Reverse()
		}
	}

	// Calculate tag sets and apply SLIMIT/SOFFSET.
	tagSets = query.LimitTagSets(tagSets, opt.SLimit, opt.SOffset)
	itrs := make([]query.Iterator, 0, len(tagSets))
	if err := func() error {
		for _, t := range tagSets {
			inputs, err := engine.createTagSetIterators(ctx, ref, measurement, t, opt)
			if err != nil {
				return err
			} else if len(inputs) == 0 {
				continue
			}

			// If we have a LIMIT or OFFSET and the grouping of the outer query
			// is different than the current grouping, we need to perform the
			// limit on each of the individual series keys instead to improve
			// performance.
			if (opt.Limit > 0 || opt.Offset > 0) && len(opt.Dimensions) != len(opt.GroupBy) {
				for i, input := range inputs {
					inputs[i] = newLimitIterator(input, opt)
				}
			}

			itr, err := query.Iterators(inputs).Merge(opt)
			if err != nil {
				query.Iterators(inputs).Close()
				return err
			}

			// Apply a limit on the merged iterator.
			if opt.Limit > 0 || opt.Offset > 0 {
				if len(opt.Dimensions) == len(opt.GroupBy) {
					// When the final dimensions and the current grouping are
					// the same, we will only produce one series so we can use
					// the faster limit iterator.
					itr = newLimitIterator(itr, opt)
				} else {
					// When the dimensions are different than the current
					// grouping, we need to account for the possibility there
					// will be multiple series. The limit iterator in the
					// influxql package handles that scenario.
					itr = query.NewLimitIterator(itr, opt)
				}
			}
			itrs = append(itrs, itr)
		}
		return nil
	}(); err != nil {
		query.Iterators(itrs).Close()
		return nil, err
	}

	return itrs, nil
}

// createTagSetIterators creates a set of iterators for a tagset.
func (engine *Engine) createTagSetIterators(ctx context.Context, ref *influxql.VarRef, name string, t *query.TagSet, opt query.IteratorOptions) ([]query.Iterator, error) {
	// Set parallelism by number of logical cpus.
	parallelism := runtime.GOMAXPROCS(0)
	if parallelism > len(t.SeriesKeys) {
		parallelism = len(t.SeriesKeys)
	}

	// Create series key groupings w/ return error.
	groups := make([]struct {
		keys    []string
		filters []influxql.Expr
		itrs    []query.Iterator
		err     error
	}, parallelism)

	// Group series keys.
	n := len(t.SeriesKeys) / parallelism
	for i := 0; i < parallelism; i++ {
		group := &groups[i]

		if i < parallelism-1 {
			group.keys = t.SeriesKeys[i*n : (i+1)*n]
			group.filters = t.Filters[i*n : (i+1)*n]
		} else {
			group.keys = t.SeriesKeys[i*n:]
			group.filters = t.Filters[i*n:]
		}
	}

	// Read series groups in parallel.
	var wg sync.WaitGroup
	for i := range groups {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			groups[i].itrs, groups[i].err = engine.createTagSetGroupIterators(ctx, ref, name, groups[i].keys, t, groups[i].filters, opt)
		}(i)
	}
	wg.Wait()

	// Determine total number of iterators so we can allocate only once.
	var itrN int
	for _, group := range groups {
		itrN += len(group.itrs)
	}

	// Combine all iterators together and check for errors.
	var err error
	itrs := make([]query.Iterator, 0, itrN)
	for _, group := range groups {
		if group.err != nil {
			err = group.err
		}
		itrs = append(itrs, group.itrs...)
	}

	// If an error occurred, make sure we close all created iterators.
	if err != nil {
		query.Iterators(itrs).Close()
		return nil, err
	}

	return itrs, nil
}

// creates a set of iterators for a subset of a tagset's series.
func (engine *Engine) createTagSetGroupIterators(ctx context.Context, ref *influxql.VarRef, name string, seriesKeys []string, t *query.TagSet, filters []influxql.Expr, opt query.IteratorOptions) ([]query.Iterator, error) {
	itrs := make([]query.Iterator, 0, len(seriesKeys))
	for i, seriesKey := range seriesKeys {
		var conditionFields []influxql.VarRef
		if filters[i] != nil {
			// Retrieve non-time fields from this series filter and filter out tags.
			conditionFields = influxql.ExprNames(filters[i])
		}

		itr, err := engine.createVarRefSeriesIterator(ctx, ref, name, seriesKey, t, filters[i], conditionFields, opt)
		if err != nil {
			return itrs, err
		} else if itr == nil {
			continue
		}
		itrs = append(itrs, itr)

		// Abort if the query was killed
		select {
		case <-opt.InterruptCh:
			query.Iterators(itrs).Close()
			return nil, query.ErrQueryInterrupted
		default:
		}

		// Enforce series limit at creation time.
		if opt.MaxSeriesN > 0 && len(itrs) > opt.MaxSeriesN {
			query.Iterators(itrs).Close()
			return nil, fmt.Errorf("max-select-series limit exceeded: (%d/%d)", len(itrs), opt.MaxSeriesN)
		}

	}
	return itrs, nil
}

// creates an iterator for a variable reference for a series.
func (engine *Engine) createVarRefSeriesIterator(ctx context.Context, ref *influxql.VarRef, name string, seriesKey string, t *query.TagSet, filter influxql.Expr, conditionFields []influxql.VarRef, opt query.IteratorOptions) (query.Iterator, error) {
	_, tfs := models.ParseKey([]byte(seriesKey))
	tags := query.NewTags(tfs.Map())

	// Create options specific for this series.
	itrOpt := opt
	itrOpt.Condition = filter

	var curCounter, auxCounter, condCounter *metrics.Counter
	if col := metrics.GroupFromContext(ctx); col != nil {
		curCounter = col.GetCounter(numberOfRefCursorsCounter)
		auxCounter = col.GetCounter(numberOfAuxCursorsCounter)
		condCounter = col.GetCounter(numberOfCondCursorsCounter)
	}

	// Build main cursor.
	var cur cursor
	if ref != nil {
		cur = engine.buildCursor(ctx, name, seriesKey, tfs, ref, opt)
		// If the field doesn't exist then don't build an iterator.
		if cur == nil {
			return nil, nil
		}
		if curCounter != nil {
			curCounter.Add(1)
		}
	}

	// Build auxiliary cursors.
	// Tag values should be returned if the field doesn't exist.
	var aux []cursorAt
	if len(opt.Aux) > 0 {
		aux = make([]cursorAt, len(opt.Aux))
		for i, ref := range opt.Aux {
			// Create cursor from field if a tag wasn't requested.
			if ref.Type != influxql.Tag {
				cur := engine.buildCursor(ctx, name, seriesKey, tfs, &ref, opt)
				if cur != nil {
					if auxCounter != nil {
						auxCounter.Add(1)
					}
					aux[i] = newBufCursor(cur, opt.Ascending)
					continue
				}

				// If a field was requested, use a nil cursor of the requested type.
				switch ref.Type {
				case influxql.Float, influxql.AnyField:
					aux[i] = nilFloatLiteralValueCursor
					continue
				case influxql.Integer:
					aux[i] = nilIntegerLiteralValueCursor
					continue
				case influxql.Unsigned:
					aux[i] = nilUnsignedLiteralValueCursor
					continue
				case influxql.String:
					aux[i] = nilStringLiteralValueCursor
					continue
				case influxql.Boolean:
					aux[i] = nilBooleanLiteralValueCursor
					continue
				}
			}

			// If field doesn't exist, use the tag value.
			if v := tags.Value(ref.Val); v == "" {
				// However, if the tag value is blank then return a null.
				aux[i] = nilStringLiteralValueCursor
			} else {
				aux[i] = &literalValueCursor{value: v}
			}
		}
	}

	// Remove _tagKey condition field.
	// We can't seach on it because we can't join it to _tagValue based on time.
	if varRefSliceContains(conditionFields, "_tagKey") {
		conditionFields = varRefSliceRemove(conditionFields, "_tagKey")

		// Remove _tagKey conditional references from iterator.
		itrOpt.Condition = influxql.RewriteExpr(influxql.CloneExpr(itrOpt.Condition), func(expr influxql.Expr) influxql.Expr {
			switch expr := expr.(type) {
			case *influxql.BinaryExpr:
				if ref, ok := expr.LHS.(*influxql.VarRef); ok && ref.Val == "_tagKey" {
					return &influxql.BooleanLiteral{Val: true}
				}
				if ref, ok := expr.RHS.(*influxql.VarRef); ok && ref.Val == "_tagKey" {
					return &influxql.BooleanLiteral{Val: true}
				}
			}
			return expr
		})
	}

	// Build conditional field cursors.
	// If a conditional field doesn't exist then ignore the series.
	var conds []cursorAt
	if len(conditionFields) > 0 {
		conds = make([]cursorAt, len(conditionFields))
		for i, ref := range conditionFields {
			// Create cursor from field if a tag wasn't requested.
			if ref.Type != influxql.Tag {
				cur := engine.buildCursor(ctx, name, seriesKey, tfs, &ref, opt)
				if cur != nil {
					if condCounter != nil {
						condCounter.Add(1)
					}
					conds[i] = newBufCursor(cur, opt.Ascending)
					continue
				}

				// If a field was requested, use a nil cursor of the requested type.
				switch ref.Type {
				case influxql.Float, influxql.AnyField:
					conds[i] = nilFloatLiteralValueCursor
					continue
				case influxql.Integer:
					conds[i] = nilIntegerLiteralValueCursor
					continue
				case influxql.Unsigned:
					conds[i] = nilUnsignedLiteralValueCursor
					continue
				case influxql.String:
					conds[i] = nilStringLiteralValueCursor
					continue
				case influxql.Boolean:
					conds[i] = nilBooleanLiteralValueCursor
					continue
				}
			}

			// If field doesn't exist, use the tag value.
			if v := tags.Value(ref.Val); v == "" {
				// However, if the tag value is blank then return a null.
				conds[i] = nilStringLiteralValueCursor
			} else {
				conds[i] = &literalValueCursor{value: v}
			}
		}
	}
	condNames := influxql.VarRefs(conditionFields).Strings()

	// Limit tags to only the dimensions selected.
	dimensions := opt.GetDimensions()
	tags = tags.Subset(dimensions)

	// If it's only auxiliary fields then it doesn't matter what type of iterator we use.
	if ref == nil {
		if opt.StripName {
			name = ""
		}
		return newFloatIterator(name, tags, itrOpt, nil, aux, conds, condNames), nil
	}

	// Remove name if requested.
	if opt.StripName {
		name = ""
	}

	switch cur := cur.(type) {
	case floatCursor:
		return newFloatIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	case integerCursor:
		return newIntegerIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	case unsignedCursor:
		return newUnsignedIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	case stringCursor:
		return newStringIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	case booleanCursor:
		return newBooleanIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	default:
		panic("unreachable")
	}
}

// buildCursor creates an untyped cursor for a field.
func (engine *Engine) buildCursor(ctx context.Context, measurement, seriesKey string, tags models.Tags, ref *influxql.VarRef, opt query.IteratorOptions) cursor {
	// Check if this is a system field cursor.
	switch ref.Val {
	case "_name":
		return &stringSliceCursor{values: []string{measurement}}
	case "_tagKey":
		return &stringSliceCursor{values: tags.Keys()}
	case "_tagValue":
		return &stringSliceCursor{values: matchTagValues(tags, opt.Condition)}
	case "_seriesKey":
		return &stringSliceCursor{values: []string{seriesKey}}
	}

	// Look up fields for measurement.
	mf := engine.fieldset.FieldsByString(measurement)
	if mf == nil {
		return nil
	}

	// Check for system field for field keys.
	if ref.Val == "_fieldKey" {
		return &stringSliceCursor{values: mf.FieldKeys()}
	}

	// Find individual field.
	f := mf.Field(ref.Val)
	if f == nil {
		return nil
	}

	// Check if we need to perform a cast. Performing a cast in the
	// engine (if it is possible) is much more efficient than an automatic cast.
	if ref.Type != influxql.Unknown && ref.Type != influxql.AnyField && ref.Type != f.Type {
		switch ref.Type {
		case influxql.Float:
			switch f.Type {
			case influxql.Integer:
				cur := engine.buildIntegerCursor(ctx, measurement, seriesKey, ref.Val, opt)
				return &floatCastIntegerCursor{cursor: cur}
			case influxql.Unsigned:
				cur := engine.buildUnsignedCursor(ctx, measurement, seriesKey, ref.Val, opt)
				return &floatCastUnsignedCursor{cursor: cur}
			}
		case influxql.Integer:
			switch f.Type {
			case influxql.Float:
				cur := engine.buildFloatCursor(ctx, measurement, seriesKey, ref.Val, opt)
				return &integerCastFloatCursor{cursor: cur}
			case influxql.Unsigned:
				cur := engine.buildUnsignedCursor(ctx, measurement, seriesKey, ref.Val, opt)
				return &integerCastUnsignedCursor{cursor: cur}
			}
		case influxql.Unsigned:
			switch f.Type {
			case influxql.Float:
				cur := engine.buildFloatCursor(ctx, measurement, seriesKey, ref.Val, opt)
				return &unsignedCastFloatCursor{cursor: cur}
			case influxql.Integer:
				cur := engine.buildIntegerCursor(ctx, measurement, seriesKey, ref.Val, opt)
				return &unsignedCastIntegerCursor{cursor: cur}
			}
		}
		return nil
	}

	// Return appropriate cursor based on type.
	switch f.Type {
	case influxql.Float:
		return engine.buildFloatCursor(ctx, measurement, seriesKey, ref.Val, opt)
	case influxql.Integer:
		return engine.buildIntegerCursor(ctx, measurement, seriesKey, ref.Val, opt)
	case influxql.Unsigned:
		return engine.buildUnsignedCursor(ctx, measurement, seriesKey, ref.Val, opt)
	case influxql.String:
		return engine.buildStringCursor(ctx, measurement, seriesKey, ref.Val, opt)
	case influxql.Boolean:
		return engine.buildBooleanCursor(ctx, measurement, seriesKey, ref.Val, opt)
	default:
		panic("unreachable")
	}
}

func matchTagValues(tags models.Tags, condition influxql.Expr) []string {
	if condition == nil {
		return tags.Values()
	}

	// Populate map with tag values.
	data := map[string]interface{}{}
	for _, tag := range tags {
		data[string(tag.Key)] = string(tag.Value)
	}

	// Match against each specific tag.
	var values []string
	for _, tag := range tags {
		data["_tagKey"] = string(tag.Key)
		if influxql.EvalBool(condition, data) {
			values = append(values, string(tag.Value))
		}
	}
	return values
}

// IteratorCost produces the cost of an iterator.
func (engine *Engine) IteratorCost(measurement string, opt query.IteratorOptions) (query.IteratorCost, error) {
	// Determine if this measurement exists. If it does not, then no shards are
	// accessed to begin with.
	if exists, err := engine.index.MeasurementExists([]byte(measurement)); err != nil {
		return query.IteratorCost{}, err
	} else if !exists {
		return query.IteratorCost{}, nil
	}

	// Determine all of the tag sets for this query.
	indexSet := tsdb.IndexSet{Indexes: []tsdb.Index{engine.index}, SeriesFile: engine.sfile}
	tagSets, err := indexSet.TagSets(engine.sfile, []byte(measurement), opt)
	if err != nil {
		return query.IteratorCost{}, err
	}

	// Attempt to retrieve the ref from the main expression (if it exists).
	var ref *influxql.VarRef
	if opt.Expr != nil {
		if v, ok := opt.Expr.(*influxql.VarRef); ok {
			ref = v
		} else if call, ok := opt.Expr.(*influxql.Call); ok {
			if len(call.Args) > 0 {
				ref, _ = call.Args[0].(*influxql.VarRef)
			}
		}
	}

	// Count the number of series concatenated from the tag set.
	cost := query.IteratorCost{NumShards: 1}
	for _, t := range tagSets {
		cost.NumSeries += int64(len(t.SeriesKeys))
		for i, key := range t.SeriesKeys {
			// Retrieve the cost for the main expression (if it exists).
			if ref != nil {
				c := engine.seriesCost(key, ref.Val, opt.StartTime, opt.EndTime)
				cost = cost.Combine(c)
			}

			// Retrieve the cost for every auxiliary field since these are also
			// iterators that we may have to look through.
			// We may want to separate these though as we are unlikely to incur
			// anywhere close to the full costs of the auxiliary iterators because
			// many of the selected values are usually skipped.
			for _, ref := range opt.Aux {
				c := engine.seriesCost(key, ref.Val, opt.StartTime, opt.EndTime)
				cost = cost.Combine(c)
			}

			// Retrieve the expression names in the condition (if there is a condition).
			// We will also create cursors for these too.
			if t.Filters[i] != nil {
				refs := influxql.ExprNames(t.Filters[i])
				for _, ref := range refs {
					c := engine.seriesCost(key, ref.Val, opt.StartTime, opt.EndTime)
					cost = cost.Combine(c)
				}
			}
		}
	}
	return cost, nil
}

// Type returns FieldType for a series.  If the series does not
// exist, ErrUnknownFieldType is returned.
func (engine *Engine) Type(series []byte) (models.FieldType, error) {
	if typ, err := engine.Cache.Type(series); err == nil {
		return typ, nil
	}

	typ, err := engine.FileStore.Type(series)
	if err != nil {
		return 0, err
	}
	switch typ {
	case BlockFloat64:
		return models.Float, nil
	case BlockInteger:
		return models.Integer, nil
	case BlockUnsigned:
		return models.Unsigned, nil
	case BlockString:
		return models.String, nil
	case BlockBoolean:
		return models.Boolean, nil
	}
	return 0, tsdb.ErrUnknownFieldType
}

func (engine *Engine) seriesCost(seriesKey, field string, tmin, tmax int64) query.IteratorCost {
	key := SeriesFieldKeyBytes(seriesKey, field)
	c := engine.FileStore.Cost(key, tmin, tmax)

	// Retrieve the range of values within the cache.
	cacheValues := engine.Cache.Values(key)
	c.CachedValues = int64(len(cacheValues.Include(tmin, tmax)))
	return c
}

// SeriesFieldKey combine a series key and field name for a unique string to be hashed to a numeric ID.
func SeriesFieldKey(seriesKey, field string) string {
	return seriesKey + keyFieldSeparator + field
}

func SeriesFieldKeyBytes(seriesKey, field string) []byte {
	b := make([]byte, len(seriesKey)+len(keyFieldSeparator)+len(field))
	i := copy(b, seriesKey)
	i += copy(b[i:], keyFieldSeparatorBytes)
	copy(b[i:], field)
	return b
}

var (
	blockToFieldType = [8]influxql.DataType{
		BlockFloat64:  influxql.Float,
		BlockInteger:  influxql.Integer,
		BlockBoolean:  influxql.Boolean,
		BlockString:   influxql.String,
		BlockUnsigned: influxql.Unsigned,
		5:             influxql.Unknown,
		6:             influxql.Unknown,
		7:             influxql.Unknown,
	}
)

func BlockTypeToInfluxQLDataType(typ byte) influxql.DataType { return blockToFieldType[typ&7] }

// SeriesAndFieldFromCompositeKey returns the series key and the field key extracted from the composite key.
func SeriesAndFieldFromCompositeKey(key []byte) ([]byte, []byte) {
	series, field, _ := bytes.Cut(key, keyFieldSeparatorBytes)
	return series, field
}

func varRefSliceContains(a []influxql.VarRef, v string) bool {
	for _, ref := range a {
		if ref.Val == v {
			return true
		}
	}
	return false
}

func varRefSliceRemove(a []influxql.VarRef, v string) []influxql.VarRef {
	if !varRefSliceContains(a, v) {
		return a
	}

	other := make([]influxql.VarRef, 0, len(a))
	for _, ref := range a {
		if ref.Val != v {
			other = append(other, ref)
		}
	}
	return other
}

const reindexBatchSize = 10000

func (engine *Engine) Reindex() error {
	keys := make([][]byte, reindexBatchSize)
	seriesKeys := make([][]byte, reindexBatchSize)
	names := make([][]byte, reindexBatchSize)
	tags := make([]models.Tags, reindexBatchSize)

	n := 0

	reindexBatch := func() error {
		if n == 0 {
			return nil
		}

		for i, key := range keys[:n] {
			seriesKeys[i], _ = SeriesAndFieldFromCompositeKey(key)
			names[i], tags[i] = models.ParseKeyBytes(seriesKeys[i])
			engine.traceLogger.Debug(
				"Read series during reindexing",
				logger.Shard(engine.id),
				zap.String("name", string(names[i])),
				zap.String("tags", tags[i].String()),
			)
		}

		engine.logger.Debug("Reindexing data batch", logger.Shard(engine.id), zap.Int("batch_size", n))
		if err := engine.index.CreateSeriesListIfNotExists(seriesKeys[:n], names[:n], tags[:n]); err != nil {
			return err
		}

		n = 0
		return nil
	}
	reindexKey := func(key []byte) error {
		keys[n] = key
		n++

		if n < reindexBatchSize {
			return nil
		}
		return reindexBatch()
	}

	// Index data stored in TSM files.
	engine.logger.Info("Reindexing TSM data", logger.Shard(engine.id))
	if err := engine.FileStore.WalkKeys(nil, func(key []byte, _ byte) error {
		return reindexKey(key)
	}); err != nil {
		return err
	}

	// Make sure all TSM data is indexed.
	if err := reindexBatch(); err != nil {
		return err
	}

	if !engine.WALEnabled {
		// All done.
		return nil
	}

	// Reindex data stored in the WAL cache.
	engine.logger.Info("Reindexing WAL data", logger.Shard(engine.id))
	for _, key := range engine.Cache.Keys() {
		if err := reindexKey(key); err != nil {
			return err
		}
	}

	// Make sure all WAL data is indexed.
	return reindexBatch()
}
