package tsm1

import (
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// ringShards specifies the number of partitions that the hash ring used to
// store the entry mappings contains. It must be a power of 2. From empirical
// testing, a value above the number of cores on the machine does not provide
// any additional benefit. For now we'll set it to the number of cores on the
// largest box we could imagine running influx.
const ringShards = 16

var (
	// ErrSnapshotInProgress is returned if a snapshot is attempted while one is already running.
	ErrSnapshotInProgress = fmt.Errorf("snapshot in progress")
)

// ErrCacheMemorySizeLimitExceeded returns an error indicating an operation
// could not be completed due to exceeding the cache-max-memory-size setting.
func ErrCacheMemorySizeLimitExceeded(n, limit uint64) error {
	return fmt.Errorf("cache-max-memory-size exceeded: (%d/%d)", n, limit)
}

// entry is a set of values and some metadata.
type entry struct {
	mu     sync.RWMutex
	values Values // All stored values.

	// The type of values stored. Read only so doesn't need to be protected by
	// mu.
	vtype byte
}

// newEntryValues returns a new instance of entry with the given values.  If the
// values are not valid, an error is returned.
func newEntryValues(values []Value) (*entry, error) {
	e := &entry{}
	e.values = make(Values, 0, len(values))
	e.values = append(e.values, values...)

	// No values, don't check types and ordering
	if len(values) == 0 {
		return e, nil
	}

	et := valueType(values[0])
	for _, v := range values {
		// Make sure all the values are the same type
		if et != valueType(v) {
			return nil, tsdb.ErrFieldTypeConflict
		}
	}

	// Set the type of values stored.
	e.vtype = et

	return e, nil
}

// add adds the given values to the entry.
func (e *entry) add(values []Value) error {
	if len(values) == 0 {
		return nil // Nothing to do.
	}

	// Are any of the new values the wrong type?
	if e.vtype != 0 {
		for _, v := range values {
			if e.vtype != valueType(v) {
				return tsdb.ErrFieldTypeConflict
			}
		}
	}

	// entry currently has no values, so add the new ones and we're done.
	e.mu.Lock()
	if len(e.values) == 0 {
		e.values = values
		e.vtype = valueType(values[0])
		e.mu.Unlock()
		return nil
	}

	// Append the new values to the existing ones...
	e.values = append(e.values, values...)
	e.mu.Unlock()
	return nil
}

// deduplicate sorts and orders the entry's values. If values are already deduped and sorted,
// the function does no work and simply returns.
func (e *entry) deduplicate() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.values) <= 1 {
		return
	}
	e.values = e.values.Deduplicate()
}

// count returns the number of values in this entry.
func (e *entry) count() int {
	e.mu.RLock()
	n := len(e.values)
	e.mu.RUnlock()
	return n
}

// filter removes all values with timestamps between min and max inclusive.
func (e *entry) filter(min, max int64) {
	e.mu.Lock()
	if len(e.values) > 1 {
		e.values = e.values.Deduplicate()
	}
	e.values = e.values.Exclude(min, max)
	e.mu.Unlock()
}

// size returns the size of this entry in bytes.
func (e *entry) size() int {
	e.mu.RLock()
	sz := e.values.Size()
	e.mu.RUnlock()
	return sz
}

// InfluxQLType returns for the entry the data type of its values.
func (e *entry) InfluxQLType() (influxql.DataType, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.values.InfluxQLType()
}

// storer is the interface that descibes a cache's store.
type storer interface {
	entry(key []byte) *entry                        // Get an entry by its key.
	write(key []byte, values Values) (bool, error)  // Write an entry to the store.
	remove(key []byte)                              // Remove an entry from the store.
	keys(sorted bool) [][]byte                      // Return an optionally sorted slice of entry keys.
	apply(f func([]byte, *entry) error) error       // Apply f to all entries in the store in parallel.
	applySerial(f func([]byte, *entry) error) error // Apply f to all entries in serial.
	reset()                                         // Reset the store to an initial unused state.
	split(n int) []storer                           // Split splits the store into n stores
	count() int                                     // Count returns the number of keys in the store
}

// Cache maintains an in-memory store of Values for a set of keys.
type Cache struct {
	// Due to a bug in atomic  size needs to be the first word in the struct, as
	// that's the only place where you're guaranteed to be 64-bit aligned on a
	// 32 bit system. See: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	size         uint64
	snapshotSize uint64

	mu      sync.RWMutex
	store   storer
	maxSize uint64 // 对应 storage-cache-max-memory-size

	// snapshots are the cache objects that are currently being written to tsm files
	// they're kept in memory while flushing so they can be queried along with the cache.
	// they are read only and should never be modified
	snapshot     *Cache
	snapshotting bool

	// This number is the number of pending or failed WriteSnaphot attempts since the last successful one.
	snapshotAttempts int

	stats         *cacheMetrics
	lastWriteTime time.Time

	// A one time synchronization used to initial the cache with a store.  Since the store can allocate a
	// large amount memory across shards, we lazily create it.
	initialize       atomic.Value
	initializedCount uint32
}

// NewCache returns an instance of a cache which will use a maximum of maxSize bytes of memory.
// Only used for engine caches, never for snapshots.
// Note tags are for metrics only, so if metrics are not desired tags do not have to be set.
func NewCache(maxSize uint64, tags tsdb.EngineTags) *Cache {
	c := &Cache{
		maxSize: maxSize,
		store:   emptyStore{},
		stats:   newCacheMetrics(tags),
	}
	c.stats.LastSnapshot.SetToCurrentTime()
	c.initialize.Store(&sync.Once{})
	return c
}

var globalCacheMetrics = newAllCacheMetrics()

const cacheSubsystem = "cache"

type allCacheMetrics struct {
	MemBytes     *prometheus.GaugeVec
	DiskBytes    *prometheus.GaugeVec
	LastSnapshot *prometheus.GaugeVec
	Writes       *prometheus.CounterVec
	WriteErr     *prometheus.CounterVec
	WriteDropped *prometheus.CounterVec
}

type cacheMetrics struct {
	MemBytes     prometheus.Gauge
	DiskBytes    prometheus.Gauge
	LastSnapshot prometheus.Gauge
	Writes       prometheus.Counter
	WriteErr     prometheus.Counter
	WriteDropped prometheus.Counter
}

func newAllCacheMetrics() *allCacheMetrics {
	labels := tsdb.EngineLabelNames()
	return &allCacheMetrics{
		MemBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: cacheSubsystem,
			Name:      "inuse_bytes",
			Help:      "Gauge of current memory consumption of cache",
		}, labels),
		DiskBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: cacheSubsystem,
			Name:      "disk_bytes",
			Help:      "Gauge of size of most recent snapshot",
		}, labels),
		LastSnapshot: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: cacheSubsystem,
			Name:      "latest_snapshot",
			Help:      "Unix time of most recent snapshot",
		}, labels),
		Writes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: cacheSubsystem,
			Name:      "writes_total",
			Help:      "Counter of all writes to cache",
		}, labels),
		WriteErr: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: cacheSubsystem,
			Name:      "writes_err",
			Help:      "Counter of failed writes to cache",
		}, labels),
		WriteDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: cacheSubsystem,
			Name:      "writes_dropped",
			Help:      "Counter of writes to cache with some dropped points",
		}, labels),
	}
}

func CacheCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		globalCacheMetrics.MemBytes,
		globalCacheMetrics.DiskBytes,
		globalCacheMetrics.LastSnapshot,
		globalCacheMetrics.Writes,
		globalCacheMetrics.WriteErr,
		globalCacheMetrics.WriteDropped,
	}
}

func newCacheMetrics(tags tsdb.EngineTags) *cacheMetrics {
	labels := tags.GetLabels()
	return &cacheMetrics{
		MemBytes:     globalCacheMetrics.MemBytes.With(labels),
		DiskBytes:    globalCacheMetrics.DiskBytes.With(labels),
		LastSnapshot: globalCacheMetrics.LastSnapshot.With(labels),
		Writes:       globalCacheMetrics.Writes.With(labels),
		WriteErr:     globalCacheMetrics.WriteErr.With(labels),
		WriteDropped: globalCacheMetrics.WriteDropped.With(labels),
	}
}

// initializes the cache and allocates the underlying store.
// Once initialized, the store re-used until Freed.
func (cache *Cache) init() {
	if !atomic.CompareAndSwapUint32(&cache.initializedCount, 0, 1) {
		return
	}

	cache.mu.Lock()
	cache.store, _ = newring(ringShards)
	cache.mu.Unlock()
}

// Free releases the underlying store and memory held by the Cache.
func (cache *Cache) Free() {
	if !atomic.CompareAndSwapUint32(&cache.initializedCount, 1, 0) {
		return
	}

	cache.mu.Lock()
	cache.store = emptyStore{}
	cache.mu.Unlock()
}

// writes the map of keys and associated values to the cache. This
// function is goroutine-safe. It returns an error if the cache will exceeded
// its max size by adding the new values.  The write attempts to write as many
// values as possible.  If one key fails, the others can still succeed and an
// error will be returned.
func (cache *Cache) WriteMulti(values map[string][]Value) error {
	cache.init()
	cache.stats.Writes.Inc()
	var addedSize uint64
	for _, v := range values {
		addedSize += uint64(Values(v).Size())
	}

	// Enough room in the cache?
	limit := cache.maxSize // maxSize is safe for reading without a lock.
	n := cache.Size() + addedSize
	if limit > 0 && n > limit {
		cache.stats.WriteErr.Inc()
		return ErrCacheMemorySizeLimitExceeded(n, limit)
	}

	var werr error
	cache.mu.RLock()
	store := cache.store
	cache.mu.RUnlock()

	// We'll optimistically set size here, and then decrement it for write errors.
	cache.increaseSize(addedSize)
	for k, v := range values {
		newKey, err := store.write([]byte(k), v)
		if err != nil {
			// The write failed, hold onto the error and adjust the size delta.
			werr = err
			addedSize -= uint64(Values(v).Size())
			cache.decreaseSize(uint64(Values(v).Size()))
		}
		if newKey {
			addedSize += uint64(len(k))
			cache.increaseSize(uint64(len(k)))
		}
	}

	// Some points in the batch were dropped.  An error is returned so
	// error stat is incremented as well.
	if werr != nil {
		cache.stats.WriteDropped.Inc()
		cache.stats.WriteErr.Inc()
	}

	// Update the memory size stat
	cache.stats.MemBytes.Set(float64(cache.Size()))

	cache.mu.Lock()
	cache.lastWriteTime = time.Now()
	cache.mu.Unlock()

	return werr
}

// takes a snapshot of the current cache, adds it to the slice of caches that
// are being flushed, and resets the current cache with new values.
func (cache *Cache) Snapshot() (*Cache, error) {
	cache.init()

	cache.mu.Lock()
	defer cache.mu.Unlock()

	if cache.snapshotting {
		return nil, ErrSnapshotInProgress
	}

	cache.snapshotting = true
	cache.snapshotAttempts++ // increment the number of times we tried to do this

	// If no snapshot exists, create a new one, otherwise update the existing snapshot
	if cache.snapshot == nil {
		store, err := newring(ringShards)
		if err != nil {
			return nil, err
		}

		cache.snapshot = &Cache{
			store: store,
		}
	}

	// Did a prior snapshot exist that failed?  If so, return the existing
	// snapshot to retry.
	if cache.snapshot.Size() > 0 {
		return cache.snapshot, nil
	}

	cache.snapshot.store, cache.store = cache.store, cache.snapshot.store // 互换底部的store 想到了leveldb的memtable和immutable memtable
	snapshotSize := cache.Size()

	// Save the size of the snapshot on the snapshot cache
	atomic.StoreUint64(&cache.snapshot.size, snapshotSize)
	// Save the size of the snapshot on the live cache
	atomic.StoreUint64(&cache.snapshotSize, snapshotSize)

	// Reset the cache's store.
	cache.store.reset()
	atomic.StoreUint64(&cache.size, 0)
	cache.stats.LastSnapshot.SetToCurrentTime()

	return cache.snapshot, nil
}

// Deduplicate sorts the snapshot before returning it. The compactor and any queries
// coming in while it writes will need the values sorted.
func (cache *Cache) Deduplicate() {
	cache.mu.RLock()
	store := cache.store
	cache.mu.RUnlock()

	// Apply a function that simply calls deduplicate on each entry in the ring.
	// apply cannot return an error in this invocation.
	_ = store.apply(func(_ []byte, e *entry) error { e.deduplicate(); return nil })
}

// ClearSnapshot removes the snapshot cache from the list of flushing caches and
// adjusts the size.
func (cache *Cache) ClearSnapshot(success bool) {
	cache.init()

	cache.mu.RLock()
	snapStore := cache.snapshot.store
	cache.mu.RUnlock()

	// reset the snapshot store outside of the write lock
	if success {
		snapStore.reset()
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	cache.snapshotting = false

	if success {
		cache.snapshotAttempts = 0

		// Reset the snapshot to a fresh Cache.
		cache.snapshot = &Cache{
			store: cache.snapshot.store,
		}
		cache.stats.DiskBytes.Set(float64(atomic.LoadUint64(&cache.snapshotSize)))
		atomic.StoreUint64(&cache.snapshotSize, 0)
	}
	cache.stats.MemBytes.Set(float64(cache.Size()))
}

// Size returns the number of point-calcuated bytes the cache currently uses.
func (cache *Cache) Size() uint64 {
	return atomic.LoadUint64(&cache.size) + atomic.LoadUint64(&cache.snapshotSize)
}

// increaseSize increases size by delta.
func (cache *Cache) increaseSize(delta uint64) {
	atomic.AddUint64(&cache.size, delta)
}

// decreaseSize decreases size by delta.
func (cache *Cache) decreaseSize(delta uint64) {
	// Per sync/atomic docs, bit-flip delta minus one to perform subtraction within AddUint64.
	atomic.AddUint64(&cache.size, ^(delta - 1))
}

// MaxSize returns the maximum number of bytes the cache may consume.
func (cache *Cache) MaxSize() uint64 {
	return cache.maxSize
}

func (cache *Cache) Count() int {
	cache.mu.RLock()
	n := cache.store.count()
	cache.mu.RUnlock()
	return n
}

// Keys returns a sorted slice of all keys under management by the cache.
func (cache *Cache) Keys() [][]byte {
	cache.mu.RLock()
	store := cache.store
	cache.mu.RUnlock()
	return store.keys(true)
}

func (cache *Cache) Split(n int) []*Cache {
	if n == 1 {
		return []*Cache{cache}
	}

	caches := make([]*Cache, n)
	storers := cache.store.split(n)
	for i := 0; i < n; i++ {
		caches[i] = &Cache{
			store: storers[i],
		}
	}
	return caches
}

// Type returns the series type for a key.
func (cache *Cache) Type(key []byte) (models.FieldType, error) {
	cache.mu.RLock()
	e := cache.store.entry(key)
	if e == nil && cache.snapshot != nil {
		e = cache.snapshot.store.entry(key)
	}
	cache.mu.RUnlock()

	if e != nil {
		typ, err := e.InfluxQLType()
		if err != nil {
			return models.Empty, tsdb.ErrUnknownFieldType
		}

		switch typ {
		case influxql.Float:
			return models.Float, nil
		case influxql.Integer:
			return models.Integer, nil
		case influxql.Unsigned:
			return models.Unsigned, nil
		case influxql.Boolean:
			return models.Boolean, nil
		case influxql.String:
			return models.String, nil
		}
	}

	return models.Empty, tsdb.ErrUnknownFieldType
}

// Values returns a copy of all values, deduped and sorted, for the given key.
func (cache *Cache) Values(key []byte) Values {
	var snapshotEntries *entry

	cache.mu.RLock()
	e := cache.store.entry(key)
	if cache.snapshot != nil {
		snapshotEntries = cache.snapshot.store.entry(key)
	}
	cache.mu.RUnlock()

	if e == nil {
		if snapshotEntries == nil {
			// No values in hot cache or snapshots.
			return nil
		}
	} else {
		e.deduplicate()
	}

	// Build the sequence of entries that will be returned, in the correct order.
	// Calculate the required size of the destination buffer.
	var entries []*entry
	sz := 0

	if snapshotEntries != nil {
		snapshotEntries.deduplicate() // guarantee we are deduplicated
		entries = append(entries, snapshotEntries)
		sz += snapshotEntries.count()
	}

	if e != nil {
		entries = append(entries, e)
		sz += e.count()
	}

	// Any entries? If not, return.
	if sz == 0 {
		return nil
	}

	// Create the buffer, and copy all hot values and snapshots. Individual
	// entries are sorted at this point, so now the code has to check if the
	// resultant buffer will be sorted from start to finish.
	values := make(Values, sz)
	n := 0
	for _, e := range entries {
		e.mu.RLock()
		n += copy(values[n:], e.values)
		e.mu.RUnlock()
	}
	values = values[:n]
	values = values.Deduplicate()

	return values
}

// removes all values for the given keys from the cache.
func (cache *Cache) Delete(keys [][]byte) {
	cache.DeleteRange(keys, math.MinInt64, math.MaxInt64)
}

// removes the values for all keys containing points
// with timestamps between between min and max from the cache.
//
// TODO(edd): Lock usage could possibly be optimised if necessary.
func (cache *Cache) DeleteRange(keys [][]byte, min, max int64) {
	cache.init()

	cache.mu.Lock()
	defer cache.mu.Unlock()

	for _, k := range keys {
		// Make sure key exist in the cache, skip if it does not
		e := cache.store.entry(k)
		if e == nil {
			continue
		}

		origSize := uint64(e.size())
		if min == math.MinInt64 && max == math.MaxInt64 {
			cache.decreaseSize(origSize + uint64(len(k)))
			cache.store.remove(k)
			continue
		}

		e.filter(min, max)
		if e.count() == 0 {
			cache.store.remove(k)
			cache.decreaseSize(origSize + uint64(len(k)))
			continue
		}

		cache.decreaseSize(origSize - uint64(e.size()))
	}
	cache.stats.MemBytes.Set(float64(cache.Size()))
}

// SetMaxSize updates the memory limit of the cache.
func (cache *Cache) SetMaxSize(size uint64) {
	cache.mu.Lock()
	cache.maxSize = size
	cache.mu.Unlock()
}

// values returns the values for the key. It assumes the data is already sorted.
// It doesn't lock the cache but it does read-lock the entry if there is one for the key.
// values should only be used in compact.go in the CacheKeyIterator.
func (cache *Cache) values(key []byte) Values {
	e := cache.store.entry(key)
	if e == nil {
		return nil
	}
	e.mu.RLock()
	v := e.values
	e.mu.RUnlock()
	return v
}

// ApplyEntryFn applies the function f to each entry in the Cache.
// ApplyEntryFn calls f on each entry in turn, within the same goroutine.
// It is safe for use by multiple goroutines.
func (cache *Cache) ApplyEntryFn(f func(key []byte, entry *entry) error) error {
	cache.mu.RLock()
	store := cache.store
	cache.mu.RUnlock()
	return store.applySerial(f)
}

// processes a set of WAL segment files, and loads a cache with the data contained within those files
// Processing of the supplied files take place in the
// order they exist in the files slice.
type CacheLoader struct {
	files []string

	Logger *zap.Logger
}

// returns a new instance of a CacheLoader.
func NewCacheLoader(files []string) *CacheLoader {
	return &CacheLoader{
		files:  files,
		Logger: zap.NewNop(),
	}
}

// returns a cache loaded with the data contained within the segment files.
// If, during reading of a segment file, corruption is encountered, that segment
// file is truncated up to and including the last valid byte, and processing
// continues with the next segment file.
func (cacheLoader *CacheLoader) Load(cache *Cache) error {

	var walSegmentReader *WALSegmentReader
	for _, walFilePath := range cacheLoader.files {
		if err := func() error {
			walFile, err := os.OpenFile(walFilePath, os.O_CREATE|os.O_RDWR, 0666)
			if err != nil {
				return err
			}
			defer walFile.Close()

			// Log some information about the segments.
			stat, err := os.Stat(walFile.Name())
			if err != nil {
				return err
			}
			cacheLoader.Logger.Info("Reading file", zap.String("path", walFile.Name()), zap.Int64("size", stat.Size()))

			// Nothing to read, skip it
			if stat.Size() == 0 {
				return nil
			}

			if walSegmentReader == nil {
				walSegmentReader = NewWALSegmentReader(walFile)
				defer walSegmentReader.Close()
			} else {
				walSegmentReader.Reset(walFile)
			}

			for walSegmentReader.Next() {
				entry, err := walSegmentReader.Read()
				if err != nil {
					n := walSegmentReader.Count()
					cacheLoader.Logger.Info("File corrupt", zap.Error(err), zap.String("path", walFile.Name()), zap.Int64("pos", n))
					if err := walFile.Truncate(n); err != nil {
						return err
					}
					break
				}

				switch t := entry.(type) {
				case *WriteWALEntry:
					if err := cache.WriteMulti(t.Values); err != nil {
						return err
					}
				case *DeleteRangeWALEntry:
					cache.DeleteRange(t.Keys, t.Min, t.Max)
				case *DeleteWALEntry:
					cache.Delete(t.Keys)
				}
			}

			return walSegmentReader.Close()
		}(); err != nil {
			return err
		}
	}
	return nil
}

// WithLogger sets the logger on the CacheLoader.
func (cacheLoader *CacheLoader) WithLogger(log *zap.Logger) {
	cacheLoader.Logger = log.With(zap.String("service", "cacheloader"))
}

func (cache *Cache) LastWriteTime() time.Time {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cache.lastWriteTime
}

const (
	valueTypeUndefined = 0
	valueTypeFloat64   = 1
	valueTypeInteger   = 2
	valueTypeString    = 3
	valueTypeBoolean   = 4
	valueTypeUnsigned  = 5
)

func valueType(v Value) byte {
	switch v.(type) {
	case FloatValue:
		return valueTypeFloat64
	case IntegerValue:
		return valueTypeInteger
	case StringValue:
		return valueTypeString
	case BooleanValue:
		return valueTypeBoolean
	case UnsignedValue:
		return valueTypeUnsigned
	default:
		return valueTypeUndefined
	}
}

type emptyStore struct{}

func (e emptyStore) entry(key []byte) *entry                        { return nil }
func (e emptyStore) write(key []byte, values Values) (bool, error)  { return false, nil }
func (e emptyStore) remove(key []byte)                              {}
func (e emptyStore) keys(sorted bool) [][]byte                      { return nil }
func (e emptyStore) apply(f func([]byte, *entry) error) error       { return nil }
func (e emptyStore) applySerial(f func([]byte, *entry) error) error { return nil }
func (e emptyStore) reset()                                         {}
func (e emptyStore) split(n int) []storer                           { return nil }
func (e emptyStore) count() int                                     { return 0 }
