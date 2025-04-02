package tsm1

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/pkg/file"
	"github.com/influxdata/influxdb/v2/pkg/limiter"
	"github.com/influxdata/influxdb/v2/pkg/metrics"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// The extension used to describe temporary snapshot files.
	TmpTSMFileExtension = "tmp"

	// The extension used to describe corrupt snapshot files.
	BadTSMFileExtension = "bad"
)

// TSMFile represents an on-disk TSM file.
type TSMFile interface {
	// Path returns the underlying file path for the TSMFile.  If the file
	// has not be written or loaded from disk, the zero value is returned.
	Path() string

	// Read returns all the values in the block where time t resides.
	Read(key []byte, t int64) ([]Value, error)

	// ReadAt returns all the values in the block identified by entry.
	ReadAt(entry *IndexEntry, values []Value) ([]Value, error)
	ReadFloatBlockAt(entry *IndexEntry, values *[]FloatValue) ([]FloatValue, error)
	ReadFloatArrayBlockAt(entry *IndexEntry, values *tsdb.FloatArray) error
	ReadIntegerBlockAt(entry *IndexEntry, values *[]IntegerValue) ([]IntegerValue, error)
	ReadIntegerArrayBlockAt(entry *IndexEntry, values *tsdb.IntegerArray) error
	ReadUnsignedBlockAt(entry *IndexEntry, values *[]UnsignedValue) ([]UnsignedValue, error)
	ReadUnsignedArrayBlockAt(entry *IndexEntry, values *tsdb.UnsignedArray) error
	ReadStringBlockAt(entry *IndexEntry, values *[]StringValue) ([]StringValue, error)
	ReadStringArrayBlockAt(entry *IndexEntry, values *tsdb.StringArray) error
	ReadBooleanBlockAt(entry *IndexEntry, values *[]BooleanValue) ([]BooleanValue, error)
	ReadBooleanArrayBlockAt(entry *IndexEntry, values *tsdb.BooleanArray) error

	// Entries returns the index entries for all blocks for the given key.
	Entries(key []byte) []IndexEntry
	ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry

	// Returns true if the TSMFile may contain a value with the specified
	// key and time.
	ContainsValue(key []byte, t int64) bool

	// Contains returns true if the file contains any values for the given
	// key.
	Contains(key []byte) bool

	// OverlapsTimeRange returns true if the time range of the file intersect min and max.
	OverlapsTimeRange(min, max int64) bool

	// OverlapsKeyRange returns true if the key range of the file intersects min and max.
	OverlapsKeyRange(min, max []byte) bool

	// TimeRange returns the min and max time across all keys in the file.
	TimeRange() (int64, int64)

	// TombstoneRange returns ranges of time that are deleted for the given key.
	TombstoneRange(key []byte) []TimeRange

	// KeyRange returns the min and max keys in the file.
	KeyRange() ([]byte, []byte)

	// KeyCount returns the number of distinct keys in the file.
	KeyCount() int

	// Seek returns the position in the index with the key <= key.
	Seek(key []byte) int

	// KeyAt returns the key located at index position idx.
	KeyAt(idx int) ([]byte, byte)

	// Type returns the block type of the values stored for the key.  Returns one of
	// BlockFloat64, BlockInt64, BlockBoolean, BlockString.  If key does not exist,
	// an error is returned.
	Type(key []byte) (byte, error)

	// BatchDelete return a BatchDeleter that allows for multiple deletes in batches
	// and group commit or rollback.
	BatchDelete() BatchDeleter

	// Delete removes the keys from the set of keys available in this file.
	Delete(keys [][]byte) error

	// DeleteRange removes the values for keys between timestamps min and max.
	DeleteRange(keys [][]byte, min, max int64) error

	// HasTombstones returns true if file contains values that have been deleted.
	HasTombstones() bool

	// TombstoneStats returns the tombstone filestats if there are any tombstones
	// written for this file.
	TombstoneStats() TombstoneStat

	// Close closes the underlying file resources.
	Close() error

	// Size returns the size of the file on disk in bytes.
	Size() uint32

	// Rename renames the existing TSM file to a new name and replaces the mmap backing slice using the new
	// file name. Index and Reader state are not re-initialized.
	Rename(path string) error

	// Remove deletes the file from the filesystem.
	Remove() error

	// InUse returns true if the file is currently in use by queries.
	InUse() bool

	// Ref records that this file is actively in use.
	Ref()

	// Unref records that this file is no longer in use.
	Unref()

	// Stats returns summary information about the TSM file.
	Stats() FileStat

	// BlockIterator returns an iterator pointing to the first block in the file and
	// allows sequential iteration to each and every block.
	BlockIterator() *BlockIterator

	// Free releases any resources held by the FileStore to free up system resources.
	Free() error
}

var (
	floatBlocksDecodedCounter    = metrics.MustRegisterCounter("float_blocks_decoded", metrics.WithGroup(tsmGroup))
	floatBlocksSizeCounter       = metrics.MustRegisterCounter("float_blocks_size_bytes", metrics.WithGroup(tsmGroup))
	integerBlocksDecodedCounter  = metrics.MustRegisterCounter("integer_blocks_decoded", metrics.WithGroup(tsmGroup))
	integerBlocksSizeCounter     = metrics.MustRegisterCounter("integer_blocks_size_bytes", metrics.WithGroup(tsmGroup))
	unsignedBlocksDecodedCounter = metrics.MustRegisterCounter("unsigned_blocks_decoded", metrics.WithGroup(tsmGroup))
	unsignedBlocksSizeCounter    = metrics.MustRegisterCounter("unsigned_blocks_size_bytes", metrics.WithGroup(tsmGroup))
	stringBlocksDecodedCounter   = metrics.MustRegisterCounter("string_blocks_decoded", metrics.WithGroup(tsmGroup))
	stringBlocksSizeCounter      = metrics.MustRegisterCounter("string_blocks_size_bytes", metrics.WithGroup(tsmGroup))
	booleanBlocksDecodedCounter  = metrics.MustRegisterCounter("boolean_blocks_decoded", metrics.WithGroup(tsmGroup))
	booleanBlocksSizeCounter     = metrics.MustRegisterCounter("boolean_blocks_size_bytes", metrics.WithGroup(tsmGroup))
)

// abstraction around multiple TSM files
type FileStore struct {
	mu           sync.RWMutex
	lastModified time.Time
	// Most recently known file stats. If nil then stats will need to be
	// recalculated
	lastFileStats []FileStat

	currentGeneration int
	dirPath           string // 其实是shard.path file_store.go:232

	tsmFileReaders  []TSMFile     // 其实是tsmFileReader
	tsmMMAPWillNeed bool          // If true then the kernel will be advised MMAP_WILLNEED for TSM files. 对应 storage-tsm-use-madv-willneed
	openLimiter     limiter.Fixed // limit the number of concurrent opening TSM files.

	logger       *zap.Logger // Logger to be used for important messages
	traceLogger  *zap.Logger // Logger to be used when trace-logging is on.
	traceLogging bool

	stats  *fileStoreMetrics
	purger *purger

	currentTempDirID int

	parseFileName ParseFileNameFunc

	obs tsdb.FileStoreObserver

	copyFiles bool
}

// FileStat holds information about a TSM file on disk.
type FileStat struct {
	Path             string
	HasTombstone     bool
	Size             uint32
	LastModified     int64
	MinTime, MaxTime int64
	MinKey, MaxKey   []byte
}

// TombstoneStat holds information about a possible tombstone file on disk.
type TombstoneStat struct {
	TombstoneExists bool
	Path            string
	LastModified    int64
	Size            uint32
}

// OverlapsTimeRange returns true if the time range of the file intersect min and max.
func (f FileStat) OverlapsTimeRange(min, max int64) bool {
	return f.MinTime <= max && f.MaxTime >= min
}

// OverlapsKeyRange returns true if the min and max keys of the file overlap the arguments min and max.
func (f FileStat) OverlapsKeyRange(min, max []byte) bool {
	return len(min) != 0 && len(max) != 0 && bytes.Compare(f.MinKey, max) <= 0 && bytes.Compare(f.MaxKey, min) >= 0
}

// ContainsKey returns true if the min and max keys of the file overlap the arguments min and max.
func (f FileStat) ContainsKey(key []byte) bool {
	return bytes.Compare(f.MinKey, key) >= 0 || bytes.Compare(key, f.MaxKey) <= 0
}

// returns a new instance of FileStore based on the given directory.
func NewFileStore(dir string, tags tsdb.EngineTags) *FileStore {
	logger := zap.NewNop()
	fs := &FileStore{
		dirPath:      dir,
		lastModified: time.Time{},
		logger:       logger,
		traceLogger:  logger,
		openLimiter:  limiter.NewFixed(runtime.GOMAXPROCS(0)),
		stats:        newFileStoreMetrics(tags),
		purger: &purger{
			files:  map[string]TSMFile{},
			logger: logger,
		},
		obs:           noFileStoreObserver{},
		parseFileName: DefaultParseFileName,
		copyFiles:     runtime.GOOS == "windows",
	}
	fs.purger.fileStore = fs
	return fs
}

// WithObserver sets the observer for the file store.
func (fileStore *FileStore) WithObserver(obs tsdb.FileStoreObserver) {
	fileStore.obs = obs
}

func (fileStore *FileStore) WithParseFileNameFunc(parseFileNameFunc ParseFileNameFunc) {
	fileStore.parseFileName = parseFileNameFunc
}

func (fileStore *FileStore) ParseFileName(path string) (int, int, error) {
	return fileStore.parseFileName(path)
}

// enableTraceLogging must be called before the FileStore is opened.
func (fileStore *FileStore) enableTraceLogging(enabled bool) {
	fileStore.traceLogging = enabled
	if enabled {
		fileStore.traceLogger = fileStore.logger
	}
}

// WithLogger sets the logger on the file store.
func (fileStore *FileStore) WithLogger(log *zap.Logger) {
	fileStore.logger = log.With(zap.String("service", "filestore"))
	fileStore.purger.logger = fileStore.logger

	if fileStore.traceLogging {
		fileStore.traceLogger = fileStore.logger
	}
}

var globalFileStoreMetrics = newAllFileStoreMetrics()

const filesSubsystem = "tsm_files"

type allFileStoreMetrics struct {
	files *prometheus.GaugeVec
	size  *prometheus.GaugeVec
}

type fileStoreMetrics struct {
	files      prometheus.Gauge
	size       prometheus.Gauge
	sizeAtomic int64
}

func (f *fileStoreMetrics) AddSize(n int64) {
	val := atomic.AddInt64(&f.sizeAtomic, n)
	f.size.Set(float64(val))
}

func (f *fileStoreMetrics) SetSize(n int64) {
	atomic.StoreInt64(&f.sizeAtomic, n)
	f.size.Set(float64(n))
}

func (f *fileStoreMetrics) SetFiles(n int64) {
	f.files.Set(float64(n))
}

func newAllFileStoreMetrics() *allFileStoreMetrics {
	labels := tsdb.EngineLabelNames()
	return &allFileStoreMetrics{
		files: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: filesSubsystem,
			Name:      "total",
			Help:      "Gauge of number of files per shard",
		}, labels),
		size: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: filesSubsystem,
			Name:      "disk_bytes",
			Help:      "Gauge of data size in bytes for each shard",
		}, labels),
	}
}

func FileStoreCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		globalFileStoreMetrics.files,
		globalFileStoreMetrics.size,
	}
}

func newFileStoreMetrics(tags tsdb.EngineTags) *fileStoreMetrics {
	labels := tags.GetLabels()
	return &fileStoreMetrics{
		files: globalFileStoreMetrics.files.With(labels),
		size:  globalFileStoreMetrics.size.With(labels),
	}
}

// Count returns the number of TSM files currently loaded.
func (fileStore *FileStore) Count() int {
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()
	return len(fileStore.tsmFileReaders)
}

// Files returns the slice of TSM files currently loaded. This is only used for
// tests, and the files aren't guaranteed to stay valid in the presence of compactions.
func (fileStore *FileStore) Files() []TSMFile {
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()
	return fileStore.tsmFileReaders
}

// Free releases any resources held by the FileStore.  The resources will be re-acquired
// if necessary if they are needed after freeing them.
func (fileStore *FileStore) Free() error {
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()
	for _, f := range fileStore.tsmFileReaders {
		if err := f.Free(); err != nil {
			return err
		}
	}
	return nil
}

// CurrentGeneration returns the current generation of the TSM files.
func (fileStore *FileStore) CurrentGeneration() int {
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()
	return fileStore.currentGeneration
}

// NextGeneration increments the max file ID and returns the new value.
func (fileStore *FileStore) NextGeneration() int {
	fileStore.mu.Lock()
	defer fileStore.mu.Unlock()
	fileStore.currentGeneration++
	return fileStore.currentGeneration
}

// WalkKeys calls fn for every key in every TSM file known to the FileStore.  If the key
// exists in multiple files, it will be invoked for each file.
func (fileStore *FileStore) WalkKeys(seek []byte, fn func(key []byte, typ byte) error) error {
	fileStore.mu.RLock()
	if len(fileStore.tsmFileReaders) == 0 {
		fileStore.mu.RUnlock()
		return nil
	}

	// Ensure files are not unmapped while we're iterating over them.
	for _, r := range fileStore.tsmFileReaders {
		r.Ref()
		defer r.Unref()
	}

	ki := newMergeKeyIterator(fileStore.tsmFileReaders, seek)
	fileStore.mu.RUnlock()
	for ki.Next() {
		key, typ := ki.Read()
		if err := fn(key, typ); err != nil {
			return err
		}
	}

	return nil
}

// Keys returns all keys and types for all files in the file store.
func (fileStore *FileStore) Keys() map[string]byte {
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()

	uniqueKeys := map[string]byte{}
	if err := fileStore.WalkKeys(nil, func(key []byte, typ byte) error {
		uniqueKeys[string(key)] = typ
		return nil
	}); err != nil {
		return nil
	}

	return uniqueKeys
}

// Type returns the type of values store at the block for key.
func (fileStore *FileStore) Type(key []byte) (byte, error) {
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()

	for _, f := range fileStore.tsmFileReaders {
		if f.Contains(key) {
			return f.Type(key)
		}
	}
	return 0, fmt.Errorf("unknown type for %v", key)
}

// Delete removes the keys from the set of keys available in this file.
func (fileStore *FileStore) Delete(keys [][]byte) error {
	return fileStore.DeleteRange(keys, math.MinInt64, math.MaxInt64)
}

func (fileStore *FileStore) Apply(ctx context.Context, fn func(r TSMFile) error) error {
	// Limit apply fn to number of cores
	limiter := limiter.NewFixed(runtime.GOMAXPROCS(0))

	fileStore.mu.RLock()
	errC := make(chan error, len(fileStore.tsmFileReaders))

	for _, f := range fileStore.tsmFileReaders {
		go func(r TSMFile) {
			if err := limiter.Take(ctx); err != nil {
				errC <- err
				return
			}
			defer limiter.Release()

			r.Ref()
			defer r.Unref()
			errC <- fn(r)
		}(f)
	}

	var applyErr error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			applyErr = err
		}
	}
	fileStore.mu.RUnlock()

	fileStore.mu.Lock()
	fileStore.lastModified = time.Now().UTC()
	fileStore.lastFileStats = nil
	fileStore.mu.Unlock()

	return applyErr
}

// DeleteRange removes the values for keys between timestamps min and max.  This should only
// be used with smaller batches of series keys.
func (fileStore *FileStore) DeleteRange(keys [][]byte, min, max int64) error {
	var batches BatchDeleters
	fileStore.mu.RLock()
	for _, f := range fileStore.tsmFileReaders {
		if f.OverlapsTimeRange(min, max) {
			batches = append(batches, f.BatchDelete())
		}
	}
	fileStore.mu.RUnlock()

	if len(batches) == 0 {
		return nil
	}

	if err := func() error {
		if err := batches.DeleteRange(keys, min, max); err != nil {
			return err
		}

		return batches.Commit()
	}(); err != nil {
		// Rollback the deletes
		_ = batches.Rollback()
		return err
	}

	fileStore.mu.Lock()
	fileStore.lastModified = time.Now().UTC()
	fileStore.lastFileStats = nil
	fileStore.mu.Unlock()
	return nil
}

// loads all the TSM files in the configured directory.
func (fileStore *FileStore) Open(ctx context.Context) error {
	fileStore.mu.Lock()
	defer fileStore.mu.Unlock()

	// Not loading files from disk so nothing to do
	if fileStore.dirPath == "" {
		return nil
	}

	if fileStore.openLimiter == nil {
		return errors.New("cannot open FileStore without an OpenLimiter (is EngineOptions.OpenLimiter set?)")
	}

	// find the current max ID for temp directories
	tmpfiles, err := os.ReadDir(fileStore.dirPath)
	if err != nil {
		return err
	}

	// ascertain the current temp directory number by examining the existing
	// directories and choosing the one with the higest basename when converted
	// to an integer.
	for _, fi := range tmpfiles {
		if !fi.IsDir() || !strings.HasSuffix(fi.Name(), "."+TmpTSMFileExtension) {
			continue
		}

		ss := strings.Split(filepath.Base(fi.Name()), ".")
		if len(ss) != 2 {
			continue
		}

		i, err := strconv.Atoi(ss[0])
		if err != nil || i <= fileStore.currentTempDirID {
			continue
		}

		// i must be a valid integer and greater than f.currentTempDirID at this
		// point
		fileStore.currentTempDirID = i
	}

	tsmFilePaths, err := filepath.Glob(filepath.Join(fileStore.dirPath, "*."+TSMFileExtension))
	if err != nil {
		return err
	}

	// struct to hold the result of opening each reader in a goroutine
	type res struct {
		tsmReader *TsmFileReader
		err       error
	}

	readerC := make(chan *res)
	for i, tsmFilePath := range tsmFilePaths {
		// Keep track of the latest ID
		generation, _, err := fileStore.parseFileName(tsmFilePath)
		if err != nil {
			return err
		}

		if generation >= fileStore.currentGeneration {
			fileStore.currentGeneration = generation + 1
		}

		tsmFile, err := os.OpenFile(tsmFilePath, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("error opening file %s: %v", tsmFilePath, err)
		}

		go func(idx int, tsmFile *os.File) {
			// Ensure a limited number of TSM files are loaded at once.
			// Systems which have very large datasets (1TB+) can have thousands
			// of TSM files which can cause extremely long load times.
			if err := fileStore.openLimiter.Take(ctx); err != nil {
				fileStore.logger.Error("Failed to open tsm file", zap.String("path", tsmFile.Name()), zap.Error(err))
				readerC <- &res{err: fmt.Errorf("failed to open tsm file %q: %w", tsmFile.Name(), err)}
				return
			}
			defer fileStore.openLimiter.Release()

			start := time.Now()
			tsmReader, err := NewTsmFileReader(tsmFile, WithMadviseWillNeed(fileStore.tsmMMAPWillNeed))
			fileStore.logger.Info("Opened file",
				zap.String("path", tsmFile.Name()),
				zap.Int("id", idx),
				zap.Duration("duration", time.Since(start)))

			// If we are unable to read a TSM file then log the error, rename
			// the file, and continue loading the shard without it.
			if err != nil {
				fileStore.logger.Error("Cannot read corrupt tsm file, renaming", zap.String("path", tsmFile.Name()), zap.Int("id", idx), zap.Error(err))
				_ = tsmFile.Close()
				if e := os.Rename(tsmFile.Name(), tsmFile.Name()+"."+BadTSMFileExtension); e != nil {
					fileStore.logger.Error("Cannot rename corrupt tsm file", zap.String("path", tsmFile.Name()), zap.Int("id", idx), zap.Error(e))
					readerC <- &res{tsmReader: tsmReader, err: fmt.Errorf("cannot rename corrupt file %s: %v", tsmFile.Name(), e)}
					return
				}
				readerC <- &res{tsmReader: tsmReader, err: fmt.Errorf("cannot read corrupt file %s: %v", tsmFile.Name(), err)}
				return
			}

			tsmReader.WithObserver(fileStore.obs)
			readerC <- &res{tsmReader: tsmReader}
		}(i, tsmFile)
	}

	var lm int64
	isEmpty := true
	for range tsmFilePaths {
		res := <-readerC
		if res.err != nil {
			return res.err
		} else if res.tsmReader == nil {
			continue
		}
		fileStore.tsmFileReaders = append(fileStore.tsmFileReaders, res.tsmReader)

		// Accumulate file store size stats
		fileStore.stats.AddSize(int64(res.tsmReader.Size()))
		if ts := res.tsmReader.TombstoneStats(); ts.TombstoneExists {
			fileStore.stats.AddSize(int64(ts.Size))
		}

		// Re-initialize the lastModified time for the file store
		if res.tsmReader.LastModified() > lm {
			lm = res.tsmReader.LastModified()
		}
		isEmpty = false
	}
	if isEmpty {
		if fi, err := os.Stat(fileStore.dirPath); err == nil {
			fileStore.lastModified = fi.ModTime().UTC()
		} else {
			close(readerC)
			return err
		}
	} else {
		fileStore.lastModified = time.Unix(0, lm).UTC()
	}
	close(readerC)

	sort.Sort(tsmReaders(fileStore.tsmFileReaders))
	fileStore.stats.SetFiles(int64(len(fileStore.tsmFileReaders)))
	return nil
}

// Close closes the file store.
func (fileStore *FileStore) Close() error {
	// Make the object appear closed to other method calls.
	fileStore.mu.Lock()

	files := fileStore.tsmFileReaders

	fileStore.lastFileStats = nil
	fileStore.tsmFileReaders = nil

	fileStore.stats.SetFiles(0)

	// Let other methods access this closed object while we do the actual closing.
	fileStore.mu.Unlock()

	for _, file := range files {
		err := file.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (fileStore *FileStore) DiskSizeBytes() int64 {
	return atomic.LoadInt64(&fileStore.stats.sizeAtomic)
}

// Read returns the slice of values for the given key and the given timestamp,
// if any file matches those constraints.
func (fileStore *FileStore) Read(key []byte, t int64) ([]Value, error) {
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()

	for _, f := range fileStore.tsmFileReaders {
		// Can this file possibly contain this key and timestamp?
		if !f.Contains(key) {
			continue
		}

		// May have the key and time we are looking for so try to find
		v, err := f.Read(key, t)
		if err != nil {
			return nil, err
		}

		if len(v) > 0 {
			return v, nil
		}
	}
	return nil, nil
}

func (fileStore *FileStore) Cost(key []byte, min, max int64) query.IteratorCost {
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()
	return fileStore.cost(key, min, max)
}

// Reader returns a TSMReader for path if one is currently managed by the FileStore.
// Otherwise it returns nil. If it returns a file, you must call Unref on it when
// you are done, and never use it after that.
func (fileStore *FileStore) TSMReader(path string) *TsmFileReader {
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()
	for _, r := range fileStore.tsmFileReaders {
		if r.Path() == path {
			r.Ref()
			return r.(*TsmFileReader)
		}
	}
	return nil
}

// KeyCursor returns a KeyCursor for key and t across the files in the FileStore.
func (fileStore *FileStore) KeyCursor(ctx context.Context, key []byte, t int64, ascending bool) *KeyCursor {
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()
	return newKeyCursor(ctx, fileStore, key, t, ascending)
}

// Stats returns the stats of the underlying files, preferring the cached version if it is still valid.
func (fileStore *FileStore) Stats() []FileStat {
	fileStore.mu.RLock()
	if len(fileStore.lastFileStats) > 0 {
		defer fileStore.mu.RUnlock()
		return fileStore.lastFileStats
	}
	fileStore.mu.RUnlock()

	// The file stats cache is invalid due to changes to files. Need to
	// recalculate.
	fileStore.mu.Lock()
	defer fileStore.mu.Unlock()

	if len(fileStore.lastFileStats) > 0 {
		return fileStore.lastFileStats
	}

	// If lastFileStats's capacity is far away from the number of entries
	// we need to add, then we'll reallocate.
	if cap(fileStore.lastFileStats) < len(fileStore.tsmFileReaders)/2 {
		fileStore.lastFileStats = make([]FileStat, 0, len(fileStore.tsmFileReaders))
	}

	for _, fd := range fileStore.tsmFileReaders {
		fileStore.lastFileStats = append(fileStore.lastFileStats, fd.Stats())
	}
	return fileStore.lastFileStats
}

// ReplaceWithCallback replaces oldFiles with newFiles and calls updatedFn with the files to be added the FileStore.
func (fileStore *FileStore) ReplaceWithCallback(oldFiles, newFiles []string, updatedFn func(r []TSMFile)) error {
	return fileStore.replace(oldFiles, newFiles, updatedFn)
}

// replace oldFiles with newFiles
func (fileStore *FileStore) Replace(oldFiles, newFiles []string) error {
	return fileStore.replace(oldFiles, newFiles, nil)
}

func (fileStore *FileStore) replace(oldFiles, newFiles []string, updatedFn func(r []TSMFile)) error {
	if len(oldFiles) == 0 && len(newFiles) == 0 {
		return nil
	}

	fileStore.mu.RLock()
	maxTime := fileStore.lastModified
	fileStore.mu.RUnlock()

	updated := make([]TSMFile, 0, len(newFiles))
	tsmTmpExt := fmt.Sprintf("%s.%s", TSMFileExtension, TmpTSMFileExtension)

	// Rename all the new files to make them live on restart
	for _, newFile := range newFiles {
		if !strings.HasSuffix(newFile, tsmTmpExt) && !strings.HasSuffix(newFile, TSMFileExtension) {
			// This isn't a .tsm or .tsm.tmp file.
			continue
		}

		// give the observer a chance to process the file first.
		if err := fileStore.obs.FileFinishing(newFile); err != nil {
			return err
		}

		var oldName, newName = newFile, newFile
		if strings.HasSuffix(oldName, tsmTmpExt) {
			// The new TSM files have a tmp extension.  First rename them.
			newName = newFile[:len(newFile)-4]
			if err := os.Rename(oldName, newName); err != nil {
				return err
			}
		}

		// Any error after this point should result in the file being bein named
		// back to the original name. The caller then has the opportunity to
		// remove it.
		fd, err := os.Open(newName)
		if err != nil {
			if newName != oldName {
				if err1 := os.Rename(newName, oldName); err1 != nil {
					return err1
				}
			}
			return err
		}

		// Keep track of the new mod time
		if stat, err := fd.Stat(); err == nil {
			if maxTime.IsZero() || stat.ModTime().UTC().After(maxTime) {
				maxTime = stat.ModTime().UTC()
			}
		}

		tsmFileReader, err := NewTsmFileReader(fd, WithMadviseWillNeed(fileStore.tsmMMAPWillNeed))
		if err != nil {
			if newName != oldName {
				if err1 := os.Rename(newName, oldName); err1 != nil {
					return err1
				}
			}
			return err
		}
		tsmFileReader.WithObserver(fileStore.obs)

		updated = append(updated, tsmFileReader)
	}

	if updatedFn != nil {
		updatedFn(updated)
	}

	fileStore.mu.Lock()
	defer fileStore.mu.Unlock()

	// Copy the current set of active files while we rename
	// and load the new files.  We copy the pointers here to minimize
	// the time that locks are held as well as to ensure that the replacement
	// is atomic.©

	updated = append(updated, fileStore.tsmFileReaders...)

	// We need to prune our set of active files now
	var active, inuse []TSMFile
	for _, file := range updated {
		keep := true
		for _, remove := range oldFiles {
			if remove == file.Path() {
				keep = false

				// give the observer a chance to process the file first.
				if err := fileStore.obs.FileUnlinking(file.Path()); err != nil {
					return err
				}

				if ts := file.TombstoneStats(); ts.TombstoneExists {
					if err := fileStore.obs.FileUnlinking(ts.Path); err != nil {
						return err
					}
				}

				// If queries are running against this file, then we need to move it out of the
				// way and let them complete.  We'll then delete the original file to avoid
				// blocking callers upstream.  If the process crashes, the temp file is
				// cleaned up at startup automatically.
				//
				// In order to ensure that there are no races with this (file held externally calls Ref
				// after we check InUse), we need to maintain the invariant that every handle to a file
				// is handed out in use (Ref'd), and handlers only ever relinquish the file once (call Unref
				// exactly once, and never use it again). InUse is only valid during a write lock, since
				// we allow calls to Ref and Unref under the read lock and no lock at all respectively.
				if file.InUse() {
					// Copy all the tombstones related to this TSM file
					var deletes []string
					if ts := file.TombstoneStats(); ts.TombstoneExists {
						deletes = append(deletes, ts.Path)
					}

					// Rename the TSM file used by this reader
					tempPath := fmt.Sprintf("%s.%s", file.Path(), TmpTSMFileExtension)
					if err := file.Rename(tempPath); err != nil {
						return err
					}

					// Remove the old file and tombstones.  We can't use the normal TSMReader.Remove()
					// because it now refers to our temp file which we can't remove.
					for _, f := range deletes {
						if err := os.Remove(f); err != nil {
							return err
						}
					}

					inuse = append(inuse, file)
					continue
				}

				if err := file.Close(); err != nil {
					return err
				}

				if err := file.Remove(); err != nil {
					return err
				}
				break
			}
		}

		if keep {
			active = append(active, file)
		}
	}

	if err := file.SyncDir(fileStore.dirPath); err != nil {
		return err
	}

	// Tell the purger about our in-use files we need to remove
	fileStore.purger.add(inuse)

	// If times didn't change (which can happen since file mod times are second level),
	// then add a ns to the time to ensure that lastModified changes since files on disk
	// actually did change
	if maxTime.Equal(fileStore.lastModified) || maxTime.Before(fileStore.lastModified) {
		maxTime = fileStore.lastModified.UTC().Add(1)
	}

	fileStore.lastModified = maxTime.UTC()

	fileStore.lastFileStats = nil
	fileStore.tsmFileReaders = active
	sort.Sort(tsmReaders(fileStore.tsmFileReaders))
	fileStore.stats.SetFiles(int64(len(fileStore.tsmFileReaders)))

	// Recalculate the disk size stat
	var totalSize int64
	for _, file := range fileStore.tsmFileReaders {
		totalSize += int64(file.Size())
		if ts := file.TombstoneStats(); ts.TombstoneExists {
			totalSize += int64(ts.Size)
		}
	}
	fileStore.stats.SetSize(totalSize)

	return nil
}

// LastModified returns the last time the file store was updated with new
// TSM files or a delete.
func (fileStore *FileStore) LastModified() time.Time {
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()

	return fileStore.lastModified
}

// BlockCount returns number of values stored in the block at location idx
// in the file at path.  If path does not match any file in the store, 0 is
// returned.  If idx is out of range for the number of blocks in the file,
// 0 is returned.
func (fileStore *FileStore) BlockCount(path string, idx int) int {
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()

	if idx < 0 {
		return 0
	}

	for _, fd := range fileStore.tsmFileReaders {
		if fd.Path() == path {
			iter := fd.BlockIterator()
			for i := 0; i < idx; i++ {
				if !iter.Next() {
					return 0
				}
			}
			_, _, _, _, _, block, _ := iter.Read()
			// on Error, BlockCount(block) returns 0 for cnt
			cnt, _ := BlockCount(block)
			return cnt
		}
	}
	return 0
}

// We need to determine the possible files that may be accessed by this query given
// the time range.
func (fileStore *FileStore) cost(key []byte, min, max int64) query.IteratorCost {
	var cache []IndexEntry
	cost := query.IteratorCost{}
	for _, fd := range fileStore.tsmFileReaders {
		minTime, maxTime := fd.TimeRange()
		if !(maxTime > min && minTime < max) {
			continue
		}
		skipped := true
		tombstones := fd.TombstoneRange(key)

		entries := fd.ReadEntries(key, &cache)
	ENTRIES:
		for i := 0; i < len(entries); i++ {
			ie := entries[i]

			if !(ie.MaxTime > min && ie.MinTime < max) {
				continue
			}

			// Skip any blocks only contain values that are tombstoned.
			for _, t := range tombstones {
				if t.Min <= ie.MinTime && t.Max >= ie.MaxTime {
					continue ENTRIES
				}
			}

			cost.BlocksRead++
			cost.BlockSize += int64(ie.Size)
			skipped = false
		}

		if !skipped {
			cost.NumFiles++
		}
	}
	return cost
}

// locations returns the files and index blocks for a key and time.  ascending indicates
// whether the key will be scan in ascending time order or descenging time order.
// This function assumes the read-lock has been taken.
func (fileStore *FileStore) locations(key []byte, t int64, ascending bool) []*location {
	var cachedIndexEntries []IndexEntry
	locations := make([]*location, 0, len(fileStore.tsmFileReaders))
	for _, tsmFile := range fileStore.tsmFileReaders {
		minTime, maxTime := tsmFile.TimeRange()

		// If we ascending and the max time of the file is before where we want to start
		// skip it.
		if ascending && maxTime < t {
			continue
			// If we are descending and the min time of the file is after where we want to start,
			// then skip it.
		} else if !ascending && minTime > t {
			continue
		}
		tombstones := tsmFile.TombstoneRange(key)

		// This file could potential contain points we are looking for so find the blocks for
		// the given key.
		entries := tsmFile.ReadEntries(key, &cachedIndexEntries)
	LOOP:
		for i := 0; i < len(entries); i++ {
			indexEntry := entries[i]

			// Skip any blocks only contain values that are tombstoned.
			for _, t := range tombstones {
				if t.Min <= indexEntry.MinTime && t.Max >= indexEntry.MaxTime {
					continue LOOP
				}
			}

			// If we ascending and the max time of a block is before where we are looking, skip
			// it since the data is out of our range
			if ascending && indexEntry.MaxTime < t {
				continue
				// If we descending and the min time of a block is after where we are looking, skip
				// it since the data is out of our range
			} else if !ascending && indexEntry.MinTime > t {
				continue
			}

			location := &location{
				tsmFile:    tsmFile,
				indexEntry: indexEntry,
			}

			if ascending {
				// For an ascending cursor, mark everything before the seek time as read
				// so we can filter it out at query time
				location.readMin = math.MinInt64
				location.readMax = t - 1
			} else {
				// For an ascending cursort, mark everything after the seek time as read
				// so we can filter it out at query time
				location.readMin = t + 1
				location.readMax = math.MaxInt64
			}
			// Otherwise, add this file and block location
			locations = append(locations, location)
		}
	}
	return locations
}

// MakeSnapshotLinks creates hardlinks from the supplied TSMFiles to
// corresponding files under a supplied directory.
func (fileStore *FileStore) MakeSnapshotLinks(destPath string, files []TSMFile) (returnErr error) {
	for _, tsmf := range files {
		newpath := filepath.Join(destPath, filepath.Base(tsmf.Path()))
		err := fileStore.copyOrLink(tsmf.Path(), newpath)
		if err != nil {
			return err
		}
		if tf := tsmf.TombstoneStats(); tf.TombstoneExists {
			newpath := filepath.Join(destPath, filepath.Base(tf.Path))
			err := fileStore.copyOrLink(tf.Path, newpath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (fileStore *FileStore) copyOrLink(oldpath string, newpath string) error {
	if fileStore.copyFiles {
		fileStore.logger.Info("copying backup snapshots", zap.String("OldPath", oldpath), zap.String("NewPath", newpath))
		if err := fileStore.copyNotLink(oldpath, newpath); err != nil {
			return err
		}
	} else {
		fileStore.logger.Info("linking backup snapshots", zap.String("OldPath", oldpath), zap.String("NewPath", newpath))
		if err := fileStore.linkNotCopy(oldpath, newpath); err != nil {
			return err
		}
	}
	return nil
}

// copyNotLink - use file copies instead of hard links for 2 scenarios:
// Windows does not permit deleting a file with open file handles
// Azure does not support hard links in its default file system
func (fileStore *FileStore) copyNotLink(oldPath, newPath string) (returnErr error) {
	rfd, err := os.Open(oldPath)
	if err != nil {
		return fmt.Errorf("error opening file for backup %s: %q", oldPath, err)
	} else {
		defer func() {
			if e := rfd.Close(); returnErr == nil && e != nil {
				returnErr = fmt.Errorf("error closing source file for backup %s: %w", oldPath, e)
			}
		}()
	}
	fi, err := rfd.Stat()
	if err != nil {
		return fmt.Errorf("error collecting statistics from file for backup %s: %w", oldPath, err)
	}
	wfd, err := os.OpenFile(newPath, os.O_RDWR|os.O_CREATE, fi.Mode())
	if err != nil {
		return fmt.Errorf("error creating temporary file for backup %s:  %w", newPath, err)
	} else {
		defer func() {
			if e := wfd.Close(); returnErr == nil && e != nil {
				returnErr = fmt.Errorf("error closing temporary file for backup %s: %w", newPath, e)
			}
		}()
	}
	if _, err := io.Copy(wfd, rfd); err != nil {
		return fmt.Errorf("unable to copy file for backup from %s to %s: %w", oldPath, newPath, err)
	}
	if err := os.Chtimes(newPath, fi.ModTime(), fi.ModTime()); err != nil {
		return fmt.Errorf("unable to set modification time on temporary backup file %s: %w", newPath, err)
	}
	return nil
}

// linkNotCopy - use hard links for backup snapshots
func (fileStore *FileStore) linkNotCopy(oldPath, newPath string) error {
	if err := os.Link(oldPath, newPath); err != nil {
		if errors.Is(err, syscall.ENOTSUP) {
			if fi, e := os.Stat(oldPath); e == nil && !fi.IsDir() {
				fileStore.logger.Info("file system does not support hard links, switching to copies for backup", zap.String("OldPath", oldPath), zap.String("NewPath", newPath))
				// Force future snapshots to copy
				fileStore.copyFiles = true
				return fileStore.copyNotLink(oldPath, newPath)
			} else if e != nil {
				// Stat failed
				return fmt.Errorf("error creating hard link for backup, cannot determine if %s is a file or directory: %w", oldPath, e)
			} else {
				return fmt.Errorf("error creating hard link for backup - %s is a directory, not a file: %q", oldPath, err)
			}
		} else {
			return fmt.Errorf("error creating hard link for backup from %s to %s: %w", oldPath, newPath, err)
		}
	} else {
		return nil
	}
}

// CreateSnapshot creates hardlinks for all tsm and tombstone files
// in the path provided.
func (fileStore *FileStore) CreateSnapshot() (string, error) {
	fileStore.traceLogger.Info("Creating snapshot", zap.String("dir", fileStore.dirPath))

	fileStore.mu.Lock()
	// create a copy of the files slice and ensure they aren't closed out from
	// under us, nor the slice mutated.
	files := make([]TSMFile, len(fileStore.tsmFileReaders))
	copy(files, fileStore.tsmFileReaders)

	for _, tsmf := range files {
		tsmf.Ref()
		defer tsmf.Unref()
	}

	// increment and keep track of the current temp dir for when we drop the lock.
	// this ensures we are the only writer to the directory.
	fileStore.currentTempDirID += 1
	tmpPath := fmt.Sprintf("%d.%s", fileStore.currentTempDirID, TmpTSMFileExtension)
	tmpPath = filepath.Join(fileStore.dirPath, tmpPath)
	fileStore.mu.Unlock()

	// create the tmp directory and add the hard links. there is no longer any shared
	// mutable state.
	err := os.Mkdir(tmpPath, 0777)
	if err != nil {
		return "", err
	}
	if err := fileStore.MakeSnapshotLinks(tmpPath, files); err != nil {
		// remove temporary directory since we couldn't create our hard links.
		_ = os.RemoveAll(tmpPath)
		return "", fmt.Errorf("CreateSnapshot() failed to create links %v: %w", tmpPath, err)
	}

	return tmpPath, nil
}

// FormatFileNameFunc is executed when generating a new TSM filename.
// Source filenames are provided via src.
type FormatFileNameFunc func(generation, sequence int) string

func DefaultFormatFileName(generation, sequence int) string {
	return fmt.Sprintf("%09d-%09d", generation, sequence)
}

// executed when parsing a TSM filename into generation & sequence.
type ParseFileNameFunc func(name string) (generation, sequence int, err error)

// used to parse the filenames of TSM files.
func DefaultParseFileName(name string) (int, int, error) {
	base := filepath.Base(name)
	idx := strings.Index(base, ".")
	if idx == -1 {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	id := base[:idx]

	idx = strings.Index(id, "-")
	if idx == -1 {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	generation, err := strconv.ParseUint(id[:idx], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	sequence, err := strconv.ParseUint(id[idx+1:], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	return int(generation), int(sequence), nil
}

// KeyCursor allows iteration through keys in a set of files within a FileStore.
type KeyCursor struct {
	key []byte

	// seeks is all the file locations that we need to return during iteration.
	seeks []*location

	// current is the set of blocks possibly containing the next set of points.
	// Normally this is just one entry, but there may be multiple if points have
	// been overwritten.
	current []*location
	buf     []Value

	ctx context.Context
	col *metrics.Group

	// pos is the index within seeks.  Based on ascending, it will increment or
	// decrement through the size of seeks slice.
	pos       int
	ascending bool
}

type location struct {
	tsmFile    TSMFile
	indexEntry IndexEntry

	readMin, readMax int64
}

func (l *location) read() bool {
	return l.readMin <= l.indexEntry.MinTime && l.readMax >= l.indexEntry.MaxTime
}

func (l *location) markRead(min, max int64) {
	if min < l.readMin {
		l.readMin = min
	}

	if max > l.readMax {
		l.readMax = max
	}
}

type descLocations []*location

// Sort methods
func (a descLocations) Len() int      { return len(a) }
func (a descLocations) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a descLocations) Less(i, j int) bool {
	if a[i].indexEntry.OverlapsTimeRange(a[j].indexEntry.MinTime, a[j].indexEntry.MaxTime) {
		return a[i].tsmFile.Path() < a[j].tsmFile.Path()
	}
	return a[i].indexEntry.MaxTime < a[j].indexEntry.MaxTime
}

type ascLocations []*location

// Sort methods
func (a ascLocations) Len() int      { return len(a) }
func (a ascLocations) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ascLocations) Less(i, j int) bool {
	if a[i].indexEntry.OverlapsTimeRange(a[j].indexEntry.MinTime, a[j].indexEntry.MaxTime) {
		return a[i].tsmFile.Path() < a[j].tsmFile.Path()
	}
	return a[i].indexEntry.MinTime < a[j].indexEntry.MinTime
}

// newKeyCursor returns a new instance of KeyCursor.
// This function assumes the read-lock has been taken.
func newKeyCursor(ctx context.Context, fileStore *FileStore, key []byte, t int64, ascending bool) *KeyCursor {
	keyCursor := &KeyCursor{
		key:       key,
		seeks:     fileStore.locations(key, t, ascending),
		ctx:       ctx,
		col:       metrics.GroupFromContext(ctx),
		ascending: ascending,
	}

	if ascending {
		sort.Sort(ascLocations(keyCursor.seeks))
	} else {
		sort.Sort(descLocations(keyCursor.seeks))
	}

	// Determine the distinct set of TSM files in use and mark then as in-use
	for _, f := range keyCursor.seeks {
		f.tsmFile.Ref()
	}

	keyCursor.seek(t)
	return keyCursor
}

// Close removes all references on the cursor.
func (c *KeyCursor) Close() {
	// Remove all of our in-use references since we're done
	for _, f := range c.seeks {
		f.tsmFile.Unref()
	}

	c.buf = nil
	c.seeks = nil
	c.current = nil
}

// seek positions the cursor at the given time.
func (c *KeyCursor) seek(t int64) {
	if len(c.seeks) == 0 {
		return
	}
	c.current = nil

	if c.ascending {
		c.seekAscending(t)
	} else {
		c.seekDescending(t)
	}
}

func (c *KeyCursor) seekAscending(t int64) {
	for i, e := range c.seeks {
		if t < e.indexEntry.MinTime || e.indexEntry.Contains(t) {
			// Record the position of the first block matching our seek time
			if len(c.current) == 0 {
				c.pos = i
			}

			c.current = append(c.current, e)
		}
	}
}

func (c *KeyCursor) seekDescending(t int64) {
	for i := len(c.seeks) - 1; i >= 0; i-- {
		e := c.seeks[i]
		if t > e.indexEntry.MaxTime || e.indexEntry.Contains(t) {
			// Record the position of the first block matching our seek time
			if len(c.current) == 0 {
				c.pos = i
			}
			c.current = append(c.current, e)
		}
	}
}

// Next moves the cursor to the next position.
// Data should be read by the ReadBlock functions.
func (c *KeyCursor) Next() {
	if len(c.current) == 0 {
		return
	}
	// Do we still have unread values in the current block
	if !c.current[0].read() {
		return
	}
	c.current = c.current[:0]
	if c.ascending {
		c.nextAscending()
	} else {
		c.nextDescending()
	}
}

func (c *KeyCursor) nextAscending() {
	for {
		c.pos++
		if c.pos >= len(c.seeks) {
			return
		} else if !c.seeks[c.pos].read() {
			break
		}
	}

	// Append the first matching block
	if len(c.current) == 0 {
		c.current = append(c.current, nil)
	} else {
		c.current = c.current[:1]
	}
	c.current[0] = c.seeks[c.pos]

	// If we have ovelapping blocks, append all their values so we can dedup
	for i := c.pos + 1; i < len(c.seeks); i++ {
		if c.seeks[i].read() {
			continue
		}

		c.current = append(c.current, c.seeks[i])
	}
}

func (c *KeyCursor) nextDescending() {
	for {
		c.pos--
		if c.pos < 0 {
			return
		} else if !c.seeks[c.pos].read() {
			break
		}
	}

	// Append the first matching block
	if len(c.current) == 0 {
		c.current = make([]*location, 1)
	} else {
		c.current = c.current[:1]
	}
	c.current[0] = c.seeks[c.pos]

	// If we have ovelapping blocks, append all their values so we can dedup
	for i := c.pos; i >= 0; i-- {
		if c.seeks[i].read() {
			continue
		}
		c.current = append(c.current, c.seeks[i])
	}
}

type purger struct {
	mu        sync.RWMutex
	fileStore *FileStore
	files     map[string]TSMFile
	running   bool

	logger *zap.Logger
}

func (p *purger) add(files []TSMFile) {
	p.mu.Lock()
	for _, f := range files {
		p.files[f.Path()] = f
	}
	p.mu.Unlock()
	p.purge()
}

func (p *purger) purge() {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return
	}
	p.running = true
	p.mu.Unlock()

	go func() {
		for {
			p.mu.Lock()
			for k, v := range p.files {
				// In order to ensure that there are no races with this (file held externally calls Ref
				// after we check InUse), we need to maintain the invariant that every handle to a file
				// is handed out in use (Ref'd), and handlers only ever relinquish the file once (call Unref
				// exactly once, and never use it again). InUse is only valid during a write lock, since
				// we allow calls to Ref and Unref under the read lock and no lock at all respectively.
				if !v.InUse() {
					if err := v.Close(); err != nil {
						p.logger.Info("Purge: close file", zap.Error(err))
						continue
					}

					if err := v.Remove(); err != nil {
						p.logger.Info("Purge: remove file", zap.Error(err))
						continue
					}
					delete(p.files, k)
				}
			}

			if len(p.files) == 0 {
				p.running = false
				p.mu.Unlock()
				return
			}

			p.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()
}

type tsmReaders []TSMFile

func (a tsmReaders) Len() int           { return len(a) }
func (a tsmReaders) Less(i, j int) bool { return a[i].Path() < a[j].Path() }
func (a tsmReaders) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
