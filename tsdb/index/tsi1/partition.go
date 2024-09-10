package tsi1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/bytesutil"
	errors2 "github.com/influxdata/influxdb/v2/pkg/errors"
	"github.com/influxdata/influxdb/v2/pkg/estimator"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

// Version is the current version of the TSI index.
const Version = 1

// File extensions.
const (
	LogFileExt   = ".tsl"
	IndexFileExt = ".tsi"

	CompactingExt = ".compacting"
)

// ManifestFileName is the name of the index manifest file.
const ManifestFileName = "MANIFEST"

// Partition represents a collection of layered index files and WAL.
type Partition struct {
	mu     sync.RWMutex
	opened bool

	sfile         *tsdb.SeriesFile // series lookup file
	activeLogFile *LogFile         // current log file 其实应该是wal
	fileSet       *FileSet         // current file set
	seq           int              // file id sequence

	// Fast series lookup of series IDs in the series file that have been present
	// in this partition. This set tracks both insertions and deletions of a series.
	seriesIDSet *tsdb.SeriesIDSet

	// Compaction management
	levels          []CompactionLevel // compaction levels
	levelCompacting []bool            // level compaction status

	// Close management.
	once    sync.Once
	closing chan struct{} // closing is used to inform iterators the partition is closing.

	// Fieldset shared with engine.
	fieldset *tsdb.MeasurementFieldSet

	currentCompactionN int // counter of in-progress compactions

	// Directory of the Partition's index files.
	path string
	id   string // id portion of path.

	// Log file compaction thresholds.
	MaxLogFileSize int64 // 对应 storage-max-index-log-file-size
	MaxLogFileAge  time.Duration
	nosync         bool // when true, flushing and syncing of LogFile will be disabled.
	logbufferSize  int  // the LogFile's buffer is set to this value.

	// Frequency of compaction checks.
	compactionInterrupt chan struct{}
	compactionsDisabled int

	logger *zap.Logger

	// Current size of MANIFEST. Used to determine partition size.
	manifestSize int64

	// Index's version.
	version int

	manifestPathFn func() string
}

// NewPartition returns a new instance of Partition.
func NewPartition(sfile *tsdb.SeriesFile, path string) *Partition {
	p := &Partition{
		closing:        make(chan struct{}),
		path:           path,
		sfile:          sfile,
		seriesIDSet:    tsdb.NewSeriesIDSet(),
		fileSet:        &FileSet{},
		MaxLogFileSize: tsdb.DefaultMaxIndexLogFileSize,

		// compactionEnabled: true,
		compactionInterrupt: make(chan struct{}),

		logger:  zap.NewNop(),
		version: Version,
	}
	p.manifestPathFn = p.manifestPath
	return p
}

// bytes estimates the memory footprint of this Partition, in bytes.
func (partition *Partition) bytes() int {
	var b int
	b += 24 // mu RWMutex is 24 bytes
	b += int(unsafe.Sizeof(partition.opened))
	// Do not count SeriesFile because it belongs to the code that constructed this Partition.
	b += int(unsafe.Sizeof(partition.activeLogFile)) + partition.activeLogFile.bytes()
	b += int(unsafe.Sizeof(partition.fileSet)) + partition.fileSet.bytes()
	b += int(unsafe.Sizeof(partition.seq))
	b += int(unsafe.Sizeof(partition.seriesIDSet)) + partition.seriesIDSet.Bytes()
	b += int(unsafe.Sizeof(partition.levels))
	for _, level := range partition.levels {
		b += int(unsafe.Sizeof(level))
	}
	b += int(unsafe.Sizeof(partition.levelCompacting))
	for _, levelCompacting := range partition.levelCompacting {
		b += int(unsafe.Sizeof(levelCompacting))
	}
	b += 12 // once sync.Once is 12 bytes
	b += int(unsafe.Sizeof(partition.closing))
	b += int(unsafe.Sizeof(partition.currentCompactionN))
	b += int(unsafe.Sizeof(partition.fieldset)) + partition.fieldset.Bytes()
	b += int(unsafe.Sizeof(partition.path)) + len(partition.path)
	b += int(unsafe.Sizeof(partition.id)) + len(partition.id)
	b += int(unsafe.Sizeof(partition.MaxLogFileSize))
	b += int(unsafe.Sizeof(partition.MaxLogFileAge))
	b += int(unsafe.Sizeof(partition.compactionInterrupt))
	b += int(unsafe.Sizeof(partition.compactionsDisabled))
	b += int(unsafe.Sizeof(partition.logger))
	b += int(unsafe.Sizeof(partition.manifestSize))
	b += int(unsafe.Sizeof(partition.version))
	return b
}

// ErrIncompatibleVersion is returned when attempting to read from an
// incompatible tsi1 manifest file.
var ErrIncompatibleVersion = errors.New("incompatible tsi1 index MANIFEST")

// Open opens the partition.
func (partition *Partition) Open() (rErr error) {
	partition.mu.Lock()
	defer partition.mu.Unlock()

	partition.closing = make(chan struct{})

	if partition.opened {
		return fmt.Errorf("index partition already open: %q", partition.path)
	}

	// Validate path is correct.
	partition.id = filepath.Base(partition.path)
	_, err := strconv.Atoi(partition.id)
	if err != nil {
		return fmt.Errorf("poorly formed manifest file path, %q: %w", partition.path, err)
	}

	// Create directory if it doesn't exist.
	if err := os.MkdirAll(partition.path, 0777); err != nil {
		return err
	}

	filename := filepath.Join(partition.path, ManifestFileName)
	// Read manifest file.
	m, manifestSize, err := ReadManifestFile(filename)
	if os.IsNotExist(err) {
		m = NewManifest(partition.ManifestPath())
	} else if err != nil {
		return err
	}
	// Set manifest size on the partition
	partition.manifestSize = manifestSize

	// Check to see if the MANIFEST file is compatible with the current Index.
	if err := m.Validate(); err != nil {
		return err
	}

	// Copy compaction levels to the index.
	partition.levels = make([]CompactionLevel, len(m.Levels))
	copy(partition.levels, m.Levels)

	// Set up flags to track whether a level is compacting.
	partition.levelCompacting = make([]bool, len(partition.levels))

	// Open each file in the manifest.
	var files []File
	defer func() {
		if rErr != nil {
			Files(files).Close()
		}
	}()

	for _, filename := range m.Files {
		switch filepath.Ext(filename) {
		case LogFileExt:
			f, err := partition.openLogFile(filepath.Join(partition.path, filename))
			if err != nil {
				return err
			}
			files = append(files, f)

			// Make first log file active, if within threshold.
			sz, _ := f.Stat()
			if partition.activeLogFile == nil && sz < partition.MaxLogFileSize {
				partition.activeLogFile = f
			}

		case IndexFileExt:
			f, err := partition.openIndexFile(filepath.Join(partition.path, filename))
			if err != nil {
				return err
			}
			files = append(files, f)
		}
	}
	partition.fileSet = NewFileSet(files)

	// Set initial sequence number.
	partition.seq = partition.fileSet.MaxID()

	// Delete any files not in the manifest.
	if err := partition.deleteNonManifestFiles(m); err != nil {
		return err
	}

	// Ensure a log file exists.
	if partition.activeLogFile == nil {
		if err := partition.prependActiveLogFile(); err != nil {
			return err
		}
	}

	// Build series existence set.
	if err := partition.buildSeriesSet(); err != nil {
		return err
	}

	// Mark opened.
	partition.opened = true

	// Send a compaction request on start up.
	go partition.runPeriodicCompaction()

	return nil
}

func (partition *Partition) IsOpen() bool {
	return partition.opened
}

// openLogFile opens a log file and appends it to the index.
func (partition *Partition) openLogFile(path string) (*LogFile, error) {
	f := NewLogFile(partition.sfile, path)
	f.nosync = partition.nosync
	f.bufferSize = partition.logbufferSize

	if err := f.Open(); err != nil {
		return nil, err
	}
	return f, nil
}

// openIndexFile opens a log file and appends it to the index.
func (partition *Partition) openIndexFile(path string) (*IndexFile, error) {
	f := NewIndexFile(partition.sfile)
	f.SetPath(path)
	if err := f.Open(); err != nil {
		return nil, err
	}
	return f, nil
}

// deleteNonManifestFiles removes all files not in the manifest.
func (partition *Partition) deleteNonManifestFiles(m *Manifest) (rErr error) {
	dir, err := os.Open(partition.path)
	if err != nil {
		return err
	}
	defer errors2.Capture(&rErr, dir.Close)()

	fis, err := dir.Readdir(-1)
	if err != nil {
		return err
	}

	// Loop over all files and remove any not in the manifest.
	for _, fi := range fis {
		filename := filepath.Base(fi.Name())
		if filename == ManifestFileName || m.HasFile(filename) {
			continue
		}

		if err := os.RemoveAll(filename); err != nil {
			return err
		}
	}

	return nil
}

func (partition *Partition) buildSeriesSet() error {
	fs := partition.retainFileSet()
	defer fs.Release()

	partition.seriesIDSet = tsdb.NewSeriesIDSet()

	// Read series sets from files in reverse.
	for i := len(fs.files) - 1; i >= 0; i-- {
		f := fs.files[i]

		// Delete anything that's been tombstoned.
		ts, err := f.TombstoneSeriesIDSet()
		if err != nil {
			return err
		}
		partition.seriesIDSet.Diff(ts)

		// Add series created within the file.
		ss, err := f.SeriesIDSet()
		if err != nil {
			return err
		}
		partition.seriesIDSet.Merge(ss)
	}
	return nil
}

// CurrentCompactionN returns the number of compactions currently running.
func (partition *Partition) CurrentCompactionN() int {
	partition.mu.RLock()
	defer partition.mu.RUnlock()
	return partition.currentCompactionN
}

// Wait will block until all compactions are finished.
// Must only be called while they are disabled.
func (partition *Partition) Wait() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if partition.CurrentCompactionN() == 0 {
			return
		}
		<-ticker.C
	}
}

// Close closes the index.
func (partition *Partition) Close() error {
	// Wait for goroutines to finish outstanding compactions.
	partition.once.Do(func() {
		close(partition.closing)
		close(partition.compactionInterrupt)
	})
	partition.Wait()

	// Lock index and close remaining
	partition.mu.Lock()
	defer partition.mu.Unlock()

	if partition.fileSet == nil {
		return nil
	}

	// Close log files.
	var err error
	for _, f := range partition.fileSet.files {
		if localErr := f.Close(); localErr != nil {
			err = localErr
		}
	}
	partition.fileSet.files = nil

	return err
}

// closing returns true if the partition is currently closing. It does not require
// a lock so will always return to callers.
func (partition *Partition) isClosing() bool {
	select {
	case <-partition.closing:
		return true
	default:
		return false
	}
}

// Path returns the path to the partition.
func (partition *Partition) Path() string { return partition.path }

// SeriesFile returns the attached series file.
func (partition *Partition) SeriesFile() *tsdb.SeriesFile { return partition.sfile }

// NextSequence returns the next file identifier.
func (partition *Partition) NextSequence() int {
	partition.mu.Lock()
	defer partition.mu.Unlock()
	return partition.nextSequence()
}

func (partition *Partition) nextSequence() int {
	partition.seq++
	return partition.seq
}

func (partition *Partition) ManifestPath() string {
	return partition.manifestPathFn()
}

// ManifestPath returns the path to the index's manifest file.
func (partition *Partition) manifestPath() string {
	return filepath.Join(partition.path, ManifestFileName)
}

// Manifest returns a manifest for the index.
func (partition *Partition) Manifest() *Manifest {
	return partition.manifest(partition.fileSet)
}

// manifest returns a manifest for the index, possibly using a
// new FileSet to account for compaction or log prepending
func (partition *Partition) manifest(newFileSet *FileSet) *Manifest {
	m := &Manifest{
		Levels:  partition.levels,
		Files:   make([]string, len(newFileSet.files)),
		Version: partition.version,
		path:    partition.ManifestPath(),
	}

	for j, f := range newFileSet.files {
		m.Files[j] = filepath.Base(f.Path())
	}

	return m
}

// SetManifestPathForTest is only to force a bad path in testing
func (partition *Partition) SetManifestPathForTest(path string) {
	partition.mu.Lock()
	defer partition.mu.Unlock()
	partition.manifestPathFn = func() string { return path }
}

// WithLogger sets the logger for the index.
func (partition *Partition) WithLogger(logger *zap.Logger) {
	partition.logger = logger.With(zap.String("index", "tsi"))
}

// SetFieldSet sets a shared field set from the engine.
func (partition *Partition) SetFieldSet(fs *tsdb.MeasurementFieldSet) {
	partition.mu.Lock()
	partition.fieldset = fs
	partition.mu.Unlock()
}

// FieldSet returns the fieldset.
func (partition *Partition) FieldSet() *tsdb.MeasurementFieldSet {
	partition.mu.Lock()
	fs := partition.fieldset
	partition.mu.Unlock()
	return fs
}

// RetainFileSet returns the current fileset and adds a reference count.
func (partition *Partition) RetainFileSet() (*FileSet, error) {
	select {
	case <-partition.closing:
		return nil, tsdb.ErrIndexClosing
	default:
		partition.mu.RLock()
		defer partition.mu.RUnlock()
		return partition.retainFileSet(), nil
	}
}

func (partition *Partition) retainFileSet() *FileSet {
	fs := partition.fileSet
	fs.Retain()
	return fs
}

// FileN returns the active files in the file set.
func (partition *Partition) FileN() int {
	partition.mu.RLock()
	defer partition.mu.RUnlock()
	return len(partition.fileSet.files)
}

// prependActiveLogFile adds a new log file so that the current log file can be compacted.
func (partition *Partition) prependActiveLogFile() (rErr error) {
	// Open file and insert it into the first position.
	f, err := partition.openLogFile(filepath.Join(partition.path, FormatLogFileName(partition.nextSequence())))
	if err != nil {
		return err
	}
	var oldActiveFile *LogFile
	partition.activeLogFile, oldActiveFile = f, partition.activeLogFile

	// Prepend and generate new fileset but do not yet update the partition
	newFileSet := partition.fileSet.PrependLogFile(f)

	defer errors2.Capture(&rErr, func() error {
		if rErr != nil {
			// close the new file.
			f.Close()
			partition.activeLogFile = oldActiveFile
		}
		return rErr
	})()

	// Write new manifest.
	manifestSize, err := partition.manifest(newFileSet).Write()
	if err != nil {
		return fmt.Errorf("manifest write failed for %q: %w", partition.ManifestPath(), err)
	}
	partition.manifestSize = manifestSize
	// Store the new FileSet in the partition now that the manifest has been written
	partition.fileSet = newFileSet
	return nil
}

// ForEachMeasurementName iterates over all measurement names in the index.
func (partition *Partition) ForEachMeasurementName(fn func(name []byte) error) error {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return err
	}
	defer fs.Release()

	itr := fs.MeasurementIterator()
	if itr == nil {
		return nil
	}

	for e := itr.Next(); e != nil; e = itr.Next() {
		if err := fn(e.Name()); err != nil {
			return err
		}
	}

	return nil
}

// MeasurementHasSeries returns true if a measurement has at least one non-tombstoned series.
func (partition *Partition) MeasurementHasSeries(name []byte) (bool, error) {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return false, err
	}
	defer fs.Release()

	for _, f := range fs.files {
		if f.MeasurementHasSeries(partition.seriesIDSet, name) {
			return true, nil
		}
	}

	return false, nil
}

// MeasurementIterator returns an iterator over all measurement names.
func (partition *Partition) MeasurementIterator() (tsdb.MeasurementIterator, error) {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return nil, err
	}
	itr := fs.MeasurementIterator()
	if itr == nil {
		fs.Release()
		return nil, nil
	}
	return newFileSetMeasurementIterator(fs, NewTSDBMeasurementIteratorAdapter(itr)), nil
}

// MeasurementExists returns true if a measurement exists.
func (partition *Partition) MeasurementExists(name []byte) (bool, error) {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return false, err
	}
	defer fs.Release()
	m := fs.Measurement(name)
	return m != nil && !m.Deleted(), nil
}

func (partition *Partition) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return nil, err
	}
	defer fs.Release()

	itr := fs.MeasurementIterator()
	if itr == nil {
		return nil, nil
	}

	var a [][]byte
	for e := itr.Next(); e != nil; e = itr.Next() {
		if re.Match(e.Name()) {
			// Clone bytes since they will be used after the fileset is released.
			a = append(a, bytesutil.Clone(e.Name()))
		}
	}
	return a, nil
}

func (partition *Partition) MeasurementSeriesIDIterator(name []byte) (tsdb.SeriesIDIterator, error) {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return nil, err
	}
	return newFileSetSeriesIDIterator(fs, fs.MeasurementSeriesIDIterator(name)), nil
}

// DropMeasurement deletes a measurement from the index. DropMeasurement does
// not remove any series from the index directly.
func (partition *Partition) DropMeasurement(name []byte) error {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return err
	}
	defer fs.Release()

	// Delete all keys and values.
	if kitr := fs.TagKeyIterator(name); kitr != nil {
		for k := kitr.Next(); k != nil; k = kitr.Next() {
			// Delete key if not already deleted.
			if !k.Deleted() {
				if err := func() error {
					partition.mu.RLock()
					defer partition.mu.RUnlock()
					return partition.activeLogFile.DeleteTagKey(name, k.Key())
				}(); err != nil {
					return err
				}
			}

			// Delete each value in key.
			if vitr := k.TagValueIterator(); vitr != nil {
				for v := vitr.Next(); v != nil; v = vitr.Next() {
					if !v.Deleted() {
						if err := func() error {
							partition.mu.RLock()
							defer partition.mu.RUnlock()
							return partition.activeLogFile.DeleteTagValue(name, k.Key(), v.Value())
						}(); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	// Delete all series.
	if itr := fs.MeasurementSeriesIDIterator(name); itr != nil {
		defer itr.Close()
		for {
			elem, err := itr.Next()
			if err != nil {
				return err
			} else if elem.SeriesID == 0 {
				break
			}
			if err := func() error {
				partition.mu.RLock()
				defer partition.mu.RUnlock()
				return partition.activeLogFile.DeleteSeriesID(elem.SeriesID)
			}(); err != nil {
				return err
			}
		}
		if err = itr.Close(); err != nil {
			return err
		}
	}

	// Mark measurement as deleted.
	if err := func() error {
		partition.mu.RLock()
		defer partition.mu.RUnlock()
		return partition.activeLogFile.DeleteMeasurement(name)
	}(); err != nil {
		return err
	}

	// Check if the log file needs to be swapped.
	if err := partition.CheckLogFile(); err != nil {
		return err
	}

	return nil
}

// createSeriesListIfNotExists creates a list of series if they doesn't exist in
// bulk.
func (partition *Partition) createSeriesListIfNotExists(names [][]byte, tagsSlice []models.Tags) ([]uint64, error) {
	// Is there anything to do? The partition may have been sent an empty batch.
	if len(names) == 0 {
		return nil, nil
	} else if len(names) != len(tagsSlice) {
		return nil, fmt.Errorf("uneven batch, partition %s sent %d names and %d tags", partition.id, len(names), len(tagsSlice))
	}

	// Maintain reference count on files in file set.
	fs, err := partition.RetainFileSet()
	if err != nil {
		return nil, err
	}
	defer fs.Release()

	// Ensure fileset cannot change during insert.
	partition.mu.RLock()
	// Insert series into index write ahead log file.
	ids, err := partition.activeLogFile.AddSeriesList(partition.seriesIDSet, names, tagsSlice)
	if err != nil {
		partition.mu.RUnlock()
		return nil, err
	}
	partition.mu.RUnlock()

	if err := partition.CheckLogFile(); err != nil { // CheckLogFile() 只是用来compact index的wal
		return nil, err
	}
	return ids, nil
}

func (partition *Partition) DropSeries(seriesID uint64) error {
	// Delete series from index.
	if err := func() error {
		partition.mu.RLock()
		defer partition.mu.RUnlock()
		return partition.activeLogFile.DeleteSeriesID(seriesID)
	}(); err != nil {
		return err
	}

	partition.seriesIDSet.Remove(seriesID)

	// Swap log file, if necessary.
	return partition.CheckLogFile()
}

// MeasurementsSketches returns the two sketches for the partition by merging all
// instances of the type sketch types in all the index files.
func (partition *Partition) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return nil, nil, err
	}
	defer fs.Release()
	return fs.MeasurementsSketches()
}

// SeriesSketches returns the two sketches for the partition by merging all
// instances of the type sketch types in all the index files.
func (partition *Partition) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return nil, nil, err
	}
	defer fs.Release()
	return fs.SeriesSketches()
}

// HasTagKey returns true if tag key exists.
func (partition *Partition) HasTagKey(name, key []byte) (bool, error) {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return false, err
	}
	defer fs.Release()
	return fs.HasTagKey(name, key), nil
}

// HasTagValue returns true if tag value exists.
func (partition *Partition) HasTagValue(name, key, value []byte) (bool, error) {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return false, err
	}
	defer fs.Release()
	return fs.HasTagValue(name, key, value), nil
}

// TagKeyIterator returns an iterator for all keys across a single measurement.
func (partition *Partition) TagKeyIterator(name []byte) tsdb.TagKeyIterator {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return nil // TODO(edd): this should probably return an error.
	}

	itr := fs.TagKeyIterator(name)
	if itr == nil {
		fs.Release()
		return nil
	}
	return newFileSetTagKeyIterator(fs, NewTSDBTagKeyIteratorAdapter(itr))
}

// TagValueIterator returns an iterator for all values across a single key.
func (partition *Partition) TagValueIterator(name, key []byte) tsdb.TagValueIterator {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return nil // TODO(edd): this should probably return an error.
	}

	itr := fs.TagValueIterator(name, key)
	if itr == nil {
		fs.Release()
		return nil
	}
	return newFileSetTagValueIterator(fs, NewTSDBTagValueIteratorAdapter(itr))
}

// TagKeySeriesIDIterator returns a series iterator for all values across a single key.
func (partition *Partition) TagKeySeriesIDIterator(name, key []byte) (tsdb.SeriesIDIterator, error) {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return nil, err
	}

	itr, err := fs.TagKeySeriesIDIterator(name, key)
	if err != nil {
		fs.Release()
		return nil, err
	} else if itr == nil {
		fs.Release()
		return nil, nil
	}
	return newFileSetSeriesIDIterator(fs, itr), nil
}

// TagValueSeriesIDIterator returns a series iterator for a single key value.
func (partition *Partition) TagValueSeriesIDIterator(name, key, value []byte) (tsdb.SeriesIDIterator, error) {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return nil, err
	}

	itr, err := fs.TagValueSeriesIDIterator(name, key, value)
	if err != nil {
		fs.Release()
		return nil, err
	} else if itr == nil {
		fs.Release()
		return nil, nil
	}
	return newFileSetSeriesIDIterator(fs, itr), nil
}

// MeasurementTagKeysByExpr extracts the tag keys wanted by the expression.
func (partition *Partition) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return nil, err
	}
	defer fs.Release()

	return fs.MeasurementTagKeysByExpr(name, expr)
}

// ForEachMeasurementTagKey iterates over all tag keys in a measurement.
func (partition *Partition) ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error {
	fs, err := partition.RetainFileSet()
	if err != nil {
		return err
	}
	defer fs.Release()

	itr := fs.TagKeyIterator(name)
	if itr == nil {
		return nil
	}

	for e := itr.Next(); e != nil; e = itr.Next() {
		if err := fn(e.Key()); err != nil {
			return err
		}
	}

	return nil
}

// TagKeyCardinality always returns zero.
// It is not possible to determine cardinality of tags across index files.
func (partition *Partition) TagKeyCardinality(name, key []byte) int {
	return 0
}

func (partition *Partition) SetFieldName(measurement []byte, name string) {}
func (partition *Partition) RemoveShard(shardID uint64)                   {}
func (partition *Partition) AssignShard(k string, shardID uint64)         {}

// Compact requests a compaction of log files.
func (partition *Partition) Compact() {
	partition.mu.Lock()
	defer partition.mu.Unlock()
	partition.compact()
}

func (partition *Partition) DisableCompactions() {
	partition.mu.Lock()
	defer partition.mu.Unlock()
	partition.compactionsDisabled++

	select {
	case <-partition.closing:
		return
	default:
	}

	if partition.compactionsDisabled == 0 {
		close(partition.compactionInterrupt)
		partition.compactionInterrupt = make(chan struct{})
	}
}

func (partition *Partition) EnableCompactions() {
	partition.mu.Lock()
	defer partition.mu.Unlock()

	// Already enabled?
	if partition.compactionsEnabled() {
		return
	}
	partition.compactionsDisabled--
}

func (partition *Partition) compactionsEnabled() bool {
	return partition.compactionsDisabled == 0
}

func (partition *Partition) runPeriodicCompaction() {
	// kick off an initial compaction at startup without the optimization check
	partition.Compact()

	// Avoid a race when using Reopen in tests
	partition.mu.RLock()
	closing := partition.closing
	partition.mu.RUnlock()

	// check for compactions once an hour (usually not necessary but a nice safety check)
	t := time.NewTicker(1 * time.Hour)
	defer t.Stop()
	for {
		select {
		case <-closing:
			return
		case <-t.C:
			if partition.NeedsCompaction(true) {
				partition.Compact()
			}
		}
	}
}

// NeedsCompaction only requires a read lock and checks if there are files that could be compacted.
//
// If compact() is updated we should also update needsCompaction
// If checkRunning = true, only count as needing a compaction if there is not a compaction already
// in progress for the level that would be compacted
func (partition *Partition) NeedsCompaction(checkRunning bool) bool {
	partition.mu.RLock()
	defer partition.mu.RUnlock()
	if partition.needsLogCompaction() {
		return true
	}
	levelCount := make(map[int]int)
	maxLevel := len(partition.levels) - 2
	// If we have 2 log files (level 0), or if we have 2 files at the same level, we should do a compaction.
	for _, f := range partition.fileSet.files {
		level := f.Level()
		levelCount[level]++
		if level <= maxLevel && levelCount[level] > 1 && !(checkRunning && partition.levelCompacting[level]) {
			return true
		}
	}
	return false
}

// compact compacts continguous groups of files that are not currently compacting.
//
// compact requires that mu is write-locked.
func (partition *Partition) compact() {
	if partition.isClosing() {
		return
	} else if !partition.compactionsEnabled() {
		return
	}
	interrupt := partition.compactionInterrupt

	fs := partition.retainFileSet()
	defer fs.Release()

	// check if the current active log file should be rolled
	if partition.needsLogCompaction() {
		if err := partition.prependActiveLogFile(); err != nil {
			partition.logger.Error("failed to retire active log file", zap.Error(err))
		}
	}

	// compact any non-active log files first
	for _, file := range partition.fileSet.files {
		if file.Level() == 0 {
			logFile := file.(*LogFile) // It is an invariant that a file is level 0 iff it is a log file
			if logFile == partition.activeLogFile {
				continue
			}
			if partition.levelCompacting[0] {
				break
			}
			// Mark the level as compacting.
			partition.levelCompacting[0] = true
			partition.currentCompactionN++
			go func() {
				partition.compactLogFile(logFile)
				partition.mu.Lock()
				partition.currentCompactionN--
				partition.levelCompacting[0] = false
				partition.mu.Unlock()
				partition.Compact()
			}()
		}
	}

	// Iterate over each level we are going to compact.
	// We skip the first level (0) because it is log files and they are compacted separately.
	// We skip the last level because the files have no higher level to compact into.
	minLevel, maxLevel := 1, len(partition.levels)-2
	for level := minLevel; level <= maxLevel; level++ {
		// Skip level if it is currently compacting.
		if partition.levelCompacting[level] {
			continue
		}

		// Collect contiguous files from the end of the level.
		files := fs.LastContiguousIndexFilesByLevel(level)
		if len(files) < 2 {
			continue
		} else if len(files) > MaxIndexMergeCount {
			files = files[len(files)-MaxIndexMergeCount:]
		}

		// Retain files during compaction.
		IndexFiles(files).Retain()

		// Mark the level as compacting.
		partition.levelCompacting[level] = true

		// Execute in closure to save reference to the group within the loop.
		func(files []*IndexFile, level int) {
			// Start compacting in a separate goroutine.
			partition.currentCompactionN++
			go func() {

				// Compact to a new level.
				partition.compactToLevel(files, level+1, interrupt)

				// Ensure compaction lock for the level is released.
				partition.mu.Lock()
				partition.levelCompacting[level] = false
				partition.currentCompactionN--
				partition.mu.Unlock()

				// Check for new compactions
				partition.Compact()
			}()
		}(files, level)
	}
}

// compactToLevel compacts a set of files into a new file. Replaces old files with
// compacted file on successful completion. This runs in a separate goroutine.
func (partition *Partition) compactToLevel(files []*IndexFile, level int, interrupt <-chan struct{}) {
	assert(len(files) >= 2, "at least two index files are required for compaction")
	assert(level > 0, "cannot compact level zero")

	// Files have already been retained by caller.
	// Ensure files are released only once.
	var once sync.Once
	defer once.Do(func() { IndexFiles(files).Release() })

	// Build a logger for this compaction.
	log, logEnd := logger.NewOperation(context.TODO(), partition.logger, "TSI level compaction", "tsi1_compact_to_level", zap.Int("tsi1_level", level))
	defer logEnd()

	// Check for cancellation.
	select {
	case <-interrupt:
		log.Error("Cannot begin compaction", zap.Error(ErrCompactionInterrupted))
		return
	default:
	}

	// Track time to compact.
	start := time.Now()

	// Create new index file.
	path := filepath.Join(partition.path, FormatIndexFileName(partition.NextSequence(), level))
	f, err := os.Create(path)
	if err != nil {
		log.Error("Cannot create compaction files", zap.Error(err))
		return
	}
	defer f.Close()

	log.Info("Performing full compaction",
		zap.String("src", joinIntSlice(IndexFiles(files).IDs(), ",")),
		zap.String("dst", path),
	)

	// Compact all index files to new index file.
	lvl := partition.levels[level]
	n, err := IndexFiles(files).CompactTo(f, partition.sfile, lvl.M, lvl.K, interrupt)
	if err != nil {
		log.Error("Cannot compact index files", zap.Error(err))
		return
	}

	if err = f.Sync(); err != nil {
		log.Error("Error sync index file", zap.Error(err))
		return
	}

	// Close file.
	if err := f.Close(); err != nil {
		log.Error("Error closing index file", zap.Error(err))
		return
	}

	// Reopen as an index file.
	file := NewIndexFile(partition.sfile)
	file.SetPath(path)
	if err := file.Open(); err != nil {
		log.Error("Cannot open new index file", zap.Error(err))
		return
	}

	// Obtain lock to swap in index file and write manifest.
	if err := func() (rErr error) {
		partition.mu.Lock()
		defer partition.mu.Unlock()

		// Replace previous files with new index file.
		newFileSet := partition.fileSet.MustReplace(IndexFiles(files).Files(), file)

		// Write new manifest.
		manifestSize, err := partition.manifest(newFileSet).Write()
		defer errors2.Capture(&rErr, func() error {
			if rErr != nil {
				// Close the new file to avoid leaks.
				file.Close()
			}
			return rErr
		})()
		if err != nil {
			return fmt.Errorf("manifest file write failed compacting index %q: %w", partition.ManifestPath(), err)
		}
		partition.manifestSize = manifestSize
		// Store the new FileSet in the partition now that the manifest has been written
		partition.fileSet = newFileSet
		return nil
	}(); err != nil {
		log.Error("Cannot write manifest", zap.Error(err))
		return
	}

	elapsed := time.Since(start)
	log.Info("Full compaction complete",
		zap.String("path", path),
		logger.DurationLiteral("elapsed", elapsed),
		zap.Int64("bytes", n),
		zap.Int("kb_per_sec", int(float64(n)/elapsed.Seconds())/1024),
	)

	// Release old files.
	once.Do(func() { IndexFiles(files).Release() })

	// Close and delete all old index files.
	for _, f := range files {
		log.Info("Removing index file", zap.String("path", f.Path()))

		if err := f.Close(); err != nil {
			log.Error("Cannot close index file", zap.Error(err))
			return
		} else if err := os.Remove(f.Path()); err != nil {
			log.Error("Cannot remove index file", zap.Error(err))
			return
		}
	}
}

func (partition *Partition) Rebuild() {}

// returns true if the log file is too big or too old
// The caller must have at least a read lock on the partition
func (partition *Partition) needsLogCompaction() bool {
	size := partition.activeLogFile.Size()
	modTime := partition.activeLogFile.ModTime()
	return size >= partition.MaxLogFileSize || (size > 0 && modTime.Before(time.Now().Add(-partition.MaxLogFileAge)))
}

func (partition *Partition) CheckLogFile() error {
	// Check log file under read lock.
	needsCompaction := func() bool {
		partition.mu.RLock()
		defer partition.mu.RUnlock()
		return partition.needsLogCompaction()
	}()
	if !needsCompaction {
		return nil
	}

	// If file size exceeded then recheck under write lock and swap files.
	partition.mu.Lock()
	defer partition.mu.Unlock()
	return partition.checkLogFile()
}

func (partition *Partition) checkLogFile() error {
	if !partition.needsLogCompaction() {
		return nil
	}

	// Open new log file and insert it into the first position.
	if err := partition.prependActiveLogFile(); err != nil {
		return err
	}

	// Begin compacting in a background goroutine.
	go func() {
		partition.Compact() // check for new compactions
	}()

	return nil
}

// compactLogFile compacts f into a tsi file. The new file will share the
// same identifier but will have a ".tsi" extension. Once the log file is
// compacted then the manifest is updated and the log file is discarded.
func (partition *Partition) compactLogFile(logFile *LogFile) {
	if partition.isClosing() {
		return
	}

	partition.mu.Lock()
	interrupt := partition.compactionInterrupt
	partition.mu.Unlock()

	start := time.Now()

	// Retrieve identifier from current path.
	id := logFile.ID()
	assert(id != 0, "cannot parse log file id: %s", logFile.Path())

	// Build a logger for this compaction.
	log, logEnd := logger.NewOperation(context.TODO(), partition.logger, "TSI log compaction", "tsi1_compact_log_file", zap.Int("tsi1_log_file_id", id))
	defer logEnd()

	// Create new index file.
	path := filepath.Join(partition.path, FormatIndexFileName(id, 1))
	f, err := os.Create(path)
	if err != nil {
		log.Error("Cannot create index file", zap.Error(err))
		return
	}
	defer f.Close()

	// Compact log file to new index file.
	lvl := partition.levels[1]
	n, err := logFile.CompactTo(f, lvl.M, lvl.K, interrupt)
	if err != nil {
		log.Error("Cannot compact log file", zap.Error(err), zap.String("path", logFile.Path()))
		return
	}

	if err = f.Sync(); err != nil {
		log.Error("Cannot sync index file", zap.Error(err))
		return
	}

	// Close file.
	if err := f.Close(); err != nil {
		log.Error("Cannot close index file", zap.Error(err))
		return
	}

	// Reopen as an index file.
	file := NewIndexFile(partition.sfile)
	file.SetPath(path)
	if err := file.Open(); err != nil {
		log.Error("Cannot open compacted index file", zap.Error(err), zap.String("path", file.Path()))
		return
	}

	// Obtain lock to swap in index file and write manifest.
	if err := func() (rErr error) {
		partition.mu.Lock()
		defer partition.mu.Unlock()

		// Replace previous log file with index file.
		newFileSet := partition.fileSet.MustReplace([]File{logFile}, file)

		defer errors2.Capture(&rErr, func() error {
			if rErr != nil {
				// close new file
				file.Close()
			}
			return rErr
		})()

		// Write new manifest.
		manifestSize, err := partition.manifest(newFileSet).Write()

		if err != nil {
			return fmt.Errorf("manifest file write failed compacting log file %q: %w", partition.ManifestPath(), err)
		}
		// Store the new FileSet in the partition now that the manifest has been written
		partition.fileSet = newFileSet
		partition.manifestSize = manifestSize
		return nil
	}(); err != nil {
		log.Error("Cannot update manifest", zap.Error(err))
		return
	}

	elapsed := time.Since(start)
	log.Info("Log file compacted",
		logger.DurationLiteral("elapsed", elapsed),
		zap.Int64("bytes", n),
		zap.Int("kb_per_sec", int(float64(n)/elapsed.Seconds())/1024),
	)

	// Closing the log file will automatically wait until the ref count is zero.
	if err := logFile.Close(); err != nil {
		log.Error("Cannot close log file", zap.Error(err))
		return
	} else if err := os.Remove(logFile.Path()); err != nil {
		log.Error("Cannot remove log file", zap.Error(err))
		return
	}
}

// unionStringSets returns the union of two sets
func unionStringSets(a, b map[string]struct{}) map[string]struct{} {
	other := make(map[string]struct{})
	for k := range a {
		other[k] = struct{}{}
	}
	for k := range b {
		other[k] = struct{}{}
	}
	return other
}

// intersectStringSets returns the intersection of two sets.
func intersectStringSets(a, b map[string]struct{}) map[string]struct{} {
	if len(a) < len(b) {
		a, b = b, a
	}

	other := make(map[string]struct{})
	for k := range a {
		if _, ok := b[k]; ok {
			other[k] = struct{}{}
		}
	}
	return other
}

var fileIDRegex = regexp.MustCompile(`^L(\d+)-(\d+)\..+$`)

// ParseFilename extracts the numeric id from a log or index file path.
// Returns 0 if it cannot be parsed.
func ParseFilename(name string) (level, id int) {
	a := fileIDRegex.FindStringSubmatch(filepath.Base(name))
	if a == nil {
		return 0, 0
	}

	level, _ = strconv.Atoi(a[1])
	id, _ = strconv.Atoi(a[2])
	return id, level
}

// Manifest represents the list of log & index files that make up the index.
// The files are listed in time order, not necessarily ID order.
type Manifest struct {
	Levels []CompactionLevel `json:"levels,omitempty"`
	Files  []string          `json:"files,omitempty"`

	// Version should be updated whenever the TSI format has changed.
	Version int `json:"version,omitempty"`

	path string // location on disk of the manifest.
}

// NewManifest returns a new instance of Manifest with default compaction levels.
func NewManifest(path string) *Manifest {
	m := &Manifest{
		Levels:  make([]CompactionLevel, len(DefaultCompactionLevels)),
		Version: Version,
		path:    path,
	}
	copy(m.Levels, DefaultCompactionLevels)
	return m
}

// HasFile returns true if name is listed in the log files or index files.
func (m *Manifest) HasFile(name string) bool {
	for _, filename := range m.Files {
		if filename == name {
			return true
		}
	}
	return false
}

// Validate checks if the Manifest's version is compatible with this version
// of the tsi1 index.
func (m *Manifest) Validate() error {
	// If we don't have an explicit version in the manifest file then we know
	// it's not compatible with the latest tsi1 Index.
	if m.Version != Version {
		return fmt.Errorf("%q: %w", m.path, ErrIncompatibleVersion)
	}
	return nil
}

// Write writes the manifest file to the provided path, returning the number of
// bytes written and an error, if any.
func (m *Manifest) Write() (int64, error) {
	var tmp string
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return 0, fmt.Errorf("failed marshaling %q: %w", m.path, err)
	}
	buf = append(buf, '\n')

	f, err := os.CreateTemp(filepath.Dir(m.path), ManifestFileName)

	if err != nil {
		return 0, err
	}

	// In correct operation, Remove() should fail because the file was renamed
	defer os.Remove(tmp)

	err = func() (rErr error) {
		// Close() before rename for Windows
		defer errors2.Capture(&rErr, f.Close)()

		tmp = f.Name()

		if _, err = f.Write(buf); err != nil {
			return fmt.Errorf("failed writing temporary manifest file %q: %w", tmp, err)
		}
		if err = f.Sync(); err != nil {
			return fmt.Errorf("failed syncing temporary manifest file to disk %q: %w", tmp, err)
		}
		return nil
	}()
	if err != nil {
		return 0, err
	}

	if err = os.Rename(tmp, m.path); err != nil {
		return 0, err
	}

	return int64(len(buf)), nil
}

// ReadManifestFile reads a manifest from a file path and returns the Manifest,
// the size of the manifest on disk, and any error if appropriate.
func ReadManifestFile(path string) (*Manifest, int64, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}

	// Decode manifest.
	var m Manifest
	if err := json.Unmarshal(buf, &m); err != nil {
		return nil, 0, fmt.Errorf("failed unmarshaling %q: %w", path, err)
	}

	// Set the path of the manifest.
	m.path = path
	return &m, int64(len(buf)), nil
}

func joinIntSlice(a []int, sep string) string {
	other := make([]string, len(a))
	for i := range a {
		other[i] = strconv.Itoa(a[i])
	}
	return strings.Join(other, sep)
}

// CompactionLevel represents a grouping of index files based on bloom filter
// settings. By having the same bloom filter settings, the filters
// can be merged and evaluated at a higher level.
type CompactionLevel struct {
	// Bloom filter bit size & hash count
	M uint64 `json:"m,omitempty"`
	K uint64 `json:"k,omitempty"`
}

// DefaultCompactionLevels is the default settings used by the index.
var DefaultCompactionLevels = []CompactionLevel{
	{M: 0, K: 0},       // L0: Log files, no filter.
	{M: 1 << 25, K: 6}, // L1: Initial compaction
	{M: 1 << 25, K: 6}, // L2
	{M: 1 << 26, K: 6}, // L3
	{M: 1 << 27, K: 6}, // L4
	{M: 1 << 28, K: 6}, // L5
	{M: 1 << 29, K: 6}, // L6
	{M: 1 << 30, K: 6}, // L7
}

// MaxIndexMergeCount is the maximum number of files that can be merged together at once.
const MaxIndexMergeCount = 2

// MaxIndexFileSize is the maximum expected size of an index file.
const MaxIndexFileSize = 4 * (1 << 30)

// IsPartitionDir returns true if directory contains a MANIFEST file.
func IsPartitionDir(path string) (bool, error) {
	if _, err := os.Stat(filepath.Join(path, ManifestFileName)); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}
