package tsm1

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
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
	"time"

	"github.com/golang/snappy"
	"github.com/influxdata/influxdb/v2/pkg/limiter"
	"github.com/influxdata/influxdb/v2/pkg/pool"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// DefaultSegmentSize of 10MB is the size at which segment files will be rolled over.
	DefaultSegmentSize = 10 * 1024 * 1024

	// WALFileExtension is the file extension we expect for wal segments.
	WALFileExtension = "wal"

	// WALFilePrefix is the prefix on all wal segment files.
	WALFilePrefix = "_"

	// walEncodeBufSize is the size of the wal entry encoding buffer
	walEncodeBufSize = 4 * 1024 * 1024

	float64EntryType  = 1
	integerEntryType  = 2
	booleanEntryType  = 3
	stringEntryType   = 4
	unsignedEntryType = 5
)

// WalEntryType is a byte written to a wal segment file that indicates what the following compressed block contains.
type WalEntryType byte

const (
	// WriteWALEntryType indicates a write entry.
	WriteWALEntryType WalEntryType = 0x01

	// DeleteWALEntryType indicates a delete entry.
	DeleteWALEntryType WalEntryType = 0x02

	// DeleteRangeWALEntryType indicates a delete range entry.
	DeleteRangeWALEntryType WalEntryType = 0x03
)

var (
	// ErrWALClosed is returned when attempting to write to a closed WAL file.
	ErrWALClosed = fmt.Errorf("WAL closed")

	// ErrWALCorrupt is returned when reading a corrupt WAL entry.
	ErrWALCorrupt = fmt.Errorf("corrupted WAL entry")

	defaultWaitingWALWrites = runtime.GOMAXPROCS(0) * 2

	// bytePool is a shared bytes pool buffer re-cycle []byte slices to reduce allocations.
	bytesPool = pool.NewLimitedBytes(256, walEncodeBufSize*2)
)

// represent the write-ahead log used for writing TSM files.
type WAL struct {
	// goroutines waiting for the next fsync
	syncCount   uint64
	syncWaiters chan chan error

	mu            sync.RWMutex
	lastWriteTime time.Time

	dirPath string // 对应shard.walPath 证明 shard.go:415

	// 对应的wal的segmentFile的id
	currentSegmentID     int
	currentSegmentWriter *WALSegmentWriter

	// cache and flush variables
	once    sync.Once
	closing chan struct{}

	// set the duration to wait before fsyncing writes.  A value of 0 (default)
	// will cause every write to be fsync'd.  This must be set before the WAL
	// is opened if a non-default value is required.
	syncDelay time.Duration // 对应 storage-wal-fsync-delay

	// WALOutput is the writer used by the logger.
	logger       *zap.Logger // Logger to be used for important messages
	traceLogger  *zap.Logger // Logger to be used when trace-logging is on.
	traceLogging bool

	// file size at which a segment file will be rotated 默认10mb的
	SegmentSize int

	// statistics for the WAL
	stats *walMetrics

	// limit the max concurrency of waiting WAL writes.
	limiter limiter.Fixed // 对应 storage-wal-max-concurrent-writes

	// set the max duration the WAL will wait when limiter has no available
	// values to take.
	maxWriteWait time.Duration // 对应 storage-wal-max-write-delay
}

// initializes a new WAL at the given directory.
func NewWAL(path string, maxConcurrentWrites int, maxWriteDelay time.Duration, tags tsdb.EngineTags) *WAL {
	logger := zap.NewNop()
	if maxConcurrentWrites == 0 {
		maxConcurrentWrites = defaultWaitingWALWrites
	}

	return &WAL{
		dirPath: path,

		// these options should be overridden by any options in the config
		SegmentSize:  DefaultSegmentSize,
		closing:      make(chan struct{}),
		syncWaiters:  make(chan chan error, 1024),
		stats:        newWALMetrics(tags),
		limiter:      limiter.NewFixed(maxConcurrentWrites),
		maxWriteWait: maxWriteDelay,
		logger:       logger,
		traceLogger:  logger,
	}
}

// enableTraceLogging must be called before the WAL is opened.
func (wal *WAL) enableTraceLogging(enabled bool) {
	wal.traceLogging = enabled
	if enabled {
		wal.traceLogger = wal.logger
	}
}

// WithLogger sets the WAL's logger.
func (wal *WAL) WithLogger(log *zap.Logger) {
	wal.logger = log.With(zap.String("service", "wal"))

	if wal.traceLogging {
		wal.traceLogger = wal.logger
	}
}

var globalWALMetrics = newAllWALMetrics()

const walSubsystem = "wal"

type allWALMetrics struct {
	size      *prometheus.GaugeVec
	writes    *prometheus.CounterVec
	writesErr *prometheus.CounterVec
}

type walMetrics struct {
	// size should never be updated directly, only through SetSize/AddSize
	size prometheus.Gauge
	// sizeAtomic should never be updated directly, only through SetSize/AddSize
	sizeAtomic int64
	writes     prometheus.Counter
	writesErr  prometheus.Counter
}

func (f *walMetrics) AddSize(n int64) {
	val := atomic.AddInt64(&f.sizeAtomic, n)
	f.size.Set(float64(val))
}

func (f *walMetrics) SetSize(n int64) {
	atomic.StoreInt64(&f.sizeAtomic, n)
	f.size.Set(float64(n))
}

func newAllWALMetrics() *allWALMetrics {
	labels := tsdb.EngineLabelNames()
	return &allWALMetrics{
		size: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: walSubsystem,
			Name:      "size",
			Help:      "Gauge of size of WAL in bytes",
		}, labels),
		writes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: walSubsystem,
			Name:      "writes",
			Help:      "Number of write attempts to the WAL",
		}, labels),
		writesErr: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: walSubsystem,
			Name:      "writes_err",
			Help:      "Number of failed write attempts to the WAL",
		}, labels),
	}
}

func WALCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		globalWALMetrics.size,
		globalWALMetrics.writes,
		globalWALMetrics.writesErr,
	}
}

func newWALMetrics(tags tsdb.EngineTags) *walMetrics {
	labels := tags.GetLabels()
	return &walMetrics{
		size:      globalWALMetrics.size.With(labels),
		writes:    globalWALMetrics.writes.With(labels),
		writesErr: globalWALMetrics.writesErr.With(labels),
	}
}

// Path returns the directory the log was initialized with.
func (wal *WAL) Path() string {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	return wal.dirPath
}

// Open opens and initializes the Log. Open can recover from previous unclosed shutdowns.
func (wal *WAL) Open() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	wal.traceLogger.Info("tsm1 WAL starting", zap.Int("segment_size", wal.SegmentSize))
	wal.traceLogger.Info("tsm1 WAL writing", zap.String("path", wal.dirPath))

	if err := os.MkdirAll(wal.dirPath, 0777); err != nil {
		return err
	}

	segments, err := segmentFilePaths(wal.dirPath)
	if err != nil {
		return err
	}

	if len(segments) > 0 {
		lastSegment := segments[len(segments)-1]
		id, err := idFromFileName(lastSegment)
		if err != nil {
			return err
		}

		wal.currentSegmentID = id
		stat, err := os.Stat(lastSegment)
		if err != nil {
			return err
		}

		if stat.Size() == 0 {
			os.Remove(lastSegment)
			segments = segments[:len(segments)-1]
		} else {
			fd, err := os.OpenFile(lastSegment, os.O_RDWR, 0666)
			if err != nil {
				return err
			}
			if _, err := fd.Seek(0, io.SeekEnd); err != nil {
				_ = fd.Close()
				return err
			}
			wal.currentSegmentWriter = NewWALSegmentWriter(fd)

			// Set the correct size on the segment writer
			wal.currentSegmentWriter.size = int(stat.Size())
		}
	}

	var totalSize int64
	for _, seg := range segments {
		stat, err := os.Stat(seg)
		if err != nil {
			return err
		}

		if stat.Size() > 0 {
			totalSize += stat.Size()
			if stat.ModTime().After(wal.lastWriteTime) {
				wal.lastWriteTime = stat.ModTime().UTC()
			}
		}
	}

	wal.stats.SetSize(totalSize)

	wal.closing = make(chan struct{})

	return nil
}

// will schedule an fsync to the current wal segment and notify any
// waiting gorutines.  If an fsync is already scheduled, subsequent calls will
// not schedule a new fsync and will be handle by the existing scheduled fsync.
func (wal *WAL) scheduleSync() {
	// If we're not the first to sync, then another goroutine is fsyncing the wal for us.
	if !atomic.CompareAndSwapUint64(&wal.syncCount, 0, 1) {
		return
	}

	// Fsync the wal and notify all pending waiters
	go func() {
		var timerCh <-chan time.Time

		// 如果是0的话当写入wal文件后立即调用fsync的
		// 如不是那么需要的话可以写大点这样性能压力低 会将这段时间内的写入合并处理的
		if wal.syncDelay == 0 {
			// Create a RW chan and close it
			timerChrw := make(chan time.Time)
			close(timerChrw)
			// Convert it to a read-only
			timerCh = timerChrw
		} else {
			t := time.NewTicker(wal.syncDelay)
			defer t.Stop()
			timerCh = t.C
		}
		for {
			select {
			case <-timerCh:
				wal.mu.Lock()
				if len(wal.syncWaiters) == 0 {
					atomic.StoreUint64(&wal.syncCount, 0)
					wal.mu.Unlock()
					return
				}

				wal.sync()
				wal.mu.Unlock()
			case <-wal.closing:
				atomic.StoreUint64(&wal.syncCount, 0)
				return
			}
		}
	}()
}

// fsyncs the current wal segments and notifies any waiters.  Callers must ensure
// a write lock on the WAL is obtained before calling sync.
func (wal *WAL) sync() {
	err := wal.currentSegmentWriter.sync()
	for len(wal.syncWaiters) > 0 {
		errC := <-wal.syncWaiters
		errC <- err
	}
}

// write the given values to the WAL. It returns the WAL segment ID to
// which the points were written. If an error is returned the segment ID should
// be ignored.
func (wal *WAL) WriteMulti(ctx context.Context, measurementTagsFieldKey2FieldValues map[string][]Value) (int, error) {
	writeWALEntry := &WriteWALEntry{
		MeasurementTagsFieldKey2FieldValues: measurementTagsFieldKey2FieldValues,
	}

	id, err := wal.writeToLog(ctx, writeWALEntry)
	wal.stats.writes.Inc()
	if err != nil {
		wal.stats.writesErr.Inc()
		return -1, err
	}

	return id, nil
}

// returns a slice of the names of the closed segment files.
func (wal *WAL) ClosedSegmentFilePaths() ([]string, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	// Not loading files from disk so nothing to do
	if wal.dirPath == "" {
		return nil, nil
	}

	var currentSegmentFilePath string
	if wal.currentSegmentWriter != nil {
		currentSegmentFilePath = wal.currentSegmentWriter.path()
	}

	segmentFilePaths, err := segmentFilePaths(wal.dirPath)
	if err != nil {
		return nil, err
	}

	var closedSegmentFilePaths []string
	for _, segmentFilePath := range segmentFilePaths {
		// Skip the current path
		if segmentFilePath == currentSegmentFilePath {
			continue
		}

		closedSegmentFilePaths = append(closedSegmentFilePaths, segmentFilePath)
	}

	return closedSegmentFilePaths, nil
}

// delete the given segment file paths from disk and cleans up any associated objects.
func (wal *WAL) Remove(files []string) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	for _, fn := range files {
		wal.traceLogger.Info("Removing WAL file", zap.String("path", fn))
		os.RemoveAll(fn)
	}

	// Refresh the on-disk size stats
	segments, err := segmentFilePaths(wal.dirPath)
	if err != nil {
		return err
	}

	var totalSize int64
	for _, seg := range segments {
		stat, err := os.Stat(seg)
		if err != nil {
			return err
		}

		totalSize += stat.Size()
	}

	wal.stats.SetSize(totalSize)

	return nil
}

// the last time anything was written to the WAL.
func (wal *WAL) LastWriteTime() time.Time {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	return wal.lastWriteTime
}

func (wal *WAL) DiskSizeBytes() int64 {
	return atomic.LoadInt64(&wal.stats.sizeAtomic)
}

func (wal *WAL) writeToLog(ctx context.Context, walEntry WALEntry) (int, error) {
	// limit how many concurrent encodings can be in flight.  Since we can only
	// write one at a time to disk, a slow disk can cause the allocations below
	// to increase quickly.  If we're backed up, wait until others have completed.
	cancel := func() {}
	if wal.maxWriteWait > 0 {
		ctx, cancel = context.WithTimeout(ctx, wal.maxWriteWait)
	}
	if err := wal.limiter.Take(ctx); err != nil {
		cancel()
		return 0, err
	}
	defer wal.limiter.Release()
	cancel()

	byteSlice := bytesPool.Get(walEntry.MarshalSize())
	//  encoded源自byteSlice
	encoded, err := walEntry.Encode(byteSlice)
	if err != nil {
		bytesPool.Put(byteSlice)
		return -1, err
	}
	// 得到压缩后的最大长度
	encBuf := bytesPool.Get(snappy.MaxEncodedLen(len(encoded)))
	// compressed源自encBuf的
	compressed := snappy.Encode(encBuf, encoded)
	bytesPool.Put(byteSlice)

	syncErrChan := make(chan error)

	segID, err := func() (int, error) {
		wal.mu.Lock()
		defer wal.mu.Unlock()

		// Make sure the log has not been closed
		select {
		case <-wal.closing:
			return -1, ErrWALClosed
		default:
		}

		// roll the segment file if needed
		if err = wal.rollSegmentIfNeed(); err != nil {
			return -1, fmt.Errorf("error rolling WAL segment: %v", err)
		}

		// write and sync
		//oldSize := wal.currentSegmentWriter.size
		if err = wal.currentSegmentWriter.Write(walEntry.Type(), compressed); err != nil {
			return -1, fmt.Errorf("error writing WAL entry: %v", err)
		}
		//sizeDelta := wal.currentSegmentWriter.size - oldSize

		select {
		case wal.syncWaiters <- syncErrChan:
		default:
			return -1, fmt.Errorf("error syncing wal")
		}
		wal.scheduleSync()

		// Update stats for current segment size
		//wal.stats.AddSize(int64(sizeDelta))

		wal.lastWriteTime = time.Now().UTC()

		return wal.currentSegmentID, nil

	}()

	bytesPool.Put(encBuf)

	if err != nil {
		return segID, err
	}

	// schedule an fsync and wait for it to complete
	return segID, <-syncErrChan
}

// check if the current segment is due to roll over to a new segment;
// and if so, opens a new segment file for future writes.
func (wal *WAL) rollSegmentIfNeed() error {
	if wal.currentSegmentWriter == nil || wal.currentSegmentWriter.size > wal.SegmentSize {
		if err := wal.newSegmentFile(); err != nil {
			// A drop database or RP call could trigger this error if writes were in-flight
			// when the drop statement executes.
			return fmt.Errorf("error opening new segment file for wal (2): %v", err)
		}
		return nil
	}

	return nil
}

// closes the current segment if it is non-empty and opens a new one.
func (wal *WAL) CloseSegment() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if wal.currentSegmentWriter == nil || wal.currentSegmentWriter.size > 0 {
		if err := wal.newSegmentFile(); err != nil {
			// A drop database or RP call could trigger this error if writes were in-flight
			// when the drop statement executes.
			return fmt.Errorf("error opening new segment file for wal (1): %v", err)
		}
		return nil
	}
	return nil
}

// deletes the given keys, returning the segment ID for the operation.
func (wal *WAL) Delete(ctx context.Context, keys [][]byte) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	entry := &DeleteWALEntry{
		Keys: keys,
	}

	id, err := wal.writeToLog(ctx, entry)
	if err != nil {
		return -1, err
	}
	return id, nil
}

// deletes the given keys within the given time range,
// returning the segment ID for the operation.
func (wal *WAL) DeleteRange(ctx context.Context, keys [][]byte, min, max int64) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	entry := &DeleteRangeWALEntry{
		Keys: keys,
		Min:  min,
		Max:  max,
	}

	id, err := wal.writeToLog(ctx, entry)
	if err != nil {
		return -1, err
	}
	return id, nil
}

// Close will finish any flush that is currently in progress and close file handles.
func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Always attempt to close the segment writer. We cannot do this in once.Do
	// because if we have already closed the WAL before and reopened it again,
	// the next Close() call will not close the new segment writer. For example:
	// func main() {
	//   w.Close() -- (1)
	//   w.Open()
	//   w.Close() -- (2)
	// }
	// (2) needs to close the reopened `currentSegmentWriter` again.
	wal.traceLogger.Info("Closing WAL file", zap.String("path", wal.dirPath))
	if wal.currentSegmentWriter != nil {
		wal.sync()
		_ = wal.currentSegmentWriter.close()
		wal.currentSegmentWriter = nil
	}

	wal.once.Do(func() {
		// Close, but don't set to nil so future goroutines can still be signaled
		close(wal.closing)
	})

	return nil
}

// return all files that are WAL segment files in sorted order by ascending ID.
func segmentFilePaths(dir string) ([]string, error) {
	paths, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("%s*.%s", WALFilePrefix, WALFileExtension)))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}

// will close the current segment file and open a new one, updating bookkeeping info on the log.
func (wal *WAL) newSegmentFile() error {
	wal.currentSegmentID++
	if wal.currentSegmentWriter != nil {
		wal.sync()

		if err := wal.currentSegmentWriter.close(); err != nil {
			return err
		}
	}
	// wal.dirPath/_segmentId.wal
	segmentFilePath := filepath.Join(wal.dirPath, fmt.Sprintf("%s%05d.%s", WALFilePrefix, wal.currentSegmentID, WALFileExtension))
	segmentFile, err := os.OpenFile(segmentFilePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	wal.currentSegmentWriter = NewWALSegmentWriter(segmentFile)

	return nil
}

// WALEntry is record stored in each WAL segment.  Each entry has a type
// and an opaque, type dependent byte slice data attribute.
type WALEntry interface {
	Type() WalEntryType
	Encode(dst []byte) ([]byte, error)
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(b []byte) error
	MarshalSize() int
}

// represents a write of points.
type WriteWALEntry struct {
	MeasurementTagsFieldKey2FieldValues map[string][]Value
	sz                                  int
}

func (writeWALEntry *WriteWALEntry) MarshalSize() int {
	if writeWALEntry.sz > 0 || len(writeWALEntry.MeasurementTagsFieldKey2FieldValues) == 0 {
		return writeWALEntry.sz
	}

	encLen := 7 * len(writeWALEntry.MeasurementTagsFieldKey2FieldValues) // Type (1), Key Length (2), and Count (4) for each key

	// determine required length
	for k, v := range writeWALEntry.MeasurementTagsFieldKey2FieldValues {
		encLen += len(k)
		if len(v) == 0 {
			return 0
		}

		encLen += 8 * len(v) // timestamps (8)

		switch v[0].(type) {
		case FloatValue, IntegerValue, UnsignedValue:
			encLen += 8 * len(v)
		case BooleanValue:
			encLen += 1 * len(v)
		case StringValue:
			for _, vv := range v {
				str, ok := vv.(StringValue)
				if !ok {
					return 0
				}
				encLen += 4 + len(str.value)
			}
		default:
			return 0
		}
	}

	writeWALEntry.sz = encLen

	return writeWALEntry.sz
}

// Encode converts the WriteWALEntry into a byte stream using dst if it
// is large enough.  If dst is too small, the slice will be grown to fit the
// encoded entry.
func (writeWALEntry *WriteWALEntry) Encode(dst []byte) ([]byte, error) {
	// The entries values are encode as follows:
	//
	// For each key and slice of values, first a 1 byte type for the []Values
	// slice is written.  Following the type, the length and key bytes are written.
	// Following the key, a 4 byte count followed by each value as a 8 byte time
	// and N byte value.  The value is dependent on the type being encoded.  float64,
	// int64, use 8 bytes, boolean uses 1 byte, and string is similar to the key encoding,
	// except that string values have a 4-byte length, and keys only use 2 bytes.
	//
	// This structure is then repeated for each key an value slices.
	//
	// ┌──────────────────────────────────────────────────────────────────────────────┐
	// │        WriteWALEntry 对应1对 MeasurementTagsFieldKey -> FieldValues           │
	// ├──────┬─────────┬────────┬─────────────────┬─────────┬─────────┬──────┬───────┤
	// │ Type │ Key Len │   Key  │ fieldValueCount │  Time   │  Value  │Time  │ Value │
	// │1 byte│ 2 bytes │ N bytes│   4 bytes       │ 8 bytes │ N bytes │8 byte│N byte │
	// └──────┴─────────┴────────┴─────────────────┴─────────┴─────────┴──────┴───────┘

	encLen := writeWALEntry.MarshalSize() // Type (1), Key Length (2), and Count (4) for each key

	// allocate or re-slice to correct size
	if len(dst) < encLen {
		dst = make([]byte, encLen)
	} else {
		dst = dst[:encLen]
	}

	// Finally, encode the entry
	var totalLen int
	var curType byte

	for measurementTagsFieldKey, fieldValues := range writeWALEntry.MeasurementTagsFieldKey2FieldValues {
		switch fieldValues[0].(type) {
		case FloatValue:
			curType = float64EntryType
		case IntegerValue:
			curType = integerEntryType
		case UnsignedValue:
			curType = unsignedEntryType
		case BooleanValue:
			curType = booleanEntryType
		case StringValue:
			curType = stringEntryType
		default:
			return nil, fmt.Errorf("unsupported value type: %T", fieldValues[0])
		}
		dst[totalLen] = curType // 写1字节fieldValueType
		totalLen++
		// 写2字节keyLen
		binary.BigEndian.PutUint16(dst[totalLen:totalLen+2], uint16(len(measurementTagsFieldKey)))
		totalLen += 2
		totalLen += copy(dst[totalLen:], measurementTagsFieldKey) // 写 key

		binary.BigEndian.PutUint32(dst[totalLen:totalLen+4], uint32(len(fieldValues))) // 写4字节fieldValueCount
		totalLen += 4

		for _, fieldValue := range fieldValues {
			binary.BigEndian.PutUint64(dst[totalLen:totalLen+8], uint64(fieldValue.UnixNano())) // 写8字节的time
			totalLen += 8

			switch vv := fieldValue.(type) {
			case FloatValue:
				if curType != float64EntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", fieldValues[0].Value(), vv)
				}
				binary.BigEndian.PutUint64(dst[totalLen:totalLen+8], math.Float64bits(vv.value))
				totalLen += 8
			case IntegerValue:
				if curType != integerEntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", fieldValues[0].Value(), vv)
				}
				binary.BigEndian.PutUint64(dst[totalLen:totalLen+8], uint64(vv.value))
				totalLen += 8
			case UnsignedValue:
				if curType != unsignedEntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", fieldValues[0].Value(), vv)
				}
				binary.BigEndian.PutUint64(dst[totalLen:totalLen+8], vv.value)
				totalLen += 8
			case BooleanValue:
				if curType != booleanEntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", fieldValues[0].Value(), vv)
				}
				if vv.value {
					dst[totalLen] = 1
				} else {
					dst[totalLen] = 0
				}
				totalLen++
			case StringValue:
				if curType != stringEntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", fieldValues[0].Value(), vv)
				}
				binary.BigEndian.PutUint32(dst[totalLen:totalLen+4], uint32(len(vv.value)))
				totalLen += 4
				totalLen += copy(dst[totalLen:], vv.value)
			default:
				return nil, fmt.Errorf("unsupported value found in %T slice: %T", fieldValues[0].Value(), vv)
			}
		}
	}

	return dst[:totalLen], nil
}

// MarshalBinary returns a binary representation of the entry in a new byte slice.
func (writeWALEntry *WriteWALEntry) MarshalBinary() ([]byte, error) {
	// Temp buffer to write marshaled points into
	b := make([]byte, writeWALEntry.MarshalSize())
	return writeWALEntry.Encode(b)
}

// UnmarshalBinary deserializes the byte slice into w.
func (writeWALEntry *WriteWALEntry) UnmarshalBinary(b []byte) error {
	var i int
	for i < len(b) {
		typ := b[i]
		i++

		if i+2 > len(b) {
			return ErrWALCorrupt
		}

		length := int(binary.BigEndian.Uint16(b[i : i+2]))
		i += 2

		if i+length > len(b) {
			return ErrWALCorrupt
		}

		k := string(b[i : i+length])
		i += length

		if i+4 > len(b) {
			return ErrWALCorrupt
		}

		nvals := int(binary.BigEndian.Uint32(b[i : i+4]))
		i += 4

		if nvals <= 0 || nvals > len(b) {
			return ErrWALCorrupt
		}

		switch typ {
		case float64EntryType:
			if i+16*nvals > len(b) {
				return ErrWALCorrupt
			}

			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				v := math.Float64frombits((binary.BigEndian.Uint64(b[i : i+8])))
				i += 8
				values = append(values, NewFloatValue(un, v))
			}
			writeWALEntry.MeasurementTagsFieldKey2FieldValues[k] = values
		case integerEntryType:
			if i+16*nvals > len(b) {
				return ErrWALCorrupt
			}

			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				v := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				values = append(values, NewIntegerValue(un, v))
			}
			writeWALEntry.MeasurementTagsFieldKey2FieldValues[k] = values

		case unsignedEntryType:
			if i+16*nvals > len(b) {
				return ErrWALCorrupt
			}

			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				v := binary.BigEndian.Uint64(b[i : i+8])
				i += 8
				values = append(values, NewUnsignedValue(un, v))
			}
			writeWALEntry.MeasurementTagsFieldKey2FieldValues[k] = values

		case booleanEntryType:
			if i+9*nvals > len(b) {
				return ErrWALCorrupt
			}

			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8

				v := b[i]
				i += 1
				if v == 1 {
					values = append(values, NewBooleanValue(un, true))
				} else {
					values = append(values, NewBooleanValue(un, false))
				}
			}
			writeWALEntry.MeasurementTagsFieldKey2FieldValues[k] = values

		case stringEntryType:
			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				if i+12 > len(b) {
					return ErrWALCorrupt
				}

				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8

				length := int(binary.BigEndian.Uint32(b[i : i+4]))
				if i+length > len(b) {
					return ErrWALCorrupt
				}

				i += 4

				if i+length > len(b) {
					return ErrWALCorrupt
				}

				v := string(b[i : i+length])
				i += length
				values = append(values, NewStringValue(un, v))
			}
			writeWALEntry.MeasurementTagsFieldKey2FieldValues[k] = values

		default:
			return fmt.Errorf("unsupported value type: %#v", typ)
		}
	}
	return nil
}

// Type returns WriteWALEntryType.
func (writeWALEntry *WriteWALEntry) Type() WalEntryType {
	return WriteWALEntryType
}

// DeleteWALEntry represents the deletion of multiple series.
type DeleteWALEntry struct {
	Keys [][]byte
	sz   int
}

// MarshalBinary returns a binary representation of the entry in a new byte slice.
func (w *DeleteWALEntry) MarshalBinary() ([]byte, error) {
	b := make([]byte, w.MarshalSize())
	return w.Encode(b)
}

// UnmarshalBinary deserializes the byte slice into w.
func (w *DeleteWALEntry) UnmarshalBinary(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	// b originates from a pool. Copy what needs to be retained.
	buf := make([]byte, len(b))
	copy(buf, b)
	w.Keys = bytes.Split(buf, []byte("\n"))
	return nil
}

func (w *DeleteWALEntry) MarshalSize() int {
	if w.sz > 0 || len(w.Keys) == 0 {
		return w.sz
	}

	encLen := len(w.Keys) // newlines
	for _, k := range w.Keys {
		encLen += len(k)
	}

	w.sz = encLen

	return encLen
}

// Encode converts the DeleteWALEntry into a byte slice, appending to dst.
func (w *DeleteWALEntry) Encode(dst []byte) ([]byte, error) {
	sz := w.MarshalSize()

	if len(dst) < sz {
		dst = make([]byte, sz)
	}

	var n int
	for _, k := range w.Keys {
		n += copy(dst[n:], k)
		n += copy(dst[n:], "\n")
	}

	// We return n-1 to strip off the last newline so that unmarshalling the value
	// does not produce an empty string
	return []byte(dst[:n-1]), nil
}

// Type returns DeleteWALEntryType.
func (w *DeleteWALEntry) Type() WalEntryType {
	return DeleteWALEntryType
}

// DeleteRangeWALEntry represents the deletion of multiple series.
type DeleteRangeWALEntry struct {
	Keys     [][]byte
	Min, Max int64
	sz       int
}

// MarshalBinary returns a binary representation of the entry in a new byte slice.
func (w *DeleteRangeWALEntry) MarshalBinary() ([]byte, error) {
	b := make([]byte, w.MarshalSize())
	return w.Encode(b)
}

// UnmarshalBinary deserializes the byte slice into w.
func (w *DeleteRangeWALEntry) UnmarshalBinary(b []byte) error {
	if len(b) < 16 {
		return ErrWALCorrupt
	}

	w.Min = int64(binary.BigEndian.Uint64(b[:8]))
	w.Max = int64(binary.BigEndian.Uint64(b[8:16]))

	i := 16
	for i < len(b) {
		if i+4 > len(b) {
			return ErrWALCorrupt
		}
		sz := int(binary.BigEndian.Uint32(b[i : i+4]))
		i += 4

		if i+sz > len(b) {
			return ErrWALCorrupt
		}

		// b originates from a pool. Copy what needs to be retained.
		buf := make([]byte, sz)
		copy(buf, b[i:i+sz])
		w.Keys = append(w.Keys, buf)
		i += sz
	}
	return nil
}

func (w *DeleteRangeWALEntry) MarshalSize() int {
	if w.sz > 0 {
		return w.sz
	}

	sz := 16 + len(w.Keys)*4
	for _, k := range w.Keys {
		sz += len(k)
	}

	w.sz = sz

	return sz
}

// Encode converts the DeleteRangeWALEntry into a byte slice, appending to b.
func (w *DeleteRangeWALEntry) Encode(b []byte) ([]byte, error) {
	sz := w.MarshalSize()

	if len(b) < sz {
		b = make([]byte, sz)
	}

	binary.BigEndian.PutUint64(b[:8], uint64(w.Min))
	binary.BigEndian.PutUint64(b[8:16], uint64(w.Max))

	i := 16
	for _, k := range w.Keys {
		binary.BigEndian.PutUint32(b[i:i+4], uint32(len(k)))
		i += 4
		i += copy(b[i:], k)
	}

	return b[:i], nil
}

// Type returns DeleteRangeWALEntryType.
func (w *DeleteRangeWALEntry) Type() WalEntryType {
	return DeleteRangeWALEntryType
}

// WALSegmentWriter writes WAL segments.
type WALSegmentWriter struct {
	writer      *bufio.Writer  // 实际的使用write
	writeCloser io.WriteCloser // 不实际的使用write
	size        int
}

// returns a new WALSegmentWriter writing to w.
func NewWALSegmentWriter(w io.WriteCloser) *WALSegmentWriter {
	return &WALSegmentWriter{
		writer:      bufio.NewWriterSize(w, 16*1024),
		writeCloser: w,
	}
}

func (walSegmentWriter *WALSegmentWriter) path() string {
	if f, ok := walSegmentWriter.writeCloser.(*os.File); ok {
		return f.Name()
	}
	return ""
}

// write entryType and the buffer containing compressed entry data.
func (walSegmentWriter *WALSegmentWriter) Write(entryType WalEntryType, compressed []byte) error {
	var buf [5]byte
	buf[0] = byte(entryType)                                      // 1个字节 walEntryType
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(compressed))) // 4个字节 压缩后的长度

	if _, err := walSegmentWriter.writer.Write(buf[:]); err != nil {
		return err
	}

	if _, err := walSegmentWriter.writer.Write(compressed); err != nil {
		return err
	}

	walSegmentWriter.size += len(buf) + len(compressed)

	return nil
}

// Sync flushes the file systems in-memory copy of recently written data to disk,
// if w is writing to an os.File.
func (walSegmentWriter *WALSegmentWriter) sync() error {
	if err := walSegmentWriter.writer.Flush(); err != nil {
		return err
	}

	if f, ok := walSegmentWriter.writeCloser.(*os.File); ok {
		return f.Sync()
	}
	return nil
}

func (walSegmentWriter *WALSegmentWriter) Flush() error {
	return walSegmentWriter.writer.Flush()
}

func (walSegmentWriter *WALSegmentWriter) close() error {
	if err := walSegmentWriter.Flush(); err != nil {
		return err
	}
	return walSegmentWriter.writeCloser.Close()
}

// WALSegmentReader reads WAL segments.
type WALSegmentReader struct {
	rc    io.ReadCloser
	r     *bufio.Reader
	entry WALEntry
	n     int64
	err   error
}

// NewWALSegmentReader returns a new WALSegmentReader reading from r.
func NewWALSegmentReader(r io.ReadCloser) *WALSegmentReader {
	return &WALSegmentReader{
		rc: r,
		r:  bufio.NewReader(r),
	}
}

func (r *WALSegmentReader) Reset(rc io.ReadCloser) {
	r.rc = rc
	r.r.Reset(rc)
	r.entry = nil
	r.n = 0
	r.err = nil
}

// Next indicates if there is a value to read.
func (r *WALSegmentReader) Next() bool {
	var nReadOK int

	// read the type and the length of the entry
	var lv [5]byte
	n, err := io.ReadFull(r.r, lv[:])
	if err == io.EOF {
		return false
	}

	if err != nil {
		r.err = err
		// We return true here because we want the client code to call read which
		// will return the this error to be handled.
		return true
	}
	nReadOK += n

	entryType := lv[0]
	length := binary.BigEndian.Uint32(lv[1:5])

	b := *(getBuf(int(length)))
	defer putBuf(&b)

	// read the compressed block and decompress it
	n, err = io.ReadFull(r.r, b[:length])
	if err != nil {
		r.err = err
		return true
	}
	nReadOK += n

	decLen, err := snappy.DecodedLen(b[:length])
	if err != nil {
		r.err = err
		return true
	}
	decBuf := *(getBuf(decLen))
	defer putBuf(&decBuf)

	data, err := snappy.Decode(decBuf, b[:length])
	if err != nil {
		r.err = err
		return true
	}

	// and marshal it and send it to the cache
	switch WalEntryType(entryType) {
	case WriteWALEntryType:
		r.entry = &WriteWALEntry{
			MeasurementTagsFieldKey2FieldValues: make(map[string][]Value),
		}
	case DeleteWALEntryType:
		r.entry = &DeleteWALEntry{}
	case DeleteRangeWALEntryType:
		r.entry = &DeleteRangeWALEntry{}
	default:
		r.err = fmt.Errorf("unknown wal entry type: %v", entryType)
		return true
	}
	r.err = r.entry.UnmarshalBinary(data)
	if r.err == nil {
		// Read and decode of this entry was successful.
		r.n += int64(nReadOK)
	}

	return true
}

// Read returns the next entry in the reader.
func (r *WALSegmentReader) Read() (WALEntry, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.entry, nil
}

// Count returns the total number of bytes read successfully from the segment, as
// of the last call to Read(). The segment is guaranteed to be valid up to and
// including this number of bytes.
func (r *WALSegmentReader) Count() int64 {
	return r.n
}

// Error returns the last error encountered by the reader.
func (r *WALSegmentReader) Error() error {
	return r.err
}

// Close closes the underlying io.Reader.
func (r *WALSegmentReader) Close() error {
	if r.rc == nil {
		return nil
	}
	err := r.rc.Close()
	r.rc = nil
	return err
}

// idFromFileName parses the segment file ID from its name.
func idFromFileName(name string) (int, error) {
	parts := strings.Split(filepath.Base(name), ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("file %s has wrong name format to have an id", name)
	}

	id, err := strconv.ParseUint(parts[0][1:], 10, 32)

	return int(id), err
}
