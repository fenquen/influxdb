package tsdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/limiter"
	"github.com/influxdata/influxdb/v2/pkg/rhh"
	"go.uber.org/zap"
)

var (
	ErrSeriesPartitionClosed              = errors.New("tsdb: series partition closed")
	ErrSeriesPartitionCompactionCancelled = errors.New("tsdb: series partition compaction cancelled")
)

// DefaultSeriesPartitionCompactThreshold is the number of series IDs to hold in the in-memory
// series map before compacting and rebuilding the on-disk representation.
const DefaultSeriesPartitionCompactThreshold = 1 << 17 // 128K

// SeriesPartition represents a subset of series file data.
type SeriesPartition struct {
	mu   sync.RWMutex
	wg   sync.WaitGroup
	id   int
	path string

	closed  bool
	closing chan struct{}
	once    sync.Once

	segments []*SeriesSegment
	index    *SeriesIndex
	seq      uint64 // series id sequence

	compacting          bool
	compactionLimiter   limiter.Fixed
	compactionsDisabled int

	CompactThreshold int

	Logger *zap.Logger
}

// NewSeriesPartition returns a new instance of SeriesPartition.
func NewSeriesPartition(id int, path string, compactionLimiter limiter.Fixed) *SeriesPartition {
	return &SeriesPartition{
		id:                id,
		path:              path,
		closing:           make(chan struct{}),
		compactionLimiter: compactionLimiter,
		CompactThreshold:  DefaultSeriesPartitionCompactThreshold,
		Logger:            zap.NewNop(),
		seq:               uint64(id) + 1,
	}
}

// Open memory maps the data file at the partition's path.
func (seriesPartition *SeriesPartition) Open() error {
	if seriesPartition.closed {
		return errors.New("tsdb: cannot reopen series partition")
	}

	// Create path if it doesn't exist.
	if err := os.MkdirAll(filepath.Join(seriesPartition.path), 0777); err != nil {
		return err
	}

	// Open components.
	if err := func() (err error) {
		if err := seriesPartition.openSegments(); err != nil {
			return err
		}

		// Init last segment for writes.
		if err := seriesPartition.activeSegment().InitForWrite(); err != nil {
			return err
		}

		seriesPartition.index = NewSeriesIndex(seriesPartition.IndexPath())
		if err := seriesPartition.index.Open(); err != nil {
			return err
		} else if err := seriesPartition.index.Recover(seriesPartition.segments); err != nil {
			return err
		}

		return nil
	}(); err != nil {
		seriesPartition.Close()
		return err
	}

	return nil
}

func (seriesPartition *SeriesPartition) openSegments() error {
	des, err := os.ReadDir(seriesPartition.path)
	if err != nil {
		return err
	}

	for _, de := range des {
		segmentID, err := ParseSeriesSegmentFilename(de.Name())
		if err != nil {
			continue
		}

		segment := NewSeriesSegment(segmentID, filepath.Join(seriesPartition.path, de.Name()))
		if err := segment.Open(); err != nil {
			return err
		}
		seriesPartition.segments = append(seriesPartition.segments, segment)
	}

	// Find max series id by searching segments in reverse order.
	for i := len(seriesPartition.segments) - 1; i >= 0; i-- {
		if seq := seriesPartition.segments[i].MaxSeriesID(); seq >= seriesPartition.seq {
			// Reset our sequence num to the next one to assign
			seriesPartition.seq = seq + SeriesFilePartitionN
			break
		}
	}

	// Create initial segment if none exist.
	if len(seriesPartition.segments) == 0 {
		segment, err := CreateSeriesSegment(0, filepath.Join(seriesPartition.path, "0000"))
		if err != nil {
			return err
		}
		seriesPartition.segments = append(seriesPartition.segments, segment)
	}

	return nil
}

// Close unmaps the data files.
func (seriesPartition *SeriesPartition) Close() (err error) {
	seriesPartition.once.Do(func() { close(seriesPartition.closing) })
	seriesPartition.wg.Wait()

	seriesPartition.mu.Lock()
	defer seriesPartition.mu.Unlock()

	seriesPartition.closed = true

	for _, s := range seriesPartition.segments {
		if e := s.Close(); e != nil && err == nil {
			err = e
		}
	}
	seriesPartition.segments = nil

	if seriesPartition.index != nil {
		if e := seriesPartition.index.Close(); e != nil && err == nil {
			err = e
		}
	}
	seriesPartition.index = nil

	return err
}

// ID returns the partition id.
func (seriesPartition *SeriesPartition) ID() int { return seriesPartition.id }

// Path returns the path to the partition.
func (seriesPartition *SeriesPartition) Path() string { return seriesPartition.path }

// IndexPath returns the path to the series index.
func (seriesPartition *SeriesPartition) IndexPath() string { return filepath.Join(seriesPartition.path, "index") }

// Index returns the partition's index.
func (seriesPartition *SeriesPartition) Index() *SeriesIndex { return seriesPartition.index }

// Segments returns a list of partition segments. Used for testing.
func (seriesPartition *SeriesPartition) Segments() []*SeriesSegment { return seriesPartition.segments }

// FileSize returns the size of all partitions, in bytes.
func (seriesPartition *SeriesPartition) FileSize() (n int64, err error) {
	for _, ss := range seriesPartition.segments {
		fi, err := os.Stat(ss.Path())
		if err != nil {
			return 0, err
		}
		n += fi.Size()
	}
	return n, err
}

// creates a list of series in bulk if they don't exist.
// The ids parameter is modified to contain series IDs for all keys belonging to this partition.
func (seriesPartition *SeriesPartition) CreateSeriesListIfNotExists(keys [][]byte, keyPartitionIDs []int, ids []uint64) error {
	var writeRequired bool
	seriesPartition.mu.RLock()
	if seriesPartition.closed {
		seriesPartition.mu.RUnlock()
		return ErrSeriesPartitionClosed
	}
	for i := range keys {
		if keyPartitionIDs[i] != seriesPartition.id {
			continue
		}
		id := seriesPartition.index.FindIDBySeriesKey(seriesPartition.segments, keys[i])
		if id == 0 {
			writeRequired = true
			continue
		}
		ids[i] = id
	}
	seriesPartition.mu.RUnlock()

	// Exit if all series for this partition already exist.
	if !writeRequired {
		return nil
	}

	type keyRange struct {
		id     uint64
		offset int64
	}
	newKeyRanges := make([]keyRange, 0, len(keys))

	// Obtain write lock to create new series.
	seriesPartition.mu.Lock()
	defer seriesPartition.mu.Unlock()

	if seriesPartition.closed {
		return ErrSeriesPartitionClosed
	}

	// Track offsets of duplicate series.
	newIDs := make(map[string]uint64, len(ids))

	for i := range keys {
		// Skip series that don't belong to the partition or have already been created.
		if keyPartitionIDs[i] != seriesPartition.id || ids[i] != 0 {
			continue
		}

		// Re-attempt lookup under write lock.
		key := keys[i]
		if ids[i] = newIDs[string(key)]; ids[i] != 0 {
			continue
		} else if ids[i] = seriesPartition.index.FindIDBySeriesKey(seriesPartition.segments, key); ids[i] != 0 {
			continue
		}

		// Write to series log and save offset.
		id, offset, err := seriesPartition.insert(key)
		if err != nil {
			return err
		}
		// Append new key to be added to hash map after flush.
		ids[i] = id
		newIDs[string(key)] = id
		newKeyRanges = append(newKeyRanges, keyRange{id, offset})
	}

	// Flush active segment writes so we can access data in mmap.
	if segment := seriesPartition.activeSegment(); segment != nil {
		if err := segment.Flush(); err != nil {
			return err
		}
	}

	// Add keys to hash map(s).
	for _, keyRange := range newKeyRanges {
		seriesPartition.index.Insert(seriesPartition.seriesKeyByOffset(keyRange.offset), keyRange.id, keyRange.offset)
	}

	// Check if we've crossed the compaction threshold.
	if seriesPartition.compactionsEnabled() && !seriesPartition.compacting &&
		seriesPartition.CompactThreshold != 0 && seriesPartition.index.InMemCount() >= uint64(seriesPartition.CompactThreshold) &&
		seriesPartition.compactionLimiter.TryTake() {
		seriesPartition.compacting = true
		log, logEnd := logger.NewOperation(context.TODO(), seriesPartition.Logger, "Series partition compaction", "series_partition_compaction", zap.String("path", seriesPartition.path))

		seriesPartition.wg.Add(1)
		go func() {
			defer seriesPartition.wg.Done()
			defer seriesPartition.compactionLimiter.Release()

			seriesPartitionCompactor := NewSeriesPartitionCompactor()
			seriesPartitionCompactor.cancel = seriesPartition.closing
			if err := seriesPartitionCompactor.Compact(seriesPartition); err != nil {
				log.Error("series partition compaction failed", zap.Error(err))
			}

			logEnd()

			// Clear compaction flag.
			seriesPartition.mu.Lock()
			seriesPartition.compacting = false
			seriesPartition.mu.Unlock()
		}()
	}

	return nil
}

// Compacting returns if the SeriesPartition is currently compacting.
func (seriesPartition *SeriesPartition) Compacting() bool {
	seriesPartition.mu.RLock()
	defer seriesPartition.mu.RUnlock()
	return seriesPartition.compacting
}

// DeleteSeriesID flags a series as permanently deleted.
// If the series is reintroduced later then it must create a new id.
func (seriesPartition *SeriesPartition) DeleteSeriesID(id uint64) error {
	seriesPartition.mu.Lock()
	defer seriesPartition.mu.Unlock()

	if seriesPartition.closed {
		return ErrSeriesPartitionClosed
	}

	// Already tombstoned, ignore.
	if seriesPartition.index.IsDeleted(id) {
		return nil
	}

	// Write tombstone entry.
	_, err := seriesPartition.writeLogEntry(AppendSeriesEntry(nil, SeriesEntryTombstoneFlag, id, nil))
	if err != nil {
		return err
	}

	// Flush active segment write.
	if segment := seriesPartition.activeSegment(); segment != nil {
		if err := segment.Flush(); err != nil {
			return err
		}
	}

	// Mark tombstone in memory.
	seriesPartition.index.Delete(id)

	return nil
}

// IsDeleted returns true if the ID has been deleted before.
func (seriesPartition *SeriesPartition) IsDeleted(id uint64) bool {
	seriesPartition.mu.RLock()
	if seriesPartition.closed {
		seriesPartition.mu.RUnlock()
		return false
	}
	v := seriesPartition.index.IsDeleted(id)
	seriesPartition.mu.RUnlock()
	return v
}

// SeriesKey returns the series key for a given id.
func (seriesPartition *SeriesPartition) SeriesKey(id uint64) []byte {
	if id == 0 {
		return nil
	}
	seriesPartition.mu.RLock()
	if seriesPartition.closed {
		seriesPartition.mu.RUnlock()
		return nil
	}
	key := seriesPartition.seriesKeyByOffset(seriesPartition.index.FindOffsetByID(id))
	seriesPartition.mu.RUnlock()
	return key
}

// Series returns the parsed series name and tags for an offset.
func (seriesPartition *SeriesPartition) Series(id uint64) ([]byte, models.Tags) {
	key := seriesPartition.SeriesKey(id)
	if key == nil {
		return nil, nil
	}
	return ParseSeriesKey(key)
}

// FindIDBySeriesKey return the series id for the series key.
func (seriesPartition *SeriesPartition) FindIDBySeriesKey(key []byte) uint64 {
	seriesPartition.mu.RLock()
	if seriesPartition.closed {
		seriesPartition.mu.RUnlock()
		return 0
	}
	id := seriesPartition.index.FindIDBySeriesKey(seriesPartition.segments, key)
	seriesPartition.mu.RUnlock()
	return id
}

// SeriesCount returns the number of series.
func (seriesPartition *SeriesPartition) SeriesCount() uint64 {
	seriesPartition.mu.RLock()
	if seriesPartition.closed {
		seriesPartition.mu.RUnlock()
		return 0
	}
	n := seriesPartition.index.Count()
	seriesPartition.mu.RUnlock()
	return n
}

func (seriesPartition *SeriesPartition) DisableCompactions() {
	seriesPartition.mu.Lock()
	defer seriesPartition.mu.Unlock()
	seriesPartition.compactionsDisabled++
}

func (seriesPartition *SeriesPartition) EnableCompactions() {
	seriesPartition.mu.Lock()
	defer seriesPartition.mu.Unlock()

	if seriesPartition.compactionsEnabled() {
		return
	}
	seriesPartition.compactionsDisabled--
}

func (seriesPartition *SeriesPartition) compactionsEnabled() bool {
	return seriesPartition.compactionLimiter != nil && seriesPartition.compactionsDisabled == 0
}

// AppendSeriesIDs returns a list of all series ids.
func (seriesPartition *SeriesPartition) AppendSeriesIDs(a []uint64) []uint64 {
	for _, segment := range seriesPartition.segments {
		a = segment.AppendSeriesIDs(a)
	}
	return a
}

// activeSegment returns the last segment.
func (seriesPartition *SeriesPartition) activeSegment() *SeriesSegment {
	if len(seriesPartition.segments) == 0 {
		return nil
	}
	return seriesPartition.segments[len(seriesPartition.segments)-1]
}

func (seriesPartition *SeriesPartition) insert(key []byte) (id uint64, offset int64, err error) {
	id = seriesPartition.seq
	offset, err = seriesPartition.writeLogEntry(AppendSeriesEntry(nil, SeriesEntryInsertFlag, id, key))
	if err != nil {
		return 0, 0, err
	}

	seriesPartition.seq += SeriesFilePartitionN
	return id, offset, nil
}

// writeLogEntry appends an entry to the end of the active segment.
// If there is no more room in the segment then a new segment is added.
func (seriesPartition *SeriesPartition) writeLogEntry(data []byte) (offset int64, err error) {
	segment := seriesPartition.activeSegment()
	if segment == nil || !segment.CanWrite(data) {
		if segment, err = seriesPartition.createSegment(); err != nil {
			return 0, err
		}
	}
	return segment.WriteLogEntry(data)
}

// createSegment appends a new segment
func (seriesPartition *SeriesPartition) createSegment() (*SeriesSegment, error) {
	// Close writer for active segment, if one exists.
	if segment := seriesPartition.activeSegment(); segment != nil {
		if err := segment.CloseForWrite(); err != nil {
			return nil, err
		}
	}

	// Generate a new sequential segment identifier.
	var id uint16
	if len(seriesPartition.segments) > 0 {
		id = seriesPartition.segments[len(seriesPartition.segments)-1].ID() + 1
	}
	filename := fmt.Sprintf("%04x", id)

	// Generate new empty segment.
	segment, err := CreateSeriesSegment(id, filepath.Join(seriesPartition.path, filename))
	if err != nil {
		return nil, err
	}
	seriesPartition.segments = append(seriesPartition.segments, segment)

	// Allow segment to write.
	if err := segment.InitForWrite(); err != nil {
		return nil, err
	}

	return segment, nil
}

func (seriesPartition *SeriesPartition) seriesKeyByOffset(offset int64) []byte {
	if offset == 0 {
		return nil
	}

	segmentID, pos := SplitSeriesOffset(offset)
	for _, segment := range seriesPartition.segments {
		if segment.ID() != segmentID {
			continue
		}

		key, _ := ReadSeriesKey(segment.Slice(pos + SeriesEntryHeaderSize))
		return key
	}

	return nil
}

// SeriesPartitionCompactor represents an object reindexes a series partition and optionally compacts segments.
type SeriesPartitionCompactor struct {
	cancel <-chan struct{}
}

// NewSeriesPartitionCompactor returns a new instance of SeriesPartitionCompactor.
func NewSeriesPartitionCompactor() *SeriesPartitionCompactor {
	return &SeriesPartitionCompactor{}
}

// Compact rebuilds the series partition index.
func (c *SeriesPartitionCompactor) Compact(seriesPartition *SeriesPartition) error {
	// Snapshot the partitions and index so we can check tombstones and replay at the end under lock.
	seriesPartition.mu.RLock()
	segments := CloneSeriesSegments(seriesPartition.segments)
	seriesIndex := seriesPartition.index.Clone()
	seriesN := seriesPartition.index.Count()
	seriesPartition.mu.RUnlock()

	// Compact index to a temporary location.
	indexPath := seriesIndex.path + ".compacting"
	if err := c.compactIndexTo(seriesIndex, seriesN, segments, indexPath); err != nil {
		return err
	}

	// Swap compacted index under lock & replay since compaction.
	if err := func() error {
		seriesPartition.mu.Lock()
		defer seriesPartition.mu.Unlock()

		// Reopen index with new file.
		if err := seriesPartition.index.Close(); err != nil {
			return err
		} else if err := os.Rename(indexPath, seriesIndex.path); err != nil {
			return err
		} else if err := seriesPartition.index.Open(); err != nil {
			return err
		}

		// Replay new entries.
		if err := seriesPartition.index.Recover(seriesPartition.segments); err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	return nil
}

func (c *SeriesPartitionCompactor) compactIndexTo(index *SeriesIndex, seriesN uint64, segments []*SeriesSegment, path string) error {
	hdr := NewSeriesIndexHeader()
	hdr.Count = seriesN
	hdr.Capacity = pow2((int64(hdr.Count) * 100) / SeriesIndexLoadFactor)

	// Allocate space for maps.
	keyIDMap := make([]byte, (hdr.Capacity * SeriesIndexElemSize))
	idOffsetMap := make([]byte, (hdr.Capacity * SeriesIndexElemSize))

	// Reindex all partitions.
	var entryN int
	for _, segment := range segments {
		errDone := errors.New("done")

		if err := segment.ForEachEntry(func(flag uint8, id uint64, offset int64, key []byte) error {

			// Make sure we don't go past the offset where the compaction began.
			if offset > index.maxOffset {
				return errDone
			}

			// Check for cancellation periodically.
			if entryN++; entryN%1000 == 0 {
				select {
				case <-c.cancel:
					return ErrSeriesPartitionCompactionCancelled
				default:
				}
			}

			// Only process insert entries.
			switch flag {
			case SeriesEntryInsertFlag: // fallthrough
			case SeriesEntryTombstoneFlag:
				return nil
			default:
				return fmt.Errorf("unexpected series partition log entry flag: %d", flag)
			}

			// Save max series identifier processed.
			hdr.MaxSeriesID, hdr.MaxOffset = id, offset

			// Ignore entry if tombstoned.
			if index.IsDeleted(id) {
				return nil
			}

			// Insert into maps.
			c.insertIDOffsetMap(idOffsetMap, hdr.Capacity, id, offset)
			return c.insertKeyIDMap(keyIDMap, hdr.Capacity, segments, key, offset, id)
		}); err == errDone {
			break
		} else if err != nil {
			return err
		}
	}

	// Open file handler.
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Calculate map positions.
	hdr.KeyIDMap.Offset, hdr.KeyIDMap.Size = SeriesIndexHeaderSize, int64(len(keyIDMap))
	hdr.IDOffsetMap.Offset, hdr.IDOffsetMap.Size = hdr.KeyIDMap.Offset+hdr.KeyIDMap.Size, int64(len(idOffsetMap))

	// Write header.
	if _, err := hdr.WriteTo(f); err != nil {
		return err
	}

	// Write maps.
	if _, err := f.Write(keyIDMap); err != nil {
		return err
	} else if _, err := f.Write(idOffsetMap); err != nil {
		return err
	}

	// Sync & close.
	if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	}

	return nil
}

func (c *SeriesPartitionCompactor) insertKeyIDMap(dst []byte, capacity int64, segments []*SeriesSegment, key []byte, offset int64, id uint64) error {
	mask := capacity - 1
	hash := rhh.HashKey(key)

	// Continue searching until we find an empty slot or lower probe distance.
	for i, dist, pos := int64(0), int64(0), hash&mask; ; i, dist, pos = i+1, dist+1, (pos+1)&mask {
		assert(i <= capacity, "key/id map full")
		elem := dst[(pos * SeriesIndexElemSize):]

		// If empty slot found or matching offset, insert and exit.
		elemOffset := int64(binary.BigEndian.Uint64(elem[:8]))
		elemID := binary.BigEndian.Uint64(elem[8:])
		if elemOffset == 0 || elemOffset == offset {
			binary.BigEndian.PutUint64(elem[:8], uint64(offset))
			binary.BigEndian.PutUint64(elem[8:], id)
			return nil
		}

		// Read key at position & hash.
		elemKey := ReadSeriesKeyFromSegments(segments, elemOffset+SeriesEntryHeaderSize)
		elemHash := rhh.HashKey(elemKey)

		// If the existing elem has probed less than us, then swap places with
		// existing elem, and keep going to find another slot for that elem.
		if d := rhh.Dist(elemHash, pos, capacity); d < dist {
			// Insert current values.
			binary.BigEndian.PutUint64(elem[:8], uint64(offset))
			binary.BigEndian.PutUint64(elem[8:], id)

			// Swap with values in that position.
			_, _, offset, id = elemHash, elemKey, elemOffset, elemID

			// Update current distance.
			dist = d
		}
	}
}

func (c *SeriesPartitionCompactor) insertIDOffsetMap(dst []byte, capacity int64, id uint64, offset int64) {
	mask := capacity - 1
	hash := rhh.HashUint64(id)

	// Continue searching until we find an empty slot or lower probe distance.
	for i, dist, pos := int64(0), int64(0), hash&mask; ; i, dist, pos = i+1, dist+1, (pos+1)&mask {
		assert(i <= capacity, "id/offset map full")
		elem := dst[(pos * SeriesIndexElemSize):]

		// If empty slot found or matching id, insert and exit.
		elemID := binary.BigEndian.Uint64(elem[:8])
		elemOffset := int64(binary.BigEndian.Uint64(elem[8:]))
		if elemOffset == 0 || elemOffset == offset {
			binary.BigEndian.PutUint64(elem[:8], id)
			binary.BigEndian.PutUint64(elem[8:], uint64(offset))
			return
		}

		// Hash key.
		elemHash := rhh.HashUint64(elemID)

		// If the existing elem has probed less than us, then swap places with
		// existing elem, and keep going to find another slot for that elem.
		if d := rhh.Dist(elemHash, pos, capacity); d < dist {
			// Insert current values.
			binary.BigEndian.PutUint64(elem[:8], id)
			binary.BigEndian.PutUint64(elem[8:], uint64(offset))

			// Swap with values in that position.
			_, id, offset = elemHash, elemID, elemOffset

			// Update current distance.
			dist = d
		}
	}
}

// pow2 returns the number that is the next highest power of 2.
// Returns v if it is a power of 2.
func pow2(v int64) int64 {
	for i := int64(2); i < 1<<62; i *= 2 {
		if i >= v {
			return i
		}
	}
	panic("unreachable")
}
