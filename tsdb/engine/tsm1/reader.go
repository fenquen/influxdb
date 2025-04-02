package tsm1

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/influxdata/influxdb/v2/pkg/bytesutil"
	"github.com/influxdata/influxdb/v2/pkg/file"
	"github.com/influxdata/influxdb/v2/tsdb"
)

// ErrFileInUse is returned when attempting to remove or close a TSM file that is still being used.
var ErrFileInUse = fmt.Errorf("file still in use")

// nilOffset is the value written to the offsets to indicate that position is deleted.  The value is the max
// uint32 which is an invalid position.  We don't use 0 as 0 is actually a valid position.
var nilOffset = []byte{255, 255, 255, 255}

// a reader for a TSM file.
type TsmFileReader struct {
	// refs is the count of active references to this reader.
	refs   int64
	refsWG sync.WaitGroup

	madviseWillNeed bool // Hint to the kernel with MADV_WILLNEED. 对应 storage-tsm-use-madv-willneed
	mu              sync.RWMutex

	// accessor provides access and decoding of blocks for the reader.
	blockAccessor blockAccessor

	// index is the index of all blocks.
	tsmIndex TSMIndex

	// tombstoner ensures tombstoned keys are not available by the index.
	tombstoner *Tombstoner

	// size is the size of the file on disk.
	tsmFileSize int64

	// lastModified is the last time this file was modified on disk
	lastModified int64

	// deleteMu limits concurrent deletes
	deleteMu sync.Mutex
}

// TSMIndex represent the index section of a TSM file.  The index records all
// blocks, their locations, sizes, min and max times.
type TSMIndex interface {
	// Delete removes the given keys from the index.
	Delete(keys [][]byte)

	// DeleteRange removes the given keys with data between minTime and maxTime from the index.
	DeleteRange(keys [][]byte, minTime, maxTime int64)

	// ContainsKey returns true if the given key may exist in the index.  This func is faster than
	// Contains but, may return false positives.
	ContainsKey(key []byte) bool

	// Contains return true if the given key exists in the index.
	Contains(key []byte) bool

	// ContainsValue returns true if key and time might exist in this file.  This function could
	// return true even though the actual point does not exists.  For example, the key may
	// exist in this file, but not have a point exactly at time t.
	ContainsValue(key []byte, timestamp int64) bool

	// Entries returns all index entries for a key.
	Entries(key []byte) []IndexEntry

	// ReadEntries reads the index entries for key into entries.
	ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry

	// Entry returns the index entry for the specified key and timestamp.  If no entry
	// matches the key and timestamp, nil is returned.
	Entry(key []byte, timestamp int64) *IndexEntry

	// Key returns the key in the index at the given position, using entries to avoid allocations.
	Key(index int, entries *[]IndexEntry) ([]byte, byte, []IndexEntry)

	// KeyAt returns the key in the index at the given position.
	KeyAt(index int) ([]byte, byte)

	// KeyCount returns the count of unique keys in the index.
	KeyCount() int

	// Seek returns the position in the index where key <= value in the index.
	Seek(key []byte) int

	// OverlapsTimeRange returns true if the time range of the file intersect min and max.
	OverlapsTimeRange(min, max int64) bool

	// OverlapsKeyRange returns true if the min and max keys of the file overlap the arguments min and max.
	OverlapsKeyRange(min, max []byte) bool

	// Size returns the size of the current index in bytes.
	Size() uint32

	// TimeRange returns the min and max time across all keys in the file.
	TimeRange() (int64, int64)

	// TombstoneRange returns ranges of time that are deleted for the given key.
	TombstoneRange(key []byte) []TimeRange

	// KeyRange returns the min and max keys in the file.
	KeyRange() ([]byte, []byte)

	// Type returns the block type of the values stored for the key.  Returns one of
	// BlockFloat64, BlockInt64, BlockBool, BlockString.  If key does not exist,
	// an error is returned.
	Type(key []byte) (byte, error)

	// UnmarshalBinary populates an index from an encoded byte slice
	// representation of an index.
	UnmarshalBinary(b []byte) error

	// Close closes the index and releases any resources.
	Close() error
}

// BlockIterator allows iterating over each block in a TSM file in order.  It provides
// raw access to the block bytes without decoding them.
type BlockIterator struct {
	r *TsmFileReader

	// i is the current key index
	i int

	// n is the total number of keys
	n int

	key     []byte
	cache   []IndexEntry
	entries []IndexEntry
	err     error
	typ     byte
}

// PeekNext returns the next key to be iterated or an empty string.
func (b *BlockIterator) PeekNext() []byte {
	if len(b.entries) > 1 {
		return b.key
	} else if b.n-b.i > 1 {
		key, _ := b.r.KeyAt(b.i + 1)
		return key
	}
	return nil
}

// Next returns true if there are more blocks to iterate through.
func (b *BlockIterator) Next() bool {
	if b.err != nil {
		return false
	}

	if b.n-b.i == 0 && len(b.entries) == 0 {
		return false
	}

	if len(b.entries) > 0 {
		b.entries = b.entries[1:]
		if len(b.entries) > 0 {
			return true
		}
	}

	if b.n-b.i > 0 {
		b.key, b.typ, b.entries = b.r.Key(b.i, &b.cache)
		b.i++

		// If there were deletes on the TSMReader, then our index is now off and we
		// can't proceed.  What we just read may not actually the next block.
		if b.n != b.r.KeyCount() {
			b.err = fmt.Errorf("delete during iteration")
			return false
		}

		if len(b.entries) > 0 {
			return true
		}
	}

	return false
}

// Read reads information about the next block to be iterated.
func (b *BlockIterator) Read() (key []byte, minTime int64, maxTime int64, typ byte, checksum uint32, buf []byte, err error) {
	if b.err != nil {
		return nil, 0, 0, 0, 0, nil, b.err
	}
	checksum, buf, err = b.r.ReadBytes(&b.entries[0], nil)
	if err != nil {
		b.err = err
		return nil, 0, 0, 0, 0, nil, err
	}
	return b.key, b.entries[0].MinTime, b.entries[0].MaxTime, b.typ, checksum, buf, err
}

// Err returns any errors encounter during iteration.
func (b *BlockIterator) Err() error {
	return b.err
}

type tsmReaderOption func(*TsmFileReader)

// WithMadviseWillNeed is an option for specifying whether to provide a MADV_WILL need hint to the kernel.
var WithMadviseWillNeed = func(willNeed bool) tsmReaderOption {
	return func(tsmReader *TsmFileReader) {
		tsmReader.madviseWillNeed = willNeed
	}
}

// returns a new TSMReader from the given file.
func NewTsmFileReader(tsmFile *os.File, options ...tsmReaderOption) (*TsmFileReader, error) {
	tsmReader := &TsmFileReader{}
	for _, option := range options {
		option(tsmReader)
	}

	tsmFileInfo, err := tsmFile.Stat()
	if err != nil {
		return nil, err
	}
	tsmReader.tsmFileSize = tsmFileInfo.Size()
	tsmReader.lastModified = tsmFileInfo.ModTime().UnixNano()
	tsmReader.blockAccessor = &mmapAccessor{
		tsmFile:      tsmFile,
		mmapWillNeed: tsmReader.madviseWillNeed,
	}

	indirectTsmIndex, err := tsmReader.blockAccessor.init()
	if err != nil {
		_ = tsmReader.blockAccessor.close()
		return nil, err
	}

	tsmReader.tsmIndex = indirectTsmIndex
	tsmReader.tombstoner = NewTombstoner(tsmReader.Path(), indirectTsmIndex.ContainsKey)

	if err := tsmReader.applyTombstones(); err != nil {
		return nil, err
	}

	return tsmReader, nil
}

// WithObserver sets the observer for the TSM reader.
func (tsmReader *TsmFileReader) WithObserver(obs tsdb.FileStoreObserver) {
	tsmReader.tombstoner.WithObserver(obs)
}

func (tsmReader *TsmFileReader) applyTombstones() error {
	var cur, prev Tombstone
	batch := make([][]byte, 0, 4096)

	if err := tsmReader.tombstoner.Walk(func(ts Tombstone) error {
		cur = ts
		if len(batch) > 0 {
			if prev.Min != cur.Min || prev.Max != cur.Max {
				tsmReader.tsmIndex.DeleteRange(batch, prev.Min, prev.Max)
				batch = batch[:0]
			}
		}

		// Copy the tombstone key and re-use the buffers to avoid allocations
		n := len(batch)
		batch = batch[:n+1]
		if cap(batch[n]) < len(ts.Key) {
			batch[n] = make([]byte, len(ts.Key))
		} else {
			batch[n] = batch[n][:len(ts.Key)]
		}
		copy(batch[n], ts.Key)

		if len(batch) >= 4096 {
			tsmReader.tsmIndex.DeleteRange(batch, prev.Min, prev.Max)
			batch = batch[:0]
		}

		prev = ts
		return nil
	}); err != nil {
		return fmt.Errorf("init: read tombstones: %v", err)
	}

	if len(batch) > 0 {
		tsmReader.tsmIndex.DeleteRange(batch, cur.Min, cur.Max)
	}
	return nil
}

func (tsmReader *TsmFileReader) Free() error {
	tsmReader.mu.RLock()
	defer tsmReader.mu.RUnlock()
	return tsmReader.blockAccessor.free()
}

// Path returns the path of the file the TSMReader was initialized with.
func (tsmReader *TsmFileReader) Path() string {
	tsmReader.mu.RLock()
	p := tsmReader.blockAccessor.path()
	tsmReader.mu.RUnlock()
	return p
}

// Key returns the key and the underlying entry at the numeric index.
func (tsmReader *TsmFileReader) Key(index int, entries *[]IndexEntry) ([]byte, byte, []IndexEntry) {
	return tsmReader.tsmIndex.Key(index, entries)
}

// KeyAt returns the key and key type at position idx in the index.
func (tsmReader *TsmFileReader) KeyAt(idx int) ([]byte, byte) {
	return tsmReader.tsmIndex.KeyAt(idx)
}

func (tsmReader *TsmFileReader) Seek(key []byte) int {
	return tsmReader.tsmIndex.Seek(key)
}

// ReadAt returns the values corresponding to the given index entry.
func (tsmReader *TsmFileReader) ReadAt(entry *IndexEntry, vals []Value) ([]Value, error) {
	tsmReader.mu.RLock()
	v, err := tsmReader.blockAccessor.readBlock(entry, vals)
	tsmReader.mu.RUnlock()
	return v, err
}

// Read returns the values corresponding to the block at the given key and timestamp.
func (tsmReader *TsmFileReader) Read(key []byte, timestamp int64) ([]Value, error) {
	tsmReader.mu.RLock()
	v, err := tsmReader.blockAccessor.read(key, timestamp)
	tsmReader.mu.RUnlock()
	return v, err
}

// ReadAll returns all values for a key in all blocks.
func (tsmReader *TsmFileReader) ReadAll(key []byte) ([]Value, error) {
	tsmReader.mu.RLock()
	v, err := tsmReader.blockAccessor.readAll(key)
	tsmReader.mu.RUnlock()
	return v, err
}

func (tsmReader *TsmFileReader) ReadBytes(e *IndexEntry, b []byte) (uint32, []byte, error) {
	tsmReader.mu.RLock()
	n, v, err := tsmReader.blockAccessor.readBytes(e, b)
	tsmReader.mu.RUnlock()
	return n, v, err
}

// Type returns the type of values stored at the given key.
func (tsmReader *TsmFileReader) Type(key []byte) (byte, error) {
	return tsmReader.tsmIndex.Type(key)
}

// Close closes the TSMReader.
func (tsmReader *TsmFileReader) Close() error {
	tsmReader.refsWG.Wait()

	tsmReader.mu.Lock()
	defer tsmReader.mu.Unlock()

	if err := tsmReader.blockAccessor.close(); err != nil {
		return err
	}

	return tsmReader.tsmIndex.Close()
}

// Ref records a usage of this TSMReader.  If there are active references
// when the reader is closed or removed, the reader will remain open until
// there are no more references.
func (tsmReader *TsmFileReader) Ref() {
	atomic.AddInt64(&tsmReader.refs, 1)
	tsmReader.refsWG.Add(1)
}

// removes a usage record of this TSMReader.  If the Reader was closed
// by another goroutine while there were active references, the file will
// be closed and remove
func (tsmReader *TsmFileReader) Unref() {
	atomic.AddInt64(&tsmReader.refs, -1)
	tsmReader.refsWG.Done()
}

// InUse returns whether the TSMReader currently has any active references.
func (tsmReader *TsmFileReader) InUse() bool {
	refs := atomic.LoadInt64(&tsmReader.refs)
	return refs > 0
}

// Remove removes any underlying files stored on disk for this reader.
func (tsmReader *TsmFileReader) Remove() error {
	tsmReader.mu.Lock()
	defer tsmReader.mu.Unlock()
	return tsmReader.remove()
}

// Rename renames the underlying file to the new path.
func (tsmReader *TsmFileReader) Rename(path string) error {
	tsmReader.mu.Lock()
	defer tsmReader.mu.Unlock()
	return tsmReader.blockAccessor.rename(path)
}

// Remove removes any underlying files stored on disk for this reader.
func (tsmReader *TsmFileReader) remove() error {
	path := tsmReader.blockAccessor.path()

	if tsmReader.InUse() {
		return ErrFileInUse
	}

	if path != "" {
		err := os.RemoveAll(path)
		if err != nil {
			return err
		}
	}

	if err := tsmReader.tombstoner.Delete(); err != nil {
		return err
	}
	return nil
}

// Contains returns whether the given key is present in the index.
func (tsmReader *TsmFileReader) Contains(key []byte) bool {
	return tsmReader.tsmIndex.Contains(key)
}

// ContainsValue returns true if key and time might exists in this file.  This function could
// return true even though the actual point does not exist.  For example, the key may
// exist in this file, but not have a point exactly at time t.
func (tsmReader *TsmFileReader) ContainsValue(key []byte, ts int64) bool {
	return tsmReader.tsmIndex.ContainsValue(key, ts)
}

// DeleteRange removes the given points for keys between minTime and maxTime.   The series
// keys passed in must be sorted.
func (tsmReader *TsmFileReader) DeleteRange(keys [][]byte, minTime, maxTime int64) error {
	if len(keys) == 0 {
		return nil
	}

	batch := tsmReader.BatchDelete()
	if err := batch.DeleteRange(keys, minTime, maxTime); err != nil {
		batch.Rollback()
		return err
	}
	return batch.Commit()
}

// Delete deletes blocks indicated by keys.
func (tsmReader *TsmFileReader) Delete(keys [][]byte) error {
	if err := tsmReader.tombstoner.Add(keys); err != nil {
		return err
	}

	if err := tsmReader.tombstoner.Flush(); err != nil {
		return err
	}

	tsmReader.tsmIndex.Delete(keys)
	return nil
}

// OverlapsTimeRange returns true if the time range of the file intersect min and max.
func (tsmReader *TsmFileReader) OverlapsTimeRange(min, max int64) bool {
	return tsmReader.tsmIndex.OverlapsTimeRange(min, max)
}

// OverlapsKeyRange returns true if the key range of the file intersect min and max.
func (tsmReader *TsmFileReader) OverlapsKeyRange(min, max []byte) bool {
	return tsmReader.tsmIndex.OverlapsKeyRange(min, max)
}

// TimeRange returns the min and max time across all keys in the file.
func (tsmReader *TsmFileReader) TimeRange() (int64, int64) {
	return tsmReader.tsmIndex.TimeRange()
}

// KeyRange returns the min and max key across all keys in the file.
func (tsmReader *TsmFileReader) KeyRange() ([]byte, []byte) {
	return tsmReader.tsmIndex.KeyRange()
}

// KeyCount returns the count of unique keys in the TSMReader.
func (tsmReader *TsmFileReader) KeyCount() int {
	return tsmReader.tsmIndex.KeyCount()
}

// Entries returns all index entries for key.
func (tsmReader *TsmFileReader) Entries(key []byte) []IndexEntry {
	return tsmReader.tsmIndex.Entries(key)
}

// ReadEntries reads the index entries for key into entries.
func (tsmReader *TsmFileReader) ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry {
	return tsmReader.tsmIndex.ReadEntries(key, entries)
}

// IndexSize returns the size of the index in bytes.
func (tsmReader *TsmFileReader) IndexSize() uint32 {
	return tsmReader.tsmIndex.Size()
}

// Size returns the size of the underlying file in bytes.
func (tsmReader *TsmFileReader) Size() uint32 {
	tsmReader.mu.RLock()
	size := tsmReader.tsmFileSize
	tsmReader.mu.RUnlock()
	return uint32(size)
}

// LastModified returns the last time the underlying file was modified.
func (tsmReader *TsmFileReader) LastModified() int64 {
	tsmReader.mu.RLock()
	lm := tsmReader.lastModified
	if ts := tsmReader.tombstoner.TombstoneStats(); ts.TombstoneExists {
		if ts.LastModified > lm {
			lm = ts.LastModified
		}
	}
	tsmReader.mu.RUnlock()
	return lm
}

// HasTombstones return true if there are any tombstone entries recorded.
func (tsmReader *TsmFileReader) HasTombstones() bool {
	tsmReader.mu.RLock()
	b := tsmReader.tombstoner.HasTombstones()
	tsmReader.mu.RUnlock()
	return b
}

// TombstoneFiles returns any tombstone files associated with this TSM file.
func (tsmReader *TsmFileReader) TombstoneStats() TombstoneStat {
	tsmReader.mu.RLock()
	fs := tsmReader.tombstoner.TombstoneStats()
	tsmReader.mu.RUnlock()
	return fs
}

// TombstoneRange returns ranges of time that are deleted for the given key.
func (tsmReader *TsmFileReader) TombstoneRange(key []byte) []TimeRange {
	tsmReader.mu.RLock()
	tr := tsmReader.tsmIndex.TombstoneRange(key)
	tsmReader.mu.RUnlock()
	return tr
}

// Stats returns the FileStat for the TSMReader's underlying file.
func (tsmReader *TsmFileReader) Stats() FileStat {
	minTime, maxTime := tsmReader.tsmIndex.TimeRange()
	minKey, maxKey := tsmReader.tsmIndex.KeyRange()
	return FileStat{
		Path:         tsmReader.Path(),
		Size:         tsmReader.Size(),
		LastModified: tsmReader.LastModified(),
		MinTime:      minTime,
		MaxTime:      maxTime,
		MinKey:       minKey,
		MaxKey:       maxKey,
		HasTombstone: tsmReader.tombstoner.HasTombstones(),
	}
}

// BlockIterator returns a BlockIterator for the underlying TSM file.
func (tsmReader *TsmFileReader) BlockIterator() *BlockIterator {
	return &BlockIterator{
		r: tsmReader,
		n: tsmReader.tsmIndex.KeyCount(),
	}
}

type BatchDeleter interface {
	DeleteRange(keys [][]byte, min, max int64) error
	Commit() error
	Rollback() error
}

type batchDelete struct {
	r *TsmFileReader
}

func (b *batchDelete) DeleteRange(keys [][]byte, minTime, maxTime int64) error {
	if len(keys) == 0 {
		return nil
	}

	// If the keys can't exist in this TSM file, skip it.
	minKey, maxKey := keys[0], keys[len(keys)-1]
	if !b.r.tsmIndex.OverlapsKeyRange(minKey, maxKey) {
		return nil
	}

	// If the timerange can't exist in this TSM file, skip it.
	if !b.r.tsmIndex.OverlapsTimeRange(minTime, maxTime) {
		return nil
	}

	if err := b.r.tombstoner.AddRange(keys, minTime, maxTime); err != nil {
		return err
	}

	return nil
}

func (b *batchDelete) Commit() error {
	defer b.r.deleteMu.Unlock()
	if err := b.r.tombstoner.Flush(); err != nil {
		return err
	}

	return b.r.applyTombstones()
}

func (b *batchDelete) Rollback() error {
	defer b.r.deleteMu.Unlock()
	return b.r.tombstoner.Rollback()
}

// BatchDelete returns a BatchDeleter.  Only a single goroutine may run a BatchDelete at a time.
// Callers must either Commit or Rollback the operation.
func (tsmReader *TsmFileReader) BatchDelete() BatchDeleter {
	tsmReader.deleteMu.Lock()
	return &batchDelete{r: tsmReader}
}

type BatchDeleters []BatchDeleter

func (a BatchDeleters) DeleteRange(keys [][]byte, min, max int64) error {
	errC := make(chan error, len(a))
	for _, b := range a {
		go func(b BatchDeleter) { errC <- b.DeleteRange(keys, min, max) }(b)
	}

	var err error
	for i := 0; i < len(a); i++ {
		dErr := <-errC
		if dErr != nil {
			err = dErr
		}
	}
	return err
}

func (a BatchDeleters) Commit() error {
	errC := make(chan error, len(a))
	for _, b := range a {
		go func(b BatchDeleter) { errC <- b.Commit() }(b)
	}

	var err error
	for i := 0; i < len(a); i++ {
		dErr := <-errC
		if dErr != nil {
			err = dErr
		}
	}
	return err
}

func (a BatchDeleters) Rollback() error {
	errC := make(chan error, len(a))
	for _, b := range a {
		go func(b BatchDeleter) { errC <- b.Rollback() }(b)
	}

	var err error
	for i := 0; i < len(a); i++ {
		dErr := <-errC
		if dErr != nil {
			err = dErr
		}
	}
	return err
}

// indirectIndex is a TSMIndex that uses a raw byte slice representation of an index.  This
// implementation can be used for indexes that may be MMAPed into memory.
type indirectTsmIndex struct {
	mu sync.RWMutex

	// indirectIndex works a follows.  Assuming we have an index structure in memory as
	// the diagram below:
	//
	// ┌────────────────────────────────────────────────────────────────────┐
	// │                               Index                                │
	// ├─┬──────────────────────┬──┬───────────────────────┬───┬────────────┘
	// │0│                      │62│                       │145│
	// ├─┴───────┬─────────┬────┼──┴──────┬─────────┬──────┼───┴─────┬──────┐
	// │Key 1 Len│   Key   │... │Key 2 Len│  Key 2  │ ...  │  Key 3  │ ...  │
	// │ 2 bytes │ N bytes │    │ 2 bytes │ N bytes │      │ 2 bytes │      │
	// └─────────┴─────────┴────┴─────────┴─────────┴──────┴─────────┴──────┘

	// We would build an `offset` slices where each element pointers to the byte location
	// for the first key in the index slice.

	// ┌────────────────────────────────────────────────────────────────────┐
	// │                              Offsets                               │
	// ├────┬────┬────┬─────────────────────────────────────────────────────┘
	// │ 0  │ 62 │145 │
	// └────┴────┴────┘

	// Using this offset slice we can find `Key 2` by doing a binary search
	// over the offsets slice.  Instead of comparing the value in the offsets
	// (e.g. `62`), we use that as an index into the underlying index to
	// retrieve the key at position `62` and perform our comparisons with that.

	// When we have identified the correct position in the index for a given
	// key, we could perform another binary search or a linear scan.  This
	// should be fast as well since each index entry is 28 bytes and all
	// contiguous in memory.  The current implementation uses a linear scan since the
	// number of block entries is expected to be < 100 per key.

	// b is the underlying index byte slice.  This could be a copy on the heap or an MMAP
	// slice reference
	b []byte

	// offsets contains the positions in b for each key.  It points to the 2 byte length of
	// key.
	offsets []byte

	// minKey, maxKey are the minimum and maximum (lexicographically sorted) contained in the
	// file
	minKey, maxKey []byte

	// minTime, maxTime are the minimum and maximum times contained in the file across all
	// series.
	minTime, maxTime int64

	// tombstones contains only the tombstoned keys with subset of time values deleted.  An
	// entry would exist here if a subset of the points for a key were deleted and the file
	// had not be re-compacted to remove the points on disk.
	tombstones map[string][]TimeRange
}

// TimeRange holds a min and max timestamp.
type TimeRange struct {
	Min, Max int64
}

func (t TimeRange) Overlaps(min, max int64) bool {
	return t.Min <= max && t.Max >= min
}

// NewIndirectIndex returns a new indirect index.
func NewIndirectIndex() *indirectTsmIndex {
	return &indirectTsmIndex{
		tombstones: make(map[string][]TimeRange),
	}
}

func (indirectTsmIndex *indirectTsmIndex) offset(i int) int {
	if i < 0 || i+4 > len(indirectTsmIndex.offsets) {
		return -1
	}
	return int(binary.BigEndian.Uint32(indirectTsmIndex.offsets[i*4 : i*4+4]))
}

func (indirectTsmIndex *indirectTsmIndex) Seek(key []byte) int {
	indirectTsmIndex.mu.RLock()
	defer indirectTsmIndex.mu.RUnlock()
	return indirectTsmIndex.searchOffset(key)
}

// searchOffset searches the offsets slice for key and returns the position in
// offsets where key would exist.
func (indirectTsmIndex *indirectTsmIndex) searchOffset(key []byte) int {
	// We use a binary search across our indirect offsets (pointers to all the keys
	// in the index slice).
	i := bytesutil.SearchBytesFixed(indirectTsmIndex.offsets, 4, func(x []byte) bool {
		// i is the position in offsets we are at so get offset it points to
		offset := int32(binary.BigEndian.Uint32(x))

		// It's pointing to the start of the key which is a 2 byte length
		keyLen := int32(binary.BigEndian.Uint16(indirectTsmIndex.b[offset : offset+2]))

		// See if it matches
		return bytes.Compare(indirectTsmIndex.b[offset+2:offset+2+keyLen], key) >= 0
	})

	// See if we might have found the right index
	if i < len(indirectTsmIndex.offsets) {
		return int(i / 4)
	}

	// The key is not in the index.  i is the index where it would be inserted so return
	// a value outside our offset range.
	return int(len(indirectTsmIndex.offsets)) / 4
}

// search returns the byte position of key in the index.  If key is not
// in the index, len(index) is returned.
func (indirectTsmIndex *indirectTsmIndex) search(key []byte) int {
	if !indirectTsmIndex.ContainsKey(key) {
		return len(indirectTsmIndex.b)
	}

	// We use a binary search across our indirect offsets (pointers to all the keys
	// in the index slice).
	// TODO(sgc): this should be inlined to `indirectIndex` as it is only used here
	i := bytesutil.SearchBytesFixed(indirectTsmIndex.offsets, 4, func(x []byte) bool {
		// i is the position in offsets we are at so get offset it points to
		offset := int32(binary.BigEndian.Uint32(x))

		// It's pointing to the start of the key which is a 2 byte length
		keyLen := int32(binary.BigEndian.Uint16(indirectTsmIndex.b[offset : offset+2]))

		// See if it matches
		return bytes.Compare(indirectTsmIndex.b[offset+2:offset+2+keyLen], key) >= 0
	})

	// See if we might have found the right index
	if i < len(indirectTsmIndex.offsets) {
		ofs := binary.BigEndian.Uint32(indirectTsmIndex.offsets[i : i+4])
		_, k := readKey(indirectTsmIndex.b[ofs:])

		// The search may have returned an i == 0 which could indicated that the value
		// searched should be inserted at position 0.  Make sure the key in the index
		// matches the search value.
		if !bytes.Equal(key, k) {
			return len(indirectTsmIndex.b)
		}

		return int(ofs)
	}

	// The key is not in the index.  i is the index where it would be inserted so return
	// a value outside our offset range.
	return len(indirectTsmIndex.b)
}

// ContainsKey returns true of key may exist in this index.
func (indirectTsmIndex *indirectTsmIndex) ContainsKey(key []byte) bool {
	return bytes.Compare(key, indirectTsmIndex.minKey) >= 0 && bytes.Compare(key, indirectTsmIndex.maxKey) <= 0
}

// Entries returns all index entries for a key.
func (indirectTsmIndex *indirectTsmIndex) Entries(key []byte) []IndexEntry {
	return indirectTsmIndex.ReadEntries(key, nil)
}

func (indirectTsmIndex *indirectTsmIndex) readEntriesAt(ofs int, entries *[]IndexEntry) ([]byte, []IndexEntry) {
	n, k := readKey(indirectTsmIndex.b[ofs:])

	// Read and return all the entries
	ofs += n
	var ie indexEntries
	if entries != nil {
		ie.entries = *entries
	}
	if _, err := readEntries(indirectTsmIndex.b[ofs:], &ie); err != nil {
		panic(fmt.Sprintf("error reading entries: %v", err))
	}
	if entries != nil {
		*entries = ie.entries
	}
	return k, ie.entries
}

// ReadEntries returns all index entries for a key.
func (indirectTsmIndex *indirectTsmIndex) ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry {
	indirectTsmIndex.mu.RLock()
	defer indirectTsmIndex.mu.RUnlock()

	ofs := indirectTsmIndex.search(key)
	if ofs < len(indirectTsmIndex.b) {
		k, entries := indirectTsmIndex.readEntriesAt(ofs, entries)
		// The search may have returned an i == 0 which could indicated that the value
		// searched should be inserted at position 0.  Make sure the key in the index
		// matches the search value.
		if !bytes.Equal(key, k) {
			return nil
		}

		return entries
	}

	// The key is not in the index.  i is the index where it would be inserted.
	return nil
}

// Entry returns the index entry for the specified key and timestamp.  If no entry
// matches the key an timestamp, nil is returned.
func (indirectTsmIndex *indirectTsmIndex) Entry(key []byte, timestamp int64) *IndexEntry {
	entries := indirectTsmIndex.Entries(key)
	for _, entry := range entries {
		if entry.Contains(timestamp) {
			return &entry
		}
	}
	return nil
}

// Key returns the key in the index at the given position.
func (indirectTsmIndex *indirectTsmIndex) Key(idx int, entries *[]IndexEntry) ([]byte, byte, []IndexEntry) {
	indirectTsmIndex.mu.RLock()
	defer indirectTsmIndex.mu.RUnlock()

	if idx < 0 || idx*4+4 > len(indirectTsmIndex.offsets) {
		return nil, 0, nil
	}
	ofs := binary.BigEndian.Uint32(indirectTsmIndex.offsets[idx*4 : idx*4+4])
	n, key := readKey(indirectTsmIndex.b[ofs:])

	typ := indirectTsmIndex.b[int(ofs)+n]

	var ie indexEntries
	if entries != nil {
		ie.entries = *entries
	}
	if _, err := readEntries(indirectTsmIndex.b[int(ofs)+n:], &ie); err != nil {
		return nil, 0, nil
	}
	if entries != nil {
		*entries = ie.entries
	}

	return key, typ, ie.entries
}

// KeyAt returns the key in the index at the given position.
func (indirectTsmIndex *indirectTsmIndex) KeyAt(idx int) ([]byte, byte) {
	indirectTsmIndex.mu.RLock()

	if idx < 0 || idx*4+4 > len(indirectTsmIndex.offsets) {
		indirectTsmIndex.mu.RUnlock()
		return nil, 0
	}
	ofs := int32(binary.BigEndian.Uint32(indirectTsmIndex.offsets[idx*4 : idx*4+4]))

	n, key := readKey(indirectTsmIndex.b[ofs:])
	ofs = ofs + int32(n)
	typ := indirectTsmIndex.b[ofs]
	indirectTsmIndex.mu.RUnlock()
	return key, typ
}

// KeyCount returns the count of unique keys in the index.
func (indirectTsmIndex *indirectTsmIndex) KeyCount() int {
	indirectTsmIndex.mu.RLock()
	n := len(indirectTsmIndex.offsets) / 4
	indirectTsmIndex.mu.RUnlock()
	return n
}

// Delete removes the given keys from the index.
func (indirectTsmIndex *indirectTsmIndex) Delete(keys [][]byte) {
	if len(keys) == 0 {
		return
	}

	if !bytesutil.IsSorted(keys) {
		bytesutil.Sort(keys)
	}

	// Both keys and offsets are sorted.  Walk both in order and skip
	// any keys that exist in both.
	indirectTsmIndex.mu.Lock()
	start := indirectTsmIndex.searchOffset(keys[0])
	for i := start * 4; i+4 <= len(indirectTsmIndex.offsets) && len(keys) > 0; i += 4 {
		offset := binary.BigEndian.Uint32(indirectTsmIndex.offsets[i : i+4])
		_, indexKey := readKey(indirectTsmIndex.b[offset:])

		for len(keys) > 0 && bytes.Compare(keys[0], indexKey) < 0 {
			keys = keys[1:]
		}

		if len(keys) > 0 && bytes.Equal(keys[0], indexKey) {
			keys = keys[1:]
			copy(indirectTsmIndex.offsets[i:i+4], nilOffset)
		}
	}
	indirectTsmIndex.offsets = bytesutil.Pack(indirectTsmIndex.offsets, 4, 255)
	indirectTsmIndex.mu.Unlock()
}

// DeleteRange removes the given keys with data between minTime and maxTime from the index.
func (indirectTsmIndex *indirectTsmIndex) DeleteRange(keys [][]byte, minTime, maxTime int64) {
	// No keys, nothing to do
	if len(keys) == 0 {
		return
	}

	if !bytesutil.IsSorted(keys) {
		bytesutil.Sort(keys)
	}

	// If we're deleting the max time range, just use tombstoning to remove the
	// key from the offsets slice
	if minTime == math.MinInt64 && maxTime == math.MaxInt64 {
		indirectTsmIndex.Delete(keys)
		return
	}

	// Is the range passed in outside of the time range for the file?
	min, max := indirectTsmIndex.TimeRange()
	if minTime > max || maxTime < min {
		return
	}

	fullKeys := make([][]byte, 0, len(keys))
	tombstones := map[string][]TimeRange{}
	var ie []IndexEntry

	for i := 0; len(keys) > 0 && i < indirectTsmIndex.KeyCount(); i++ {
		k, entries := indirectTsmIndex.readEntriesAt(indirectTsmIndex.offset(i), &ie)

		// Skip any keys that don't exist.  These are less than the current key.
		for len(keys) > 0 && bytes.Compare(keys[0], k) < 0 {
			keys = keys[1:]
		}

		// No more keys to delete, we're done.
		if len(keys) == 0 {
			break
		}

		// If the current key is greater than the index one, continue to the next
		// index key.
		if len(keys) > 0 && bytes.Compare(keys[0], k) > 0 {
			continue
		}

		// If multiple tombstones are saved for the same key
		if len(entries) == 0 {
			continue
		}

		// Is the time range passed outside of the time range we've have stored for this key?
		min, max := entries[0].MinTime, entries[len(entries)-1].MaxTime
		if minTime > max || maxTime < min {
			continue
		}

		// Does the range passed in cover every value for the key?
		if minTime <= min && maxTime >= max {
			fullKeys = append(fullKeys, keys[0])
			keys = keys[1:]
			continue
		}

		indirectTsmIndex.mu.RLock()
		existing := indirectTsmIndex.tombstones[string(k)]
		indirectTsmIndex.mu.RUnlock()

		// Append the new tombonstes to the existing ones
		newTs := append(existing, append(tombstones[string(k)], TimeRange{minTime, maxTime})...)
		fn := func(i, j int) bool {
			a, b := newTs[i], newTs[j]
			if a.Min == b.Min {
				return a.Max <= b.Max
			}
			return a.Min < b.Min
		}

		// Sort the updated tombstones if necessary
		if len(newTs) > 1 && !sort.SliceIsSorted(newTs, fn) {
			sort.Slice(newTs, fn)
		}

		tombstones[string(k)] = newTs

		// We need to see if all the tombstones end up deleting the entire series.  This
		// could happen if their is one tombstore with min,max time spanning all the block
		// time ranges or from multiple smaller tombstones the delete segments.  To detect
		// this cases, we use a window starting at the first tombstone and grow it be each
		// tombstone that is immediately adjacent to the current window or if it overlaps.
		// If there are any gaps, we abort.
		minTs, maxTs := newTs[0].Min, newTs[0].Max
		for j := 1; j < len(newTs); j++ {
			prevTs := newTs[j-1]
			ts := newTs[j]

			// Make sure all the tombstone line up for a continuous range.  We don't
			// want to have two small deletes on each edges end up causing us to
			// remove the full key.
			if prevTs.Max != ts.Min-1 && !prevTs.Overlaps(ts.Min, ts.Max) {
				minTs, maxTs = int64(math.MaxInt64), int64(math.MinInt64)
				break
			}

			if ts.Min < minTs {
				minTs = ts.Min
			}
			if ts.Max > maxTs {
				maxTs = ts.Max
			}
		}

		// If we have a fully deleted series, delete it all of it.
		if minTs <= min && maxTs >= max {
			fullKeys = append(fullKeys, keys[0])
			keys = keys[1:]
			continue
		}
	}

	// Delete all the keys that fully deleted in bulk
	if len(fullKeys) > 0 {
		indirectTsmIndex.Delete(fullKeys)
	}

	if len(tombstones) == 0 {
		return
	}

	indirectTsmIndex.mu.Lock()
	for k, v := range tombstones {
		indirectTsmIndex.tombstones[k] = v
	}
	indirectTsmIndex.mu.Unlock()
}

// TombstoneRange returns ranges of time that are deleted for the given key.
func (indirectTsmIndex *indirectTsmIndex) TombstoneRange(key []byte) []TimeRange {
	indirectTsmIndex.mu.RLock()
	r := indirectTsmIndex.tombstones[string(key)]
	indirectTsmIndex.mu.RUnlock()
	return r
}

// Contains return true if the given key exists in the index.
func (indirectTsmIndex *indirectTsmIndex) Contains(key []byte) bool {
	return len(indirectTsmIndex.Entries(key)) > 0
}

// ContainsValue returns true if key and time might exist in this file.
func (indirectTsmIndex *indirectTsmIndex) ContainsValue(key []byte, timestamp int64) bool {
	entry := indirectTsmIndex.Entry(key, timestamp)
	if entry == nil {
		return false
	}

	indirectTsmIndex.mu.RLock()
	tombstones := indirectTsmIndex.tombstones[string(key)]
	indirectTsmIndex.mu.RUnlock()

	for _, t := range tombstones {
		if t.Min <= timestamp && t.Max >= timestamp {
			return false
		}
	}
	return true
}

// Type returns the block type of the values stored for the key.
func (indirectTsmIndex *indirectTsmIndex) Type(key []byte) (byte, error) {
	indirectTsmIndex.mu.RLock()
	defer indirectTsmIndex.mu.RUnlock()

	ofs := indirectTsmIndex.search(key)
	if ofs < len(indirectTsmIndex.b) {
		n, _ := readKey(indirectTsmIndex.b[ofs:])
		ofs += n
		return indirectTsmIndex.b[ofs], nil
	}
	return 0, fmt.Errorf("key does not exist: %s", key)
}

// OverlapsTimeRange returns true if the time range of the file intersect min and max.
func (indirectTsmIndex *indirectTsmIndex) OverlapsTimeRange(min, max int64) bool {
	return indirectTsmIndex.minTime <= max && indirectTsmIndex.maxTime >= min
}

// OverlapsKeyRange returns true if the min and max keys of the file overlap the arguments min and max.
func (indirectTsmIndex *indirectTsmIndex) OverlapsKeyRange(min, max []byte) bool {
	return bytes.Compare(indirectTsmIndex.minKey, max) <= 0 && bytes.Compare(indirectTsmIndex.maxKey, min) >= 0
}

// KeyRange returns the min and max keys in the index.
func (indirectTsmIndex *indirectTsmIndex) KeyRange() ([]byte, []byte) {
	return indirectTsmIndex.minKey, indirectTsmIndex.maxKey
}

// TimeRange returns the min and max time across all keys in the index.
func (indirectTsmIndex *indirectTsmIndex) TimeRange() (int64, int64) {
	return indirectTsmIndex.minTime, indirectTsmIndex.maxTime
}

// MarshalBinary returns a byte slice encoded version of the index.
func (indirectTsmIndex *indirectTsmIndex) MarshalBinary() ([]byte, error) {
	indirectTsmIndex.mu.RLock()
	defer indirectTsmIndex.mu.RUnlock()

	return indirectTsmIndex.b, nil
}

// UnmarshalBinary populates an index from an encoded byte slice
// representation of an index.
func (indirectTsmIndex *indirectTsmIndex) UnmarshalBinary(b []byte) error {
	indirectTsmIndex.mu.Lock()
	defer indirectTsmIndex.mu.Unlock()

	// Keep a reference to the actual index bytes
	indirectTsmIndex.b = b
	if len(b) == 0 {
		return nil
	}

	//var minKey, maxKey []byte
	var minTime, maxTime int64 = math.MaxInt64, 0

	// To create our "indirect" index, we need to find the location of all the keys in
	// the raw byte slice.  The keys are listed once each (in sorted order).  Following
	// each key is a time ordered list of index entry blocks for that key.  The loop below
	// basically skips across the slice keeping track of the counter when we are at a key
	// field.
	var i int32
	var offsets []int32
	iMax := int32(len(b))
	for i < iMax {
		offsets = append(offsets, i)

		// Skip to the start of the values
		// key length value (2) + type (1) + length of key
		if i+2 >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for key length value")
		}
		i += 3 + int32(binary.BigEndian.Uint16(b[i:i+2]))

		// count of index entries
		if i+indexCountSize >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for index entries count")
		}
		count := int32(binary.BigEndian.Uint16(b[i : i+indexCountSize]))
		i += indexCountSize

		// Find the min time for the block
		if i+8 >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for min time")
		}
		minT := int64(binary.BigEndian.Uint64(b[i : i+8]))
		if minT < minTime {
			minTime = minT
		}

		i += (count - 1) * indexEntrySize

		// Find the max time for the block
		if i+16 >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for max time")
		}
		maxT := int64(binary.BigEndian.Uint64(b[i+8 : i+16]))
		if maxT > maxTime {
			maxTime = maxT
		}

		i += indexEntrySize
	}

	firstOfs := offsets[0]
	_, key := readKey(b[firstOfs:])
	indirectTsmIndex.minKey = key

	lastOfs := offsets[len(offsets)-1]
	_, key = readKey(b[lastOfs:])
	indirectTsmIndex.maxKey = key

	indirectTsmIndex.minTime = minTime
	indirectTsmIndex.maxTime = maxTime

	var err error
	indirectTsmIndex.offsets, err = mmap(nil, 0, len(offsets)*4)
	if err != nil {
		return err
	}
	for i, v := range offsets {
		binary.BigEndian.PutUint32(indirectTsmIndex.offsets[i*4:i*4+4], uint32(v))
	}

	return nil
}

// Size returns the size of the current index in bytes.
func (indirectTsmIndex *indirectTsmIndex) Size() uint32 {
	indirectTsmIndex.mu.RLock()
	defer indirectTsmIndex.mu.RUnlock()

	return uint32(len(indirectTsmIndex.b))
}

func (indirectTsmIndex *indirectTsmIndex) Close() error {
	// Windows doesn't use the anonymous map for the offsets index
	if runtime.GOOS == "windows" {
		return nil
	}
	return munmap(indirectTsmIndex.offsets[:cap(indirectTsmIndex.offsets)])
}

// mmapAccess is mmap based block accessor.  It access blocks through an
// MMAP file interface.
type mmapAccessor struct {
	accessCount uint64 // Counter incremented everytime the mmapAccessor is accessed
	freeCount   uint64 // Counter to determine whether the accessor can free its resources

	mmapWillNeed bool // If true then mmap advise value MADV_WILLNEED will be provided the kernel for b. 对应 storage-tsm-use-madv-willneed

	mu      sync.RWMutex
	b       []byte
	tsmFile *os.File

	index *indirectTsmIndex
}

func (m *mmapAccessor) init() (*indirectTsmIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := verifyVersion(m.tsmFile); err != nil {
		return nil, err
	}

	var err error

	if _, err := m.tsmFile.Seek(0, 0); err != nil {
		return nil, err
	}

	stat, err := m.tsmFile.Stat()
	if err != nil {
		return nil, err
	}

	m.b, err = mmap(m.tsmFile, 0, int(stat.Size()))
	if err != nil {
		return nil, err
	}
	if len(m.b) < 8 {
		return nil, fmt.Errorf("mmapAccessor: byte slice too small for indirectIndex")
	}

	// Hint to the kernel that we will be reading the file.  It would be better to hint
	// that we will be reading the index section, but that's not been
	// implemented as yet.
	if m.mmapWillNeed {
		if err := madviseWillNeed(m.b); err != nil {
			return nil, err
		}
	}

	indexOfsPos := len(m.b) - 8
	indexStart := binary.BigEndian.Uint64(m.b[indexOfsPos : indexOfsPos+8])
	if indexStart >= uint64(indexOfsPos) {
		return nil, fmt.Errorf("mmapAccessor: invalid indexStart")
	}

	m.index = NewIndirectIndex()
	if err := m.index.UnmarshalBinary(m.b[indexStart:indexOfsPos]); err != nil {
		return nil, err
	}

	// Allow resources to be freed immediately if requested
	m.incAccess()
	atomic.StoreUint64(&m.freeCount, 1)

	return m.index, nil
}

func (m *mmapAccessor) free() error {
	accessCount := atomic.LoadUint64(&m.accessCount)
	freeCount := atomic.LoadUint64(&m.freeCount)

	// Already freed everything.
	if freeCount == 0 && accessCount == 0 {
		return nil
	}

	// Were there accesses after the last time we tried to free?
	// If so, don't free anything and record the access count that we
	// see now for the next check.
	if accessCount != freeCount {
		atomic.StoreUint64(&m.freeCount, accessCount)
		return nil
	}

	// Reset both counters to zero to indicate that we have freed everything.
	atomic.StoreUint64(&m.accessCount, 0)
	atomic.StoreUint64(&m.freeCount, 0)

	m.mu.RLock()
	defer m.mu.RUnlock()

	return madviseDontNeed(m.b)
}

func (m *mmapAccessor) incAccess() {
	atomic.AddUint64(&m.accessCount, 1)
}

func (m *mmapAccessor) rename(path string) error {
	m.incAccess()

	m.mu.Lock()
	defer m.mu.Unlock()

	err := munmap(m.b)
	if err != nil {
		return err
	}

	if err := m.tsmFile.Close(); err != nil {
		return err
	}

	if err := file.RenameFile(m.tsmFile.Name(), path); err != nil {
		return err
	}

	m.tsmFile, err = os.Open(path)
	if err != nil {
		return err
	}

	if _, err := m.tsmFile.Seek(0, 0); err != nil {
		return err
	}

	stat, err := m.tsmFile.Stat()
	if err != nil {
		return err
	}

	m.b, err = mmap(m.tsmFile, 0, int(stat.Size()))
	if err != nil {
		return err
	}

	if m.mmapWillNeed {
		return madviseWillNeed(m.b)
	}
	return nil
}

func (m *mmapAccessor) read(key []byte, timestamp int64) ([]Value, error) {
	entry := m.index.Entry(key, timestamp)
	if entry == nil {
		return nil, nil
	}

	return m.readBlock(entry, nil)
}

func (m *mmapAccessor) readBlock(entry *IndexEntry, values []Value) ([]Value, error) {
	m.incAccess()

	m.mu.RLock()
	defer m.mu.RUnlock()

	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		return nil, ErrTSMClosed
	}
	//TODO: Validate checksum
	var err error
	values, err = DecodeBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (m *mmapAccessor) readBytes(entry *IndexEntry, b []byte) (uint32, []byte, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return 0, nil, ErrTSMClosed
	}

	// return the bytes after the 4 byte checksum
	crc, block := binary.BigEndian.Uint32(m.b[entry.Offset:entry.Offset+4]), m.b[entry.Offset+4:entry.Offset+int64(entry.Size)]
	m.mu.RUnlock()

	return crc, block, nil
}

// readAll returns all values for a key in all blocks.
func (m *mmapAccessor) readAll(key []byte) ([]Value, error) {
	m.incAccess()

	blocks := m.index.Entries(key)
	if len(blocks) == 0 {
		return nil, nil
	}

	tombstones := m.index.TombstoneRange(key)

	m.mu.RLock()
	defer m.mu.RUnlock()

	var temp []Value
	var err error
	var values []Value
	for _, block := range blocks {
		var skip bool
		for _, t := range tombstones {
			// Should we skip this block because it contains points that have been deleted
			if t.Min <= block.MinTime && t.Max >= block.MaxTime {
				skip = true
				break
			}
		}

		if skip {
			continue
		}
		//TODO: Validate checksum
		temp = temp[:0]
		// The +4 is the 4 byte checksum length
		temp, err = DecodeBlock(m.b[block.Offset+4:block.Offset+int64(block.Size)], temp)
		if err != nil {
			return nil, err
		}

		// Filter out any values that were deleted
		for _, t := range tombstones {
			temp = Values(temp).Exclude(t.Min, t.Max)
		}

		values = append(values, temp...)
	}

	return values, nil
}

func (m *mmapAccessor) path() string {
	m.mu.RLock()
	path := m.tsmFile.Name()
	m.mu.RUnlock()
	return path
}

func (m *mmapAccessor) close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.b == nil {
		return nil
	}

	err := munmap(m.b)
	if err != nil {
		return err
	}

	m.b = nil
	return m.tsmFile.Close()
}

type indexEntries struct {
	Type    byte
	entries []IndexEntry
}

func (a *indexEntries) Len() int      { return len(a.entries) }
func (a *indexEntries) Swap(i, j int) { a.entries[i], a.entries[j] = a.entries[j], a.entries[i] }
func (a *indexEntries) Less(i, j int) bool {
	return a.entries[i].MinTime < a.entries[j].MinTime
}

func (a *indexEntries) MarshalBinary() ([]byte, error) {
	buf := make([]byte, len(a.entries)*indexEntrySize)

	for i, entry := range a.entries {
		entry.AppendTo(buf[indexEntrySize*i:])
	}

	return buf, nil
}

func (a *indexEntries) WriteTo(w io.Writer) (total int64, err error) {
	var buf [indexEntrySize]byte
	var n int

	for _, entry := range a.entries {
		entry.AppendTo(buf[:])
		n, err = w.Write(buf[:])
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func readKey(b []byte) (n int, key []byte) {
	// 2 byte size of key
	n, size := 2, int(binary.BigEndian.Uint16(b[:2]))

	// N byte key
	key = b[n : n+size]

	n += len(key)
	return
}

func readEntries(b []byte, entries *indexEntries) (n int, err error) {
	if len(b) < 1+indexCountSize {
		return 0, fmt.Errorf("readEntries: data too short for headers")
	}

	// 1 byte block type
	entries.Type = b[n]
	n++

	// 2 byte count of index entries
	count := int(binary.BigEndian.Uint16(b[n : n+indexCountSize]))
	n += indexCountSize

	if cap(entries.entries) < count {
		entries.entries = make([]IndexEntry, count)
	} else {
		entries.entries = entries.entries[:count]
	}

	b = b[indexCountSize+indexTypeSize:]
	for i := 0; i < len(entries.entries); i++ {
		if err = entries.entries[i].UnmarshalBinary(b); err != nil {
			return 0, fmt.Errorf("readEntries: unmarshal error: %v", err)
		}
		b = b[indexEntrySize:]
	}

	n += count * indexEntrySize

	return
}
