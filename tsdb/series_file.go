package tsdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/binaryutil"
	"github.com/influxdata/influxdb/v2/pkg/limiter"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	ErrSeriesFileClosed         = errors.New("tsdb: series file closed")
	ErrInvalidSeriesPartitionID = errors.New("tsdb: invalid series partition id")
)

// SeriesIDSize is the size in bytes of a series key ID.
const SeriesIDSize = 8

const (
	// SeriesFilePartitionN is the number of partitions a series file is split into.
	SeriesFilePartitionN = 8
)

// represent the section of the index that holds series data.
type SeriesFile struct {
	path       string
	partitions []*SeriesPartition

	maxSnapshotConcurrency int

	refs sync.RWMutex // RWMutex to track references to the SeriesFile that are in use.

	Logger *zap.Logger
}

// NewSeriesFile returns a new instance of SeriesFile.
func NewSeriesFile(path string) *SeriesFile {
	maxSnapshotConcurrency := runtime.GOMAXPROCS(0)
	if maxSnapshotConcurrency < 1 {
		maxSnapshotConcurrency = 1
	}

	return &SeriesFile{
		path:                   path,
		maxSnapshotConcurrency: maxSnapshotConcurrency,
		Logger:                 zap.NewNop(),
	}
}

func (seriesFile *SeriesFile) WithMaxCompactionConcurrency(maxCompactionConcurrency int) {
	if maxCompactionConcurrency < 1 {
		maxCompactionConcurrency = runtime.GOMAXPROCS(0)
		if maxCompactionConcurrency < 1 {
			maxCompactionConcurrency = 1
		}
	}

	seriesFile.maxSnapshotConcurrency = maxCompactionConcurrency
}

// Open memory maps the data file at the file's path.
func (seriesFile *SeriesFile) Open() error {
	// Wait for all references to be released and prevent new ones from being acquired.
	seriesFile.refs.Lock()
	defer seriesFile.refs.Unlock()

	// Create path if it doesn't exist.
	if err := os.MkdirAll(filepath.Join(seriesFile.path), 0777); err != nil {
		return err
	}

	// Limit concurrent series file compactions
	compactionLimiter := limiter.NewFixed(seriesFile.maxSnapshotConcurrency)

	// Open partitions.
	seriesFile.partitions = make([]*SeriesPartition, 0, SeriesFilePartitionN)
	for i := 0; i < SeriesFilePartitionN; i++ {
		seriesPartition := NewSeriesPartition(i, seriesFile.SeriesPartitionPath(i), compactionLimiter)
		seriesPartition.Logger = seriesFile.Logger.With(zap.Int("partition", seriesPartition.ID()))
		if err := seriesPartition.Open(); err != nil {
			seriesFile.Logger.Error("Unable to open series file",
				zap.String("path", seriesFile.path),
				zap.Int("partition", seriesPartition.ID()),
				zap.Error(err))
			seriesFile.close()
			return err
		}
		seriesFile.partitions = append(seriesFile.partitions, seriesPartition)
	}

	return nil
}

func (seriesFile *SeriesFile) close() (err error) {
	for _, p := range seriesFile.partitions {
		if e := p.Close(); e != nil && err == nil {
			err = e
		}
	}

	return err
}

// Close unmaps the data file.
func (seriesFile *SeriesFile) Close() (err error) {
	seriesFile.refs.Lock()
	defer seriesFile.refs.Unlock()
	return seriesFile.close()
}

// Path returns the path to the file.
func (seriesFile *SeriesFile) Path() string { return seriesFile.path }

// SeriesPartitionPath returns the path to a given partition.
func (seriesFile *SeriesFile) SeriesPartitionPath(i int) string {
	return filepath.Join(seriesFile.path, fmt.Sprintf("%02x", i))
}

// Partitions returns all partitions.
func (seriesFile *SeriesFile) Partitions() []*SeriesPartition { return seriesFile.partitions }

// Retain adds a reference count to the file.  It returns a release func.
func (seriesFile *SeriesFile) Retain() func() {
	if seriesFile != nil {
		seriesFile.refs.RLock()

		// Return the RUnlock func as the release func to be called when done.
		return seriesFile.refs.RUnlock
	}
	return nop
}

// EnableCompactions allows compactions to run.
func (seriesFile *SeriesFile) EnableCompactions() {
	for _, p := range seriesFile.partitions {
		p.EnableCompactions()
	}
}

// DisableCompactions prevents new compactions from running.
func (seriesFile *SeriesFile) DisableCompactions() {
	for _, p := range seriesFile.partitions {
		p.DisableCompactions()
	}
}

// Wait waits for all Retains to be released.
func (seriesFile *SeriesFile) Wait() {
	seriesFile.refs.Lock()
	defer seriesFile.refs.Unlock()
}

// FileSize returns the size of all partitions, in bytes.
func (seriesFile *SeriesFile) FileSize() (n int64, err error) {
	for _, p := range seriesFile.partitions {
		v, err := p.FileSize()
		n += v
		if err != nil {
			return n, err
		}
	}
	return n, err
}

// create a list of series in bulk if they don't exist.
// The returned ids slice returns IDs for every name+tags, creating new series IDs as needed.
func (seriesFile *SeriesFile) CreateSeriesListIfNotExists(names [][]byte, tagsSlice []models.Tags) ([]uint64, error) {
	keys := GenerateSeriesKeys(names, tagsSlice)
	keyPartitionIDs := seriesFile.SeriesKeysPartitionIDs(keys)
	ids := make([]uint64, len(keys))

	var g errgroup.Group
	for i := range seriesFile.partitions {
		seriesPartition := seriesFile.partitions[i]
		g.Go(func() error {
			return seriesPartition.CreateSeriesListIfNotExists(keys, keyPartitionIDs, ids)
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return ids, nil
}

// DeleteSeriesID flags a series as permanently deleted.
// If the series is reintroduced later then it must create a new id.
func (seriesFile *SeriesFile) DeleteSeriesID(id uint64) error {
	p := seriesFile.SeriesIDPartition(id)
	if p == nil {
		return ErrInvalidSeriesPartitionID
	}
	return p.DeleteSeriesID(id)
}

// IsDeleted returns true if the ID has been deleted before.
func (seriesFile *SeriesFile) IsDeleted(id uint64) bool {
	p := seriesFile.SeriesIDPartition(id)
	if p == nil {
		return false
	}
	return p.IsDeleted(id)
}

// SeriesKey returns the series key for a given id.
func (seriesFile *SeriesFile) SeriesKey(id uint64) []byte {
	if id == 0 {
		return nil
	}
	p := seriesFile.SeriesIDPartition(id)
	if p == nil {
		return nil
	}
	return p.SeriesKey(id)
}

// SeriesKeys returns a list of series keys from a list of ids.
func (seriesFile *SeriesFile) SeriesKeys(ids []uint64) [][]byte {
	keys := make([][]byte, len(ids))
	for i := range ids {
		keys[i] = seriesFile.SeriesKey(ids[i])
	}
	return keys
}

// Series returns the parsed series name and tags for an offset.
func (seriesFile *SeriesFile) Series(id uint64) ([]byte, models.Tags) {
	key := seriesFile.SeriesKey(id)
	if key == nil {
		return nil, nil
	}
	return ParseSeriesKey(key)
}

// SeriesID return the series id for the series.
func (seriesFile *SeriesFile) SeriesID(name []byte, tags models.Tags, buf []byte) uint64 {
	key := AppendSeriesKey(buf[:0], name, tags)
	keyPartition := seriesFile.SeriesKeyPartition(key)
	if keyPartition == nil {
		return 0
	}
	return keyPartition.FindIDBySeriesKey(key)
}

// HasSeries return true if the series exists.
func (seriesFile *SeriesFile) HasSeries(name []byte, tags models.Tags, buf []byte) bool {
	return seriesFile.SeriesID(name, tags, buf) > 0
}

// SeriesCount returns the number of series.
func (seriesFile *SeriesFile) SeriesCount() uint64 {
	var n uint64
	for _, p := range seriesFile.partitions {
		n += p.SeriesCount()
	}
	return n
}

// SeriesIDIterator returns an iterator over all the series.
func (seriesFile *SeriesFile) SeriesIDIterator() SeriesIDIterator {
	var ids []uint64
	for _, p := range seriesFile.partitions {
		ids = p.AppendSeriesIDs(ids)
	}
	sort.Sort(uint64Slice(ids))
	return NewSeriesIDSliceIterator(ids)
}

func (seriesFile *SeriesFile) SeriesIDPartitionID(id uint64) int {
	return int((id - 1) % SeriesFilePartitionN)
}

func (seriesFile *SeriesFile) SeriesIDPartition(id uint64) *SeriesPartition {
	partitionID := seriesFile.SeriesIDPartitionID(id)
	if partitionID >= len(seriesFile.partitions) {
		return nil
	}
	return seriesFile.partitions[partitionID]
}

func (seriesFile *SeriesFile) SeriesKeysPartitionIDs(keys [][]byte) []int {
	partitionIDs := make([]int, len(keys))
	for i := range keys {
		partitionIDs[i] = seriesFile.SeriesKeyPartitionID(keys[i])
	}
	return partitionIDs
}

func (seriesFile *SeriesFile) SeriesKeyPartitionID(key []byte) int {
	return int(xxhash.Sum64(key) % SeriesFilePartitionN)
}

func (seriesFile *SeriesFile) SeriesKeyPartition(key []byte) *SeriesPartition {
	partitionID := seriesFile.SeriesKeyPartitionID(key)
	if partitionID >= len(seriesFile.partitions) {
		return nil
	}
	return seriesFile.partitions[partitionID]
}

// AppendSeriesKey serializes name and tags to a byte slice.
// The total length is prepended as a uvarint.
func AppendSeriesKey(dst []byte, name []byte, tags models.Tags) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	origLen := len(dst)

	// The tag count is variable encoded, so we need to know ahead of time what
	// the size of the tag count value will be.
	tcBuf := make([]byte, binary.MaxVarintLen64)
	tcSz := binary.PutUvarint(tcBuf, uint64(len(tags)))

	// Size of name/tags. Does not include total length.
	size := 0 + //
		2 + // size of measurement
		len(name) + // measurement
		tcSz + // size of number of tags
		(4 * len(tags)) + // length of each tag key and value
		tags.Size() // size of tag keys/values

	// Variable encode length.
	totalSz := binary.PutUvarint(buf, uint64(size))

	// If caller doesn't provide a buffer then pre-allocate an exact one.
	if dst == nil {
		dst = make([]byte, 0, size+totalSz)
	}

	// Append total length.
	dst = append(dst, buf[:totalSz]...)

	// Append name.
	binary.BigEndian.PutUint16(buf, uint16(len(name)))
	dst = append(dst, buf[:2]...)
	dst = append(dst, name...)

	// Append tag count.
	dst = append(dst, tcBuf[:tcSz]...)

	// Append tags.
	for _, tag := range tags {
		binary.BigEndian.PutUint16(buf, uint16(len(tag.Key)))
		dst = append(dst, buf[:2]...)
		dst = append(dst, tag.Key...)

		binary.BigEndian.PutUint16(buf, uint16(len(tag.Value)))
		dst = append(dst, buf[:2]...)
		dst = append(dst, tag.Value...)
	}

	// Verify that the total length equals the encoded byte count.
	if got, exp := len(dst)-origLen, size+totalSz; got != exp {
		panic(fmt.Sprintf("series key encoding does not match calculated total length: actual=%d, exp=%d, key=%x", got, exp, dst))
	}

	return dst
}

// ReadSeriesKey returns the series key from the beginning of the buffer.
func ReadSeriesKey(data []byte) (key, remainder []byte) {
	sz, n := binary.Uvarint(data)
	return data[:int(sz)+n], data[int(sz)+n:]
}

func ReadSeriesKeyLen(data []byte) (sz int, remainder []byte) {
	sz64, i := binary.Uvarint(data)
	return int(sz64), data[i:]
}

func ReadSeriesKeyMeasurement(data []byte) (name, remainder []byte) {
	n, data := binary.BigEndian.Uint16(data), data[2:]
	return data[:n], data[n:]
}

func ReadSeriesKeyTagN(data []byte) (n int, remainder []byte) {
	n64, i := binary.Uvarint(data)
	return int(n64), data[i:]
}

func ReadSeriesKeyTag(data []byte) (key, value, remainder []byte) {
	n, data := binary.BigEndian.Uint16(data), data[2:]
	key, data = data[:n], data[n:]

	n, data = binary.BigEndian.Uint16(data), data[2:]
	value, data = data[:n], data[n:]
	return key, value, data
}

// ParseSeriesKey extracts the name & tags from a series key.
func ParseSeriesKey(data []byte) (name []byte, tags models.Tags) {
	return parseSeriesKey(data, nil)
}

// ParseSeriesKeyInto extracts the name and tags for data, parsing the tags into
// dstTags, which is then returned.
//
// The returned dstTags may have a different length and capacity.
func ParseSeriesKeyInto(data []byte, dstTags models.Tags) ([]byte, models.Tags) {
	return parseSeriesKey(data, dstTags)
}

// parseSeriesKey extracts the name and tags from data, attempting to re-use the
// provided tags value rather than allocating. The returned tags may have a
// different length and capacity to those provided.
func parseSeriesKey(data []byte, dst models.Tags) ([]byte, models.Tags) {
	var name []byte
	_, data = ReadSeriesKeyLen(data)
	name, data = ReadSeriesKeyMeasurement(data)
	tagN, data := ReadSeriesKeyTagN(data)

	dst = dst[:cap(dst)] // Grow dst to use full capacity
	if got, want := len(dst), tagN; got < want {
		dst = append(dst, make(models.Tags, want-got)...)
	} else if got > want {
		dst = dst[:want]
	}
	dst = dst[:tagN]

	for i := 0; i < tagN; i++ {
		var key, value []byte
		key, value, data = ReadSeriesKeyTag(data)
		dst[i].Key, dst[i].Value = key, value
	}

	return name, dst
}

func CompareSeriesKeys(a, b []byte) int {
	// Handle 'nil' keys.
	if len(a) == 0 && len(b) == 0 {
		return 0
	} else if len(a) == 0 {
		return -1
	} else if len(b) == 0 {
		return 1
	}

	// Read total size.
	_, a = ReadSeriesKeyLen(a)
	_, b = ReadSeriesKeyLen(b)

	// Read names.
	name0, a := ReadSeriesKeyMeasurement(a)
	name1, b := ReadSeriesKeyMeasurement(b)

	// Compare names, return if not equal.
	if cmp := bytes.Compare(name0, name1); cmp != 0 {
		return cmp
	}

	// Read tag counts.
	tagN0, a := ReadSeriesKeyTagN(a)
	tagN1, b := ReadSeriesKeyTagN(b)

	// Compare each tag in order.
	for i := 0; ; i++ {
		// Check for EOF.
		if i == tagN0 && i == tagN1 {
			return 0
		} else if i == tagN0 {
			return -1
		} else if i == tagN1 {
			return 1
		}

		// Read keys.
		var key0, key1, value0, value1 []byte
		key0, value0, a = ReadSeriesKeyTag(a)
		key1, value1, b = ReadSeriesKeyTag(b)

		// Compare keys & values.
		if cmp := bytes.Compare(key0, key1); cmp != 0 {
			return cmp
		} else if cmp := bytes.Compare(value0, value1); cmp != 0 {
			return cmp
		}
	}
}

// GenerateSeriesKeys generates series keys for a list of names & tags using
// a single large memory block.
func GenerateSeriesKeys(names [][]byte, tagsSlice []models.Tags) [][]byte {
	buf := make([]byte, 0, SeriesKeysSize(names, tagsSlice))
	keys := make([][]byte, len(names))
	for i := range names {
		offset := len(buf)
		buf = AppendSeriesKey(buf, names[i], tagsSlice[i])
		keys[i] = buf[offset:]
	}
	return keys
}

// SeriesKeysSize returns the number of bytes required to encode a list of name/tags.
func SeriesKeysSize(names [][]byte, tagsSlice []models.Tags) int {
	var n int
	for i := range names {
		n += SeriesKeySize(names[i], tagsSlice[i])
	}
	return n
}

// SeriesKeySize returns the number of bytes required to encode a series key.
func SeriesKeySize(name []byte, tags models.Tags) int {
	var n int
	n += 2 + len(name)
	n += binaryutil.UvarintSize(uint64(len(tags)))
	for _, tag := range tags {
		n += 2 + len(tag.Key)
		n += 2 + len(tag.Value)
	}
	n += binaryutil.UvarintSize(uint64(n))
	return n
}

type seriesKeys [][]byte

func (a seriesKeys) Len() int      { return len(a) }
func (a seriesKeys) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a seriesKeys) Less(i, j int) bool {
	return CompareSeriesKeys(a[i], a[j]) == -1
}

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }

func nop() {}
