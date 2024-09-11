package tsi1

import (
	"container/list"
	"sync"

	"github.com/influxdata/influxdb/v2/tsdb"
)

// an LRU cache for series id sets associated with
// name -> key -> value . The purpose of the cache is to provide
// efficient means to get sets of series ids that would otherwise involve merging
// many individual bitmaps at query time.
//
// When initialising a TagValueSeriesIDCache a capacity must be provided. When
// more than c items are added to the cache, the least recently used item is
// evicted from the cache.
//
// A TagValueSeriesIDCache comprises a linked list implementation to track the
// order by which items should be evicted from the cache, and a hashmap implementation
// to provide constant time retrievals of items from the cache.
type TagValueSeriesIDCache struct {
	sync.RWMutex
	cache   map[string]map[string]map[string]*list.Element // measurement -> tagKey -> tagValue -> SeriesIDSet
	evictor *list.List

	capacity int // 对应 storage-series-id-set-cache-size
}

// NewTagValueSeriesIDCache returns a TagValueSeriesIDCache with capacity c.
func NewTagValueSeriesIDCache(c int) *TagValueSeriesIDCache {
	return &TagValueSeriesIDCache{
		cache:    map[string]map[string]map[string]*list.Element{},
		evictor:  list.New(),
		capacity: c,
	}
}

// Get returns the SeriesIDSet associated with the {name, key, value} tuple if it
// exists.
func (tagValueSeriesIDCache *TagValueSeriesIDCache) Get(name, key, value []byte) *tsdb.SeriesIDSet {
	tagValueSeriesIDCache.Lock()
	defer tagValueSeriesIDCache.Unlock()
	return tagValueSeriesIDCache.get(name, key, value)
}

func (tagValueSeriesIDCache *TagValueSeriesIDCache) get(name, key, value []byte) *tsdb.SeriesIDSet {
	if mmap, ok := tagValueSeriesIDCache.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			if ele, ok := tkmap[string(value)]; ok {
				tagValueSeriesIDCache.evictor.MoveToFront(ele) // This now becomes most recently used.
				return ele.Value.(*seriesIDCacheElement).SeriesIDSet
			}
		}
	}
	return nil
}

// exists returns true if the an item exists for the tuple {name, key, value}.
func (tagValueSeriesIDCache *TagValueSeriesIDCache) exists(name, key, value []byte) bool {
	if mmap, ok := tagValueSeriesIDCache.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			_, ok := tkmap[string(value)]
			return ok
		}
	}
	return false
}

// addToSet adds x to the SeriesIDSet associated with the tuple {name, key, value}
// if it exists. This method takes a lock on the underlying SeriesIDSet.
//
// NB this does not count as an access on the set—therefore the set is not promoted
// within the LRU cache.
func (tagValueSeriesIDCache *TagValueSeriesIDCache) addToSet(name, key, value []byte, x uint64) {
	if mmap, ok := tagValueSeriesIDCache.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			if ele, ok := tkmap[string(value)]; ok {
				ss := ele.Value.(*seriesIDCacheElement).SeriesIDSet
				if ss == nil {
					ele.Value.(*seriesIDCacheElement).SeriesIDSet = tsdb.NewSeriesIDSet(x)
					return
				}
				ele.Value.(*seriesIDCacheElement).SeriesIDSet.Add(x)
			}
		}
	}
}

// measurementContainsSets returns true if there are sets cached for the provided measurement.
func (tagValueSeriesIDCache *TagValueSeriesIDCache) measurementContainsSets(name []byte) bool {
	_, ok := tagValueSeriesIDCache.cache[string(name)]
	return ok
}

// add the SeriesIDSet to the cache under the tuple {name, key, value}. If
// the cache is at its limit, then the least recently used item is evicted.
func (tagValueSeriesIDCache *TagValueSeriesIDCache) Put(name, key, value []byte, ss *tsdb.SeriesIDSet) {
	tagValueSeriesIDCache.Lock()
	// Check under the write lock if the relevant item is now in the cache.
	if tagValueSeriesIDCache.exists(name, key, value) {
		tagValueSeriesIDCache.Unlock()
		return
	}
	defer tagValueSeriesIDCache.Unlock()

	// Ensure our SeriesIDSet is go heap backed.
	if ss != nil {
		ss = ss.Clone()
	}

	// Create list item, and add to the front of the eviction list.
	listElement := tagValueSeriesIDCache.evictor.PushFront(&seriesIDCacheElement{
		name:        string(name),
		key:         string(key),
		value:       string(value),
		SeriesIDSet: ss,
	})

	// Add the listElement to the set of items.
	if mmap, ok := tagValueSeriesIDCache.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			if _, ok := tkmap[string(value)]; ok {
				goto EVICT
			}

			// Add the set to the map
			tkmap[string(value)] = listElement
			goto EVICT
		}

		// No series set map for the tag key - first tag value for the tag key.
		mmap[string(key)] = map[string]*list.Element{string(value): listElement}
		goto EVICT
	}

	// No map for the measurement - first tag key for the measurement.
	tagValueSeriesIDCache.cache[string(name)] = map[string]map[string]*list.Element{
		string(key): {string(value): listElement},
	}

EVICT:
	tagValueSeriesIDCache.checkEviction()
}

// Delete removes x from the tuple {name, key, value} if it exists.
// This method takes a lock on the underlying SeriesIDSet.
func (tagValueSeriesIDCache *TagValueSeriesIDCache) Delete(name, key, value []byte, x uint64) {
	tagValueSeriesIDCache.Lock()
	tagValueSeriesIDCache.delete(name, key, value, x)
	tagValueSeriesIDCache.Unlock()
}

// delete removes x from the tuple {name, key, value} if it exists.
func (tagValueSeriesIDCache *TagValueSeriesIDCache) delete(name, key, value []byte, x uint64) {
	if mmap, ok := tagValueSeriesIDCache.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			if ele, ok := tkmap[string(value)]; ok {
				if ss := ele.Value.(*seriesIDCacheElement).SeriesIDSet; ss != nil {
					ele.Value.(*seriesIDCacheElement).SeriesIDSet.Remove(x)
				}
			}
		}
	}
}

// checkEviction checks if the cache is too big, and evicts the least recently used
// item if it is.
func (tagValueSeriesIDCache *TagValueSeriesIDCache) checkEviction() {
	if tagValueSeriesIDCache.evictor.Len() <= tagValueSeriesIDCache.capacity {
		return
	}

	e := tagValueSeriesIDCache.evictor.Back() // Least recently used item.
	listElement := e.Value.(*seriesIDCacheElement)
	name := listElement.name
	key := listElement.key
	value := listElement.value

	tagValueSeriesIDCache.evictor.Remove(e)               // Remove from evictor
	delete(tagValueSeriesIDCache.cache[name][key], value) // Remove from hashmap of items.

	// Check if there are no more tag values for the tag key.
	if len(tagValueSeriesIDCache.cache[name][key]) == 0 {
		delete(tagValueSeriesIDCache.cache[name], key)
	}

	// Check there are no more tag keys for the measurement.
	if len(tagValueSeriesIDCache.cache[name]) == 0 {
		delete(tagValueSeriesIDCache.cache, name)
	}
}

// seriesIDCacheElement is an item stored within a cache.
type seriesIDCacheElement struct {
	name        string
	key         string
	value       string
	SeriesIDSet *tsdb.SeriesIDSet
}
