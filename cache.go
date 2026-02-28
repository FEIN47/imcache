// Package imcache is a lightweight, zero-dependency, generic, sharded,
// thread-safe in-memory cache for Go 1.26+.
//
// It is built entirely on the Go standard library with no external dependencies.
//
// Features:
//   - Generics: fully type-safe Cache[V any]; no interface{} casts at call sites.
//   - Sharded locking: 256 independent RWMutex shards (configurable) so reads
//     and writes on different keys never block each other.
//   - LRU eviction: optional per-shard capacity limit with O(1) eviction via
//     container/list.
//   - Lazy + periodic expiry: expired items are removed on access and during
//     periodic janitor sweeps; the janitor stops cleanly on Close().
//   - Atomic stats: lock-free hit/miss/eviction counters.
//   - Range iterators: All() and Keys() return iter.Seq2/iter.Seq for lazy,
//     allocation-free iteration.
package imcache

import (
	"iter"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// NoExpiration indicates that an item should never expire.
	NoExpiration time.Duration = -1

	// DefaultExpiration uses the Cache's default TTL passed to New.
	DefaultExpiration time.Duration = 0

	defaultNumShards = 256
)

// EvictCallback is invoked whenever an item is removed from the cache,
// whether by TTL expiry, LRU capacity eviction, or explicit [Cache.Delete].
//
// The callback runs synchronously in the goroutine that triggered the eviction,
// but outside any shard lock. Slow callbacks will block the calling goroutine
// without affecting other cache operations.
//
// Register a callback with [WithOnEvict].
type EvictCallback[V any] func(key string, value V)

// Item represents a point-in-time snapshot of a cached entry,
// returned by [Cache.Items].
type Item[V any] struct {
	// Value is the cached value.
	Value V
	// ExpiresAt is the absolute time when this item expires.
	// A zero value means the item has no expiry.
	ExpiresAt time.Time
}

// Stats holds cache performance counters.
// Obtain a snapshot with [Cache.Stats] and reset with [Cache.ResetStats].
type Stats struct {
	// Hits is the number of [Cache.Get] calls that found a live entry.
	Hits int64
	// Misses is the number of [Cache.Get] calls that found no live entry,
	// including lookups where the key existed but had expired.
	Misses int64
	// Evictions is the total number of items removed by TTL expiry,
	// LRU capacity eviction, or explicit [Cache.Delete].
	Evictions int64
	// HitRate is Hits / (Hits + Misses). It is 0 when no requests have
	// been made yet.
	HitRate float64
}

// Cache is a generic, sharded, thread-safe in-memory key-value cache.
//
// V is the value type; keys are always strings. Create one with [New].
// A Cache must not be copied after first use.
type Cache[V any] struct {
	shards     []*shard[V]
	numShards  uint32
	defaultTTL time.Duration
	onEvict    EvictCallback[V]

	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64

	closeOnce sync.Once
	done      chan struct{}
}

// New creates a new Cache.
//
//   - defaultTTL: TTL applied when Set is called with [DefaultExpiration].
//     Use [NoExpiration] to make items live forever by default.
//   - cleanupInterval: how often a background goroutine sweeps expired items.
//     Pass 0 or a negative value to disable automatic sweeps (you can call
//     [Cache.DeleteExpired] manually).
//   - opts: optional configuration via [WithNumShards], [WithMaxItemsPerShard],
//     and [WithOnEvict].
//
// Remember to call [Cache.Close] when the cache is no longer needed to stop
// the background goroutine (if cleanupInterval > 0).
func New[V any](defaultTTL, cleanupInterval time.Duration, opts ...Option) *Cache[V] {
	cfg := config{numShards: defaultNumShards}
	for _, o := range opts {
		o(&cfg)
	}

	ns := nextPow2(cfg.numShards)
	shards := make([]*shard[V], ns)
	for i := range shards {
		shards[i] = newShard[V](cfg.maxItemsPerShard)
	}

	var onEvict EvictCallback[V]
	if fn, ok := cfg.onEvict.(EvictCallback[V]); ok {
		onEvict = fn
	}

	c := &Cache[V]{
		shards:     shards,
		numShards:  uint32(ns),
		defaultTTL: defaultTTL,
		onEvict:    onEvict,
		done:       make(chan struct{}),
	}

	if cleanupInterval > 0 {
		go c.runJanitor(cleanupInterval)
	}
	return c
}

// Close stops the background janitor goroutine started by New.
// It is safe to call Close more than once and from multiple goroutines.
func (c *Cache[V]) Close() {
	c.closeOnce.Do(func() { close(c.done) })
}

// Set adds or replaces an item in the cache.
//
//   - ttl == [DefaultExpiration]: use the cache's default TTL.
//   - ttl == [NoExpiration]: item never expires.
//   - ttl > 0: item expires after the given duration.
func (c *Cache[V]) Set(key string, value V, ttl time.Duration) {
	expiry := c.expiryFor(ttl)
	evicted := c.shardFor(key).set(key, value, expiry)
	if evicted != nil {
		c.evictions.Add(1)
		if c.onEvict != nil {
			c.onEvict(evicted.key, evicted.value)
		}
	}
}

// SetIfAbsent sets key only if it does not already exist (or has expired).
//
// If the key already holds a live value, it returns that existing value and
// loaded=true. Otherwise it stores value, returns it, and reports loaded=false.
func (c *Cache[V]) SetIfAbsent(key string, value V, ttl time.Duration) (actual V, loaded bool) {
	v, ok, evicted := c.shardFor(key).setIfAbsent(key, value, c.expiryFor(ttl))
	if evicted != nil {
		c.evictions.Add(1)
		if c.onEvict != nil {
			c.onEvict(evicted.key, evicted.value)
		}
	}
	return v, ok
}

// Get returns the value associated with key and true, or the zero value and
// false if the key is absent or expired. Accessing a key updates its LRU
// position when [WithMaxItemsPerShard] is set.
func (c *Cache[V]) Get(key string) (V, bool) {
	v, ok, expired := c.shardFor(key).get(key)
	if ok {
		c.hits.Add(1)
	} else {
		c.misses.Add(1)
		if expired != nil {
			c.evictions.Add(1)
			if c.onEvict != nil {
				c.onEvict(expired.key, expired.value)
			}
		}
	}
	return v, ok
}

// Peek returns the value for key without updating LRU order or recording stats.
// Returns zero value and false when the key is absent or expired.
func (c *Cache[V]) Peek(key string) (V, bool) {
	return c.shardFor(key).peek(key)
}

// GetOrSet returns the existing value for key if it is present and not expired.
// Otherwise it stores value under key and returns value.
// The boolean reports whether an existing value was returned.
func (c *Cache[V]) GetOrSet(key string, value V, ttl time.Duration) (actual V, loaded bool) {
	v, ok, evicted := c.shardFor(key).getOrSet(key, value, c.expiryFor(ttl))
	if evicted != nil {
		c.evictions.Add(1)
		if c.onEvict != nil {
			c.onEvict(evicted.key, evicted.value)
		}
	}
	return v, ok
}

// Delete removes key from the cache. The eviction callback is invoked if set.
// It is a no-op if the key does not exist.
func (c *Cache[V]) Delete(key string) {
	e, ok := c.shardFor(key).delete(key)
	if ok && c.onEvict != nil {
		c.onEvict(e.key, e.value)
	}
}

// DeleteExpired scans all shards and removes items that have passed their TTL.
// This is called automatically by the background janitor; you only need to call
// it directly if you disabled automatic cleanup.
func (c *Cache[V]) DeleteExpired() {
	for _, sh := range c.shards {
		evicted := sh.deleteExpired()
		c.evictions.Add(int64(len(evicted)))
		if c.onEvict != nil {
			for _, e := range evicted {
				c.onEvict(e.key, e.value)
			}
		}
	}
}

// All returns an iterator over all non-expired entries across all shards.
// Entries are yielded lazily under per-shard RLocks — no map allocation.
// The iteration order is non-deterministic.
func (c *Cache[V]) All() iter.Seq2[string, V] {
	return func(yield func(string, V) bool) {
		for _, sh := range c.shards {
			if !sh.iterEntries(yield) {
				return
			}
		}
	}
}

// Keys returns an iterator over all non-expired keys across all shards.
// The iteration order is non-deterministic.
// Each shard is held under an RLock while its keys are yielded.
func (c *Cache[V]) Keys() iter.Seq[string] {
	return func(yield func(string) bool) {
		c.All()(func(k string, _ V) bool {
			return yield(k)
		})
	}
}

// Items returns a point-in-time snapshot of all non-expired items across all
// shards. The returned map is a copy; mutations do not affect the cache.
//
// For large caches, prefer [Cache.All] which iterates lazily without allocating.
func (c *Cache[V]) Items() map[string]Item[V] {
	result := make(map[string]Item[V])
	for _, sh := range c.shards {
		for k, e := range sh.snapshot() {
			var exp time.Time
			if e.expiry > 0 {
				exp = time.Unix(0, e.expiry)
			}
			result[k] = Item[V]{Value: e.value, ExpiresAt: exp}
		}
	}
	return result
}

// Count returns the number of items currently held (including expired but
// not-yet-deleted items). For an exact live count, call DeleteExpired first.
func (c *Cache[V]) Count() int {
	n := 0
	for _, sh := range c.shards {
		n += sh.count()
	}
	return n
}

// Flush removes all items from every shard. The eviction callback is NOT
// invoked for flushed items for performance reasons; if you need per-item
// cleanup, iterate Items() before calling Flush.
func (c *Cache[V]) Flush() {
	for _, sh := range c.shards {
		sh.flush()
	}
}

// Stats returns a point-in-time snapshot of hit/miss/eviction counters.
// Counters are updated atomically and can be read while the cache is in use.
func (c *Cache[V]) Stats() Stats {
	hits := c.hits.Load()
	misses := c.misses.Load()
	total := hits + misses
	var hr float64
	if total > 0 {
		hr = float64(hits) / float64(total)
	}
	return Stats{
		Hits:      hits,
		Misses:    misses,
		Evictions: c.evictions.Load(),
		HitRate:   hr,
	}
}

// ResetStats zeroes all performance counters.
func (c *Cache[V]) ResetStats() {
	c.hits.Store(0)
	c.misses.Store(0)
	c.evictions.Store(0)
}

// ── internal helpers ──────────────────────────────────────────────────────────

func (c *Cache[V]) shardFor(key string) *shard[V] {
	// numShards is always a power of 2, so bitmasking replaces modulo.
	return c.shards[fnv32a(key)&(c.numShards-1)]
}

func (c *Cache[V]) expiryFor(ttl time.Duration) int64 {
	switch ttl {
	case NoExpiration:
		return 0
	case DefaultExpiration:
		if c.defaultTTL <= 0 {
			return 0
		}
		return time.Now().Add(c.defaultTTL).UnixNano()
	default:
		if ttl <= 0 {
			return 0
		}
		return time.Now().Add(ttl).UnixNano()
	}
}

func (c *Cache[V]) runJanitor(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			c.DeleteExpired()
		case <-c.done:
			return
		}
	}
}

// fnv32a is a zero-allocation inline FNV-1a 32-bit hash for string keys.
func fnv32a(s string) uint32 {
	const (
		offset32 uint32 = 2166136261
		prime32  uint32 = 16777619
	)
	h := offset32
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}

// nextPow2 returns the smallest power of 2 >= n.
func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	return n + 1
}
