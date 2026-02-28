package imcache

import (
	"container/list"
	"sync"
	"time"
)

// entry is the internal representation of a single cached item.
type entry[V any] struct {
	key    string
	value  V
	expiry int64        // UnixNano; 0 means no expiry
	elem   *list.Element // non-nil only when LRU is active (maxItems > 0)
}

func (e *entry[V]) expired() bool {
	return e.expiry > 0 && time.Now().UnixNano() > e.expiry
}

// shard is one independently-locked segment of the cache.
//
// When maxItems == 0 the LRU list is nil and reads use a shared RLock,
// allowing many concurrent readers on the same shard.
// When maxItems > 0 the LRU list is active; every Get needs a full Lock to
// update recency ordering, but capacity is bounded with O(1) eviction.
type shard[V any] struct {
	mu       sync.RWMutex
	items    map[string]*entry[V]
	lru      *list.List // nil when maxItems == 0; front = MRU, back = LRU
	maxItems int
}

func newShard[V any](maxItems int) *shard[V] {
	s := &shard[V]{
		items:    make(map[string]*entry[V]),
		maxItems: maxItems,
	}
	if maxItems > 0 {
		s.lru = list.New()
	}
	return s
}

// ── write operations ──────────────────────────────────────────────────────────

// set inserts or updates key→value. If the shard is at capacity (LRU mode),
// the least-recently-used item is evicted and returned so the caller can fire
// the eviction callback without holding the shard lock.
func (s *shard[V]) set(key string, value V, expiry int64) *entry[V] {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update existing key.
	if e, ok := s.items[key]; ok {
		e.value = value
		e.expiry = expiry
		if s.lru != nil {
			s.lru.MoveToFront(e.elem)
		}
		return nil
	}

	// Evict LRU item if the shard is full.
	var evicted *entry[V]
	if s.maxItems > 0 && len(s.items) >= s.maxItems {
		evicted = s.evictLRULocked()
	}

	e := &entry[V]{key: key, value: value, expiry: expiry}
	if s.lru != nil {
		e.elem = s.lru.PushFront(key)
	}
	s.items[key] = e
	return evicted
}

// setIfAbsent sets key only when it is absent or expired.
// Returns (existingValue, true, nil) if the key was already live,
// or (newValue, false, evictedEntry) if it was set.
func (s *shard[V]) setIfAbsent(key string, value V, expiry int64) (V, bool, *entry[V]) {
	return s.setNX(key, value, expiry)
}

// getOrSet atomically returns the existing value if live, or stores+returns value.
func (s *shard[V]) getOrSet(key string, value V, expiry int64) (V, bool, *entry[V]) {
	return s.setNX(key, value, expiry)
}

// setNX is the shared implementation for setIfAbsent and getOrSet.
// It returns the existing live value if present, otherwise inserts value.
func (s *shard[V]) setNX(key string, value V, expiry int64) (V, bool, *entry[V]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.items[key]; ok {
		if !e.expired() {
			if s.lru != nil {
				s.lru.MoveToFront(e.elem)
			}
			return e.value, true, nil
		}
		delete(s.items, key)
		if s.lru != nil && e.elem != nil {
			s.lru.Remove(e.elem)
		}
	}

	var evicted *entry[V]
	if s.maxItems > 0 && len(s.items) >= s.maxItems {
		evicted = s.evictLRULocked()
	}
	e := &entry[V]{key: key, value: value, expiry: expiry}
	if s.lru != nil {
		e.elem = s.lru.PushFront(key)
	}
	s.items[key] = e
	return value, false, evicted
}

// delete removes key and returns the entry if it existed.
func (s *shard[V]) delete(key string) (*entry[V], bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[key]
	if !ok {
		return nil, false
	}
	delete(s.items, key)
	if s.lru != nil && e.elem != nil {
		s.lru.Remove(e.elem)
	}
	return e, true
}

// deleteExpired removes all expired entries and returns them.
func (s *shard[V]) deleteExpired() []*entry[V] {
	now := time.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()

	var evicted []*entry[V]
	for key, e := range s.items {
		if e.expiry > 0 && now > e.expiry {
			delete(s.items, key)
			if s.lru != nil && e.elem != nil {
				s.lru.Remove(e.elem)
			}
			evicted = append(evicted, e)
		}
	}
	return evicted
}

// flush discards all items.
func (s *shard[V]) flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = make(map[string]*entry[V])
	if s.lru != nil {
		s.lru.Init()
	}
}

// ── read operations ───────────────────────────────────────────────────────────

// get returns (value, true, nil) on a live hit.
// Returns (zero, false, expiredEntry) when the key existed but was expired
// (the caller is responsible for firing the eviction callback).
// Returns (zero, false, nil) on a clean miss.
//
// Without LRU it uses a shared RLock for maximum read throughput.
// With LRU it must promote the entry to MRU position under a full Lock.
func (s *shard[V]) get(key string) (V, bool, *entry[V]) {
	if s.lru == nil {
		return s.getNoLRU(key)
	}
	return s.getWithLRU(key)
}

func (s *shard[V]) getNoLRU(key string) (V, bool, *entry[V]) {
	// Optimistic read – copy value+expiry under RLock to avoid racing
	// with a concurrent Set that updates the entry in-place.
	s.mu.RLock()
	e, ok := s.items[key]
	if !ok {
		s.mu.RUnlock()
		var zero V
		return zero, false, nil
	}
	v, exp := e.value, e.expiry
	s.mu.RUnlock()

	if exp == 0 || time.Now().UnixNano() <= exp {
		return v, true, nil
	}

	// Lazy delete under write lock (double-checked). A concurrent Set may
	// have replaced the value between our RUnlock and this Lock, so only
	// report the eviction if we actually removed the entry.
	s.mu.Lock()
	var expired *entry[V]
	if e2, ok2 := s.items[key]; ok2 && e2.expired() {
		delete(s.items, key)
		if e2.elem != nil {
			s.lru.Remove(e2.elem)
		}
		expired = e2
	}
	s.mu.Unlock()

	var zero V
	return zero, false, expired
}

func (s *shard[V]) getWithLRU(key string) (V, bool, *entry[V]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[key]
	if !ok {
		var zero V
		return zero, false, nil
	}
	if e.expired() {
		delete(s.items, key)
		if e.elem != nil {
			s.lru.Remove(e.elem)
		}
		var zero V
		return zero, false, e
	}
	s.lru.MoveToFront(e.elem)
	return e.value, true, nil
}

// peek returns the value without updating LRU order and without recording stats.
func (s *shard[V]) peek(key string) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.items[key]
	if !ok || e.expired() {
		var zero V
		return zero, false
	}
	return e.value, true
}

// snapshot returns a copy of all live entries (safe to iterate without locks).
func (s *shard[V]) snapshot() map[string]entry[V] {
	now := time.Now().UnixNano()
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]entry[V], len(s.items))
	for k, e := range s.items {
		if e.expiry == 0 || now <= e.expiry {
			out[k] = *e
		}
	}
	return out
}

func (s *shard[V]) count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items)
}

// iterEntries yields all live entries to the callback under RLock.
// Returns false if the caller signalled early termination.
func (s *shard[V]) iterEntries(yield func(string, V) bool) bool {
	now := time.Now().UnixNano()
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, e := range s.items {
		if e.expiry == 0 || now <= e.expiry {
			if !yield(e.key, e.value) {
				return false
			}
		}
	}
	return true
}

// ── LRU helpers ───────────────────────────────────────────────────────────────

// evictLRULocked removes and returns the least-recently-used entry.
// Caller MUST hold the write lock.
func (s *shard[V]) evictLRULocked() *entry[V] {
	back := s.lru.Back()
	if back == nil {
		return nil
	}
	key := back.Value.(string)
	e := s.items[key]
	delete(s.items, key)
	s.lru.Remove(back)
	return e
}
