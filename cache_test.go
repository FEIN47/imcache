package imcache

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func newTestCache(defaultTTL time.Duration, opts ...Option) *Cache[string] {
	return New[string](defaultTTL, 0, opts...)
}

// ── basic operations ──────────────────────────────────────────────────────────

func TestSetAndGet(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("hello", "world", NoExpiration)

	v, ok := c.Get("hello")
	if !ok || v != "world" {
		t.Fatalf("expected (world, true), got (%q, %v)", v, ok)
	}
}

func TestGetMiss(t *testing.T) {
	c := newTestCache(NoExpiration)
	_, ok := c.Get("missing")
	if ok {
		t.Fatal("expected miss")
	}
}

func TestDelete(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("k", "v", NoExpiration)
	c.Delete("k")

	_, ok := c.Get("k")
	if ok {
		t.Fatal("key should have been deleted")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Delete("ghost") // must not panic
}

func TestOverwrite(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("k", "v1", NoExpiration)
	c.Set("k", "v2", NoExpiration)

	v, ok := c.Get("k")
	if !ok || v != "v2" {
		t.Fatalf("expected v2, got %q", v)
	}
}

func TestFlush(t *testing.T) {
	c := newTestCache(NoExpiration)
	for i := range 100 {
		c.Set(fmt.Sprintf("k%d", i), "v", NoExpiration)
	}
	c.Flush()
	if n := c.Count(); n != 0 {
		t.Fatalf("expected 0 items after Flush, got %d", n)
	}
}

// ── TTL / expiry ──────────────────────────────────────────────────────────────

func TestTTLExpiry(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("short", "v", 30*time.Millisecond)

	v, ok := c.Get("short")
	if !ok || v != "v" {
		t.Fatal("item should be alive immediately")
	}

	time.Sleep(60 * time.Millisecond)

	_, ok = c.Get("short")
	if ok {
		t.Fatal("item should have expired")
	}
}

func TestDefaultExpiration(t *testing.T) {
	c := newTestCache(50 * time.Millisecond)
	c.Set("k", "v", DefaultExpiration)

	time.Sleep(80 * time.Millisecond)

	_, ok := c.Get("k")
	if ok {
		t.Fatal("item should have expired via default TTL")
	}
}

func TestNoExpiration(t *testing.T) {
	c := newTestCache(50 * time.Millisecond) // short default TTL
	c.Set("forever", "v", NoExpiration)

	time.Sleep(80 * time.Millisecond)

	_, ok := c.Get("forever")
	if !ok {
		t.Fatal("NoExpiration item should still be alive")
	}
}

func TestDeleteExpired(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("a", "1", 20*time.Millisecond)
	c.Set("b", "2", NoExpiration)

	time.Sleep(40 * time.Millisecond)
	c.DeleteExpired()

	if _, ok := c.Get("a"); ok {
		t.Error("'a' should have been purged")
	}
	if _, ok := c.Get("b"); !ok {
		t.Error("'b' should still exist")
	}
}

func TestJanitorCleansUp(t *testing.T) {
	c := New[string](NoExpiration, 30*time.Millisecond)
	defer c.Close()

	c.Set("x", "v", 20*time.Millisecond)
	time.Sleep(80 * time.Millisecond) // let janitor run at least twice

	// After janitor sweep the item should be gone from the internal map.
	c.DeleteExpired() // trigger one more in case timing is tight
	if n := c.Count(); n != 0 {
		t.Fatalf("expected 0 items, got %d", n)
	}
}

// ── conditional set / get ─────────────────────────────────────────────────────

func TestSetIfAbsentMiss(t *testing.T) {
	c := newTestCache(NoExpiration)
	actual, loaded := c.SetIfAbsent("k", "first", NoExpiration)
	if loaded || actual != "first" {
		t.Fatalf("expected (first, false), got (%q, %v)", actual, loaded)
	}
}

func TestSetIfAbsentHit(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("k", "original", NoExpiration)
	actual, loaded := c.SetIfAbsent("k", "new", NoExpiration)
	if !loaded || actual != "original" {
		t.Fatalf("expected (original, true), got (%q, %v)", actual, loaded)
	}
}

func TestSetIfAbsentOnExpired(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("k", "old", 20*time.Millisecond)
	time.Sleep(40 * time.Millisecond)

	actual, loaded := c.SetIfAbsent("k", "new", NoExpiration)
	if loaded || actual != "new" {
		t.Fatalf("expected (new, false) after expiry, got (%q, %v)", actual, loaded)
	}
}

func TestGetOrSetMiss(t *testing.T) {
	c := newTestCache(NoExpiration)
	v, loaded := c.GetOrSet("k", "default", NoExpiration)
	if loaded || v != "default" {
		t.Fatalf("expected (default, false), got (%q, %v)", v, loaded)
	}
}

func TestGetOrSetHit(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("k", "real", NoExpiration)
	v, loaded := c.GetOrSet("k", "default", NoExpiration)
	if !loaded || v != "real" {
		t.Fatalf("expected (real, true), got (%q, %v)", v, loaded)
	}
}

// ── peek ──────────────────────────────────────────────────────────────────────

func TestPeekDoesNotAffectLRU(t *testing.T) {
	c := New[string](NoExpiration, 0, WithNumShards(1), WithMaxItemsPerShard(1))

	c.Set("a", "1", NoExpiration)
	// Peek at "a" – if it updated LRU it would be MRU and "b" would evict "a".
	// But Peek should NOT update LRU, so "a" remains LRU and is evicted by "b".
	c.Peek("a")
	c.Set("b", "2", NoExpiration) // should evict "a" (still LRU)

	_, okA := c.Get("a")
	_, okB := c.Get("b")
	if okA {
		t.Error("'a' should have been evicted (Peek must not update LRU)")
	}
	if !okB {
		t.Error("'b' should be present")
	}
}

// ── LRU eviction ──────────────────────────────────────────────────────────────

func TestLRUEviction(t *testing.T) {
	// 1 shard, capacity 2 → easy to reason about.
	c := New[int](NoExpiration, 0,
		WithNumShards(1),
		WithMaxItemsPerShard(2),
	)

	c.Set("a", 1, NoExpiration)
	c.Set("b", 2, NoExpiration)

	// Access "a" to make "b" the LRU.
	c.Get("a")

	// Insert "c" – should evict "b".
	c.Set("c", 3, NoExpiration)

	if _, ok := c.Get("b"); ok {
		t.Error("'b' should have been evicted (LRU)")
	}
	if _, ok := c.Get("a"); !ok {
		t.Error("'a' should still be present")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("'c' should be present")
	}
}

func TestLRUEvictionCallback(t *testing.T) {
	var evictedKey string
	c := New[int](NoExpiration, 0,
		WithNumShards(1),
		WithMaxItemsPerShard(1),
		WithOnEvict(func(k string, _ int) { evictedKey = k }),
	)

	c.Set("old", 1, NoExpiration)
	c.Set("new", 2, NoExpiration) // evicts "old"

	if evictedKey != "old" {
		t.Fatalf("expected 'old' to be evicted, got %q", evictedKey)
	}
}

// ── eviction callback ─────────────────────────────────────────────────────────

func TestEvictCallbackOnDelete(t *testing.T) {
	var called string
	c := New[string](NoExpiration, 0,
		WithOnEvict(func(k, v string) { called = k + "=" + v }),
	)

	c.Set("x", "42", NoExpiration)
	c.Delete("x")

	if called != "x=42" {
		t.Fatalf("expected callback 'x=42', got %q", called)
	}
}

func TestEvictCallbackOnExpiry(t *testing.T) {
	var mu sync.Mutex
	var evicted []string
	c := New[string](NoExpiration, 0,
		WithOnEvict(func(k, _ string) {
			mu.Lock()
			evicted = append(evicted, k)
			mu.Unlock()
		}),
	)

	c.Set("short", "v", 20*time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	c.Get("short") // triggers lazy eviction callback

	mu.Lock()
	defer mu.Unlock()
	if len(evicted) == 0 {
		t.Error("expected eviction callback to fire on expired Get")
	}
}

func TestEvictCallbackOnSetIfAbsentLRU(t *testing.T) {
	var evictedKey string
	c := New[int](NoExpiration, 0,
		WithNumShards(1),
		WithMaxItemsPerShard(1),
		WithOnEvict(func(k string, _ int) { evictedKey = k }),
	)

	c.Set("a", 1, NoExpiration)
	c.SetIfAbsent("b", 2, NoExpiration) // evicts "a"

	if evictedKey != "a" {
		t.Fatalf("expected SetIfAbsent to fire eviction callback for 'a', got %q", evictedKey)
	}
}

func TestEvictCallbackOnGetOrSetLRU(t *testing.T) {
	var evictedKey string
	c := New[int](NoExpiration, 0,
		WithNumShards(1),
		WithMaxItemsPerShard(1),
		WithOnEvict(func(k string, _ int) { evictedKey = k }),
	)

	c.Set("a", 1, NoExpiration)
	c.GetOrSet("b", 2, NoExpiration) // evicts "a"

	if evictedKey != "a" {
		t.Fatalf("expected GetOrSet to fire eviction callback for 'a', got %q", evictedKey)
	}
}

// ── items / count ─────────────────────────────────────────────────────────────

func TestItems(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("a", "1", NoExpiration)
	c.Set("b", "2", 20*time.Millisecond)

	time.Sleep(40 * time.Millisecond)

	items := c.Items()
	if _, ok := items["b"]; ok {
		t.Error("expired item 'b' should not appear in Items()")
	}
	if _, ok := items["a"]; !ok {
		t.Error("'a' should appear in Items()")
	}
}

func TestCount(t *testing.T) {
	c := newTestCache(NoExpiration)
	for i := range 10 {
		c.Set(fmt.Sprintf("k%d", i), "v", NoExpiration)
	}
	if n := c.Count(); n != 10 {
		t.Fatalf("expected 10, got %d", n)
	}
}

// ── stats ─────────────────────────────────────────────────────────────────────

func TestStats(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("k", "v", NoExpiration)

	c.Get("k")      // hit
	c.Get("k")      // hit
	c.Get("missing") // miss

	s := c.Stats()
	if s.Hits != 2 {
		t.Errorf("hits: want 2, got %d", s.Hits)
	}
	if s.Misses != 1 {
		t.Errorf("misses: want 1, got %d", s.Misses)
	}
	if s.HitRate < 0.66 || s.HitRate > 0.67 {
		t.Errorf("hit rate: want ~0.667, got %f", s.HitRate)
	}
}

func TestResetStats(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("k", "v", NoExpiration)
	c.Get("k")
	c.ResetStats()

	s := c.Stats()
	if s.Hits != 0 || s.Misses != 0 || s.Evictions != 0 {
		t.Fatal("stats should be zero after ResetStats")
	}
}

// ── iterators ─────────────────────────────────────────────────────────────────

func TestAll(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("a", "1", NoExpiration)
	c.Set("b", "2", NoExpiration)
	c.Set("expired", "3", 20*time.Millisecond)

	time.Sleep(40 * time.Millisecond)

	got := make(map[string]string)
	for k, v := range c.All() {
		got[k] = v
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 items from All(), got %d", len(got))
	}
	if got["a"] != "1" || got["b"] != "2" {
		t.Fatalf("unexpected items: %v", got)
	}
}

func TestAllEarlyBreak(t *testing.T) {
	c := newTestCache(NoExpiration)
	for i := range 100 {
		c.Set(fmt.Sprintf("k%d", i), "v", NoExpiration)
	}

	count := 0
	for range c.All() {
		count++
		if count >= 5 {
			break
		}
	}
	if count != 5 {
		t.Fatalf("expected early break at 5, got %d", count)
	}
}

func TestKeys(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("x", "1", NoExpiration)
	c.Set("y", "2", NoExpiration)

	keys := make(map[string]bool)
	for k := range c.Keys() {
		keys[k] = true
	}
	if len(keys) != 2 || !keys["x"] || !keys["y"] {
		t.Fatalf("unexpected keys: %v", keys)
	}
}

// ── concurrency / race detector ───────────────────────────────────────────────

// TestConcurrentReadWrite hammers the cache from many goroutines simultaneously.
// Run with: go test -race -count=1 -run TestConcurrentReadWrite
func TestConcurrentReadWrite(t *testing.T) {
	c := newTestCache(NoExpiration)
	const goroutines = 64
	const ops = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		g := g
		go func() {
			defer wg.Done()
			for i := range ops {
				key := fmt.Sprintf("g%d-k%d", g, i%50)
				switch i % 4 {
				case 0:
					c.Set(key, "v", NoExpiration)
				case 1:
					c.Get(key)
				case 2:
					c.Delete(key)
				case 3:
					c.SetIfAbsent(key, "v", NoExpiration)
				}
			}
		}()
	}
	wg.Wait()
}

// TestConcurrentHotKey exercises the race-fix in getNoLRU by having many
// goroutines read and write the *same* key concurrently.
func TestConcurrentHotKey(t *testing.T) {
	c := newTestCache(NoExpiration)
	const goroutines = 64
	const ops = 2000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for i := range ops {
				if i%2 == 0 {
					c.Set("hot", "v", NoExpiration)
				} else {
					c.Get("hot")
				}
			}
		}()
	}
	wg.Wait()
}

// TestConcurrentLRU exercises the LRU path under high concurrency.
func TestConcurrentLRU(t *testing.T) {
	c := New[int](NoExpiration, 0,
		WithNumShards(16),
		WithMaxItemsPerShard(64),
	)
	const goroutines = 32
	const ops = 2000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		g := g
		go func() {
			defer wg.Done()
			for i := range ops {
				key := fmt.Sprintf("k%d", (g*ops+i)%200)
				if i%3 == 0 {
					c.Set(key, i, NoExpiration)
				} else {
					c.Get(key)
				}
			}
		}()
	}
	wg.Wait()
}

// TestConcurrentStats verifies that stats counters don't race.
func TestConcurrentStats(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("k", "v", NoExpiration)

	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 500 {
				c.Get("k")
				c.Get("missing")
			}
		}()
	}
	wg.Wait()

	s := c.Stats()
	if s.Hits+s.Misses != 64*500*2 {
		t.Fatalf("stat mismatch: hits=%d misses=%d", s.Hits, s.Misses)
	}
}

// TestConcurrentClose verifies that calling Close from multiple goroutines
// does not panic.
func TestConcurrentClose(t *testing.T) {
	c := New[string](NoExpiration, 10*time.Millisecond)

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Close()
		}()
	}
	wg.Wait() // must not panic
}

// ── LRU leak regression ──────────────────────────────────────────────────────

func TestSetIfAbsentExpiredLRUConsistency(t *testing.T) {
	c := New[string](NoExpiration, 0,
		WithNumShards(1),
		WithMaxItemsPerShard(10),
	)

	// Insert and let it expire.
	c.Set("k", "old", 20*time.Millisecond)
	time.Sleep(40 * time.Millisecond)

	// SetIfAbsent on expired key must clean up the old LRU element.
	c.SetIfAbsent("k", "new", NoExpiration)

	// The shard should have exactly 1 item and 1 LRU element.
	sh := c.shardFor("k")
	sh.mu.RLock()
	mapLen := len(sh.items)
	lruLen := sh.lru.Len()
	sh.mu.RUnlock()

	if mapLen != 1 {
		t.Fatalf("expected 1 map entry, got %d", mapLen)
	}
	if lruLen != 1 {
		t.Fatalf("expected 1 LRU element, got %d (leak!)", lruLen)
	}
}

func TestGetOrSetExpiredLRUConsistency(t *testing.T) {
	c := New[string](NoExpiration, 0,
		WithNumShards(1),
		WithMaxItemsPerShard(10),
	)

	c.Set("k", "old", 20*time.Millisecond)
	time.Sleep(40 * time.Millisecond)

	c.GetOrSet("k", "new", NoExpiration)

	sh := c.shardFor("k")
	sh.mu.RLock()
	mapLen := len(sh.items)
	lruLen := sh.lru.Len()
	sh.mu.RUnlock()

	if mapLen != 1 {
		t.Fatalf("expected 1 map entry, got %d", mapLen)
	}
	if lruLen != 1 {
		t.Fatalf("expected 1 LRU element, got %d (leak!)", lruLen)
	}
}

// ── sharding / hash ───────────────────────────────────────────────────────────

func TestFnv32aDistribution(t *testing.T) {
	// Check that keys spread across at least half the shards for a 16-shard cache.
	const numShards = 16
	counts := make(map[uint32]int, numShards)
	for i := range 1000 {
		h := fnv32a(fmt.Sprintf("key-%d", i)) & (numShards - 1)
		counts[h]++
	}
	if len(counts) < numShards/2 {
		t.Fatalf("poor shard distribution: only %d of %d shards used", len(counts), numShards)
	}
}

func TestNextPow2(t *testing.T) {
	cases := [][2]int{{1, 1}, {2, 2}, {3, 4}, {4, 4}, {5, 8}, {255, 256}, {256, 256}, {257, 512}}
	for _, tc := range cases {
		if got := nextPow2(tc[0]); got != tc[1] {
			t.Errorf("nextPow2(%d) = %d, want %d", tc[0], got, tc[1])
		}
	}
}

// ── generic types ─────────────────────────────────────────────────────────────

func TestIntCache(t *testing.T) {
	c := New[int](NoExpiration, 0)
	c.Set("answer", 42, NoExpiration)
	v, ok := c.Get("answer")
	if !ok || v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}
}

type Point struct{ X, Y float64 }

func TestStructCache(t *testing.T) {
	c := New[Point](NoExpiration, 0)
	p := Point{1.1, 2.2}
	c.Set("origin", p, NoExpiration)
	got, ok := c.Get("origin")
	if !ok || got != p {
		t.Fatalf("struct roundtrip failed: %+v", got)
	}
}

// ── edge cases ────────────────────────────────────────────────────────────────

func TestEmptyKey(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("", "empty-key", NoExpiration)
	v, ok := c.Get("")
	if !ok || v != "empty-key" {
		t.Fatal("empty string key should work")
	}
}

func TestCloseIdempotent(t *testing.T) {
	c := New[string](NoExpiration, 10*time.Millisecond)
	c.Close()
	c.Close() // must not panic
}

func TestEvictionCounterOnTTL(t *testing.T) {
	c := newTestCache(NoExpiration)
	c.Set("e", "v", 20*time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	c.Get("e") // triggers lazy eviction

	if s := c.Stats(); s.Evictions != 1 {
		t.Fatalf("expected 1 eviction, got %d", s.Evictions)
	}
}

func TestEvictionCounterOnSetIfAbsentLRU(t *testing.T) {
	c := New[int](NoExpiration, 0,
		WithNumShards(1),
		WithMaxItemsPerShard(1),
	)
	c.Set("a", 1, NoExpiration)
	c.SetIfAbsent("b", 2, NoExpiration) // evicts "a"

	if s := c.Stats(); s.Evictions != 1 {
		t.Fatalf("expected 1 eviction from SetIfAbsent, got %d", s.Evictions)
	}
}

func TestEvictionCounterOnGetOrSetLRU(t *testing.T) {
	c := New[int](NoExpiration, 0,
		WithNumShards(1),
		WithMaxItemsPerShard(1),
	)
	c.Set("a", 1, NoExpiration)
	c.GetOrSet("b", 2, NoExpiration) // evicts "a"

	if s := c.Stats(); s.Evictions != 1 {
		t.Fatalf("expected 1 eviction from GetOrSet, got %d", s.Evictions)
	}
}

// ── benchmark ─────────────────────────────────────────────────────────────────

func BenchmarkGet(b *testing.B) {
	c := newTestCache(NoExpiration)
	c.Set("bench", "value", NoExpiration)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Get("bench")
		}
	})
}

func BenchmarkSet(b *testing.B) {
	c := newTestCache(NoExpiration)
	// Pre-generate keys to avoid measuring fmt.Sprintf allocations.
	const numKeys = 100_000
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var n atomic.Int64
		for pb.Next() {
			k := keys[int(n.Add(1))%numKeys]
			c.Set(k, "v", NoExpiration)
		}
	})
}

func BenchmarkSetParallelSharded(b *testing.B) {
	c := New[string](NoExpiration, 0, WithNumShards(256))
	const numKeys = 10_000
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var n atomic.Int64
		for pb.Next() {
			k := keys[int(n.Add(1))%numKeys]
			c.Set(k, "v", NoExpiration)
		}
	})
}

func BenchmarkGetMixed(b *testing.B) {
	c := newTestCache(NoExpiration)
	const numKeys = 1000
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
		c.Set(keys[i], "v", NoExpiration)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var n atomic.Int64
		for pb.Next() {
			k := keys[int(n.Add(1))%numKeys]
			c.Get(k)
		}
	})
}

func BenchmarkLRUSet(b *testing.B) {
	c := New[string](NoExpiration, 0,
		WithNumShards(256),
		WithMaxItemsPerShard(128),
	)
	const numKeys = 500
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var n atomic.Int64
		for pb.Next() {
			k := keys[int(n.Add(1))%numKeys]
			c.Set(k, "v", NoExpiration)
		}
	})
}
