package bench_test

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	bigcache "github.com/allegro/bigcache/v3"
	"github.com/coocood/freecache"
	lru "github.com/hashicorp/golang-lru/v2"
	gocache "github.com/patrickmn/go-cache"
	"github.com/psdhajare/imcache"
)

const benchItems = 10_000

var (
	sKeys []string
	sVals []string
	bKeys [][]byte
	bVals [][]byte
)

func init() {
	sKeys = make([]string, benchItems)
	sVals = make([]string, benchItems)
	bKeys = make([][]byte, benchItems)
	bVals = make([][]byte, benchItems)
	for i := range benchItems {
		sKeys[i] = "key:" + strconv.Itoa(i)
		sVals[i] = "value-" + strconv.Itoa(i)
		bKeys[i] = []byte(sKeys[i])
		bVals[i] = []byte(sVals[i])
	}
}

// benchCache wraps any cache behind index-based get/set closures.
// Using indices avoids measuring string→[]byte conversion overhead
// for byte-oriented caches, keeping the comparison fair.
type benchCache struct {
	name    string
	get     func(i int)
	set     func(i int)
	cleanup func()
}

// ── cache constructors ────────────────────────────────────────────────────────

func newImcache() benchCache {
	c := imcache.New[string](imcache.NoExpiration, 0)
	for i, k := range sKeys {
		c.Set(k, sVals[i], imcache.NoExpiration)
	}
	return benchCache{
		name:    "imcache",
		get:     func(i int) { c.Get(sKeys[i]) },
		set:     func(i int) { c.Set(sKeys[i], sVals[i], imcache.NoExpiration) },
		cleanup: func() { c.Close() },
	}
}

func newImcacheLRU() benchCache {
	c := imcache.New[string](imcache.NoExpiration, 0,
		imcache.WithNumShards(256),
		imcache.WithMaxItemsPerShard(256),
	)
	for i, k := range sKeys {
		c.Set(k, sVals[i], imcache.NoExpiration)
	}
	return benchCache{
		name:    "imcache_lru",
		get:     func(i int) { c.Get(sKeys[i]) },
		set:     func(i int) { c.Set(sKeys[i], sVals[i], imcache.NoExpiration) },
		cleanup: func() { c.Close() },
	}
}

func newSyncMap() benchCache {
	var m sync.Map
	for i, k := range sKeys {
		m.Store(k, sVals[i])
	}
	return benchCache{
		name:    "sync.Map",
		get:     func(i int) { m.Load(sKeys[i]) },
		set:     func(i int) { m.Store(sKeys[i], sVals[i]) },
		cleanup: func() {},
	}
}

func newMutexMap() benchCache {
	var mu sync.Mutex
	m := make(map[string]string, benchItems)
	for i, k := range sKeys {
		m[k] = sVals[i]
	}
	return benchCache{
		name: "mutexMap",
		get: func(i int) {
			mu.Lock()
			_ = m[sKeys[i]]
			mu.Unlock()
		},
		set: func(i int) {
			mu.Lock()
			m[sKeys[i]] = sVals[i]
			mu.Unlock()
		},
		cleanup: func() {},
	}
}

func newRWMutexMap() benchCache {
	var mu sync.RWMutex
	m := make(map[string]string, benchItems)
	for i, k := range sKeys {
		m[k] = sVals[i]
	}
	return benchCache{
		name: "rwMutexMap",
		get: func(i int) {
			mu.RLock()
			_ = m[sKeys[i]]
			mu.RUnlock()
		},
		set: func(i int) {
			mu.Lock()
			m[sKeys[i]] = sVals[i]
			mu.Unlock()
		},
		cleanup: func() {},
	}
}

func newGoCache() benchCache {
	c := gocache.New(gocache.NoExpiration, 0)
	for i, k := range sKeys {
		c.Set(k, sVals[i], gocache.NoExpiration)
	}
	return benchCache{
		name:    "go-cache",
		get:     func(i int) { c.Get(sKeys[i]) },
		set:     func(i int) { c.Set(sKeys[i], sVals[i], gocache.NoExpiration) },
		cleanup: func() {},
	}
}

func newGolangLRU() benchCache {
	c, _ := lru.New[string, string](benchItems * 2)
	for i, k := range sKeys {
		c.Add(k, sVals[i])
	}
	return benchCache{
		name:    "golang-lru",
		get:     func(i int) { c.Get(sKeys[i]) },
		set:     func(i int) { c.Add(sKeys[i], sVals[i]) },
		cleanup: func() { c.Purge() },
	}
}

func newBigcache() benchCache {
	cfg := bigcache.DefaultConfig(10 * time.Minute)
	cfg.Shards = 256
	cfg.MaxEntriesInWindow = benchItems * 10
	cfg.MaxEntrySize = 256
	cfg.Verbose = false
	c, _ := bigcache.New(context.Background(), cfg)
	for i, k := range sKeys {
		c.Set(k, bVals[i])
	}
	return benchCache{
		name:    "bigcache",
		get:     func(i int) { c.Get(sKeys[i]) },
		set:     func(i int) { c.Set(sKeys[i], bVals[i]) },
		cleanup: func() { c.Close() },
	}
}

func newFreecache() benchCache {
	c := freecache.NewCache(64 * 1024 * 1024)
	for i := range sKeys {
		c.Set(bKeys[i], bVals[i], 0)
	}
	return benchCache{
		name:    "freecache",
		get:     func(i int) { c.Get(bKeys[i]) },
		set:     func(i int) { c.Set(bKeys[i], bVals[i], 0) },
		cleanup: func() {},
	}
}

type cacheConstructor func() benchCache

func allCaches() []cacheConstructor {
	return []cacheConstructor{
		newImcache,
		newImcacheLRU,
		newSyncMap,
		newMutexMap,
		newRWMutexMap,
		newGoCache,
		newGolangLRU,
		newBigcache,
		newFreecache,
	}
}

// ── concurrent benchmarks ─────────────────────────────────────────────────────

// benchConcurrent runs a parallel benchmark. writeEvery=0 means pure reads;
// writeEvery=N means every Nth operation is a write.
func benchConcurrent(b *testing.B, bc benchCache, writeEvery int) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			idx := i % benchItems
			if writeEvery > 0 && i%writeEvery == 0 {
				bc.set(idx)
			} else {
				bc.get(idx)
			}
			i++
		}
	})
}

func runSuite(b *testing.B, writeEvery int) {
	for _, ctor := range allCaches() {
		bc := ctor()
		b.Run(bc.name, func(b *testing.B) {
			benchConcurrent(b, bc, writeEvery)
		})
		bc.cleanup()
	}
}

func BenchmarkConcurrentReads(b *testing.B)         { runSuite(b, 0) }
func BenchmarkConcurrentWrite_0_1pct(b *testing.B)  { runSuite(b, 1000) }
func BenchmarkConcurrentWrite_1pct(b *testing.B)    { runSuite(b, 100) }
func BenchmarkConcurrentWrite_10pct(b *testing.B)   { runSuite(b, 10) }
func BenchmarkConcurrentWrite_50pct(b *testing.B)   { runSuite(b, 2) }

// ── single-thread read-only ───────────────────────────────────────────────────

func BenchmarkSingleRead(b *testing.B) {
	for _, ctor := range allCaches() {
		bc := ctor()
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := range b.N {
				bc.get(i % benchItems)
			}
		})
		bc.cleanup()
	}
}

// ── memory usage ──────────────────────────────────────────────────────────────

func TestMemoryUsage(t *testing.T) {
	const memItems = 1_000_000

	memKeys := make([]string, memItems)
	memVals := make([]string, memItems)
	memKeyBytes := make([][]byte, memItems)
	memValBytes := make([][]byte, memItems)
	for i := range memItems {
		memKeys[i] = "key:" + strconv.Itoa(i)
		memVals[i] = "val:" + strconv.Itoa(i)
		memKeyBytes[i] = []byte(memKeys[i])
		memValBytes[i] = []byte(memVals[i])
	}

	type memCase struct {
		name string
		run  func() func()
	}

	cases := []memCase{
		{"imcache", func() func() {
			c := imcache.New[string](imcache.NoExpiration, 0)
			for i, k := range memKeys {
				c.Set(k, memVals[i], imcache.NoExpiration)
			}
			return func() { c.Close() }
		}},
		{"sync.Map", func() func() {
			var m sync.Map
			for i, k := range memKeys {
				m.Store(k, memVals[i])
			}
			return func() { m.Range(func(k, v any) bool { m.Delete(k); return true }) }
		}},
		{"go-cache", func() func() {
			c := gocache.New(gocache.NoExpiration, 0)
			for i, k := range memKeys {
				c.Set(k, memVals[i], gocache.NoExpiration)
			}
			return func() { c.Flush() }
		}},
		{"golang-lru", func() func() {
			c, _ := lru.New[string, string](memItems)
			for i, k := range memKeys {
				c.Add(k, memVals[i])
			}
			return func() { c.Purge() }
		}},
		{"bigcache", func() func() {
			cfg := bigcache.DefaultConfig(10 * time.Minute)
			cfg.Shards = 256
			cfg.MaxEntriesInWindow = memItems * 10
			cfg.MaxEntrySize = 256
			cfg.Verbose = false
			c, _ := bigcache.New(context.Background(), cfg)
			for i, k := range memKeys {
				c.Set(k, memValBytes[i])
			}
			return func() { c.Close() }
		}},
		{"freecache", func() func() {
			c := freecache.NewCache(256 * 1024 * 1024)
			for i := range memKeys {
				c.Set(memKeyBytes[i], memValBytes[i], 0)
			}
			return func() { c.Clear() }
		}},
	}

	t.Logf("\n%-15s %10s", "Cache", "MB/inuse")
	t.Logf("%-15s %10s", "─────", "────────")

	for _, mc := range cases {
		runtime.GC()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		cleanup := mc.run()

		runtime.GC()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		mb := float64(after.HeapInuse-before.HeapInuse) / (1024 * 1024)
		t.Logf("%-15s %8.1f MB", mc.name, mb)

		cleanup()
		runtime.GC()
		runtime.GC()
	}
}
