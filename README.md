# imcache

A tiny, zero-dependency, generic, sharded, thread-safe in-memory cache for Go 1.26+.

**Zero external dependencies.** Only the Go standard library. No `go.sum` file. Nothing to audit, nothing to update.

[![Go Reference](https://pkg.go.dev/badge/github.com/psdhajare/imcache.svg)](https://pkg.go.dev/github.com/psdhajare/imcache)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Features

- **Lightweight, zero dependencies** -- built entirely on the Go standard library. No transitive dependency tree, no supply chain risk, no version conflicts.
- **Generics** -- fully type-safe `Cache[V any]`; no `interface{}` casts at call sites.
- **Sharded locking** -- 256 independent `RWMutex` shards (configurable), so reads and writes on different keys never block each other.
- **TTL + LRU eviction** -- per-item TTL with lazy expiry on `Get`, periodic janitor sweeps, and optional per-shard LRU capacity limits with O(1) eviction.
- **Atomic operations** -- `GetOrSet`, `SetIfAbsent`, and `Peek` (read without updating LRU order).
- **Range iterators** -- `All()` and `Keys()` via `iter.Seq2`/`iter.Seq` for lazy, allocation-free iteration.
- **Built-in stats** -- lock-free atomic hit/miss/eviction counters with `Stats()` and `ResetStats()`.
- **Eviction callbacks** -- get notified on TTL expiry, LRU eviction, or explicit deletes.
- **Lowest memory footprint** -- uses less heap memory than `sync.Map`, `go-cache`, and `golang-lru` for the same dataset (see [BENCHMARKS.md](BENCHMARKS.md)).

### Why imcache over `patrickmn/go-cache`?

| | go-cache | **imcache** |
|---|---|---|
| Type safety | `interface{}` + manual casts | Generics (`Cache[V any]`) |
| Concurrency | Single global `RWMutex` | 256 independent shard locks |
| Eviction policy | TTL only | TTL + LRU capacity eviction |
| Expiry on read | Janitor only | Lazy delete on `Get` + janitor |
| `GetOrSet` / `SetIfAbsent` | No | Yes |
| `Peek` (no LRU touch) | No | Yes |
| Range iterators | No | `All()`, `Keys()` via `iter.Seq2` |
| Hit/miss/eviction stats | No | Atomic counters |
| Dependencies | 0 | 0 |

---

## Installation

```bash
go get github.com/psdhajare/imcache
```

Requires **Go 1.26+**.

---

## Quick start

```go
package main

import (
    "fmt"
    "time"

    "github.com/psdhajare/imcache"
)

func main() {
    // defaultTTL=5m, janitor runs every 10m
    c := imcache.New[string](5*time.Minute, 10*time.Minute)
    defer c.Close()

    // Set with explicit TTL
    c.Set("session:abc", "user-42", 30*time.Minute)

    // Set using the default TTL
    c.Set("config:theme", "dark", imcache.DefaultExpiration)

    // Set with no expiry
    c.Set("static:logo", "/img/logo.png", imcache.NoExpiration)

    if val, ok := c.Get("session:abc"); ok {
        fmt.Println("session:", val)
    }

    // Atomic get-or-set
    val, loaded := c.GetOrSet("once", "computed-value", time.Hour)
    fmt.Println(val, loaded) // "computed-value", false

    // Lazy iteration (no allocation)
    for key, value := range c.All() {
        fmt.Println(key, value)
    }

    // Stats
    s := c.Stats()
    fmt.Printf("hits=%d misses=%d evictions=%d hitRate=%.2f\n",
        s.Hits, s.Misses, s.Evictions, s.HitRate)
}
```

---

## API reference

### Creating a cache

```go
// Basic – string values, 5-minute default TTL, 10-minute janitor sweep.
c := imcache.New[string](5*time.Minute, 10*time.Minute)

// With options
c := imcache.New[MyStruct](
    imcache.NoExpiration,     // items never expire by default
    0,                        // no automatic janitor
    imcache.WithNumShards(512),          // more shards for ultra-high concurrency
    imcache.WithMaxItemsPerShard(1024),  // LRU cap; total ~ 512 x 1024 items
    imcache.WithOnEvict(func(key string, val MyStruct) {
        log.Printf("evicted %s", key)
    }),
)
defer c.Close()
```

### Writing

```go
c.Set("k", value, ttl)                          // insert or update
c.Set("k", value, imcache.DefaultExpiration)     // use cache default TTL
c.Set("k", value, imcache.NoExpiration)          // never expires

actual, loaded := c.SetIfAbsent("k", value, ttl) // set only if absent/expired
```

### Reading

```go
val, ok := c.Get("k")                 // updates LRU order; records stats
val, ok := c.Peek("k")               // does NOT update LRU; does NOT record stats
val, loaded := c.GetOrSet("k", v, ttl) // atomic get-or-set
```

### Iterating

```go
// Lazy iteration over all live entries (no map allocation).
for key, value := range c.All() {
    fmt.Println(key, value)
}

// Iterate over keys only.
for key := range c.Keys() {
    fmt.Println(key)
}

// Snapshot (allocates a map copy) — prefer All() for large caches.
items := c.Items()
```

### Deleting

```go
c.Delete("k")          // explicit delete; fires eviction callback
c.DeleteExpired()      // manual sweep of all expired items
c.Flush()              // remove everything (callbacks NOT fired)
```

### Inspection

```go
n := c.Count()                    // number of items (may include expired)
items := c.Items()                // snapshot of all live items
s := c.Stats()                    // Stats{Hits, Misses, Evictions, HitRate}
c.ResetStats()                    // zero all counters
```

### Eviction callback

```go
c := imcache.New[MyStruct](ttl, cleanup,
    imcache.WithOnEvict(func(key string, val MyStruct) {
        log.Printf("evicted %s", key)
    }),
)
```

Fired on TTL expiry (lazy on `Get`, or bulk on `DeleteExpired`/janitor), LRU capacity eviction, explicit `Delete`, and LRU eviction during `SetIfAbsent`/`GetOrSet`.

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithNumShards(n)` | 256 | Number of shards (rounded to next power of 2) |
| `WithMaxItemsPerShard(n)` | 0 (unbounded) | Per-shard LRU capacity limit |
| `WithOnEvict(fn)` | nil | Eviction callback, set at construction time |

---

## Architecture

### Sharded locking

The cache maintains **N independent shards** (default 256, always a power of 2). Each shard owns its own `sync.RWMutex`. A key is assigned to a shard via an inline zero-allocation **FNV-1a** hash:

```
shard = fnv32a(key) & (numShards - 1)   // bitmasking, no division
```

Reads and writes on different shards never block each other, giving near-linear throughput scaling as goroutine count grows.

### Read paths

**Without LRU** (`WithMaxItemsPerShard` not set): `Get` acquires a shared **RLock** and copies the value while holding it, allowing unlimited parallel readers on the same shard. Expired items are lazily deleted under a write lock only when detected.

**With LRU**: `Get` must promote the entry to the MRU head of a `container/list`, which requires an exclusive lock. Throughput is still much better than a single global lock because contention is spread across 256 shards.

### Expiry

Items store their deadline as a **Unix nanosecond timestamp** (`int64`). `expired()` is a single integer compare — no `time.Time` allocation on the hot path.

Expiry happens in two ways:
1. **Lazy** — detected and cleaned up on the first `Get` after expiry.
2. **Periodic** — a background janitor goroutine calls `DeleteExpired` at the configured interval. The janitor stops cleanly on `Close()`.

> **Important:** Always call `Close()` when the cache is no longer needed to stop
> the background janitor goroutine and prevent goroutine leaks.

### LRU eviction

When `WithMaxItemsPerShard(n)` is set each shard maintains a `container/list` (doubly-linked list from the standard library). Insertion and promotion are O(1). When a shard reaches capacity the tail (LRU) entry is removed before the new entry is inserted.

---

## Performance

On an Apple M1 Max (10 cores, Go 1.26), `imcache` is 2x faster than `go-cache` and 3-6x faster than `golang-lru` under concurrency, with zero allocations per operation and the lowest memory footprint among all tested libraries.

| Benchmark (parallel, 10 goroutines) | ns/op | allocs/op |
|---|---|---|
| `BenchmarkGet` (pure reads) | ~66 | 0 |
| `BenchmarkSet` (pure writes) | ~37 | 0 |
| `BenchmarkGetMixed` (1,000 keys) | ~61 | 0 |
| `BenchmarkLRUSet` (bounded, 256 shards) | ~53 | 0 |

For a detailed comparison against `sync.Map`, `go-cache`, `golang-lru`, `bigcache`, and `freecache`, see [BENCHMARKS.md](BENCHMARKS.md).

---

## Running tests

```bash
# Unit tests
go test ./...

# With race detector (recommended before release)
go test -race -count=3 ./...

# Benchmarks
go test -bench=. -benchmem ./...
```

---

## Contributing

PRs and issues are welcome. Please run `go test -race ./...` before submitting.

---

## License

MIT — see [LICENSE](LICENSE).
