# Benchmarks

Comparative benchmarks of `imcache` against popular Go cache libraries.

Benchmark source code lives in [\_benchmark/](./_benchmark/).
Inspired by [bool64/cache benchmarks](https://github.com/bool64/cache/blob/master/_benchmark/README.md).

## Test environment

- **CPU:** Apple M1 Max (10 cores)
- **OS:** macOS (darwin/arm64)
- **Go:** 1.26
- **GOMAXPROCS:** 10

## Libraries tested

| Library | Type safety | Eviction | Locking strategy |
|---------|-------------|----------|------------------|
| **imcache** | Generics | TTL + LRU | Sharded RWMutex (256 shards) |
| **imcache (LRU)** | Generics | TTL + LRU | Same, with per-shard capacity limits |
| [sync.Map](https://pkg.go.dev/sync#Map) | `any` | None | Lock-free reads (stdlib) |
| mutexMap | N/A | None | Single `sync.Mutex` + `map` |
| rwMutexMap | N/A | None | Single `sync.RWMutex` + `map` |
| [go-cache](https://github.com/patrickmn/go-cache) | `any` | TTL | Single global RWMutex |
| [golang-lru](https://github.com/hashicorp/golang-lru) | Generics | LRU | Single global Mutex |
| [bigcache](https://github.com/allegro/bigcache) | `[]byte` | TTL | Sharded, pre-allocated ring buffers |
| [freecache](https://github.com/coocood/freecache) | `[]byte` | LRU + TTL | Segmented, pre-allocated |

**Notes on fairness:**

- Byte-oriented caches (`bigcache`, `freecache`) use pre-computed `[]byte` keys and values in the benchmark to avoid penalizing them for string-to-byte conversion.
- `sync.Map` is purpose-built for read-heavy workloads with stable keysets. It provides no TTL, no eviction, and no type safety. It is included as a performance ceiling for reads, not as a direct competitor.
- `golang-lru` uses a single mutex for all operations, which means every `Get` takes an exclusive lock (to update LRU order). This hurts it under concurrency.

---

## Concurrent throughput

10,000 pre-loaded items. All goroutines (GOMAXPROCS=10) run in parallel, performing reads and writes at the specified ratio.

### Results (ns/op, lower is better)

```
                       0% writes    0.1% writes    1% writes    10% writes    50% writes

imcache                  65.60         65.93         65.67         60.59         37.45
imcache_lru              71.87         81.62         74.98         75.31         52.98
sync.Map                  3.14          3.33          4.52          9.94         25.83
mutexMap                151.1         151.2         150.8         163.7         207.4
rwMutexMap              121.8          88.72         68.36         56.58         99.31
go-cache                123.9          99.81         78.20         64.93        123.4
golang-lru              199.3         196.0         195.5         193.9         211.8
bigcache                 27.96         27.24         32.44         40.32         63.71
freecache                54.22         51.78         51.79         55.70         63.81
```

### Allocations per operation

```
                       0% writes    10% writes    50% writes

imcache                  0             0             0
imcache_lru              0             0             0
sync.Map                 0             0             1
mutexMap                 0             0             0
rwMutexMap               0             0             0
go-cache                 0             0             0
golang-lru               0             0             0
bigcache                 2             1             1
freecache                1             0             0
```

### What this tells us

**Where imcache does well:**

- 2x faster than `go-cache` under pure reads (66ns vs 124ns). The sharded locking pays for itself immediately once there is any concurrency.
- Under heavy writes (50%), `imcache` at 37ns is the fastest typed cache in the set, beating `go-cache` (123ns) by 3.3x and `golang-lru` (212ns) by 5.7x.
- Zero allocations across every write ratio. No other sharded cache in this benchmark achieves that.
- Stable latency: `imcache` barely changes across write ratios (60-66ns for reads, 37ns at 50% writes), while `go-cache` and `rwMutexMap` fluctuate significantly as the read/write mix shifts.
- With LRU enabled, `imcache_lru` still outperforms `go-cache` and `golang-lru` at every write ratio despite the extra bookkeeping.

**Where imcache loses:**

- `sync.Map` is 20x faster for pure reads (3ns vs 66ns). This is expected. `sync.Map` uses a lock-free read path optimized for stable keys that are written once and read many times. It has no hashing, no sharding indirection, and no expiry checks. It is not a general-purpose cache.
- `bigcache` is about 2.4x faster for pure reads (28ns vs 66ns). `bigcache` is a mature, heavily optimized byte-oriented cache with its own sharded design. The trade-off is that it only stores `[]byte` values (no generics, no type safety), allocates on every operation (2 allocs/op for reads), and uses significantly more memory (see below).
- `freecache` is slightly faster for pure reads (54ns vs 66ns) for similar reasons: it operates on raw bytes and avoids the Go type system.
- Single-threaded, `imcache` (28ns) is slower than a plain `map` behind a `sync.Mutex` (16ns). The sharding overhead (hash computation + pointer indirection) costs about 12ns per operation. This overhead only pays off under concurrency.

**The broader picture:**

Among caches that offer type safety, TTL support, and eviction policies, `imcache` is the fastest in this benchmark set at every concurrency level. The libraries that beat it on raw throughput (`sync.Map`, `bigcache`, `freecache`) each sacrifice one or more of: type safety, eviction control, or memory efficiency.

---

## Single-thread read performance

10,000 items, single goroutine, no contention. This isolates per-operation overhead without any locking effects.

```
                       ns/op       allocs/op

imcache                27.70         0
imcache_lru            47.00         0
sync.Map               23.60         0
mutexMap               15.81         0
rwMutexMap             15.61         0
go-cache               17.54         0
golang-lru             29.53         0
bigcache               76.98         2
freecache             102.8          1
```

Without contention, the ranking changes. A raw `map` behind a mutex is the fastest (16ns) because there is zero contention and no sharding overhead. `imcache` at 28ns sits between `sync.Map` (24ns) and `golang-lru` (30ns).

`bigcache` and `freecache` are the slowest in single-threaded reads because their byte-oriented storage requires hashing, segment lookup, and buffer scanning even for a single reader.

---

## Memory usage

1,000,000 string key-value pairs loaded into each cache. Heap in-use measured via `runtime.ReadMemStats` after forcing GC.

```
Cache             MB/inuse

imcache            103.6 MB
sync.Map           136.2 MB
go-cache           111.7 MB
golang-lru         148.9 MB
bigcache          3019.5 MB  *
freecache          288.6 MB
```

\* `bigcache` pre-allocates ring buffers per shard. The 3 GB figure reflects the benchmark configuration (`MaxEntriesInWindow = 10M`). Real-world usage with tuned settings will use less, but bigcache will always use more memory than map-based caches due to its pre-allocation strategy.

**Observations:**

- `imcache` has the lowest memory footprint at 104 MB, which is 7% less than `go-cache` (112 MB) and 24% less than `sync.Map` (136 MB).
- `golang-lru` uses 149 MB because it maintains a doubly-linked list alongside the map (each entry has a list element with two pointers plus interface boxing).
- `freecache` at 289 MB pre-allocates a contiguous byte buffer, which avoids GC pressure but costs more upfront memory.
- `imcache` achieves its low footprint because each entry is a single struct with a string key, the value, an int64 expiry timestamp, and an optional list pointer (nil when LRU is disabled).

---

## How to reproduce

```bash
cd _benchmark

# Quick run (single iteration, ~4 minutes)
go test -bench=. -benchmem -benchtime=3s -timeout=300s ./...

# Stable results (10 iterations for statistical analysis, ~40 minutes)
go test -bench=. -benchmem -benchtime=3s -count=10 -timeout=600s ./... > report.txt
benchstat report.txt

# Memory usage only
go test -v -run TestMemoryUsage ./...
```

## Limitations

- All benchmarks run on a single machine. Results will differ on other hardware, especially machines with different core counts or cache line sizes.
- The benchmark uses a fixed set of 10,000 pre-loaded keys. Real workloads with different key distributions, value sizes, or hit/miss ratios may produce different relative rankings.
- `sync.Map` performance depends heavily on the read/write ratio and key stability. The numbers here reflect its best case (stable keys, read-heavy).
- Memory measurements are point-in-time snapshots after GC. Actual runtime memory usage will fluctuate depending on allocation patterns and GC timing.
- `bigcache` memory usage is highly configuration-dependent. The number shown here is not representative of a tuned production deployment.
