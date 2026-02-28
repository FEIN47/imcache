package imcache

// config holds internal configuration built from Options.
type config struct {
	numShards        int
	maxItemsPerShard int
	onEvict          any
}

// Option is a functional option for configuring a [Cache] via [New].
//
// Available options: [WithNumShards], [WithMaxItemsPerShard], [WithOnEvict].
type Option func(*config)

// WithNumShards sets the number of internal shards.
// Must be a positive integer; it will be rounded up to the next power of 2.
// Default is 256. More shards reduce lock contention under high write concurrency
// at the cost of slightly higher memory overhead.
func WithNumShards(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.numShards = n
		}
	}
}

// WithMaxItemsPerShard sets the maximum number of items allowed per shard.
// When a shard reaches capacity, the least-recently-used (LRU) item is evicted
// before the new item is inserted.
//
// Total cache capacity ≈ numShards × maxItemsPerShard.
// Default is 0 (unbounded).
func WithMaxItemsPerShard(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxItemsPerShard = n
		}
	}
}

// WithOnEvict registers fn as the eviction callback. It is called synchronously
// (in the goroutine that triggered the eviction) for every evicted item.
// Pass nil to disable callbacks.
func WithOnEvict[V any](fn EvictCallback[V]) Option {
	return func(c *config) {
		c.onEvict = fn
	}
}
