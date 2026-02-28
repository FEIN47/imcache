package imcache_test

import (
	"fmt"
	"time"

	"github.com/psdhajare/imcache"
)

func ExampleNew() {
	// Create a cache with 5-minute default TTL and 10-minute janitor sweep.
	c := imcache.New[string](5*time.Minute, 10*time.Minute)
	defer c.Close()

	c.Set("greeting", "hello", imcache.DefaultExpiration)
	if val, ok := c.Get("greeting"); ok {
		fmt.Println(val)
	}
	// Output: hello
}

func ExampleNew_withLRU() {
	// Create a bounded cache with LRU eviction: 4 shards x 100 items each.
	c := imcache.New[int](imcache.NoExpiration, 0,
		imcache.WithNumShards(4),
		imcache.WithMaxItemsPerShard(100),
	)
	defer c.Close()

	c.Set("answer", 42, imcache.NoExpiration)
	if val, ok := c.Get("answer"); ok {
		fmt.Println(val)
	}
	// Output: 42
}

func ExampleCache_GetOrSet() {
	c := imcache.New[string](time.Hour, 0)
	defer c.Close()

	// First call stores and returns the value.
	val, loaded := c.GetOrSet("key", "computed-value", time.Hour)
	fmt.Println(val, loaded)

	// Second call returns the existing value.
	val, loaded = c.GetOrSet("key", "other-value", time.Hour)
	fmt.Println(val, loaded)
	// Output:
	// computed-value false
	// computed-value true
}

func ExampleCache_All() {
	c := imcache.New[int](imcache.NoExpiration, 0)
	defer c.Close()

	c.Set("a", 1, imcache.NoExpiration)
	c.Set("b", 2, imcache.NoExpiration)

	sum := 0
	for _, v := range c.All() {
		sum += v
	}
	fmt.Println("sum:", sum)
	// Output: sum: 3
}

func ExampleWithOnEvict() {
	c := imcache.New[string](imcache.NoExpiration, 0,
		imcache.WithNumShards(1),
		imcache.WithMaxItemsPerShard(1),
		imcache.WithOnEvict(func(key string, val string) {
			fmt.Printf("evicted %s=%s\n", key, val)
		}),
	)
	defer c.Close()

	c.Set("first", "a", imcache.NoExpiration)
	c.Set("second", "b", imcache.NoExpiration) // evicts "first"
	// Output: evicted first=a
}
