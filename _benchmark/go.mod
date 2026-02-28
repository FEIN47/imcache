module github.com/psdhajare/imcache/_benchmark

go 1.26

require (
	github.com/allegro/bigcache/v3 v3.1.0
	github.com/coocood/freecache v1.2.4
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/psdhajare/imcache v0.0.0
)

require github.com/cespare/xxhash/v2 v2.1.2 // indirect

replace github.com/psdhajare/imcache => ../
