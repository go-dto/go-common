// Package memory implements a bounded in-process cache with TTL and LRU
// eviction. It is intended for process-local data; use cache/redis when cache
// entries must be shared by multiple replicas.
package memory
