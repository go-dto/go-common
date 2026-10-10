// Package redis implements cache.Store with a dedicated go-redis client.
// Keys and values are never logged. Create one Store per cache workload and do
// not share its pool with Redis Streams consumers.
package redis
