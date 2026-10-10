// Package redis implements broker.Driver with Redis Streams consumer groups.
// It provides at-least-once delivery, explicit acknowledgement, stale pending
// message reclaim, bounded concurrency, reconnect backoff, consumer readiness,
// and a dead-letter stream. Redis Pub/Sub is deliberately not used because it
// has no persistence or acknowledgement.
package redis
