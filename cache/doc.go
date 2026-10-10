// Package cache defines a small backend-neutral cache contract.
//
// Values are opaque bytes. Implementations must not log keys or values because
// both commonly contain identifiers or sensitive application data. A zero TTL
// asks the implementation to use its configured default; negative TTLs are
// invalid. ErrMiss is returned when a key does not exist or has expired.
package cache
