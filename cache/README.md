# cache

Cache byte-oriented yang backend-neutral, bounded, aman untuk concurrency, dan
tidak mencatat key maupun value.

```go
import (
	"github.com/go-dto/go-common/cache"
	rediscache "github.com/go-dto/go-common/cache/redis"
)
```

| Package | Penggunaan |
|---|---|
| `cache` | Interface `Store`, `ErrMiss`, typed error, dan klasifikasi timeout |
| `cache/memory` | Unit test dan local development, LRU bounded + TTL |
| `cache/redis` | Distributed production cache dengan pool, TLS, timeout, startup retry, readiness, dan sanitized logging |

## Kontrak

```go
type Store interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) error
	Close() error
}
```

- `Get` mengembalikan copy dan `cache.ErrMiss` untuk key yang tidak ada/expired.
- TTL `0` memakai default driver; TTL negatif ditolak.
- Key kosong, terlalu panjang, invalid UTF-8, atau mengandung control character
  ditolak tanpa memasukkan key ke error.
- Value memiliki batas ukuran sebelum dikirim ke backend dan setelah dibaca.
- Jumlah key pada satu operasi `Delete` dibatasi agar satu request tidak membuat
  command atau alokasi tanpa batas.
- Error publik hanya berisi operation dan kind stabil; raw endpoint, credential,
  key, value, dan error backend tidak masuk ke pesan.

## Redis production

Buat pool khusus cache. Jangan berbagi client/pool dengan Redis Streams queue
karena queue memakai blocking reads dan lifecycle berbeda.

```go
cacheConfig := rediscache.Config{
	Addrs:      []string{os.Getenv("CACHE_REDIS_ADDR")},
	Username:   os.Getenv("CACHE_REDIS_USERNAME"),
	Password:   os.Getenv("CACHE_REDIS_PASSWORD"),
	KeyPrefix:  "patient-api:cache:",
	DefaultTTL: 5 * time.Minute,
	TLSConfig: &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: os.Getenv("CACHE_REDIS_SERVER_NAME"),
	},
	PoolSize:       20,
	MinIdleConns:   2,
	MaxActiveConns: 20,
	MaxKeyBytes:    256,
	MaxValueBytes:  256 << 10,
	MaxDeleteKeys:  100,
}
if err := cacheConfig.ValidateProduction(); err != nil {
	return err
}
patientCache, err := rediscache.New(ctx, cacheConfig)
if err != nil {
	return err
}
defer patientCache.Close()
```

Pemakaian:

```go
value, err := patientCache.Get(ctx, "patient:"+cacheKey)
switch {
case errors.Is(err, cache.ErrMiss):
	// Load dari source of truth lalu Set dengan TTL.
case err != nil:
	// Cache unavailable: pilih fail-open/fail-closed sesuai use case.
default:
	// Decode value dengan batas dan validasi schema milik service.
}
```

Key dan value tidak dicatat oleh package. Walaupun demikian, jangan memakai
identifier pasien mentah sebagai key; gunakan identifier terderivasi/HMAC sesuai
threat model service. Jangan menyimpan credential atau plaintext sensitif hanya
karena entry memiliki TTL.

### Readiness

```go
mux.Handle("GET /readyz", database.HealthHandler(2*time.Second,
	rediscache.Check("redis_cache", patientCache),
))
```

Gunakan Redis sebagai readiness hanya jika service memang tidak dapat menerima
traffic tanpa cache. Cache yang dirancang fail-open biasanya tidak perlu
menjatuhkan readiness.

## Memory driver untuk test

```go
store, err := memory.New(memory.Options{
	DefaultTTL:  time.Minute,
	MaxEntries: 100,
})
```

Driver memory melakukan copy value, lazy expiry, LRU eviction, size bound, dan
aman untuk concurrent test. Ia bukan pengganti distributed cache production.

## Tes integrasi

```bash
REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 ./cache/redis
```
