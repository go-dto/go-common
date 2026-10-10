# Implementasi production `go-common`

Dokumen ini adalah referensi implementasi untuk developer dan AI coding agent.
Contoh memakai API publik saat ini dan sengaja tidak memuat credential, payload
medis, atau alamat internal.

Catatan release: `cache/redis` dan `broker/redis` tersedia mulai `v0.2.0`. Pin
service ke tag semver yang benar-benar memuat package tersebut; jangan memakai
branch atau pseudo-version untuk production.

Untuk aturan siap-salin ke `AGENTS.md` repository service, gunakan
[`AGENTS_INTEGRATION.md`](AGENTS_INTEGRATION.md). Aturan contributor yang
mengembangkan library tetap berada di [`../AGENTS.md`](../AGENTS.md).

## Alur startup dan shutdown

```text
configuration
  -> production validation
  -> logger
  -> database + outbound client + cache + broker
  -> readiness + application routes
  -> HTTP server / subscriber
  -> graceful shutdown dengan deadline
```

Gunakan versi semver yang sudah dirilis, bukan branch bergerak:

```bash
go get github.com/go-dto/go-common@vX.Y.Z
```

## Contoh HTTP service + PostgreSQL + outbound client

Contoh berikut memakai `http.ServeMux`. `r.Pattern` membuat access log berisi
`/patients/{id}`, bukan ID pasien yang ada pada raw path.

```go
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-dto/go-common/database"
	"github.com/go-dto/go-common/database/postgres"
	"github.com/go-dto/go-common/httpclient"
	"github.com/go-dto/go-common/logger"
)

type application struct {
	patientAPI *httpclient.Client
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		// Pesan sengaja generik agar error startup tidak membocorkan credential,
		// DSN, atau alamat dependency ke output yang tidak terkontrol.
		_, _ = os.Stderr.WriteString("service terminated during startup or shutdown\n")
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	logConfig, err := logger.ConfigFromEnv()
	if err != nil {
		return err
	}
	if err := logConfig.ValidateProduction(); err != nil {
		return err
	}
	if err := logger.Init(logConfig); err != nil {
		return err
	}
	defer logger.Close()

	dbConfig := postgres.Config{
		DSN:              os.Getenv("POSTGRES_DSN"), // sslmode=verify-full
		ApplicationName:  logConfig.ServiceName,
		StatementTimeout: 15 * time.Second,
		MaxConns:         20,
		MinConns:         2,
	}
	if err := dbConfig.ValidateProduction(); err != nil {
		return err
	}
	pool, err := postgres.Open(ctx, dbConfig)
	if err != nil {
		return err
	}
	defer pool.Close()

	clientConfig := httpclient.Config{
		Peer:    "patient-api",
		BaseURL: os.Getenv("PATIENT_API_URL"), // wajib https://
		Timeout: 10 * time.Second,
		Auth:    httpclient.BearerToken(os.Getenv("PATIENT_API_TOKEN")),
	}
	if err := clientConfig.ValidateProduction(); err != nil {
		return err
	}
	patientAPI, err := httpclient.New(clientConfig)
	if err != nil {
		return err
	}
	defer patientAPI.CloseIdleConnections()

	app := &application{patientAPI: patientAPI}
	mux := http.NewServeMux()
	mux.Handle("GET /readyz", database.HealthHandler(2*time.Second,
		postgres.Check("postgres", pool),
	))
	mux.HandleFunc("GET /patients/{id}", app.getPatient)

	handler := logger.HTTPMiddleware(logger.HTTPMiddlewareOptions{
		Route: func(r *http.Request) string { return r.Pattern },
	})(mux)

	server := &http.Server{
		Addr:              envOrDefault("HTTP_ADDR", ":8080"),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()

	select {
	case <-ctx.Done():
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}

func (a *application) getPatient(w http.ResponseWriter, r *http.Request) {
	response, err := a.patientAPI.Get(r.Context(), "/v1/patients/{id}",
		httpclient.Path("id", r.PathValue("id")),
		httpclient.Operation("patient.get"),
		httpclient.RequestTimeout(5*time.Second),
	)
	if httpclient.IsStatus(err, http.StatusNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(response.Bytes())
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
```

Contoh berfokus pada wiring infrastruktur. Sebelum diekspos, tambahkan
authentication, authorization, CORS, rate limit, dan request-size limit sesuai
threat model service. Pertahankan `logger.HTTPMiddleware` sebagai lapisan luar
agar request yang ditolak middleware keamanan tetap memiliki correlation ID dan
access log.

Konfigurasi minimum logger untuk contoh tersebut:

```dotenv
SERVICE_NAME=patient-gateway
APP_ENV=production
APP_VERSION=v1.2.3
LOG_LEVEL=info
LOG_FORMAT=json
LOG_OUTPUT=stdout
```

Jangan menyimpan file `.env` berisi nilai sebenarnya di Git.

## Contoh write request yang aman di-retry

POST/PATCH hanya boleh di-retry ketika upstream mendukung idempotency key:

```go
response, err := client.Post(ctx, "/v1/orders",
	httpclient.JSON(request),
	httpclient.IdempotencyKey(requestID),
	httpclient.Operation("order.create"),
)
if err != nil {
	switch {
	case httpclient.IsTimeout(err):
		// Map ke timeout/retryable business outcome.
	case httpclient.IsRetryExhausted(err):
		// Circuit/alert policy berada di service pemakai.
	default:
		// Map KindOf(err), jangan parse err.Error().
	}
}
_ = response
```

Jangan memasukkan `requestID`, patient ID, URL, atau token ke
`httpclient.Operation`; gunakan nama tetap dan low-cardinality.

## Contoh worker Pub/Sub

```go
pubsubConfig := gcppubsub.Config{
	ProjectID: os.Getenv("GCP_PROJECT_ID"),
	ReceiveSettings: &pubsub.ReceiveSettings{
		MaxOutstandingMessages:     20,
		MaxOutstandingBytes:        64 << 20,
		MaxExtension:               5 * time.Minute,
		EnablePerStreamFlowControl: true,
	},
}
if err := pubsubConfig.ValidateProduction(); err != nil {
	return err
}

messageBroker, err := gcppubsub.New(ctx, pubsubConfig, broker.Options{
	HandlerTimeout: 30 * time.Second,
})
if err != nil {
	return err
}
defer func() {
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = messageBroker.CloseContext(closeCtx)
}()

err = messageBroker.Subscribe(ctx, "order-worker-sub", func(ctx context.Context, message *broker.Message) error {
	var job OrderJob
	if err := json.Unmarshal(message.Data, &job); err != nil {
		return broker.Permanent(err) // malformed payload: ack + drop
	}
	return service.Process(ctx, job) // transient error: nack + redelivery
})
```

Tambahkan import berikut pada file worker:

```go
import (
	"context"
	"encoding/json"
	"os"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/go-dto/go-common/broker"
	"github.com/go-dto/go-common/broker/gcppubsub"
)
```

Handler wajib idempotent. Konfigurasikan retry policy dan dead-letter topic pada
Terraform/IaC Pub/Sub, bukan dengan loop retry tanpa batas di handler.

## Contoh database lain

Semua driver memakai urutan yang sama: buat config, validasi production, buka,
dan tutup dengan deadline yang sesuai.

```go
mysqlConfig := mysql.Config{
	Host:     os.Getenv("MYSQL_HOST"),
	User:     os.Getenv("MYSQL_USER"),
	Password: os.Getenv("MYSQL_PASSWORD"),
	Database: os.Getenv("MYSQL_DATABASE"),
	TLS: &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: os.Getenv("MYSQL_SERVER_NAME"),
	},
	MaxOpenConns: 20,
}
if err := mysqlConfig.ValidateProduction(); err != nil {
	return err
}
db, err := mysql.Open(ctx, mysqlConfig)
if err != nil {
	return err
}
defer db.Close()
```

```go
mongoConfig := mongodb.Config{
	URI:             os.Getenv("MONGODB_URI"), // TLS + verified certificate
	AppName:         "order-worker",
	MaxPoolSize:     50,
	MinPoolSize:     2,
	MaxConnIdleTime: 5 * time.Minute,
}
if err := mongoConfig.ValidateProduction(); err != nil {
	return err
}
mongoClient, err := mongodb.Connect(ctx, mongoConfig)
if err != nil {
	return err
}
defer func() {
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = mongoClient.Disconnect(closeCtx)
}()
```

## Contoh Redis cache

Cache dan queue menggunakan connection/pool terpisah. Jangan memakai identifier
pasien mentah sebagai key dan jangan log key/value.

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

value, err := patientCache.Get(ctx, cacheKey)
if errors.Is(err, cache.ErrMiss) {
	// Load source of truth lalu Set dengan TTL.
}
```

Tambahkan import:

```go
import (
	"crypto/tls"

	"github.com/go-dto/go-common/cache"
	rediscache "github.com/go-dto/go-common/cache/redis"
)
```

## Contoh Redis Streams queue

Provision stream dan consumer group melalui IaC terlebih dahulu, misalnya:

```text
XGROUP CREATE order-service:queue:{topic:jobs}:stream order-worker 0 MKSTREAM
```

```go
queueConfig := redisbroker.Config{
	Addrs:      []string{os.Getenv("QUEUE_REDIS_ADDR")},
	Username:   os.Getenv("QUEUE_REDIS_USERNAME"),
	Password:   os.Getenv("QUEUE_REDIS_PASSWORD"),
	KeyPrefix:  "order-service:queue:",
	TLSConfig: &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: os.Getenv("QUEUE_REDIS_SERVER_NAME"),
	},
	Topics:       []string{"jobs"},
	ConsumerName: os.Getenv("POD_NAME"),
	Subscriptions: map[string]redisbroker.Subscription{
		"order-worker": {Topic: "jobs", Group: "order-worker"},
	},
	PoolSize:            30,
	MinIdleConns:        2,
	MaxActiveConns:      30,
	Concurrency:         20,
	ReadBatchSize:       20,
	BlockTimeout:        time.Second,
	ClaimMinIdle:        time.Minute,
	MaxDeliveryAttempts: 5,
	StreamMaxLen:        500_000,
	DeadLetterMaxLen:    50_000,
}
brokerOptions := broker.Options{
	HandlerTimeout: 30 * time.Second,
}
if err := queueConfig.ValidateProductionWithOptions(brokerOptions); err != nil {
	return err
}
queue, err := redisbroker.New(ctx, queueConfig, brokerOptions)
if err != nil {
	return err
}
defer func() {
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = queue.CloseContext(closeCtx)
}()

_, err = queue.Publish(ctx, "jobs", &broker.Message{Data: encodedJob})
if err != nil {
	return err
}

err = queue.Subscribe(workerCtx, "order-worker",
	func(ctx context.Context, message *broker.Message) error {
		var job Job
		if err := json.Unmarshal(message.Data, &job); err != nil {
			return broker.Permanent(errors.New("invalid job payload"))
		}
		return orderService.HandleJob(ctx, job)
	},
)
```

Tambahkan import:

```go
import (
	"encoding/json"

	"github.com/go-dto/go-common/broker"
	redisbroker "github.com/go-dto/go-common/broker/redis"
)
```

`ClaimMinIdle` harus lebih besar dari `HandlerTimeout`. Handler tetap idempotent;
error transient dibiarkan pending lalu di-claim ulang, sedangkan message yang
mencapai batas attempt dipindahkan ke DLQ stream secara atomik. Pantau panjang
stream, pending age/count, DLQ, memory, serta pool saturation. Detail lengkap
ada di [`../broker/redis/README.md`](../broker/redis/README.md).

Pasang `redisbroker.ConsumerCheck` pada readiness worker setelah goroutine
subscriber dimulai. Gunakan `redisbroker.Check` hanya pada process
publisher-only. Keduanya bukan liveness check.

Deployment queue Redis wajib memakai `noeviction`, persistence sesuai RPO,
replication/managed HA, backup/restore dan failover drill, ACL least-privilege,
serta alert untuk persistence error, replication lag, receive retry, pending
age/count, dan DLQ. `StreamMaxLen` adalah batas jumlah entry; hitung dari peak
rate dan outage window, bukan menganggapnya sebagai retention waktu.

## Checklist deployment

- Semua config menjalankan `ValidateProduction()`.
- Redis cache dan Redis queue memakai credential, pool, prefix, serta idealnya
  deployment terpisah.
- Redis queue memakai `noeviction`, persistence/HA/backup yang teruji, ACL
  minimum, capacity plan backlog, dan readiness consumer-group/receiver.
- Tidak ada raw path/URL/body/header/query/SQL/ordering key pada log.
- Semua network call memiliki context dan deadline.
- Pool dihitung terhadap kapasitas dependency dan jumlah replica.
- `/readyz` memeriksa dependency; `/livez` hanya kondisi proses.
- POST/PATCH retry menggunakan idempotency key.
- Subscriber idempotent serta memiliki retry/dead-letter policy.
- Shutdown HTTP, broker, cache, idle connection, database, dan logger bounded.
- `go test -race`, Staticcheck, govulncheck, secret scan, coverage, dan integration
  test untuk commit release semuanya hijau.
- Release memakai commit bersih dan tag semver immutable.
