# Agent rules untuk integrasi `go-common`

Dokumen ini adalah template aturan untuk AI coding agent yang bekerja di
repository **service pemakai** `go-common`. Ini bukan aturan untuk mengembangkan
library `go-common` itu sendiri.

Salin aturan yang relevan ke `AGENTS.md` pada root service. Jika service sudah
memiliki `AGENTS.md`, gabungkan tanpa menghapus aturan project yang lebih
spesifik. Contoh wiring lengkap tersedia di
[`PRODUCTION_IMPLEMENTATION.md`](PRODUCTION_IMPLEMENTATION.md). Setelah template
ini disalin ke service, pin tautan dokumentasi ke tag yang benar-benar dipakai
service tersebut.

## Ruang lingkup

- Pakai `github.com/go-dto/go-common` hanya untuk concern infrastruktur:
  structured logging, outbound HTTP, cache, database connection/readiness, dan
  broker.
- Pertahankan business logic, kontrak endpoint, model domain, dan pola
  repository milik service.
- Import hanya package yang dibutuhkan. Jangan menambahkan semua driver ke
  service yang tidak memakainya.
- Gunakan release semver eksplisit. Tag publik saat dokumen ini diperbarui adalah
  `v0.2.0`:

  ```bash
  go get github.com/go-dto/go-common@v0.2.0
  ```

- Jangan memakai `@latest`, branch, pseudo-version, atau `replace` lokal pada
  perubahan yang akan di-commit. Saat upgrade, baca release note dan review
  perubahan API terlebih dahulu.
- `cache/redis` dan `broker/redis` tersedia mulai `v0.2.0`. Pastikan versi di
  `go.mod` dan tautan dokumentasi diperbarui secara konsisten.
- `go-common@v0.2.0` membutuhkan Go `1.26.9` atau lebih baru. Jangan menaikkan
  toolchain project secara diam-diam; laporkan jika versi Go project belum
  kompatibel.

## Sebelum implementasi

1. Baca seluruh `AGENTS.md` project dan instruksi yang lebih dekat dengan file
   target.
2. Jalankan `git status --short --branch`; pertahankan perubahan pengguna yang
   sudah ada.
3. Periksa `go.mod`, versi Go, framework/router, pola dependency injection,
   mekanisme config, lifecycle process, serta test yang sudah tersedia.
4. Inventarisasi logger, HTTP client, database connector, health check, dan
   message consumer yang aktif. Jangan menyimpulkan dari file legacy atau kode
   yang tidak lagi terhubung ke bootstrap.
5. Jalankan baseline test yang relevan sebelum mengubah kode. Bedakan kegagalan
   baseline dari regresi yang dibuat perubahan.
6. Baca README package dari tag yang dipakai service: `logger`, `httpclient`,
   `cache`, `database`, atau `broker`. Jangan mengandalkan dokumentasi `main`
   untuk dependency yang masih dipin ke tag lama.
7. Buat perubahan bertahap dan endpoint/upstream-scoped. Jangan sekaligus
   mengganti business logic atau kontrak publik service.

## Pemilihan package

| Kebutuhan service | Package | Lifecycle wajib |
|---|---|---|
| Structured/access log | `logger` | `ConfigFromEnv` -> `ValidateProduction` -> `Init` -> `Close` |
| Outbound HTTP | `httpclient` | satu client per upstream -> `ValidateProduction` -> `New` -> `CloseIdleConnections` |
| Distributed cache | `cache/redis` | config/pool cache khusus -> `ValidateProduction` -> `New` -> `Close` |
| Cache unit test | `cache/memory` | bounded in-process store -> `Close` |
| MySQL | `database/mysql` | `ValidateProduction` -> `Open` -> `Close` |
| PostgreSQL | `database/postgres` | `ValidateProduction` -> `Open` -> `Close` |
| MongoDB | `database/mongodb` | `ValidateProduction` -> `Connect` -> `Disconnect` dengan deadline |
| Readiness dependency | `database` | `HealthHandler` hanya pada `/readyz` |
| Google Pub/Sub | `broker/gcppubsub` | `ValidateProduction` -> `New` -> `CloseContext` dengan deadline |
| Redis Streams queue | `broker/redis` | config/pool queue khusus -> provision group -> `ValidateProductionWithOptions` -> `New` -> `ConsumerCheck` -> `CloseContext` |
| Broker unit test | `broker/memory` | buat topic/subscription test -> tunggu idle -> `Close` |

Jangan mempertahankan dua implementasi aktif untuk concern yang sama jika itu
menyebabkan double logging, double retry, double health check, atau dua pool ke
dependency yang sama.

## Aturan implementasi

### Logger

- Inisialisasi sekali pada bootstrap. Untuk production, mulai dari
  `logger.ConfigFromEnv()`, panggil `ValidateProduction()`, kemudian
  `logger.Init()`.
- Ambil logger saat digunakan melalui `logger.L()` atau
  `logger.FromContext(ctx)`. Jangan menyimpan child `*zap.Logger` global dalam
  struct berumur panjang.
- Gunakan `logger.StartOp` pada boundary handler, service, repository, dan
  adapter; teruskan context hasilnya ke downstream dan gunakan
  `defer op.Done(&err)`.
- Pasang `logger.HTTPMiddleware` di luar middleware auth/rate-limit agar request
  yang ditolak tetap memiliki correlation ID dan access log, tetapi tetap di
  dalam recovery middleware aplikasi karena panic akan dilempar kembali.
- Callback `Route` wajib mengembalikan route template framework, misalnya
  `/patients/{id}`, bukan raw path atau nilai ID aktual.
- Jangan memakai `logger/singleton` untuk kode baru.
- Jangan mengaktifkan `LogRawPath`, `LogURL`, statement/header logging, atau
  panic stack secara default di production.
- Jangan membungkus `httpclient.Client` dengan `logger.WrapHTTPClient`; client
  high-level sudah menghasilkan satu final log per logical request.

### HTTP client

- Buat satu `httpclient.Client` per upstream pada startup dan reuse sepanjang
  umur process. Jangan membuat client per request.
- Isi `Peer` dengan nama stabil dan low-cardinality. `BaseURL` production wajib
  HTTPS dengan certificate verification, total timeout positif, dan connection
  bound.
- Panggil `Config.ValidateProduction()` sebelum `httpclient.New()` dan jangan
  memasang custom `Transport` pada jalur production.
- Gunakan placeholder bersama `httpclient.Path`, `Query`, `Header`, dan
  `Operation`. Jangan concatenate identifier ke path dan jangan memasukkan URL
  atau identifier ke nama operation.
- POST/PATCH hanya boleh di-retry dengan `IdempotencyKey`, kecuali kontrak
  upstream secara eksplisit dan teruji menjamin operasi aman diulang.
- Tangani error melalui `IsTimeout`, `IsStatus`, `IsRetryExhausted`, atau
  `KindOf`. Jangan parse `err.Error()` dan jangan meneruskan body error upstream
  sebagai error/log publik.
- `Response` sudah bounded, dibuffer, dan body transport sudah ditutup. Jangan
  menambahkan lifecycle `net/http.Response.Body` sendiri.
- Panggil `CloseIdleConnections()` pada graceful shutdown.

### Database

- Pilih hanya satu driver sub-package yang benar-benar digunakan service.
  `go-common` mengembalikan tipe driver asli; pertahankan repository/sqlc/sqlx
  yang sudah ada bila masih sesuai.
- Panggil `ValidateProduction()` sebelum `Open`/`Connect`.
- Koneksi TCP production wajib memakai TLS dengan verifikasi certificate.
  Trusted local proxy atau Unix socket hanya boleh menjadi pengecualian yang
  disetujui dan didokumentasikan; jangan menonaktifkan verifikasi secara diam-diam.
- Hitung pool dengan rumus `maksimum replica saat rollout x maksimum koneksi per
  replica`, lalu sisakan kapasitas untuk migrasi, admin, dan service lain.
- Semua query menggunakan context dengan deadline. Untuk PostgreSQL, gunakan
  server-side `StatementTimeout` bila sesuai.
- Pasang `database.HealthHandler` hanya sebagai readiness. Liveness tidak boleh
  gagal hanya karena dependency sedang unavailable.
- Jangan log raw SQL, query argument, DSN/URI, hostname sensitif, atau raw driver
  error. Gunakan operation stabil, error kind, durasi, dan rows affected.
- Tutup pool saat shutdown; `Disconnect` MongoDB harus memakai context dengan
  deadline.

### Cache

- Depend pada interface `cache.Store`; gunakan `cache/memory` pada unit test dan
  `cache/redis` untuk distributed cache production.
- Panggil `ValidateProduction()` sebelum `New()`. Gunakan verified TLS, startup
  ping, operation timeout, pool bound, namespace prefix, TTL default, serta
  key/value size limit.
- Perlakukan `cache.ErrMiss` sebagai hasil normal. Klasifikasikan error lain
  dengan `cache.KindOf` atau `cache.IsTimeout`, bukan string parsing.
- Jangan log raw cache key/value. Jangan memakai identifier pasien mentah sebagai
  key dan jangan menyimpan plaintext sensitif hanya karena entry memiliki TTL.
- Cache readiness hanya menjatuhkan `/readyz` bila service benar-benar tidak
  dapat menerima traffic tanpa cache.
- Redis cache dan Redis queue harus memakai config, credential, client/pool,
  serta idealnya deployment terpisah.

### Broker

- Untuk production Pub/Sub, gunakan ADC dan endpoint Google standar. Panggil
  `gcppubsub.Config.ValidateProduction()` sebelum `gcppubsub.New()`.
- Set `HandlerTimeout`, `MaxOutstandingMessages`,
  `MaxOutstandingBytes`, dan batas extension berdasarkan kapasitas downstream.
- Handler wajib idempotent karena delivery bersifat at-least-once.
- Gunakan `broker.Permanent(err)` hanya untuk input yang retry-nya tidak mungkin
  berhasil, misalnya payload malformed. Error dependency/transient harus
  dikembalikan biasa agar pesan di-nack.
- Provision topic, subscription, retry policy, dan dead-letter topic melalui
  IaC. Jangan membuat loop retry manual tanpa batas di handler.
- Jangan log payload, attributes, ordering key, atau error yang mengandung data
  sensitif.
- Shutdown memakai `CloseContext` dengan deadline. Handler harus menghormati
  cancellation dari context.
- Redis queue wajib memakai `broker/redis` (Streams consumer groups), bukan
  Redis Pub/Sub. Provision stream/group melalui IaC dan gunakan consumer name
  unik per process/pod. Engine minimal Redis 6.2 karena reclaim memakai
  `XAUTOCLAIM`; verifikasi versi dan command policy managed Redis di staging.
- `ClaimMinIdle` harus melebihi `HandlerTimeout`. Tentukan `StreamMaxLen` dari
  worst-case backlog, lalu monitor pending age/count dan DLQ; trimming terlalu
  kecil dapat menghilangkan payload yang belum selesai diproses.
- Panggil `ValidateProductionWithOptions()` agar `HandlerTimeout` positif dan
  relasinya dengan `ClaimMinIdle` tervalidasi sebelum membuka koneksi.
- Worker memakai `redisbroker.ConsumerCheck` setelah subscriber dimulai;
  publisher-only memakai `redisbroker.Check`. Keduanya readiness, bukan
  liveness.
- Redis queue production memakai `noeviction`, persistence sesuai RPO,
  replication/managed HA, backup/restore dan failover drill, ACL
  least-privilege, serta alert untuk persistence error, replication lag,
  receive retry, pending, dan DLQ.

## Privacy dan error boundary

- Jangan menulis token, credential, cookie, authorization header, DSN/URI,
  request/response body, raw URL/query/path, SQL argument, payload medis,
  identifier pasien, atau ordering key ke log, test fixture, dokumentasi, dan
  commit.
- Gunakan nama operation, route template, peer, status code, error kind, dan
  duration yang stabil serta low-cardinality.
- `logger.StartOp` dan broker dapat mencatat `err.Error()`. Sebelum error
  melewati boundary tersebut, map error dependency ke sentinel/typed error yang
  aman dan pertahankan cause hanya bila pesan cause juga aman.
- Response ke caller harus generik. Detail dependency disimpan sebagai
  klasifikasi terkontrol, bukan raw message.
- Jangan membuat fallback ke HTTP plaintext, TLS insecure, emulator, atau
  configuration default yang lebih lemah ketika validasi production gagal.

## Urutan bootstrap dan shutdown

```text
load config/secrets
  -> validasi semua production config
  -> init logger
  -> buka reusable DB pool / HTTP client / cache / broker
  -> pasang /livez tanpa dependency
  -> pasang /readyz dengan dependency checks
  -> pasang business route dan middleware
  -> start server / subscriber
  -> SIGTERM: stop menerima work baru
  -> shutdown HTTP server
  -> close broker dengan deadline
  -> close Redis cache
  -> close idle HTTP connections
  -> close/disconnect database
  -> flush/close logger
```

Jika startup gagal di tengah jalan, resource yang sudah berhasil dibuat tetap
wajib ditutup. Jangan mencetak config atau raw startup error yang mungkin
memuat secret.

## Testing wajib

- Pertahankan test service yang sudah ada dan tambahkan test pada boundary yang
  berubah.
- Logger/middleware: success, error, panic, request ID invalid, route template,
  serta assertion bahwa data sensitif tidak muncul pada output.
- HTTP client adapter: success, timeout/cancel, status mapping, malformed atau
  oversized response, retry exhaustion, dan bukti write tanpa idempotency key
  tidak diulang.
- Database: config invalid, context timeout, readiness success/failure/panic,
  dan integration test terhadap database nyata jika koneksi/pool/TLS/health
  diubah.
- Cache: hit, miss, expiry, delete, size bound, timeout/cancel, concurrent
  access, sanitized error, close, dan integration test Redis nyata.
- Broker: ack, nack, permanent error, malformed payload, timeout, panic,
  redelivery/idempotency, DLQ, dan shutdown. Untuk Redis, test `XACK`, stale
  reclaim, max attempts, malformed entry, reconnect/backoff, group hilang,
  Redis nyata, serta jalur TLS+auth. Gunakan
  `broker/memory` untuk unit test business handler; laporkan dengan jelas bila
  integration test cloud tidak tersedia.
- Jalankan minimal quality gate project:

  ```bash
  test -z "$(gofmt -l .)"
  go mod tidy -diff
  go vet ./...
  go test -count=1 ./...
  go test -count=1 -race ./...
  go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...
  go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
  git diff --check
  ```

- Ikuti threshold coverage dan scanner tambahan milik project. Jangan
  menyatakan production-ready jika test relevan gagal atau integration test
  wajib belum dijalankan.

## Definition of done untuk agent

Sebelum menyatakan pekerjaan selesai, pastikan:

- `go.mod` memakai tag semver eksplisit dan tidak memiliki `replace` lokal yang
  tidak sengaja ikut di-commit;
- seluruh config production divalidasi sebelum resource dibuat;
- setiap resource memiliki bounded startup dan shutdown lifecycle;
- route, operation, log, serta error tidak membocorkan data sensitif;
- readiness dan liveness memiliki semantik yang benar;
- retry HTTP dan broker tidak menggandakan side effect;
- deployment config, IaC, dan dokumentasi service diperbarui bila diperlukan;
- semua pemeriksaan yang benar-benar dijalankan dilaporkan sebagai pass, fail,
  atau skip beserta alasannya;
- agent tidak melakukan commit, push, tag, release, atau deployment kecuali
  diminta eksplisit oleh pengguna.

Laporan akhir agent harus menyebut versi `go-common`, package yang dipakai,
file yang berubah, keputusan keamanan/lifecycle, hasil test, serta validasi yang
belum dapat dijalankan.
