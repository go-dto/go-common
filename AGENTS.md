# AGENTS.md

Aturan ini berlaku untuk seluruh repository `go-common`. AI coding agent wajib
membaca file ini dan `docs/PRODUCTION_IMPLEMENTATION.md` sebelum menambah atau
mengubah integrasi package.

## Tujuan repository

`go-common` adalah library infrastruktur publik, bukan tempat business logic
service. Perubahan harus reusable, backward-compatible bila memungkinkan,
aman untuk data sensitif, dan tidak bergantung pada repository service tertentu.

Package utama:

- `logger`: structured logging, correlation ID, operation span, HTTP access log,
  redaction, dan Mongo command monitor.
- `httpclient`: HTTP client high-level dengan total timeout, retry aman, response
  body bounded, typed error, dan sanitized logging.
- `database`: koneksi, startup retry, readiness, dan production validation untuk
  MySQL, PostgreSQL, dan MongoDB.
- `broker`: publish/subscribe, trace propagation, handler timeout, panic
  isolation, ack/nack, dan graceful shutdown.

## Aturan sebelum mengubah kode

1. Jalankan `git status --short --branch`; jangan menimpa perubahan pengguna.
2. Baca README package yang diubah dan gunakan API publiknya, bukan internalnya.
3. Gunakan toolchain yang sama dengan `go.mod` dan CI.
4. Jangan menambahkan token, credential, DSN, payload medis, identifier pasien,
   atau hostname internal ke kode, test fixture, log, dokumentasi, maupun commit.
5. Jangan melakukan commit, push, tag, atau release kecuali diminta eksplisit.

## Pola implementasi wajib

### Logger

- Mulai dari `logger.ConfigFromEnv()`, lalu panggil
  `Config.ValidateProduction()` sebelum `logger.Init` pada service production.
- Ambil logger melalui `logger.L()` atau `logger.FromContext(ctx)` saat dipakai;
  jangan menyimpan `*zap.Logger` global di struct jangka panjang.
- Pasang `logger.HTTPMiddleware` di outer HTTP stack. Isi `Route` dengan template
  router (`r.Pattern` untuk `http.ServeMux`), bukan raw path.
- Gunakan `logger.StartOp` pada boundary handler/service/repository. Teruskan
  context hasilnya ke downstream dan panggil `defer op.Done(&err)`.
- Jangan log request/response body, query value, credential, raw URL/path,
  ordering key, atau panic value. Gunakan field stabil dan low-cardinality.
- `StartOp` dan broker dapat mencatat `err.Error()`. Error yang diteruskan ke
  keduanya tidak boleh menyisipkan payload, credential, raw URL/DSN, atau
  identifier pasien; map error dependency ke sentinel/kind yang aman.
- `LogRawPath`, `LogURL`, statement logging, header logging, dan panic stack
  adalah opt-in untuk data yang telah dinilai aman; jangan aktifkan sebagai
  default production.
- Jangan memakai `logger/singleton` untuk kode baru; package itu hanya facade
  kompatibilitas kode lama.

### HTTP client

- Buat satu `httpclient.Client` per upstream dan reuse sepanjang umur proses.
- Panggil `Config.ValidateProduction()` sebelum `httpclient.New`.
- Gunakan HTTPS dengan certificate verification, total timeout positif, dan
  connection bound. Jangan memasang custom `Transport` di jalur production.
- Gunakan `Path`, `Query`, `Header`, dan `Operation`; jangan concatenate ID ke
  URL dan jangan menaruh ID/URL pada nama operation.
- Retry write request hanya dengan `IdempotencyKey`, kecuali kontrak upstream
  secara eksplisit menjamin request aman diulang.
- Tangani error memakai `IsTimeout`, `IsStatus`, `IsRetryExhausted`, atau
  `KindOf`; jangan parse string error.
- `Response` sudah dibuffer dan ditutup oleh package. Jangan mencoba menutup
  body `net/http` yang tidak diekspos.
- Panggil `CloseIdleConnections()` saat graceful shutdown.

### Database

- Pilih satu driver sub-package yang memang dipakai service. Jangan membuat
  abstraksi repository baru di package ini.
- Panggil `ValidateProduction()` sebelum `Open`/`Connect`.
- Wajib verified TLS untuk koneksi TCP production. Trusted local proxy/Unix
  socket harus menjadi pengecualian yang terdokumentasi.
- Tetapkan pool berdasarkan `jumlah replica x maksimum koneksi`, termasuk
  overlap rolling deployment.
- Gunakan context deadline untuk query dan server-side statement timeout bila
  tersedia.
- Pasang `database.HealthHandler` hanya sebagai readiness, bukan liveness.
- Jangan mengaktifkan raw SQL/argument/driver-error logging. Gunakan operation,
  error kind, duration, dan rows affected yang bounded.
- Migrasi MongoDB driver v1 ke v2 harus mengubah `database/mongodb` dan
  `logger/mongolog` bersama-sama; jangan migrasi salah satunya saja.

### Broker

- Panggil `gcppubsub.Config.ValidateProduction()` sebelum `gcppubsub.New`.
- Production menggunakan ADC dan endpoint Google standar; emulator, injected
  client, dan arbitrary client options hanya untuk dev/test yang terkontrol.
- Set `HandlerTimeout`, flow-control (`MaxOutstandingMessages` dan bytes), serta
  deadline `CloseContext`.
- Handler harus idempotent karena delivery at-least-once.
- Gunakan `broker.Permanent(err)` hanya untuk pesan yang retry-nya tidak mungkin
  berhasil, misalnya payload invalid. Error transient harus dikembalikan biasa.
- Retry policy dan dead-letter topic dibuat di broker/IaC, bukan loop manual di
  handler.
- Jangan log payload, message attributes, atau ordering key.

## Urutan bootstrap service

```text
load env/config
  -> ValidateProduction semua config
  -> Init logger
  -> buat reusable HTTP client / DB pool / broker
  -> pasang /readyz dan business routes
  -> pasang HTTP middleware dengan route template
  -> start server/consumer
  -> SIGTERM: stop accepting work
  -> shutdown HTTP, broker, HTTP idle connections, DB, logger dengan deadline
```

Gunakan contoh siap-adaptasi di
[`docs/PRODUCTION_IMPLEMENTATION.md`](docs/PRODUCTION_IMPLEMENTATION.md).

## Kontrak perubahan API

- Field/option baru sebaiknya backward-compatible dan zero value-nya aman.
- Perubahan behavior default harus memiliki alasan keamanan/reliability, test
  boundary, dan pembaruan README.
- Error publik harus typed/stabil dan tidak membocorkan URL, DSN, credential,
  payload, atau response body.
- Perubahan exported API wajib memperbarui godoc, README, unit test, dan contoh.
- Jalankan `golang.org/x/exp/cmd/apidiff` terhadap base branch untuk perubahan
  exported API. Incompatible change harus dibatalkan atau dirilis dengan versi
  semver yang sesuai dan migration note.
- Breaking change setelah `v1` wajib major version baru; sebelum `v1`, tetap
  dokumentasikan migrasinya.

## Quality gate sebelum menyatakan selesai

```bash
test -z "$(gofmt -l .)"
go mod tidy -diff
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...
go test -count=1 ./...
go test -count=1 -race ./...
go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
go run github.com/zricethezav/gitleaks/v8@v8.30.1 git --redact --no-banner .
git diff --check
```

- Coverage total tidak boleh di bawah threshold CI dan package yang diubah harus
  memiliki test success, failure, timeout/cancel, panic/malformed dependency,
  privacy, serta concurrency bila relevan.
- Jalankan database integration test dengan MySQL, PostgreSQL, dan MongoDB nyata
  bila mengubah driver, pool, TLS, ping, atau health check.
- Laporkan tepat mana yang lulus, skip, atau tidak tersedia. Jangan menyatakan
  release-ready jika worktree belum menjadi commit, CI commit tersebut belum
  hijau, integration test belum berjalan, atau belum ada tag versi yang sesuai.
