# go-common

Kumpulan library Go reusable untuk kebutuhan service: logging, HTTP client,
cache, message broker, dan koneksi database. Tujuannya agar komponen
infrastruktur yang umum cukup ditulis dan dirawat di satu tempat.

Module: `github.com/go-dto/go-common`

Minimum Go version: **1.26.9** (includes standard-library security fixes used by
the networking packages).

## Package

| Package | Status | Deskripsi |
|---|---|---|
| [`logger`](logger/README.md) | ✅ stabil | Logger terstruktur berbasis zap: tracing lintas layer (`StartOp`), propagasi `X-Request-ID`, redaksi data sensitif, rotasi file, HTTP transport ber-log, monitor MongoDB (`logger/mongolog`). |
| [`httpclient`](httpclient/README.md) | 🧪 baru | HTTP client high-level: method/header/query/body helper tanpa lifecycle `net/http`, timeout, pool, retry aman, typed error, response auto-close, validasi production HTTPS, dan satu final log per logical request. |
| [`cache`](cache/README.md) | 🧪 baru | Cache byte-oriented dengan typed miss/error, TTL dan size bound. Driver: bounded `cache/memory` untuk test serta `cache/redis` dengan TLS, pool, readiness, dan sanitized logging. |
| [`broker`](broker/README.md) | 🧪 baru | Publish/subscribe netral-backend: propagasi correlation ID tervalidasi, log per pesan, ack/nack + `Permanent`, panic recovery tanpa membocorkan panic value, timeout handler, dan bounded shutdown. Driver: `broker/gcppubsub`, Redis Streams `broker/redis` dengan reconnect/readiness/DLQ, dan `broker/memory`. |
| [`database`](database/README.md) | 🧪 baru | Inisialisasi MySQL, PostgreSQL, dan MongoDB: default timeout/pool, validasi TLS production, startup retry, readiness yang tahan panic/timeout, dan query logging tanpa statement/argumen secara default. |

## Implementasi production dan AI agents

- [`docs/PRODUCTION_IMPLEMENTATION.md`](docs/PRODUCTION_IMPLEMENTATION.md) berisi
  contoh wiring HTTP service, PostgreSQL, outbound HTTP client, cache Redis,
  Pub/Sub/Redis Streams worker, MySQL/MongoDB, readiness, dan graceful shutdown.
- [`docs/AGENTS_INTEGRATION.md`](docs/AGENTS_INTEGRATION.md) adalah template
  aturan yang dapat disalin ke `AGENTS.md` repository service agar AI coding
  agent mengintegrasikan package dengan lifecycle dan privacy boundary yang
  benar.
- [`AGENTS.md`](AGENTS.md) adalah aturan khusus contributor/maintainer yang
  mengembangkan repository `go-common`.
- [`CHANGELOG.md`](CHANGELOG.md) mencatat perubahan per release, sedangkan
  [`SECURITY.md`](SECURITY.md) menjelaskan pelaporan vulnerability secara
  privat tanpa membuka detail sensitif di issue publik.

## Instalasi

Tag publik terbaru adalah `v0.2.0`:

```bash
go get github.com/go-dto/go-common@v0.2.0
```

Release `v0.2.0` memuat `cache/redis` dan `broker/redis`. Jangan menggunakan
branch atau pseudo-version untuk menyiasati pin versi production.

Untuk production, pin tag semver eksplisit dan review release note sebelum
upgrade; jangan bergantung pada `@latest` atau branch bergerak.

### CI & Docker

GitHub Actions di [`.github/workflows/ci.yml`](.github/workflows/ci.yml)
menjalankan format check, `go vet`, Staticcheck, race test, minimum coverage
80%, integration test database dan Redis, `govulncheck`, serta secret scan
seluruh history pada setiap push dan pull request. Service image integration
test dipin ke digest agar reproducible.

Karena module bersifat publik, Docker build tidak memerlukan token repository:

```dockerfile
RUN go mod download
```

## Konvensi

- **Satu module, banyak package.** Go hanya meng-compile package yang di-import, jadi service yang hanya memakai `logger` tidak ikut membawa driver database.
- **Package baru** dibuat di root (`<nama>/`), lengkap dengan `README.md`, `doc.go`, dan test.
- **Tidak boleh bergantung pada kode service** mana pun. Dependensi antar package di repo ini boleh (mis. `httpclient` → `logger`).
- **Versi** memakai semver tag (`v0.x.y` selama API belum stabil). Breaking change di `v1+` wajib naik major version.
- Sebelum merge: `gofmt`, `go vet ./...`, `go test -race ./...` harus lolos (dijalankan otomatis di CI).

## Rilis

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

Service pemakai lalu menjalankan
`go get github.com/go-dto/go-common@vX.Y.Z` dengan versi rilis aktual.

## Pengembangan lokal bersama service

Saat mengubah library dan service sekaligus, arahkan service ke salinan lokal (jangan di-commit):

```bash
go mod edit -replace github.com/go-dto/go-common=../go-common
```

Atau gunakan `go work` di folder induk.

## Lisensi

Dirilis di bawah [MIT License](LICENSE).
