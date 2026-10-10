# go-common

Kumpulan library Go reusable untuk kebutuhan service: logging, HTTP client, message broker, dan koneksi database. Tujuannya agar komponen infrastruktur yang umum cukup ditulis dan dirawat di satu tempat.

Module: `github.com/go-dto/go-common`

Minimum Go version: **1.26.9** (includes standard-library security fixes used by
the networking packages).

## Package

| Package | Status | Deskripsi |
|---|---|---|
| [`logger`](logger/README.md) | ✅ stabil | Logger terstruktur berbasis zap: tracing lintas layer (`StartOp`), propagasi `X-Request-ID`, redaksi data sensitif, rotasi file, HTTP transport ber-log, monitor MongoDB (`logger/mongolog`). |
| [`httpclient`](httpclient/README.md) | 🧪 baru | HTTP client high-level: method/header/query/body helper tanpa lifecycle `net/http`, timeout, pool, retry aman, typed error, response auto-close, validasi production HTTPS, dan satu final log per logical request. |
| [`broker`](broker/README.md) | 🧪 baru | Publish/subscribe netral-backend: propagasi correlation ID tervalidasi, log per pesan, ack/nack + `Permanent`, panic recovery tanpa membocorkan panic value, timeout handler, dan bounded shutdown. Driver: `broker/gcppubsub` dan `broker/memory`. |
| [`database`](database/README.md) | 🧪 baru | Inisialisasi MySQL, PostgreSQL, dan MongoDB: default timeout/pool, validasi TLS production, startup retry, readiness yang tahan panic/timeout, dan query logging tanpa statement/argumen secara default. |

## Implementasi production dan AI agents

- [`docs/PRODUCTION_IMPLEMENTATION.md`](docs/PRODUCTION_IMPLEMENTATION.md) berisi
  contoh wiring HTTP service, PostgreSQL, outbound HTTP client, Pub/Sub worker,
  MySQL/MongoDB, readiness, dan graceful shutdown.
- [`AGENTS.md`](AGENTS.md) adalah aturan repository untuk AI coding agents:
  pilihan API, privacy boundary, lifecycle, anti-pattern, serta quality gate yang
  wajib diperiksa sebelum menyatakan perubahan production-ready.

## Instalasi

```bash
go get github.com/go-dto/go-common@latest
```

### CI & Docker

GitHub Actions di [`.github/workflows/ci.yml`](.github/workflows/ci.yml) menjalankan format check, `go vet`, Staticcheck, race test, minimum coverage 80%, integration test database, `govulncheck`, dan secret scan seluruh history pada setiap push dan pull request. Service image integration test dipin ke digest agar reproducible.

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
git tag v0.1.0
git push origin v0.1.0
```

Service pemakai lalu menjalankan `go get github.com/go-dto/go-common@v0.1.0`.

## Pengembangan lokal bersama service

Saat mengubah library dan service sekaligus, arahkan service ke salinan lokal (jangan di-commit):

```bash
go mod edit -replace github.com/go-dto/go-common=../go-common
```

Atau gunakan `go work` di folder induk.

## Lisensi

Dirilis di bawah [MIT License](LICENSE).
