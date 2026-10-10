# database

Inisialisasi koneksi database dengan default production: timeout, connection pool yang dibatasi dan didaur ulang, ping dengan retry saat startup, health check untuk readiness probe, serta logging lewat [`logger`](../logger/README.md) **tanpa mencatat kredensial, argumen query, atau SQL statement secara default**.

| Package | Hasil | Driver |
|---|---|---|
| `database` | `WaitReady`, `HealthHandler`, `SQLCheck`, `Retry` | – |
| `database/mysql` | `*sql.DB` | `github.com/go-sql-driver/mysql` v1.10 |
| `database/postgres` | `*pgxpool.Pool` + query tracer | `github.com/jackc/pgx/v5` v5.11 |
| `database/mongodb` | `*mongo.Client` + `mongolog` | `go.mongodb.org/mongo-driver` v1.17 |

Hanya driver yang di-import yang ikut ter-compile. Library ini mengembalikan tipe asli driver, jadi tidak ada abstraksi baru yang perlu dipelajari dan sqlc/sqlx/repository yang ada tetap jalan.

## Pemakaian

```go
// MySQL
db, err := mysql.Open(ctx, mysql.Config{
	Host: cfg.DBHost, User: cfg.DBUser, Password: cfg.DBPassword, Database: "webhook",
})
if err != nil {
	return err
}
defer db.Close()

// PostgreSQL
pool, err := postgres.Open(ctx, postgres.Config{
	Host: cfg.PGHost, User: cfg.PGUser, Password: cfg.PGPassword, Database: "fhir",
	SSLMode:          "verify-full",
	ApplicationName:  "webhook-service",
	StatementTimeout: 30 * time.Second,
})
if err != nil {
	return err
}
defer pool.Close()

// MongoDB
client, err := mongodb.Connect(ctx, mongodb.Config{URI: cfg.MongoURI, AppName: "webhook-service"})
if err != nil {
	return err
}
defer func() {
    closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    _ = client.Disconnect(closeCtx)
}()

// Readiness probe
mux.Handle("/readyz", database.HealthHandler(0,
	database.SQLCheck("mysql", db),
	postgres.Check("postgres", pool),
	mongodb.Check("mongodb", client),
))
```

Field koneksi bisa diganti `DSN` / `URI` (mis. dari secret). Untuk opsi driver yang tidak diekspos, gunakan hook `Configure` (contoh: `BeforeConnect` untuk IAM token, `QueryExecModeExec` untuk PgBouncer, read preference Mongo).

## Default

| | MySQL | PostgreSQL | MongoDB |
|---|---|---|---|
| Connect/dial timeout | 5s | 5s | 5s (+ server selection 5s; default driver 30s) |
| Read / write timeout | 30s / 30s | – (pakai `StatementTimeout`) | – (pakai `Timeout`/ctx) |
| Max koneksi | 20 (`MaxIdleConns` = sama) | 20 | 50 per server |
| Lifetime / idle | 3m / – | 30m / 5m | – / 5m |
| Lainnya | `parseTime=true`, `loc=UTC` | `statement_timeout` opsional | monitor `mongolog` |

Nilai negatif pada lifetime/idle = tanpa batas. **Hitung kapasitas:** `replika × max koneksi` harus di bawah `max_connections` server, termasuk saat rolling deploy (replika lama + baru hidup bersamaan).

## Startup: `Retry`

`Open`/`Connect` melakukan ping sampai berhasil, supaya service yang start bersamaan dengan database atau sidecar Cloud SQL Proxy tidak crash-loop.

| Field | Default |
|---|---|
| `Attempts` | 5 (1 = tanpa retry) |
| `InitialBackoff` / `MaxBackoff` | 500ms / 5s (eksponensial + jitter) |
| `PingTimeout` | 5s per ping |

Setiap ping gagal dicatat warn (`database not ready, retrying`), berhasil info (`database connected`), menyerah error (`database unreachable`). `SkipPing: true` melewati ping (mis. untuk CLI yang belum tentu butuh DB).

## Health check

`HealthHandler(timeout, checks...)` menjalankan semua check paralel (default timeout 2s), menjawab **200** jika semua `ok` dan **503** jika ada yang gagal:

```json
{"status":"unavailable","checks":{"mysql":"ok","mongodb":"unavailable"}}
```

Detail error hanya dicatat di log (warn), tidak dikirim di response, supaya hostname/pesan driver tidak bocor. Pasang di **readiness** probe saja: liveness yang ikut gagal saat DB down akan membuat Kubernetes me-restart pod yang sebenarnya sehat.

Handler mengisolasi panic, `Ping=nil`, nama kosong/duplikat, dan check yang mengabaikan context. Request readiness tetap kembali saat deadline; goroutine check yang mengabaikan context dapat tetap hidup, jadi setiap implementasi `Ping` tetap wajib menghormati cancellation.

## Logging query

| | Per query | Keterangan |
|---|---|---|
| PostgreSQL | `postgres query` (debug), `… slow` (warn, ≥500ms), `… failed` (warn) | Default hanya `db_operation`, `db_rows_affected`, `duration_ms`, dan kode error PostgreSQL. Argumen, detail error, dan SQL statement tidak dicatat. Atur lewat `Config.QueryLog`. |
| MongoDB | `mongo command` (debug), slow/failed (warn) | Lewat `logger/mongolog`; body command dan raw failure tidak dicatat. Atur lewat `Config.CommandLog`. |
| MySQL | – | `database/sql` tidak punya hook query; gunakan `logger.StartOp` di repository (`db_system=mysql`) yang sudah mencatat durasi, slow, dan error. Pesan internal driver diklasifikasikan tanpa menyalin teks mentahnya. |

Kegagalan query dicatat **warn** karena error yang sama akan dicatat **error** satu kali oleh Op repository.

`QueryLog.IncludeStatement` adalah opt-in dan hanya cocok untuk query yang dipastikan tidak memiliki literal sensitif. Untuk workload medis, biarkan `false`; parameterisasi query saja tidak dapat melindungi statement yang dibangun dengan literal.

Semua baris membawa `trace_id`, `op`, `span_id` dari context — selalu teruskan `ctx` ke query (`QueryContext`, `pool.Query(ctx, …)`).

## Keamanan

- Password tidak pernah masuk log maupun pesan error dari `Open`/`Connect` (DSN/URI yang tidak valid menghasilkan pesan statis; dites).
- Password dengan karakter khusus (`@ : / ? #`) aman dipakai di field `Password` — di-escape otomatis.
- Panggil `ValidateProduction()` pada konfigurasi sebelum `Open`/`Connect`. Koneksi TCP production harus memakai TLS dengan verifikasi sertifikat. MySQL Unix socket diterima untuk trusted local proxy.
- PostgreSQL: gunakan `SSLMode: "verify-full"`. `require` mengenkripsi tetapi tidak memverifikasi identitas server dan ditolak oleh `ValidateProduction`.
- Jika memakai trusted local proxy yang memang menyediakan transport aman tanpa TLS end-to-end, dokumentasikan pengecualian dan jangan panggil validasi TLS yang lebih ketat ini.
- Readiness hanya mencatat transisi sehat → gagal dan gagal → pulih; raw error
  driver tidak dicatat agar probe berulang tidak menimbulkan log storm atau
  membocorkan alamat dependency.

```go
if err := postgresConfig.ValidateProduction(); err != nil {
	return err
}
pool, err := postgres.Open(ctx, postgresConfig)
```

## Tes integrasi

Unit test berjalan tanpa database. Tes terhadap server sungguhan aktif jika env var diisi:

```bash
docker run -d --rm -e MYSQL_ROOT_PASSWORD=pw -p 3306:3306 mysql:8
docker run -d --rm -e POSTGRES_PASSWORD=pw -p 5432:5432 postgres:17
docker run -d --rm -p 27017:27017 mongo:8

MYSQL_TEST_DSN='root:pw@tcp(127.0.0.1:3306)/mysql' \
POSTGRES_TEST_DSN='postgres://postgres:pw@127.0.0.1:5432/postgres?sslmode=disable' \
MONGODB_TEST_URI='mongodb://127.0.0.1:27017' \
go test ./database/...
```

## Catatan

- MongoDB memakai driver **v1** agar konsisten dengan `logger/mongolog`. Migrasi ke v2 perlu dilakukan bersamaan untuk keduanya.
