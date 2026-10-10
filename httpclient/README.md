# httpclient

HTTP client high-level untuk service Go. Developer cukup memakai `Get`, `Post`,
`Put`, `Patch`, atau `Delete`; package menangani URL, header, JSON, timeout,
retry, penutupan response body, error classification, trace propagation, dan
structured logging.

```go
import "github.com/go-dto/go-common/httpclient"
```

Application code tidak perlu mengimpor `net/http`.

## Quick start

Buat satu client per upstream saat startup, lalu gunakan ulang:

```go
paymentClient, err := httpclient.New(httpclient.Config{
    Peer:    "payment-api",
    BaseURL: cfg.PaymentURL,
    Timeout: 10 * time.Second,
    DefaultHeaders: httpclient.HeaderMap{
        "Accept": "application/json",
    },
    Auth: httpclient.BearerToken(cfg.PaymentToken),
})
if err != nil {
    return err
}
```

Untuk startup production, validasi HTTPS, certificate verification, timeout,
dan batas koneksi sebelum membuat client. Validator menolak custom `Transport`
karena keamanan TLS transport arbitrer tidak dapat diperiksa oleh package:

```go
clientConfig := httpclient.Config{Peer: "payment-api", BaseURL: cfg.PaymentURL}
if err := clientConfig.ValidateProduction(); err != nil {
    return err
}
paymentClient, err := httpclient.New(clientConfig)
```

Untuk konfigurasi startup yang sudah pasti valid, tersedia
`httpclient.MustNew`.

### GET

```go
response, err := paymentClient.Get(ctx, "/v1/payments/{id}",
    httpclient.Path("id", paymentID),
    httpclient.Query("include", "history"),
    httpclient.Header("X-Tenant-ID", tenantID),
    httpclient.Operation("payment.get"),
)
if err != nil {
    return err
}

var payment Payment
if err := response.JSON(&payment); err != nil {
    return err
}
```

`Path` melakukan URL escaping. Gunakan placeholder; jangan menggabungkan ID ke
path secara manual.

### POST JSON

```go
response, err := paymentClient.Post(ctx, "/v1/payments",
    httpclient.JSON(CreatePaymentRequest{
        Amount:   100_000,
        Currency: "IDR",
    }),
    httpclient.IdempotencyKey(requestID),
    httpclient.Operation("payment.create"),
)
if err != nil {
    return err
}

var result Payment
if err := response.JSON(&result); err != nil {
    return err
}
```

`IdempotencyKey` membuat POST/PATCH eligible untuk retry. Tanpanya,
non-idempotent request dikirim satu kali.

## Method dan request option

Method utama:

- `Get`
- `Head`
- `Post`
- `Put`
- `Patch`
- `Delete`
- `Request` dengan `MethodGet`, `MethodPost`, dan konstanta lainnya untuk kasus
  khusus

Request option:

| Option | Kegunaan |
|---|---|
| `Path(name, value)` | Mengisi `{name}` dengan path escaping. |
| `Query(name, value)` | Query parameter tunggal. |
| `QueryValues(name, values...)` | Query parameter berulang. |
| `Queries(map)` | Beberapa query parameter. |
| `Header(name, value)` | Header per request. |
| `Headers(map)` | Beberapa header per request. |
| `JSON(value)` | Encode JSON + `Content-Type: application/json`. |
| `Form(map)` | Encode form URL-encoded. |
| `Text(value)` | Plain-text body. |
| `Bytes(contentType, value)` | Byte body dengan content type eksplisit. |
| `IdempotencyKey(value)` | Header idempotency untuk write retry. |
| `AcceptStatus(codes...)` | Mengganti default success `200-299`. |
| `Operation(name)` | Nama aman dan low-cardinality untuk log/metrics. |
| `RequestTimeout(duration)` | Budget lebih ketat untuk satu request. |

Header transport seperti `Host`, `Content-Length`, `Connection` dan
`Transfer-Encoding` ditolak. Authentication dari `Config.Auth` diterapkan
setelah request options agar tidak dapat tertimpa tanpa sengaja.

## Authentication

```go
httpclient.BearerToken(token)
httpclient.BearerTokenProvider(func(ctx context.Context) (string, error) {
    return tokenStore.Token(ctx)
})
httpclient.BasicAuth(username, password)
httpclient.APIKey("X-API-Key", apiKey)
```

Nilai authentication tidak pernah dimasukkan ke log atau error message.

## Response

`Response` sudah dibaca dengan batas ukuran dan body sudah ditutup sebelum
method selesai. Caller tidak perlu memanggil `Close`.

```go
response.StatusCode
response.Attempts
response.Duration
response.IsSuccess()
response.Header("ETag")
response.HeaderValues("Set-Cookie")
response.Headers()
response.JSON(&destination)
response.Text()
response.Bytes()
```

Default maksimum body adalah 4 MiB dan dapat diubah melalui
`MaxResponseBodyBytes`. `Bytes()` selalu mengembalikan salinan.

## Error

Status di luar `200-299` menghasilkan `*httpclient.Error` dan tetap
mengembalikan `Response`, sehingga caller bisa membaca body error bila kontrak
upstream membutuhkannya.

```go
response, err := paymentClient.Get(ctx, "/v1/payments/{id}",
    httpclient.Path("id", paymentID),
)

switch {
case httpclient.IsTimeout(err):
    // timeout
case httpclient.IsStatus(err, 404):
    // not found
case httpclient.IsRetryExhausted(err):
    // seluruh attempt sudah digunakan
case err != nil:
    // error lain
}
```

`KindOf(err)` menghasilkan klasifikasi stabil:

- `invalid_config`
- `invalid_request`
- `auth`
- `canceled`
- `timeout`
- `tls`
- `transport`
- `http_status`
- `response_too_large`
- `decode`

Error message tidak memuat raw URL, path parameter, header, token, query, atau
response body.

## Retry

Default maksimal tiga attempt: request pertama + dua retry. Default status yang
di-retry adalah `429`, `502`, `503`, dan `504`. `500` tidak di-retry secara
default, tetapi dapat ditambahkan melalui `RetryStatuses`.

Request hanya di-retry bila:

1. Method idempotent, memiliki `Idempotency-Key`, atau
   `RetryNonIdempotent` diaktifkan.
2. Body dapat diulang.
3. Status termasuk `RetryStatuses`, atau terjadi transient transport error
   seperti timeout, EOF, connection reset/refused, atau temporary DNS failure.
4. Context dan total timeout masih memiliki budget.
5. `Retry-After` pada `429`/`503` tidak melebihi `MaxBackoff`.

Certificate error, canceled context, permanent DNS error, malformed request,
dan konfigurasi yang deterministik tidak di-retry.

`PerAttemptTimeout` tetap aktif saat body dibaca. Bila body timeout setelah
header diterima, request tidak otomatis diulang karena sebagian response sudah
diterima.

## Redirect

Redirect tidak diikuti secara default. Ini mencegah API key atau custom auth
header terbawa ke host yang tidak dipercaya.

`FollowRedirects: true` hanya mengizinkan redirect dengan scheme dan host yang
sama. `MaxRedirects` default 10 saat fitur ini diaktifkan. Cross-host redirect
selalu ditolak.

## Logging dan observability

Setiap logical request menghasilkan tepat satu final log, bukan satu warn/error
untuk setiap attempt:

```json
{
  "level": "warn",
  "msg": "http outbound recovered",
  "trace_id": "...",
  "layer": "client",
  "peer": "payment-api",
  "method": "POST",
  "operation": "payment.create",
  "status_code": 201,
  "attempt_count": 2,
  "retry_count": 1,
  "duration_ms": 312.7,
  "outcome": "success_after_retry"
}
```

Log tidak berisi URL, path parameter, query value, header, atau body. Detail
retry hanya ditulis pada level debug. Trace ID dari context otomatis diteruskan
sebagai `X-Request-ID`.

Hook untuk metrics:

```go
client, err := httpclient.New(httpclient.Config{
    Peer:    "payment-api",
    BaseURL: paymentURL,
    OnRetry: func(event httpclient.RetryEvent) {
        retryCounter.WithLabelValues(event.Peer, string(event.Method)).Inc()
    },
    OnResult: func(ctx context.Context, event httpclient.ResultEvent) {
        requestDuration.WithLabelValues(
            event.Peer,
            string(event.Method),
            event.Outcome,
        ).Observe(event.Duration.Seconds())
    },
})
```

Jangan memakai URL, trace ID, user ID, atau path parameter sebagai label metrics.
Hook berjalan synchronous dan harus cepat serta aman dipanggil concurrent. Panic dari hook diisolasi dan tidak menggagalkan request; panic value tidak dicatat. Sebelum `OnRetry` dipanggil, response attempt sebelumnya sudah di-drain dan ditutup agar hook lambat tidak menahan koneksi. Hook yang tidak pernah kembali tetap dapat menahan caller, sehingga jangan melakukan network I/O tanpa context/deadline di dalam hook.

## Config utama

| Field | Default | Keterangan |
|---|---|---|
| `Peer` | wajib | Nama logis upstream. |
| `BaseURL` | wajib | HTTP(S), tanpa credentials/query/fragment. |
| `DefaultHeaders` | kosong | Header yang berlaku untuk semua request. |
| `Auth` | kosong | Bearer, Basic, API key, atau token provider. |
| `MaxResponseBodyBytes` | 4 MiB | Batas response yang dibuffer. |
| `Timeout` | 30s | Total logical request, termasuk token provider, retry, backoff, dan body read; negatif hanya untuk dev/test. |
| `PerAttemptTimeout` | off | Batas satu attempt. |
| `MaxRetries` | 2 | Retry setelah request pertama. |
| `InitialBackoff` / `MaxBackoff` | 100ms / 2s | Exponential equal-jitter. |
| `RetryStatuses` | 429, 502, 503, 504 | Slice kosong berarti transport error saja. |
| `ResponseHeaderTimeout` | 10s | Batas menunggu header. |
| `MaxIdleConnsPerHost` | 10 | Pool per upstream. |
| `MaxConnsPerHost` | 100 | Batas seluruh koneksi aktif dan idle per upstream. |
| `FollowRedirects` | false | Hanya same-origin bila diaktifkan. |
| `DisableLogging` | false | Menonaktifkan final log dan retry debug. |
| `DisablePropagation` | false | Menonaktifkan `X-Request-ID`. |

Panggil `client.CloseIdleConnections()` saat graceful shutdown bila perlu.
