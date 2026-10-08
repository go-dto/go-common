# httpclient

HTTP client standar untuk memanggil pihak ketiga / service lain: timeout yang wajar, connection pool yang di-tune, retry dengan exponential backoff, dan logging per attempt lewat [`logger`](../logger/README.md) (propagasi `X-Request-ID`, redaksi URL/header).

```go
import "github.com/go-dto/go-common/httpclient"
```

## Pemakaian

Buat **satu client per peer**, sekali saat startup, lalu pakai ulang (aman untuk concurrent use):

```go
webhookClient := httpclient.New(httpclient.Options{
	Peer:    "client_webhook",
	Timeout: 10 * time.Second,
})

req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
if err != nil {
	return err
}
req.Header.Set("Content-Type", "application/json")
req.Header.Set("Idempotency-Key", requestID) // POST jadi boleh di-retry
resp, err := webhookClient.Do(req)
if err != nil {
	return err
}
defer resp.Body.Close()
```

Selalu pakai `http.NewRequestWithContext` agar `trace_id` & field context masuk ke log dan request bisa dibatalkan.

## Susunan

```
http.Client{Timeout}                      ← total budget semua attempt + backoff + baca body
  └─ retry (backoff, Retry-After, PerAttemptTimeout)
       └─ logger.NewHTTPTransport         ← X-Request-ID, 1 log line per attempt
            └─ *http.Transport            ← pool & dial/TLS timeout
```

## Options & default

| Field | Default | Keterangan |
|---|---|---|
| `Peer` | – | Nama logis pihak ketiga, di-log sebagai `peer`. |
| `Timeout` | 30s | Total budget satu `Do` (semua attempt). Negatif = tanpa timeout. |
| `PerAttemptTimeout` | 0 (off) | Batas satu attempt; attempt yang hang bisa di-retry dalam `Timeout`. |
| `MaxRetries` / `DisableRetry` | 2 / false | Retry setelah attempt pertama (maks 3 attempt). |
| `InitialBackoff` / `MaxBackoff` | 100ms / 2s | Exponential backoff + jitter (antara d/2 dan d). |
| `RetryStatuses` | 429, 500, 502, 503, 504 | Slice kosong non-nil = hanya retry transport error. |
| `RetryNonIdempotent` | false | Izinkan retry POST/PATCH tanpa `Idempotency-Key`. |
| `ShouldRetry` | – | Ganti aturan status/error dengan logika sendiri. Jangan baca `resp.Body` di dalamnya — response bisa dikembalikan ke pemanggil. |
| `OnRetry` | – | Callback tiap kali attempt akan di-retry (`RetryEvent`: attempt, status/error, wait). Untuk metrics. |
| `Transport` | clone `http.DefaultTransport` | Base transport; jika diisi, field tuning di bawah diabaikan. |
| `DialTimeout` / `TLSHandshakeTimeout` | 5s / 5s | |
| `ResponseHeaderTimeout` | 10s | Peer yang tidak mengirim header dalam batas ini dianggap gagal dan bisa di-retry. Negatif = tanpa batas. |
| `MaxIdleConnsPerHost` / `MaxConnsPerHost` | 10 / 0 (unlimited) | Default net/http hanya 2 idle conn per host. |
| `TLSClientConfig` | – | Root CA kustom, client certificate (mTLS), dll. Di-clone. |
| `Proxy` | `http.ProxyFromEnvironment` | Pemilih proxy per request. |
| `DisableLogging`, `DisablePropagation`, `LogHeaders`, `Redactor` | – | Diteruskan ke `logger.NewHTTPTransport`. |

## Aturan retry

Request di-retry hanya jika **semua** terpenuhi:

1. Method idempotent (`GET`, `HEAD`, `OPTIONS`, `TRACE`, `PUT`, `DELETE`), atau ada header `Idempotency-Key` / `X-Idempotency-Key`, atau `RetryNonIdempotent: true`.
2. Body bisa diulang — `http.NewRequest` otomatis mengisi `GetBody` untuk `*bytes.Buffer`, `*bytes.Reader`, `*strings.Reader`. Body lain (mis. `io.Pipe`) hanya dikirim sekali.
3. Hasilnya transport error (kecuali error sertifikat TLS) atau status di `RetryStatuses`.
4. Context request belum selesai dan backoff berikutnya masih muat dalam sisa `Timeout`.
5. `Retry-After` (429/503) tidak lebih lama dari `MaxBackoff` — kalau lebih lama, response langsung dikembalikan.

Jika retry habis, **response terakhir dikembalikan apa adanya** (cek `StatusCode` seperti biasa), atau error transport terakhir.

### Metrics

```go
retries := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "http_client_retries_total"}, []string{"peer", "status"})

c := httpclient.New(httpclient.Options{
	Peer: "client_webhook",
	OnRetry: func(e httpclient.RetryEvent) {
		retries.WithLabelValues(e.Peer, strconv.Itoa(e.StatusCode)).Inc() // status 0 = transport error
	},
})
```

### mTLS / CA internal

```go
cert, err := tls.LoadX509KeyPair("client.crt", "client.key")
// ...
c := httpclient.New(httpclient.Options{
	Peer:            "partner_api",
	TLSClientConfig: &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: caPool},
})
```

## Logging

Setiap attempt menghasilkan satu baris `http outbound` dari `logger` (level info / warn untuk 4xx-5xx & lambat / error untuk transport error). Attempt ke-2 dst. membawa field `attempt`. Keputusan retry di-log di level debug (`http outbound retrying`, dengan `backoff_ms`).

```json
{"level":"warn","msg":"http outbound error status","trace_id":"…","layer":"client","peer":"client_webhook","method":"POST","status_code":503,"duration_ms":41.2}
{"level":"info","msg":"http outbound","trace_id":"…","layer":"client","peer":"client_webhook","attempt":2,"method":"POST","status_code":200,"duration_ms":38.7}
```
