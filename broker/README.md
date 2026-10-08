# broker

Abstraksi publish/subscribe yang tidak terikat backend, lengkap dengan propagasi trace, logging terstruktur lewat [`logger`](../logger/README.md), dan semantik handler yang aman (ack/nack, error permanen, panic recovery, timeout).

```go
import (
	"github.com/go-dto/go-common/broker"
	"github.com/go-dto/go-common/broker/gcppubsub"
)
```

| Package | Untuk | Dependency |
|---|---|---|
| `broker` | API inti: `Broker`, `Message`, `Handler`, `Middleware`, `Permanent` | hanya `logger` |
| `broker/gcppubsub` | Google Cloud Pub/Sub (production) | `cloud.google.com/go/pubsub/v2` |
| `broker/memory` | Unit test & dev lokal, in-process | – |

Dependency GCP hanya ikut ter-compile jika `broker/gcppubsub` di-import.

## Susunan

```
Broker  (trace, log, Op, panic recovery, timeout, middleware, Permanent → ack)
  └─ Driver interface  (Publish / Receive / Close)
       ├─ gcppubsub.Driver
       └─ memory.Driver
```

Service cukup bergantung pada interface `broker.Publisher` / `broker.Subscriber`, sehingga mudah di-mock atau diganti dengan `memory` di test.

## Pemakaian

### Inisialisasi (sekali saat startup)

```go
b, err := gcppubsub.New(ctx, gcppubsub.Config{
	ProjectID: cfg.GCPProject,
	ReceiveSettings: &pubsub.ReceiveSettings{MaxOutstandingMessages: 20}, // concurrency handler
}, broker.Options{
	HandlerTimeout: 30 * time.Second,
})
if err != nil {
	return err
}
defer b.Close() // menghentikan Subscribe, menunggu handler selesai, flush publish
```

Untuk emulator lokal cukup set `PUBSUB_EMULATOR_HOST=localhost:8085`.

### Publish

```go
type WebhookService struct{ pub broker.Publisher }

func (s *WebhookService) Enqueue(ctx context.Context, job Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	_, err = s.pub.Publish(ctx, "webhook-dispatch", &broker.Message{
		Data:       data,
		Attributes: map[string]string{"type": "webhook.created"},
	})
	return err
}
```

`Publish` menunggu sampai broker menerima pesan (publish yang berjalan bersamaan tetap di-batch oleh library Pub/Sub). `msg` milik pemanggil tidak diubah.

### Subscribe (worker)

```go
err := b.Subscribe(ctx, "webhook-dispatch-sub", func(ctx context.Context, m *broker.Message) error {
	var job Job
	if err := json.Unmarshal(m.Data, &job); err != nil {
		return broker.Permanent(err) // payload rusak: ack & buang, jangan diulang
	}
	return svc.Dispatch(ctx, job) // error biasa → nack → dikirim ulang
})
```

`Subscribe` blocking sampai `ctx` dibatalkan atau `Close` dipanggil, lalu mengembalikan `nil` setelah handler yang sedang berjalan selesai. Error non-nil berarti subscription tidak bisa dikonsumsi (tidak ada, permission, dll.).

## Semantik handler

| Handler mengembalikan | Hasil | Log |
|---|---|---|
| `nil` | ack | info `broker message handled` |
| error biasa | nack → dikirim ulang | error `op failed` (sekali) + warn `broker message nacked` |
| `broker.Permanent(err)` | ack, pesan dibuang | error `broker message dropped` |
| panic | di-recover, nack | warn `broker message nacked` + `stack` |
| melewati `HandlerTimeout` | ctx handler dibatalkan; hasil sesuai return handler | – |

Delivery bersifat **at-least-once**, jadi handler wajib idempotent (mis. cek `request_id` / `Message.ID` sebelum memproses).

**Backoff retry dan dead letter diatur di sisi broker**, bukan di library: set *retry policy* (min/max backoff) dan *dead letter topic* pada subscription Pub/Sub (Terraform). `Message.DeliveryAttempt` terisi jika dead letter policy aktif.

## Propagasi trace

| Arah | Atribut pesan | Di consumer |
|---|---|---|
| `trace_id` (dari `ctxid`) | `X-Request-ID` | `trace_id` yang sama; jika tidak ada → id baru (`ctxid.Ensure`) |
| `span_id` Op pemanggil | `X-Span-ID` | `parent_span_id` dari Op `broker.consume` |

Hasilnya, satu pencarian `trace_id` menampilkan request HTTP **dan** pemrosesan di worker. Atribut yang sudah diisi pemanggil tidak ditimpa. Setiap log di dalam handler otomatis membawa `trace_id`, `span_id`, `subscription`, `message_id`, `delivery_attempt`, `messaging_system`.

```json
{"level":"info","msg":"broker publish","trace_id":"a1…","layer":"client","messaging_system":"gcp_pubsub","topic":"webhook-dispatch","payload_bytes":214,"duration_ms":18.3,"message_id":"1234"}
{"level":"info","msg":"broker message handled","trace_id":"a1…","layer":"worker","op":"broker.consume","span_id":"…","parent_span_id":"…","subscription":"webhook-dispatch-sub","message_id":"1234","delivery_attempt":1,"payload_bytes":214,"duration_ms":41.0,"lag_ms":63.9}
```

`lag_ms` = waktu dari publish sampai handler selesai, berguna untuk memantau backlog.

## Options

### `broker.Options`

| Field | Default | Keterangan |
|---|---|---|
| `System` | diisi driver | Nilai `messaging_system` di log. |
| `HandlerTimeout` | 0 (tanpa batas) | Batas waktu satu panggilan handler (lewat ctx). |
| `Middleware` | – | Membungkus setiap handler; `[0]` paling luar. Berjalan di dalam tracing & panic recovery. |
| `DisableLogging` | false | Matikan log milik broker (publish, consume, Op). |
| `DisablePropagation` | false | Jangan tulis/baca atribut `X-Request-ID` / `X-Span-ID`. |

### `gcppubsub.Config`

| Field | Keterangan |
|---|---|
| `ProjectID` | Wajib kecuali `Client` diisi. |
| `Client` | Pakai `*pubsub.Client` yang sudah ada (tidak ditutup oleh driver). |
| `ClientOptions` | Diteruskan ke `pubsub.NewClient` (credentials, endpoint). |
| `PublishSettings` / `ReceiveSettings` | Override default library Pub/Sub (batching, `MaxOutstandingMessages`, `MaxExtension`, …). |
| `EnableMessageOrdering` | Wajib `true` untuk publish dengan `OrderingKey`; subscription juga harus mengaktifkan ordering. Publish yang gagal otomatis me-*resume* ordering key-nya. |

Topic dan subscription harus sudah ada; buat lewat Terraform, bukan dari service.

## Testing dengan `memory`

```go
d := memory.New(memory.Options{MaxDeliveryAttempts: 3})
_ = d.CreateSubscription("sub", "topic")
b := broker.New(d, broker.Options{System: memory.System})

go b.Subscribe(ctx, "sub", handler)
_, _ = b.Publish(ctx, "topic", &broker.Message{Data: payload})

_ = d.WaitIdle(ctx, "sub")        // tunggu semua pesan selesai diproses
d.Published("topic")              // semua pesan yang dipublish
d.DeadLetters("sub")              // pesan yang gagal MaxDeliveryAttempts kali
```

| `memory.Options` | Default | Keterangan |
|---|---|---|
| `Concurrency` | 10 | Handler paralel per `Receive`. |
| `RedeliveryDelay` | 0 | Jeda sebelum pesan yang di-nack dikirim ulang. |
| `MaxDeliveryAttempts` | 5 | Setelah itu pesan masuk `DeadLetters`; negatif = tanpa batas. |

Driver memory meniru semantik Pub/Sub (fan-out ke semua subscription, at-least-once, `DeliveryAttempt`), tetapi **tidak** menjamin urutan `OrderingKey`.

## Menambah backend lain

Implementasikan `broker.Driver` di sub-package baru (mis. `broker/kafka`). Driver cukup mengurus transport, ack/nack, dan `Close`; tracing, logging, dan semantik handler sudah ditangani `broker.Broker`. Kontraknya ada di godoc `Driver`.
