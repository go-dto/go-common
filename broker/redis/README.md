# broker/redis

Driver `broker.Driver` berbasis Redis Streams consumer groups. Driver ini
menggunakan `XADD`, `XREADGROUP`, `XACK`, dan `XAUTOCLAIM` untuk delivery
at-least-once; Redis Pub/Sub tidak digunakan karena tidak memiliki persistence,
acknowledgement, atau redelivery.

Gunakan Redis **6.2 atau lebih baru** karena reclaim bergantung pada
`XAUTOCLAIM`. CI memverifikasi Redis 8.2.2; untuk managed Redis, uji versi engine
dan command policy yang benar pada staging sebelum rollout.

## Model resource

```text
topic jobs
  -> stream <prefix>{topic:jobs}:stream
       -> consumer group order-worker
            -> consumer unik per pod
            -> DLQ <prefix>{topic:jobs}:dlq:order-worker
```

Satu stream dapat memiliki beberapa consumer group agar beberapa subscription
menerima copy message masing-masing. Consumer dalam group yang sama berbagi
pekerjaan.

## Provisioning production

Stream dan group production harus dibuat melalui IaC/operational migration.
Contoh untuk prefix `order-service:queue:`:

```text
XGROUP CREATE order-service:queue:{topic:jobs}:stream order-worker 0 MKSTREAM
```

`AutoCreateGroups` hanya untuk test/dev dan ditolak oleh
`ValidateProduction()`. Startup memverifikasi seluruh group sehingga salah
konfigurasi gagal sebelum worker menerima traffic. Startup juga menjalankan
probe `XAUTOCLAIM` non-reclaiming agar versi engine atau ACL yang tidak
mendukung command wajib gagal sebelum worker dianggap siap.

## Konfigurasi production

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
	ConsumerName: os.Getenv("POD_NAME"), // unik per process/pod
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
	MaxMessageBytes:     1 << 20,
	StreamMaxLen:        500_000,
	DeadLetterMaxLen:    50_000,
}
brokerOptions := broker.Options{
	HandlerTimeout: 30 * time.Second,
}
if err := queueConfig.ValidateProductionWithOptions(brokerOptions); err != nil {
	return err
}

messageBroker, err := redisbroker.New(ctx, queueConfig, brokerOptions)
if err != nil {
	return err
}
```

Publish dan consume tetap memakai API backend-neutral. Return `nil` untuk ack,
error biasa untuk retry, dan `broker.Permanent` hanya untuk input yang tidak
mungkin membaik saat diulang:

```go
_, err = messageBroker.Publish(ctx, "jobs", &broker.Message{
	Data: encodedJob,
	Attributes: map[string]string{
		"schema_version": "1",
	},
})
if err != nil {
	return err
}

err = messageBroker.Subscribe(workerCtx, "order-worker",
	func(ctx context.Context, message *broker.Message) error {
		var job Job
		if err := json.Unmarshal(message.Data, &job); err != nil {
			return broker.Permanent(errors.New("invalid job payload"))
		}
		return orderService.HandleJob(ctx, job)
	},
)
```

Jangan memasukkan payload atau identifier domain ke error yang dikembalikan
handler karena broker dapat mencatat `err.Error()`.

Gunakan client, credential, pool, dan deployment Redis berbeda dari cache.
Logical DB atau key prefix hanya memisahkan namespace; keduanya tidak
mengisolasi memory, eviction, CPU, blocking connection, atau kegagalan.

## Delivery, retry, dan DLQ

- Handler `nil` -> `XACK`.
- Error transient -> message tetap pending, lalu diambil ulang memakai
  `XAUTOCLAIM` setelah `ClaimMinIdle`.
- Setelah `MaxDeliveryAttempts`, message dipindah ke DLQ dan di-ack melalui Lua
  script atomik. Source dan DLQ memakai Redis Cluster hash tag yang sama.
- `broker.Permanent(err)` -> ack dan drop tanpa DLQ, sesuai kontrak broker.
- Handler wajib idempotent karena ack dapat gagal setelah side effect selesai.
- Hasil publish juga bersifat ambigu saat koneksi putus setelah Redis menerima
  `XADD`; retry publisher dapat menghasilkan duplikat, sehingga consumer tetap
  wajib idempotent atau melakukan deduplikasi berbasis ID domain yang aman.
- Ordering key ditolak. Concurrent consumer group tidak menjamin urutan selesai.

Karena `Permanent` sengaja melewati DLQ, jangan gunakan marker itu bila policy
audit mewajibkan payload invalid tetap masuk quarantine. Pada kasus tersebut,
kembalikan error aman biasa agar message mencapai `MaxDeliveryAttempts`, atau
arsipkan event melalui mekanisme quarantine service yang terpisah sebelum
mengembalikan `Permanent`. Jangan memasukkan payload ke error/log.

`ClaimMinIdle` harus lebih besar dari `broker.Options.HandlerTimeout`; jika
terlalu pendek, consumer lain dapat merebut message yang masih diproses.
`ValidateProductionWithOptions` mewajibkan `HandlerTimeout` positif dan
memverifikasi relasi tersebut sebelum koneksi dibuat. `ValidateProduction`
tetap tersedia bila hanya config Redis yang dapat divalidasi pada lapisan
config, tetapi bukan pengganti validasi gabungan pada bootstrap worker.

Kegagalan sementara `XREADGROUP` atau `XAUTOCLAIM` tidak menghentikan consumer.
Driver menandai receiver tidak sehat, melakukan retry dengan exponential
backoff berjitter dari `ReceiveRetryInitialBackoff` sampai
`ReceiveRetryMaxBackoff`, lalu memulihkan readiness setelah command berhasil.
`NOGROUP` tetap terminal karena menunjukkan provisioning yang hilang atau
salah; driver tidak membuat group diam-diam di production.

## Retention dan kapasitas

`StreamMaxLen` dan `DeadLetterMaxLen` wajib dipilih eksplisit di production.
Redis melakukan approximate `MAXLEN` trimming agar memory bounded. Nilai
`StreamMaxLen` harus lebih besar dari worst-case backlog selama outage;
trimming entry yang masih pending dapat menyebabkan payload tidak dapat
diproses. Pantau stream length, pending count, oldest pending age, DLQ length,
memory, pool saturation, dan handler latency.

`MAXLEN` adalah batas jumlah entry, bukan retention berbasis waktu. Turunkan
target retention menjadi kapasitas minimum dengan rumus konservatif `peak
messages/second x detik outage yang ditoleransi x safety factor`, lalu validasi
terhadap ukuran payload dan kapasitas memory. Jangan mengandalkan trimming
sebagai pengganti lifecycle DLQ atau kebijakan penghapusan data sensitif.

## Kontrak deployment Redis queue

Redis queue tidak boleh memakai kebijakan eviction cache. Untuk production:

- gunakan instance/cluster khusus queue dengan `maxmemory-policy noeviction`;
- aktifkan persistence yang sesuai RPO, umumnya AOF dengan kebijakan fsync yang
  disetujui, dan kombinasikan dengan snapshot/backup bila diperlukan;
- gunakan replication/managed HA, uji failover, restart, restore backup, dan
  recovery backlog sebelum go-live;
- gunakan verified TLS, credential yang dirotasi, network policy, dan ACL
  least-privilege yang dibatasi ke prefix queue;
- provision stream/group lewat IaC dan alarm bila group hilang;
- monitor memory, rejected writes, replication lag, persistence error, pool
  timeout, receive retry, pending count/age, reclaim, serta DLQ.

Command runtime minimum adalah `PING`, `XADD`, `XREADGROUP`, `XACK`,
`XAUTOCLAIM`, `XPENDING`, `XINFO GROUPS`, serta `EVALSHA`/`EVAL` untuk pemindahan
DLQ atomik. `XGROUP` hanya dibutuhkan oleh job provisioning; service production
dengan `AutoCreateGroups=false` tidak membutuhkannya. Validasi ACL terhadap
versi Redis yang benar karena kategori command dapat berubah.

## Shutdown dan readiness

```go
driver, err := redisbroker.NewDriver(ctx, queueConfig)
if err != nil {
	return err
}
messageBroker := broker.New(driver, broker.Options{HandlerTimeout: 30 * time.Second})

ready := database.HealthHandler(2*time.Second,
	redisbroker.ConsumerCheck("redis_queue", driver),
)

shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
_ = messageBroker.CloseContext(shutdownCtx)
```

Close membatalkan blocking read, menunggu handler aktif, lalu menutup client
yang dibuat package. Client yang diinjeksi untuk test/dev tetap dimiliki caller.
Gunakan `Check` untuk process publisher-only. Gunakan `ConsumerCheck` untuk
worker; check ini memverifikasi `PING`, keberadaan seluruh consumer group,
status error receiver, dan satu loop `Receive` aktif untuk setiap subscription.
Keduanya adalah readiness, bukan liveness.

## Test/dev

`AutoCreateGroups: true` dapat digunakan dengan Redis lokal. Integration test:

```bash
REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 ./broker/redis
```

Integration test juga menerima `REDIS_TEST_PASSWORD`, `REDIS_TEST_TLS_CA`, dan
`REDIS_TEST_TLS_SERVER_NAME` untuk memverifikasi autentikasi dan certificate
TLS tanpa menonaktifkan verification.
