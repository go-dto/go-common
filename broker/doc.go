// Package broker is a backend-neutral publish/subscribe API with trace
// propagation, structured logging and safe handler semantics. Backends are
// Drivers in sub-packages:
//
//   - gcppubsub: Google Cloud Pub/Sub (production);
//   - memory: in-process, for unit tests and local development.
//
// Only the imported driver is compiled in, so services that do not use
// Pub/Sub do not carry the GCP client libraries.
//
// Typical usage — one Broker per process, created at startup:
//
//	b, err := gcppubsub.New(ctx, gcppubsub.Config{ProjectID: projectID}, broker.Options{})
//	if err != nil { ... }
//	defer b.Close()
//
//	// publish (service layer; depend on broker.Publisher for testability)
//	id, err := b.Publish(ctx, "webhook-dispatch", &broker.Message{Data: payload})
//
//	// consume (worker; blocks until ctx is cancelled)
//	err = b.Subscribe(ctx, "webhook-dispatch-sub", func(ctx context.Context, m *broker.Message) error {
//		var job Job
//		if err := json.Unmarshal(m.Data, &job); err != nil {
//			return broker.Permanent(err) // ack and drop: retrying cannot fix it
//		}
//		return svc.Dispatch(ctx, job) // error → nack → redelivered
//	})
//
// Tracing: Publish copies trace_id (X-Request-ID) and span_id from ctx into
// message attributes; the consumer's ctx gets the same trace_id and
// parent_span_id = the publisher's span, so one trace_id search returns the
// HTTP request and the background processing it triggered. Each message is
// handled inside a logger Op (layer=worker, op=broker.consume).
//
// Handler semantics:
//
//   - nil → ack; error → nack (redelivered, at-least-once: be idempotent);
//   - Permanent(err) → ack and drop, logged at error level;
//   - panic → recovered, nacked, logged with stack;
//   - Options.HandlerTimeout bounds each call through its ctx.
//
// Retry backoff and dead lettering are broker-side policies (Pub/Sub
// subscription retry policy and dead letter topic); Message.DeliveryAttempt
// reports the attempt number when the broker provides it.
package broker
