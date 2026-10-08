// Package logger is the production logging library for this service, built on zap.
//
// Features
//
//   - Thread-safe global logger (atomic swap), usable before Init with safe defaults.
//   - Config from code or environment (ConfigFromEnv), validated up front.
//   - JSON (production) or console (local dev) encoding, RFC3339 timestamps.
//   - Static fields service / env / version on every line.
//   - Runtime level changes: SetLevel or the LevelHandler HTTP endpoint.
//   - No sampling by default, so audit logs are never silently dropped.
//   - Context propagation: trace_id from ctxid plus fields added with WithFields.
//   - Redaction helpers for JSON/form bodies, URLs and headers.
//   - Optional rotating file output (size / daily / max backups / max age), no extra deps.
//   - Standard library "log" redirected into the structured logger.
//   - Cross-layer tracing: StartOp/Op adds layer, op and duration_ms per
//     function and reports each error at error level once.
//   - Outbound HTTP: WrapHTTPClient propagates X-Request-ID and logs every call.
//   - MongoDB: mongolog.NewMonitor logs commands with the caller's context.
//
// Typical usage
//
//	func main() {
//		cfg, err := logger.ConfigFromEnv()
//		if err != nil { panic(err) }
//		logger.MustInit(cfg)
//		defer logger.Close()
//		...
//	}
//
//	// in a handler/service: attach business fields once ...
//	ctx = logger.WithFields(ctx, zap.String(logger.KeyRequestID, reqID), zap.String("org_id", orgID))
//	// ... and every layer below gets them automatically
//	logger.FromContext(ctx).Info("webhook sent", zap.Int("status_code", code))
//
// Field naming: trace_id is the transport correlation id (X-Request-ID);
// request_id is the business id. Never log both under the same key.
package logger
