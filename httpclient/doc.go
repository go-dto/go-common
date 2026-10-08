// Package httpclient builds the standard outbound *http.Client for services:
// sane timeouts, a tuned connection pool, retry with exponential backoff and
// one structured log line per attempt via logger.NewHTTPTransport.
//
// Typical usage — one client per third party, created once and reused:
//
//	webhookClient := httpclient.New(httpclient.Options{
//		Peer:    "client_webhook",
//		Timeout: 10 * time.Second,
//	})
//
//	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
//	req.Header.Set("Idempotency-Key", requestID) // makes the POST safe to retry
//	resp, err := webhookClient.Do(req)
//
// Retry rules (defaults):
//
//   - up to 2 retries (3 attempts) on transport errors and 429/500/502/503/504;
//   - only idempotent methods (GET, HEAD, OPTIONS, TRACE, PUT, DELETE), or any
//     method carrying Idempotency-Key / X-Idempotency-Key, or all methods with
//     RetryNonIdempotent;
//   - only when the body can be replayed (Request.GetBody);
//   - never after the request context is done, on certificate errors, or when
//     the next wait would not fit in the remaining Timeout;
//   - backoff 100ms, 200ms, … capped at MaxBackoff (2s) with jitter;
//     Retry-After on 429/503 is honoured up to MaxBackoff;
//   - a peer that sends no response headers within ResponseHeaderTimeout
//     (10s) fails the attempt, which is then retried.
//
// Options.OnRetry reports every retry (attempt, status or error, wait) for
// metrics. TLSClientConfig and Proxy customise the base transport (custom CAs,
// mTLS, proxies) without giving up the tuned defaults.
//
// When retries are exhausted the last response is returned as-is (the caller
// checks StatusCode as usual), or the last transport error.
package httpclient
