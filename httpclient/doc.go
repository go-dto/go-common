// Package httpclient provides a high-level, production-oriented HTTP client.
// Applications use method helpers and request options without constructing a
// net/http request or managing a response body.
//
// Create one Client per upstream and reuse it:
//
//	client, err := httpclient.New(httpclient.Config{
//		Peer:    "payment-api",
//		BaseURL: paymentURL,
//		Auth:    httpclient.BearerToken(token),
//	})
//
//	response, err := client.Post(ctx, "/v1/payments",
//		httpclient.JSON(input),
//		httpclient.IdempotencyKey(requestID),
//		httpclient.Operation("payment.create"),
//	)
//	if err != nil {
//		return err
//	}
//	if err := response.JSON(&output); err != nil {
//		return err
//	}
//
// Client applies bounded timeouts, pooled connections, conservative retries,
// same-origin redirect policy, X-Request-ID propagation, response-size limits,
// typed errors and one sanitized final log per logical request. Call
// Config.ValidateProduction at startup when the upstream must use verified
// HTTPS.
package httpclient
