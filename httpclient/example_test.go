package httpclient_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/go-dto/go-common/httpclient"
)

func ExampleClient_Post() {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"pay-1"}`))
	}))
	defer server.Close()

	client := httpclient.MustNew(httpclient.Config{
		Peer:           "payment-api",
		BaseURL:        server.URL,
		DisableLogging: true,
	})
	response, err := client.Post(context.Background(), "/v1/payments",
		httpclient.JSON(map[string]int{"amount": 1000}),
		httpclient.IdempotencyKey("request-1"),
		httpclient.Operation("payment.create"),
	)
	if err != nil {
		panic(err)
	}
	var result struct {
		ID string `json:"id"`
	}
	if err := response.JSON(&result); err != nil {
		panic(err)
	}
	fmt.Println(response.StatusCode, result.ID)
	// Output: 201 pay-1
}
