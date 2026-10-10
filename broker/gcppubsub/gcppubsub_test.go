package gcppubsub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/go-dto/go-common/broker"
	"github.com/go-dto/go-common/logger"
	"github.com/go-dto/go-common/logger/ctxid"
)

const project = "test-project"

// fakeClient starts a pstest server with topic "topic" and subscription
// "sub" and returns a client connected to it.
func fakeClient(t *testing.T, ordering bool) *pubsub.Client {
	t.Helper()
	srv := pstest.NewServer()
	t.Cleanup(func() { _ = srv.Close() })
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	topic := "projects/" + project + "/topics/topic"
	if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:                  "projects/" + project + "/subscriptions/sub",
		Topic:                 topic,
		AckDeadlineSeconds:    10,
		EnableMessageOrdering: ordering,
	}); err != nil {
		t.Fatal(err)
	}
	return client
}

func newBroker(t *testing.T, cfg Config) *broker.Broker {
	t.Helper()
	b, err := New(context.Background(), cfg, broker.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// subscribe runs b.Subscribe in the background until the test ends.
func subscribe(t *testing.T, b *broker.Broker, h broker.Handler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Subscribe(ctx, "sub", h) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Subscribe: %v", err)
		}
	})
}

func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for message")
		panic("unreachable")
	}
}

func TestPublishReceiveRoundTrip(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	t.Cleanup(logger.ReplaceForTest(zap.New(core)))

	b := newBroker(t, Config{Client: fakeClient(t, false)})
	type got struct {
		msg     broker.Message
		traceID string
	}
	ch := make(chan got, 1)
	subscribe(t, b, func(ctx context.Context, m *broker.Message) error {
		id, _ := ctxid.FromContext(ctx)
		ch <- got{*m, id}
		return nil
	})

	ctx := ctxid.WithContext(context.Background(), "trace-1")
	id, err := b.Publish(ctx, "topic", &broker.Message{Data: []byte(`{"a":1}`), Attributes: map[string]string{"type": "created"}})
	if err != nil {
		t.Fatal(err)
	}

	g := recv(t, ch)
	if g.msg.ID != id || string(g.msg.Data) != `{"a":1}` || g.msg.Attributes["type"] != "created" {
		t.Errorf("received %+v, published id %s", g.msg, id)
	}
	if g.traceID != "trace-1" || g.msg.PublishTime.IsZero() {
		t.Errorf("trace_id = %q, publish time = %v", g.traceID, g.msg.PublishTime)
	}
	if lines := logs.FilterMessage("broker publish").All(); len(lines) != 1 || lines[0].ContextMap()[broker.KeySystem] != System {
		t.Errorf("publish lines = %v", lines)
	}
}

func TestNackRedelivers(t *testing.T) {
	t.Cleanup(logger.ReplaceForTest(zap.NewNop()))
	b := newBroker(t, Config{Client: fakeClient(t, false)})
	var calls atomic.Int32
	done := make(chan struct{})
	subscribe(t, b, func(ctx context.Context, m *broker.Message) error {
		if calls.Add(1) == 1 {
			return errors.New("try again")
		}
		close(done)
		return nil
	})
	if _, err := b.Publish(context.Background(), "topic", &broker.Message{Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	recv(t, done)
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}
}

func TestOrdering(t *testing.T) {
	t.Cleanup(logger.ReplaceForTest(zap.NewNop()))
	client := fakeClient(t, true)

	// Without EnableMessageOrdering, an ordering key is rejected.
	plain := newBroker(t, Config{Client: client})
	if _, err := plain.Publish(context.Background(), "topic", &broker.Message{OrderingKey: "k"}); err == nil {
		t.Fatal("ordering key without EnableMessageOrdering should fail")
	}

	b := newBroker(t, Config{Client: client, EnableMessageOrdering: true})
	var mu sync.Mutex
	var order []string
	done := make(chan struct{})
	subscribe(t, b, func(ctx context.Context, m *broker.Message) error {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, string(m.Data))
		if len(order) == 5 {
			close(done)
		}
		return nil
	})
	for _, d := range []string{"1", "2", "3", "4", "5"} {
		if _, err := b.Publish(context.Background(), "topic", &broker.Message{Data: []byte(d), OrderingKey: "patient-1"}); err != nil {
			t.Fatal(err)
		}
	}
	recv(t, done)
	mu.Lock()
	defer mu.Unlock()
	for i, d := range []string{"1", "2", "3", "4", "5"} {
		if order[i] != d {
			t.Fatalf("order = %v", order)
		}
	}
}

func TestCloseStopsReceiveAndPublish(t *testing.T) {
	t.Cleanup(logger.ReplaceForTest(zap.NewNop()))
	d, err := NewDriver(context.Background(), Config{Client: fakeClient(t, false)})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- d.Receive(context.Background(), "sub", func(context.Context, *broker.Message) error { return nil })
	}()
	time.Sleep(50 * time.Millisecond)

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, done); err != nil && !errors.Is(err, broker.ErrClosed) {
		t.Fatalf("Receive = %v", err)
	}
	if _, err := d.Publish(context.Background(), "topic", &broker.Message{}); !errors.Is(err, broker.ErrClosed) {
		t.Errorf("Publish after Close = %v", err)
	}
	if err := d.Receive(context.Background(), "sub", nil); !errors.Is(err, broker.ErrClosed) {
		t.Errorf("Receive after Close = %v", err)
	}
	if err := d.Close(); err != nil {
		t.Errorf("second Close = %v", err)
	}
	// The client was supplied by the caller, so it must still work.
	if _, err := d.Client().Publisher("topic").Publish(context.Background(), &pubsub.Message{Data: []byte("x")}).Get(context.Background()); err != nil {
		t.Errorf("caller's client closed by driver: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	if _, err := NewDriver(context.Background(), Config{}); err == nil {
		t.Fatal("missing ProjectID should fail")
	}
}

func TestValidateProduction(t *testing.T) {
	t.Setenv("PUBSUB_EMULATOR_HOST", "")
	if err := (Config{ProjectID: "prod-project"}).ValidateProduction(); err != nil {
		t.Fatalf("valid production config: %v", err)
	}
	for name, cfg := range map[string]Config{
		"missing project": {},
		"injected client": {ProjectID: "prod-project", Client: &pubsub.Client{}},
		"client options":  {ProjectID: "prod-project", ClientOptions: []option.ClientOption{option.WithoutAuthentication()}},
	} {
		if err := cfg.ValidateProduction(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	t.Setenv("PUBSUB_EMULATOR_HOST", "127.0.0.1:8085")
	if err := (Config{ProjectID: "prod-project"}).ValidateProduction(); err == nil {
		t.Fatal("production must reject the emulator")
	}
}

func TestFromPubsub(t *testing.T) {
	n := 3
	now := time.Now()
	m := fromPubsub(&pubsub.Message{ID: "1", Data: []byte("x"), Attributes: map[string]string{"a": "b"},
		OrderingKey: "k", PublishTime: now, DeliveryAttempt: &n})
	if m.ID != "1" || m.DeliveryAttempt != 3 || m.OrderingKey != "k" || !m.PublishTime.Equal(now) || m.Attributes["a"] != "b" {
		t.Fatalf("converted = %+v", m)
	}
	if fromPubsub(&pubsub.Message{}).DeliveryAttempt != 0 {
		t.Error("nil DeliveryAttempt should become 0")
	}
}
