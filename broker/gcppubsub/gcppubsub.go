// Package gcppubsub is the Google Cloud Pub/Sub broker.Driver, built on
// cloud.google.com/go/pubsub/v2. Importing it is what pulls the GCP client
// libraries into a build; the core broker package does not depend on them.
//
//	b, err := gcppubsub.New(ctx, gcppubsub.Config{ProjectID: "example-project"}, broker.Options{})
//	if err != nil { ... }
//	defer b.Close()
//
// Topics and subscriptions are referenced by ID ("webhook") or full name
// ("projects/p/topics/webhook") and must already exist: provision them (and
// their retry / dead letter policies) with Terraform, not from the service.
package gcppubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub/v2"
	"google.golang.org/api/option"

	"github.com/go-dto/go-common/broker"
)

// System is the messaging_system value for this driver (OpenTelemetry name).
const System = "gcp_pubsub"

// Config configures NewDriver.
type Config struct {
	// ProjectID is required unless Client is set.
	ProjectID string

	// Client, if set, is used instead of creating one; the driver does not
	// close it. Use it to share a client with admin code.
	Client *pubsub.Client

	// ClientOptions are passed to pubsub.NewClient (credentials, endpoint, …).
	// The PUBSUB_EMULATOR_HOST environment variable is honoured by the client.
	ClientOptions []option.ClientOption

	// PublishSettings overrides pubsub.DefaultPublishSettings for every topic.
	PublishSettings *pubsub.PublishSettings

	// ReceiveSettings overrides pubsub.DefaultReceiveSettings for every
	// subscription (concurrency via MaxOutstandingMessages, MaxExtension, …).
	ReceiveSettings *pubsub.ReceiveSettings

	// EnableMessageOrdering must be true to publish messages with an
	// OrderingKey; the subscription must have message ordering enabled too.
	EnableMessageOrdering bool
}

// Driver implements broker.Driver. It is safe for concurrent use.
type Driver struct {
	client     *pubsub.Client
	ownsClient bool
	cfg        Config

	ctx    context.Context // cancelled by Close
	cancel context.CancelFunc
	wg     sync.WaitGroup // active Receive calls

	mu     sync.Mutex
	closed bool
	pubs   map[string]*pubsub.Publisher
}

var _ broker.Driver = (*Driver)(nil)

// NewDriver connects to Pub/Sub.
func NewDriver(ctx context.Context, cfg Config) (*Driver, error) {
	client, owns := cfg.Client, false
	if client == nil {
		if cfg.ProjectID == "" {
			return nil, errors.New("gcppubsub: ProjectID is required")
		}
		var err error
		client, err = pubsub.NewClient(ctx, cfg.ProjectID, cfg.ClientOptions...)
		if err != nil {
			return nil, fmt.Errorf("gcppubsub: new client: %w", err)
		}
		owns = true
	}
	dctx, cancel := context.WithCancel(context.Background())
	return &Driver{
		client:     client,
		ownsClient: owns,
		cfg:        cfg,
		ctx:        dctx,
		cancel:     cancel,
		pubs:       make(map[string]*pubsub.Publisher),
	}, nil
}

// New is NewDriver wrapped in a broker.Broker (System defaults to "gcp_pubsub").
func New(ctx context.Context, cfg Config, opts broker.Options) (*broker.Broker, error) {
	d, err := NewDriver(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if opts.System == "" {
		opts.System = System
	}
	return broker.New(d, opts), nil
}

// Client returns the underlying Pub/Sub client.
func (d *Driver) Client() *pubsub.Client { return d.client }

// Publish implements broker.Driver. It waits for the server ack; concurrent
// publishes to the same topic are still batched by the client library.
//
// With message ordering, a failed publish pauses its ordering key; the key is
// resumed before returning the error, so the caller decides whether to retry.
func (d *Driver) Publish(ctx context.Context, topic string, msg *broker.Message) (string, error) {
	p, err := d.publisher(topic)
	if err != nil {
		return "", err
	}
	res := p.Publish(ctx, &pubsub.Message{
		Data:        msg.Data,
		Attributes:  msg.Attributes,
		OrderingKey: msg.OrderingKey,
	})
	id, err := res.Get(ctx)
	if err != nil {
		if msg.OrderingKey != "" && d.cfg.EnableMessageOrdering {
			p.ResumePublish(msg.OrderingKey)
		}
		if errors.Is(err, pubsub.ErrPublisherStopped) {
			return "", fmt.Errorf("%w: %w", broker.ErrClosed, err)
		}
		return "", err
	}
	return id, nil
}

func (d *Driver) publisher(topic string) (*pubsub.Publisher, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, broker.ErrClosed
	}
	if p, ok := d.pubs[topic]; ok {
		return p, nil
	}
	p := d.client.Publisher(topic)
	if d.cfg.PublishSettings != nil {
		p.PublishSettings = *d.cfg.PublishSettings
	}
	p.EnableMessageOrdering = d.cfg.EnableMessageOrdering
	d.pubs[topic] = p
	return p, nil
}

// Receive implements broker.Driver.
func (d *Driver) Receive(ctx context.Context, subscription string, h broker.Handler) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return broker.ErrClosed
	}
	d.wg.Add(1)
	d.mu.Unlock()
	defer d.wg.Done()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(d.ctx, cancel)()

	s := d.client.Subscriber(subscription)
	if d.cfg.ReceiveSettings != nil {
		s.ReceiveSettings = *d.cfg.ReceiveSettings
	}
	return s.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		if err := h(ctx, fromPubsub(m)); err != nil {
			m.Nack()
			return
		}
		m.Ack()
	})
}

// Close stops active Receive calls (waiting for in-flight handlers), flushes
// pending publishes, and closes the client if the driver created it.
func (d *Driver) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	pubs := d.pubs
	d.pubs = nil
	d.mu.Unlock()

	d.cancel()
	d.wg.Wait()
	for _, p := range pubs {
		p.Stop()
	}
	if d.ownsClient {
		return d.client.Close()
	}
	return nil
}

func fromPubsub(m *pubsub.Message) *broker.Message {
	out := &broker.Message{
		ID:          m.ID,
		Data:        m.Data,
		Attributes:  m.Attributes,
		OrderingKey: m.OrderingKey,
		PublishTime: m.PublishTime,
	}
	if m.DeliveryAttempt != nil {
		out.DeliveryAttempt = *m.DeliveryAttempt
	}
	return out
}
