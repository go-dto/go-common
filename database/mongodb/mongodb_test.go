package mongodb

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"

	"github.com/go-dto/go-common/database"
	"github.com/go-dto/go-common/logger"
)

func TestClientOptionsDefaults(t *testing.T) {
	opts, err := clientOptions(Config{URI: "mongodb://svc:pw@db1:27017,db2:27017/?replicaSet=rs0", AppName: "webhook"})
	if err != nil {
		t.Fatal(err)
	}
	if *opts.AppName != "webhook" || *opts.MaxPoolSize != DefaultMaxPoolSize || *opts.MinPoolSize != 0 {
		t.Errorf("app=%s pool=%d/%d", *opts.AppName, *opts.MaxPoolSize, *opts.MinPoolSize)
	}
	if *opts.ConnectTimeout != DefaultConnectTimeout || *opts.ServerSelectionTimeout != DefaultServerSelectionTimeout ||
		*opts.MaxConnIdleTime != DefaultMaxConnIdleTime {
		t.Errorf("timeouts = %v/%v/%v", *opts.ConnectTimeout, *opts.ServerSelectionTimeout, *opts.MaxConnIdleTime)
	}
	if opts.Timeout != nil {
		t.Errorf("CSOT should be unset by default, got %v", *opts.Timeout)
	}
	if opts.Monitor == nil {
		t.Error("command monitor should be installed")
	}
	if len(opts.Hosts) != 2 {
		t.Errorf("hosts = %v", opts.Hosts)
	}
}

func TestClientOptionsOverrides(t *testing.T) {
	opts, err := clientOptions(Config{
		URI:               "mongodb://db:27017/?maxPoolSize=500&connectTimeoutMS=60000",
		MaxPoolSize:       10,
		MinPoolSize:       2,
		Timeout:           3 * time.Second,
		MaxConnIdleTime:   -1,
		DisableCommandLog: true,
		Configure: func(o *options.ClientOptions) error {
			o.SetReadPreference(readpref.SecondaryPreferred())
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Config wins over the URI.
	if *opts.MaxPoolSize != 10 || *opts.MinPoolSize != 2 || *opts.ConnectTimeout != DefaultConnectTimeout {
		t.Errorf("pool=%d/%d connect=%v", *opts.MaxPoolSize, *opts.MinPoolSize, *opts.ConnectTimeout)
	}
	if *opts.Timeout != 3*time.Second || *opts.MaxConnIdleTime != 0 {
		t.Errorf("timeout=%v idle=%v", *opts.Timeout, *opts.MaxConnIdleTime)
	}
	if opts.Monitor != nil {
		t.Error("DisableCommandLog should remove the monitor")
	}
	if opts.ReadPreference.Mode() != readpref.SecondaryPreferredMode {
		t.Error("Configure not applied")
	}
}

func TestClientOptionsErrors(t *testing.T) {
	if _, err := clientOptions(Config{}); err == nil {
		t.Error("missing URI should fail")
	}
	for _, uri := range []string{
		"mongodb://svc:s3cret@db1,db2/?directConnection=true", // conflicting options
		"mongodb://svc:s3cret@db:notaport",                    // unparseable
	} {
		_, err := clientOptions(Config{URI: uri})
		if err == nil {
			t.Errorf("%s: should fail", uri)
			continue
		}
		if strings.Contains(err.Error(), "s3cret") {
			t.Errorf("error leaks password: %v", err)
		}
	}
	boom := errors.New("boom")
	if _, err := clientOptions(Config{URI: "mongodb://db", Configure: func(*options.ClientOptions) error { return boom }}); !errors.Is(err, boom) {
		t.Errorf("Configure error = %v", err)
	}
	if _, err := clientOptions(Config{URI: "mongodb://db", MinPoolSize: 11, MaxPoolSize: 10}); err == nil {
		t.Error("final minPoolSize > maxPoolSize must fail validation")
	}
	if _, err := clientOptions(Config{
		URI: "mongodb://db",
		Configure: func(o *options.ClientOptions) error {
			o.SetMinPoolSize(20)
			o.SetMaxPoolSize(10)
			return nil
		},
	}); err == nil {
		t.Error("invalid Configure result must fail final validation")
	}
}

func TestValidateProduction(t *testing.T) {
	if err := (Config{URI: "mongodb://db.internal:27017"}).ValidateProduction(); err == nil {
		t.Fatal("plaintext production config should fail")
	}
	if err := (Config{
		URI: "mongodb://db.internal:27017",
		Configure: func(o *options.ClientOptions) error {
			o.SetTLSConfig(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec -- validation test
			return nil
		},
	}).ValidateProduction(); err == nil {
		t.Fatal("unverified production TLS should fail")
	}
	if err := (Config{URI: "mongodb://db.internal:27017/?tls=true"}).ValidateProduction(); err != nil {
		t.Fatalf("verified production TLS: %v", err)
	}
}

func TestConnectUnreachable(t *testing.T) {
	t.Cleanup(logger.ReplaceForTest(zap.NewNop()))
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	start := time.Now()
	_, err = Connect(context.Background(), Config{
		URI:                    "mongodb://svc:s3cret@" + addr + "/?directConnection=true",
		ServerSelectionTimeout: 200 * time.Millisecond,
		Retry:                  database.Retry{Attempts: 2, InitialBackoff: time.Millisecond},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Errorf("error leaks password: %v", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("took %v; ServerSelectionTimeout should make it fail fast", d)
	}
}

func TestSkipPing(t *testing.T) {
	client, err := Connect(context.Background(), Config{URI: "mongodb://127.0.0.1:1", SkipPing: true})
	if err != nil {
		t.Fatal(err)
	}
	_ = client.Disconnect(context.Background())
}

// TestIntegration runs against a real server when MONGODB_TEST_URI is set, e.g.
// docker run -p 27017:27017 mongo:8 and MONGODB_TEST_URI='mongodb://127.0.0.1:27017'.
func TestIntegration(t *testing.T) {
	uri := os.Getenv("MONGODB_TEST_URI")
	if uri == "" {
		t.Skip("MONGODB_TEST_URI not set")
	}
	ctx := context.Background()
	client, err := Connect(ctx, Config{URI: uri, AppName: "go-common-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(ctx)

	coll := client.Database("go_common_test").Collection("items")
	defer coll.Drop(ctx)
	if _, err := coll.InsertOne(ctx, bson.M{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	if err := Check("mongo", client).Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
