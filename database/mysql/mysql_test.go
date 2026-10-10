package mysql

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	"github.com/go-dto/go-common/database"
	"github.com/go-dto/go-common/logger"
)

const secret = "p@ss:w/rd?#"

func TestDriverConfigFromFields(t *testing.T) {
	dc, err := driverConfig(Config{
		Host: "db.internal", User: "svc", Password: secret, Database: "webhook",
		Params: map[string]string{"time_zone": "'+07:00'"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if dc.Addr != "db.internal:3306" || dc.Net != "tcp" || dc.User != "svc" || dc.Passwd != secret || dc.DBName != "webhook" {
		t.Errorf("connection fields = %+v", dc)
	}
	if !dc.ParseTime || dc.Loc != time.UTC {
		t.Errorf("ParseTime=%v Loc=%v, want true/UTC", dc.ParseTime, dc.Loc)
	}
	if dc.Timeout != DefaultDialTimeout || dc.ReadTimeout != DefaultReadTimeout || dc.WriteTimeout != DefaultWriteTimeout {
		t.Errorf("timeouts = %v/%v/%v", dc.Timeout, dc.ReadTimeout, dc.WriteTimeout)
	}
	if _, ok := dc.Logger.(driverLogger); !ok {
		t.Errorf("driver logger = %T, want driverLogger", dc.Logger)
	}

	// The password must survive DSN formatting/parsing with special characters.
	back, err := driver.ParseDSN(dc.FormatDSN())
	if err != nil || back.Passwd != secret || back.Params["time_zone"] != "'+07:00'" {
		t.Errorf("round trip: err=%v passwd=%q params=%v", err, back.Passwd, back.Params)
	}
}

func TestDriverConfigFromDSN(t *testing.T) {
	// readTimeout comes from the DSN, writeTimeout is explicit, timeout is defaulted.
	dc, err := driverConfig(Config{
		DSN:          "svc:pw@tcp(10.0.0.5:3307)/app?readTimeout=7s",
		WriteTimeout: 9 * time.Second,
		Host:         "ignored",
	})
	if err != nil {
		t.Fatal(err)
	}
	if dc.Addr != "10.0.0.5:3307" || dc.DBName != "app" {
		t.Errorf("addr=%s db=%s", dc.Addr, dc.DBName)
	}
	if dc.ReadTimeout != 7*time.Second || dc.WriteTimeout != 9*time.Second || dc.Timeout != DefaultDialTimeout {
		t.Errorf("timeouts = %v/%v/%v", dc.Timeout, dc.ReadTimeout, dc.WriteTimeout)
	}
	if !dc.ParseTime {
		t.Error("ParseTime must be forced on")
	}
}

func TestDriverConfigErrors(t *testing.T) {
	if _, err := driverConfig(Config{}); err == nil {
		t.Error("missing Host and DSN should fail")
	}
	if _, err := driverConfig(Config{DSN: "not a dsn"}); err == nil {
		t.Error("invalid DSN should fail")
	}
	boom := errors.New("boom")
	if _, err := driverConfig(Config{Host: "h", Configure: func(*driver.Config) error { return boom }}); !errors.Is(err, boom) {
		t.Errorf("Configure error = %v", err)
	}
	dc, _ := driverConfig(Config{Host: "h", Configure: func(c *driver.Config) error { c.InterpolateParams = true; return nil }})
	if !dc.InterpolateParams {
		t.Error("Configure change not applied")
	}
}

func TestValidateProduction(t *testing.T) {
	if err := (Config{Host: "db.internal"}).ValidateProduction(); err == nil {
		t.Fatal("plaintext production config should fail")
	}
	if err := (Config{Host: "db.internal", TLS: &tls.Config{InsecureSkipVerify: true}}).ValidateProduction(); err == nil { //nolint:gosec -- validation test
		t.Fatal("unverified production TLS should fail")
	}
	if err := (Config{Host: "db.internal", TLS: &tls.Config{ServerName: "db.internal", MinVersion: tls.VersionTLS12}}).ValidateProduction(); err != nil {
		t.Fatalf("verified production TLS: %v", err)
	}
	if err := (Config{DSN: "u:p@unix(/cloudsql/project:region:instance)/db"}).ValidateProduction(); err != nil {
		t.Fatalf("trusted unix proxy: %v", err)
	}
}

func TestPoolDefaults(t *testing.T) {
	db, err := Open(context.Background(), Config{Host: "127.0.0.1", SkipPing: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if n := db.Stats().MaxOpenConnections; n != DefaultMaxOpenConns {
		t.Errorf("MaxOpenConnections = %d", n)
	}

	db2, _ := Open(context.Background(), Config{Host: "127.0.0.1", SkipPing: true, MaxOpenConns: 7})
	defer db2.Close()
	if n := db2.Stats().MaxOpenConnections; n != 7 {
		t.Errorf("MaxOpenConnections = %d, want 7", n)
	}
}

func TestOpenUnreachable(t *testing.T) {
	t.Cleanup(logger.ReplaceForTest(zap.NewNop()))
	addr := closedPort(t)
	host, port, _ := net.SplitHostPort(addr)
	p, _ := strconv.Atoi(port)
	_, err := Open(context.Background(), Config{
		Host: host, Port: p, User: "svc", Password: secret,
		Retry: database.Retry{Attempts: 2, InitialBackoff: time.Millisecond},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("error leaks password: %v", err)
	}
}

func TestDriverLogger(t *testing.T) {
	// Must not panic and must go through the structured logger.
	t.Cleanup(logger.ReplaceForTest(zap.NewNop()))
	driverLogger{}.Print("packets.go: ", "unexpected EOF")
}

// closedPort returns an address nothing listens on.
func closedPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// TestIntegration runs against a real server when MYSQL_TEST_DSN is set, e.g.
// docker run -e MYSQL_ROOT_PASSWORD=pw -p 3306:3306 mysql:8 and
// MYSQL_TEST_DSN='root:pw@tcp(127.0.0.1:3306)/mysql'.
func TestIntegration(t *testing.T) {
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN not set")
	}
	ctx := context.Background()
	db, err := Open(ctx, Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var now time.Time
	if err := db.QueryRowContext(ctx, "SELECT NOW()").Scan(&now); err != nil {
		t.Fatalf("parseTime should allow scanning into time.Time: %v", err)
	}
	if err := database.SQLCheck("mysql", db).Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
