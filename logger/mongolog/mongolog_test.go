package mongolog

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/go-dto/go-common/logger"
	"github.com/go-dto/go-common/logger/ctxid"
)

func TestMonitor(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	defer logger.ReplaceForTest(zap.New(core))()

	m := NewMonitor(Options{})
	ctx := ctxid.WithContext(context.Background(), "trace-m")
	ctx, op := logger.StartOp(ctx, logger.LayerRepository, "webhook_send.Insert")

	cmd, _ := bson.Marshal(bson.D{{Key: "insert", Value: "webhook_sends"}, {Key: "documents", Value: bson.A{bson.D{{Key: "password", Value: "x"}}}}})
	m.Started(ctx, &event.CommandStartedEvent{Command: cmd, CommandName: "insert", DatabaseName: "db", RequestID: 1, ConnectionID: "c1"})
	m.Succeeded(ctx, &event.CommandSucceededEvent{CommandFinishedEvent: event.CommandFinishedEvent{
		CommandName: "insert", DatabaseName: "db", RequestID: 1, ConnectionID: "c1", Duration: 3 * time.Millisecond}})

	m.Started(ctx, &event.CommandStartedEvent{Command: cmd, CommandName: "insert", DatabaseName: "db", RequestID: 2, ConnectionID: "c1"})
	m.Failed(ctx, &event.CommandFailedEvent{CommandFinishedEvent: event.CommandFinishedEvent{
		CommandName: "insert", DatabaseName: "db", RequestID: 2, ConnectionID: "c1"}, Failure: "duplicate key"})

	m.Started(ctx, &event.CommandStartedEvent{CommandName: "hello", RequestID: 3, ConnectionID: "c1"})
	m.Succeeded(ctx, &event.CommandSucceededEvent{CommandFinishedEvent: event.CommandFinishedEvent{CommandName: "hello", RequestID: 3, ConnectionID: "c1"}})
	op.End(nil)

	ok := logs.FilterMessage("mongo command").All()
	if len(ok) != 1 {
		t.Fatalf("want 1 success entry, got %d", len(ok))
	}
	cm := ok[0].ContextMap()
	if cm["db_collection"] != "webhook_sends" || cm[logger.KeyTraceID] != "trace-m" || cm[logger.KeyOperation] != "webhook_send.Insert" {
		t.Fatalf("bad fields: %v", cm)
	}
	for _, e := range logs.All() {
		for k, v := range e.ContextMap() {
			if s, ok := v.(string); ok && s == "x" {
				t.Fatalf("command body leaked via %s", k)
			}
		}
	}
	if logs.FilterMessage("mongo command failed").FilterLevelExact(zapcore.WarnLevel).Len() != 1 {
		t.Fatal("want warn for failed command")
	}
	if logs.FilterField(zap.String("db_command", "hello")).Len() != 0 {
		t.Fatal("hello must be ignored")
	}
}
