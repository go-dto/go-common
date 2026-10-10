// Package database holds what every database connection in a service needs,
// independent of the driver: startup readiness with retry (WaitReady) and a
// readiness HTTP handler (HealthHandler). Drivers live in sub-packages and
// only the imported one is compiled in:
//
//   - mysql: *sql.DB via github.com/go-sql-driver/mysql;
//   - postgres: *pgxpool.Pool via github.com/jackc/pgx/v5, with a query tracer;
//   - mongodb: *mongo.Client via go.mongodb.org/mongo-driver, with logger/mongolog.
//
// Each Open/Connect applies production defaults (timeouts, bounded pool,
// connection recycling), pings with retry so a service started next to its
// database does not crash-loop, and never puts credentials, query arguments,
// or SQL statements in logs by default.
//
//	db, err := mysql.Open(ctx, mysql.Config{Host: h, User: u, Password: pw, Database: "webhook"})
//	if err != nil { ... }
//	defer db.Close()
//
//	mux.Handle("/readyz", database.HealthHandler(0, database.SQLCheck("mysql", db)))
package database
