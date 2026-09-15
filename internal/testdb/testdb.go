// Package testdb provisions only uniquely named, disposable integration databases.
package testdb

import (
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"strings"
	"testing"
)

func New(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("STATION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("STATION_TEST_DATABASE_URL is required for PostgreSQL integration")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("test database configuration failed")
	}
	name := "station_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal("create disposable database:", err)
	}
	cfg := admin.Config()
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize())
		admin.Close()
		if err != nil {
			t.Error("drop disposable database:", err)
		}
	})
	return pool
}
