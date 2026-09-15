// Package migrations owns the executable Station database schema.
package migrations

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var files embed.FS

const ContractsMajor = 1
const Version = 2

type migration struct {
	version             int
	name, sql, checksum string
}

func load() ([]migration, error) {
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		return nil, err
	}
	out := make([]migration, 0, len(names))
	for i, name := range names {
		version, err := strconv.Atoi(strings.SplitN(name, "_", 2)[0])
		if err != nil || version != i+1 {
			return nil, errors.New("nonsequential migration")
		}
		data, err := files.ReadFile(name)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(data)
		out = append(out, migration{version, name, string(data), hex.EncodeToString(sum[:])})
	}
	if len(out) != Version {
		return nil, errors.New("migration version mismatch")
	}
	return out, nil
}
func history(ctx context.Context, tx pgx.Tx, all []migration) (int, error) {
	rows, err := tx.Query(ctx, "SELECT version, name, checksum FROM station.schema_migrations ORDER BY version")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var v int
		var name, checksum string
		if err := rows.Scan(&v, &name, &checksum); err != nil {
			return 0, err
		}
		if n >= len(all) || v != all[n].version || name != all[n].name || checksum != all[n].checksum {
			return 0, errors.New("migration history mismatch")
		}
		n++
	}
	return n, rows.Err()
}
func checkStation(ctx context.Context, tx pgx.Tx, stationID string) error {
	var id string
	var major int
	if err := tx.QueryRow(ctx, "SELECT station_id::text, contracts_major FROM station.configuration WHERE singleton").Scan(&id, &major); err != nil {
		return err
	}
	if id != stationID || major != ContractsMajor {
		return errors.New("station identity or contracts version mismatch")
	}
	return nil
}
func Apply(ctx context.Context, pool *pgxpool.Pool, stationID string) error {
	all, err := load()
	if err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(2026091501)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS station;
 CREATE TABLE IF NOT EXISTS station.schema_migrations(version integer PRIMARY KEY, name text NOT NULL, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now());`); err != nil {
		return err
	}
	n, err := history(ctx, tx, all)
	if err != nil {
		return err
	}
	for _, m := range all[n:] {
		if _, err = tx.Exec(ctx, m.sql); err != nil {
			return fmt.Errorf("migration %d failed: %w", m.version, err)
		}
		if _, err = tx.Exec(ctx, "INSERT INTO station.schema_migrations(version,name,checksum) VALUES($1,$2,$3)", m.version, m.name, m.checksum); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, "INSERT INTO station.configuration(singleton,station_id,contracts_major) VALUES(true,$1,$2) ON CONFLICT(singleton) DO NOTHING", stationID, ContractsMajor); err != nil {
		return err
	}
	if err = checkStation(ctx, tx, stationID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func Check(ctx context.Context, pool *pgxpool.Pool, stationID string) error {
	all, err := load()
	if err != nil {
		return err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	n, err := history(ctx, tx, all)
	if err != nil {
		return err
	}
	if n != len(all) {
		return errors.New("pending migrations")
	}
	if err = checkStation(ctx, tx, stationID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
