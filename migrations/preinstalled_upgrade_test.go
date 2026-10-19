package migrations

import (
	"testing"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
)

// Upgrading the published schema must retain historical operations without
// manufacturing an installation identity or a registered release binding for them.
func TestPreinstalledUpgradePreservesUnboundHistory(t *testing.T) {
	ctx := t.Context()
	pool := testdb.New(t)
	all, err := load()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `CREATE SCHEMA station; CREATE TABLE station.schema_migrations(version integer PRIMARY KEY,name text NOT NULL,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		t.Fatal(err)
	}
	for _, m := range all[:9] {
		if _, err = pool.Exec(ctx, m.sql); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `INSERT INTO station.schema_migrations(version,name,checksum) VALUES($1,$2,$3)`, m.version, m.name, m.checksum); err != nil {
			t.Fatal(err)
		}
	}
	station, org, user := uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String()
	operation := uuid.Must(uuid.NewV7()).String()
	for _, seed := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO station.configuration(singleton,station_id,contracts_major) VALUES(true,$1,1)`, []any{station}},
		{`INSERT INTO station.organizations(organization_id,name) VALUES($1,'retained organization')`, []any{org}},
		{`INSERT INTO station.users(user_id,username,password_hash) VALUES($1,'migration-fixture','disabled-fixture')`, []any{user}},
		{`INSERT INTO station.app_operations(operation_id,request_id,user_id,organization_id,station_id,app_id,app_version,action,status,phase,idempotency_key,request_hash) VALUES($1,'retained-trace',$2,$3,$4,'test-app','1.0.0','install','succeeded','completed','retained-install-key',decode(repeat('ab',32),'hex'))`, []any{operation, user, org, station}},
	} {
		if _, err = pool.Exec(ctx, seed.sql, seed.args...); err != nil {
			t.Fatal(err)
		}
	}
	queries := []string{
		`SELECT (to_jsonb(o)-'installation_id')::text FROM station.app_operations o`,
		`SELECT json_agg(m ORDER BY version)::text FROM station.schema_migrations m WHERE version<=9`,
	}
	before := make([]string, len(queries))
	for i, q := range queries {
		if err = pool.QueryRow(ctx, q).Scan(&before[i]); err != nil {
			t.Fatal(err)
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err = Apply(ctx, pool, station); err != nil {
			t.Fatal(err)
		}
		if err = Check(ctx, pool, station); err != nil {
			t.Fatal(err)
		}
		for i, q := range queries {
			var after string
			if err = pool.QueryRow(ctx, q).Scan(&after); err != nil {
				t.Fatal(err)
			}
			if after != before[i] {
				t.Fatalf("retained data changed on attempt %d, query %d", attempt, i)
			}
		}
		var unbound bool
		if err = pool.QueryRow(ctx, `SELECT installation_id IS NULL FROM station.app_operations WHERE operation_id=$1`, operation).Scan(&unbound); err != nil || !unbound {
			t.Fatal("legacy operation acquired an installation identity", err)
		}
		var count int
		if err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM station.app_installations)+(SELECT count(*) FROM station.preinstalled_operation_bindings)+(SELECT count(*) FROM station.preinstalled_installation_bindings)`).Scan(&count); err != nil || count != 0 {
			t.Fatal("legacy history acquired installation or release bindings", err)
		}
	}
}
