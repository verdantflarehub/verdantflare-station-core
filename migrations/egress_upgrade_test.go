package migrations

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
)

// Exercise the published pre-egress schema and the concurrent Runtime schema.
// Only disposable test databases are changed; retained review data is untouched.
func TestEgressUpgradePreservesExistingData(t *testing.T) {
	for _, baseline := range []int{6, 8} {
		t.Run(fmt.Sprintf("from_%d", baseline), func(t *testing.T) {
			ctx := t.Context()
			pool := testdb.New(t)
			all, err := load()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, `CREATE SCHEMA station; CREATE TABLE station.schema_migrations(version integer PRIMARY KEY,name text NOT NULL,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
				t.Fatal(err)
			}
			for _, migration := range all[:baseline] {
				if _, err = pool.Exec(ctx, migration.sql); err != nil {
					t.Fatal(err)
				}
				if _, err = pool.Exec(ctx, `INSERT INTO station.schema_migrations(version,name,checksum) VALUES($1,$2,$3)`, migration.version, migration.name, migration.checksum); err != nil {
					t.Fatal(err)
				}
			}
			station, org := uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String()
			if _, err = pool.Exec(ctx, `INSERT INTO station.configuration(singleton,station_id,contracts_major) VALUES(true,$1,1)`, station); err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, `INSERT INTO station.organizations(organization_id,name) VALUES($1,'retained organization')`, org); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if err = Apply(ctx, pool, station); err != nil {
					t.Fatal(err)
				}
			}
			if err = Check(ctx, pool, station); err != nil {
				t.Fatal(err)
			}
			var name string
			if err = pool.QueryRow(ctx, `SELECT name FROM station.organizations WHERE organization_id=$1`, org).Scan(&name); err != nil || name != "retained organization" {
				t.Fatal("existing organization lost during upgrade", err)
			}
			var count int
			if err = pool.QueryRow(ctx, `SELECT count(*) FROM station.egress_proxies`).Scan(&count); err != nil || count != 0 {
				t.Fatal("proxy table not initialized", err)
			}
		})
	}
}
