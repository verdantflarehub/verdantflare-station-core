package migrations

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
)

// Seed the previous schema only in a disposable database, then compare every
// retained row, including timestamps and both sides of the operation relation.
func TestTransitionUpgradePreservesInstallationHistory(t *testing.T) {
	testOperationJournalUpgrade(t, 13, "station.app_installation_transitions")
}
func TestAppCenterUpgradePreservesInstallationHistory(t *testing.T) {
	testOperationJournalUpgrade(t, 15, "station.appcenter_operation_inputs")
}
func TestManagementReleaseUpgradePreservesInstallationHistory(t *testing.T) {
	testOperationJournalUpgrade(t, 16, "station.appcenter_operation_inputs")
}
func TestAppStateUpgradePreservesInstallationHistory(t *testing.T) {
	testOperationJournalUpgrade(t, 17, "station.app_state_catalog")
}
func testOperationJournalUpgrade(t *testing.T, previous int, table string) {
	t.Helper()
	ctx := t.Context()
	pool := testdb.New(t)
	all, err := load()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `CREATE SCHEMA station; CREATE TABLE station.schema_migrations(version integer PRIMARY KEY,name text NOT NULL,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		t.Fatal(err)
	}
	for _, m := range all[:previous] {
		if _, err = pool.Exec(ctx, m.sql); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `INSERT INTO station.schema_migrations(version,name,checksum) VALUES($1,$2,$3)`, m.version, m.name, m.checksum); err != nil {
			t.Fatal(err)
		}
	}
	station, org, user := uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String()
	installation, operation := uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String()
	for _, seed := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO station.configuration(singleton,station_id,contracts_major) VALUES(true,$1,1)`, []any{station}},
		{`INSERT INTO station.organizations(organization_id,name) VALUES($1,'retained organization')`, []any{org}},
		{`INSERT INTO station.users(user_id,username,password_hash) VALUES($1,'migration-fixture','disabled-fixture')`, []any{user}},
		{`INSERT INTO station.app_operations(operation_id,request_id,user_id,organization_id,station_id,app_id,app_version,action,status,phase,idempotency_key,request_hash) VALUES($1,'retained-trace',$2,$3,$4,'test-app','1.9.0','install','succeeded','completed','retained-install-key',decode(repeat('ab',32),'hex'))`, []any{operation, user, org, station}},
		{`INSERT INTO station.app_installations(installation_id,created_by,organization_id,station_id,app_id,app_version,state,revision,last_operation_id) VALUES($1,$2,$3,$4,'test-app','1.9.0','installed',7,$5)`, []any{installation, user, org, station, operation}},
		{`UPDATE station.app_operations SET installation_id=$1 WHERE operation_id=$2`, []any{installation, operation}},
	} {
		if _, err = pool.Exec(ctx, seed.sql, seed.args...); err != nil {
			t.Fatal(err)
		}
	}
	queries := []string{
		`SELECT row_to_json(i)::text FROM station.app_installations i`,
		`SELECT row_to_json(o)::text FROM station.app_operations o`,
		fmt.Sprintf(`SELECT json_agg(m ORDER BY version)::text FROM station.schema_migrations m WHERE version<=%d`, previous),
	}
	if previous >= 16 {
		transition := uuid.Must(uuid.NewV7()).String()
		if _, err = pool.Exec(ctx, `INSERT INTO station.app_installation_transitions(operation_id,installation_id,user_id,organization_id,station_id,app_id,request_id,idempotency_key,request_hash,action,expected_revision,source_version,target_version,source_snapshot,target_snapshot,status,phase) VALUES($1,$2,$3,$4,$5,'test-app','retained-transition','retained-transition-key',decode(repeat('cd',32),'hex'),'upgrade',7,'1.9.0','1.10.0','{}','{}','running','verifying')`, transition, installation, user, org, station); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `INSERT INTO station.runtime_management_transitions(operation_id,station_id,app_id,input_hash,phase,checkpoint,revision) VALUES($1,$2,'test-app',decode(repeat('ef',32),'hex'),'prepared','{"fixture":"retained checkpoint"}',9)`, transition, station); err != nil {
			t.Fatal(err)
		}
		queries = append(queries, `SELECT row_to_json(t)::text FROM station.app_installation_transitions t`, `SELECT row_to_json(r)::text FROM station.runtime_management_transitions r`)
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
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&count); err != nil || count != 0 {
		t.Fatal("new operation journal not initialized empty", err)
	}
}
