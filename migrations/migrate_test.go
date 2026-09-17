package migrations

import (
	"context"
	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"testing"
)

func TestMigrationLifecycle(t *testing.T) {
	ctx := context.Background()
	p := testdb.New(t)
	id := uuid.Must(uuid.NewV7()).String()
	if Check(ctx, p, id) == nil {
		t.Fatal("empty database accepted")
	}
	for i := 0; i < 2; i++ {
		if err := Apply(ctx, p, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := Check(ctx, p, id); err != nil {
		t.Fatal(err)
	}
	if Apply(ctx, p, uuid.Must(uuid.NewV7()).String()) == nil {
		t.Fatal("station mismatch accepted")
	}
	all, err := load()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Exec(ctx, "UPDATE station.schema_migrations SET checksum='tampered' WHERE version=1"); err != nil {
		t.Fatal(err)
	}
	if Check(ctx, p, id) == nil || Apply(ctx, p, id) == nil {
		t.Fatal("tampered history accepted")
	}
	if _, err = p.Exec(ctx, "UPDATE station.schema_migrations SET checksum=$1 WHERE version=1", all[0].checksum); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Exec(ctx, "INSERT INTO station.schema_migrations(version,name,checksum) VALUES($1,'future','future')", Version+1); err != nil {
		t.Fatal(err)
	}
	if Check(ctx, p, id) == nil || Apply(ctx, p, id) == nil {
		t.Fatal("future version accepted")
	}
}
func TestUpgradeFromIdentity(t *testing.T) {
	ctx := context.Background()
	p := testdb.New(t)
	id := uuid.Must(uuid.NewV7()).String()
	all, err := load()
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Exec(ctx, `CREATE SCHEMA station; CREATE TABLE station.schema_migrations(version integer PRIMARY KEY,name text NOT NULL,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now());`+all[0].sql)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Exec(ctx, "INSERT INTO station.schema_migrations(version,name,checksum) VALUES(1,$1,$2)", all[0].name, all[0].checksum); err != nil {
		t.Fatal(err)
	}
	if Check(ctx, p, id) == nil {
		t.Fatal("pending migration accepted")
	}
	if err = Apply(ctx, p, id); err != nil {
		t.Fatal(err)
	}
	if err = Check(ctx, p, id); err != nil {
		t.Fatal(err)
	}
}
