package migrations

import (
	"strings"
	"testing"
)

func TestManagementJournalMigrationSequence(t *testing.T) {
	all, err := load()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) < 15 || all[14].name != "0015_runtime_management_transitions.sql" {
		t.Fatal("management journal migration is not in the executable sequence")
	}
}

func TestModelCacheMigrationSequenceAndDDLGuards(t *testing.T) {
	all, err := load()
	if err != nil {
		t.Fatalf("load() failed: %v", err)
	}
	if Version != 24 || len(all) != 24 {
		t.Fatalf("expected Version=24 and 24 migrations, got Version=%d len=%d", Version, len(all))
	}
	if all[21].version != 22 || all[22].version != 23 || all[22].name != "0023_gpu_scheduler_leases_and_residency.sql" {
		t.Fatalf("unexpected 0022->0023 chain: %+v -> %+v", all[21].name, all[22].name)
	}
	m24 := all[23]
	if m24.version != 24 || m24.name != "0024_comfyui_model_cache_and_catalog.sql" || len(m24.checksum) != 64 {
		t.Fatalf("unexpected 0024 migration metadata: version=%d name=%s checksum=%s", m24.version, m24.name, m24.checksum)
	}
	for _, requiredFragment := range []string{
		"CREATE TABLE station.model_catalog_entries",
		"CREATE TABLE station.model_cache_blobs",
		"CREATE TABLE station.model_ref_pins",
		"CREATE TABLE station.model_ensure_idempotency",
		"CREATE TABLE station.model_cache_storage_state",
		"paid_access boolean NOT NULL DEFAULT false CHECK (paid_access = false)",
		"'not_installed'",
		"'verifying_sha256'",
		"'evicting'",
		"protect_model_cache_blob_fence()",
		"NEW.download_fence < OLD.download_fence",
		"pin_state IN ('reserved', 'active', 'released')",
		"FROM PUBLIC;",
		"FROM vf_t4_runner;",
		"TO vf_t4_auditor;",
		"TO vf_gpu_runtime;",
	} {
		if !strings.Contains(m24.sql, requiredFragment) {
			t.Fatalf("0024 migration missing mandatory DDL guard %q", requiredFragment)
		}
	}
}
