package migrations

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
)

func computeTestCanonicalEventHash(stationID, nodeUID, gpuUUID string, seq int64, prevHash, corrUUID, entityType, entityID, toState string) string {
	canon := struct {
		StationID   string   `json:"station_id"`
		NodeUID     string   `json:"node_uid"`
		GPUUUID     string   `json:"gpu_uuid"`
		Seq         int64    `json:"card_event_seq"`
		PrevHash    string   `json:"prev_hash"`
		Correlation string   `json:"correlation_event_uuid"`
		RunNonce    string   `json:"run_nonce"`
		EntityType  string   `json:"entity_type"`
		EntityID    string   `json:"entity_id"`
		FromState   string   `json:"from_state"`
		ToState     string   `json:"to_state"`
		Fence       int64    `json:"fencing_token"`
		ResSeq      int64    `json:"residency_seq"`
		Epoch       int64    `json:"gpu_epoch"`
		Budget      int64    `json:"budget_mib"`
		Reason      string   `json:"reason_code"`
		Payload     [][2]any `json:"payload"`
	}{
		StationID: stationID, NodeUID: nodeUID, GPUUUID: gpuUUID,
		Seq: seq, PrevHash: prevHash, Correlation: corrUUID,
		EntityType: entityType, EntityID: entityID, ToState: toState,
		Payload: [][2]any{},
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(canon)
	raw := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	sum := sha256.Sum256(append([]byte(prevHash+"|"), raw...))
	return hex.EncodeToString(sum[:])
}

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

func TestUpgrade22To23ChainAndAppendOnlyGuards(t *testing.T) {
	ctx := context.Background()
	p := testdb.New(t)
	id := uuid.Must(uuid.NewV7()).String()
	all, err := load()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) < 23 || all[22].name != "0023_gpu_scheduler_leases_and_residency.sql" {
		t.Fatalf("unexpected migration tail: count=%d", len(all))
	}
	if _, err = p.Exec(ctx, `CREATE SCHEMA station; CREATE TABLE station.schema_migrations(version integer PRIMARY KEY,name text NOT NULL,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now());`); err != nil {
		t.Fatal(err)
	}
	for _, m := range all[:22] {
		if _, err = p.Exec(ctx, m.sql); err != nil {
			t.Fatalf("baseline migration %d failed: %v", m.version, err)
		}
		if _, err = p.Exec(ctx, "INSERT INTO station.schema_migrations(version,name,checksum) VALUES($1,$2,$3)", m.version, m.name, m.checksum); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = p.Exec(ctx, "INSERT INTO station.configuration(singleton,station_id,contracts_major) VALUES(true,$1,1)", id); err != nil {
		t.Fatal(err)
	}
	if Check(ctx, p, id) == nil {
		t.Fatal("version 22 accepted when version 23 required")
	}
	for range 2 {
		if err = Apply(ctx, p, id); err != nil {
			t.Fatalf("upgrade 22->23 failed: %v", err)
		}
	}
	if err = Check(ctx, p, id); err != nil {
		t.Fatal(err)
	}

	// Verify old binary (simulated all[:22]) rejects database already migrated to 23.
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = history(ctx, tx, all[:22]); err == nil {
		_ = tx.Rollback(ctx)
		t.Fatal("old Version=22 binary accepted database at Version=23")
	}
	_ = tx.Rollback(ctx)

	// Verify per-card linear chain, canonical SHA-256 verification, chain-head protection, and append-only immutability triggers.
	if _, err = p.Exec(ctx, `INSERT INTO station.gpu_physical_cards(station_id,node_uid,gpu_uuid,total_vram_mib,state) VALUES($1,'node-5090','GPU-0',32607,'ready')`, id); err != nil {
		t.Fatal(err)
	}
	forgedHash := "1111111111111111111111111111111111111111111111111111111111111111"
	corr := uuid.Must(uuid.NewV7()).String()
	hash1 := computeTestCanonicalEventHash(id, "node-5090", "GPU-0", 1, "GENESIS", corr, "card", "GPU-0", "ready")
	hash2 := computeTestCanonicalEventHash(id, "node-5090", "GPU-0", 2, hash1, corr, "card", "GPU-0", "ready")

	if _, err = p.Exec(ctx, `INSERT INTO station.gpu_scheduler_events(station_id,node_uid,gpu_uuid,card_event_seq,correlation_event_uuid,entity_type,entity_id,to_state,prev_hash,event_hash) VALUES($1,'node-5090','GPU-0',1,$2,'card','GPU-0','ready','GENESIS',$3)`, id, corr, forgedHash); err == nil {
		t.Fatal("forged non-canonical event_hash accepted")
	}
	if _, err = p.Exec(ctx, `INSERT INTO station.gpu_scheduler_events(station_id,node_uid,gpu_uuid,card_event_seq,correlation_event_uuid,entity_type,entity_id,to_state,prev_hash,event_hash) VALUES($1,'node-5090','GPU-0',2,$2,'card','GPU-0','ready','GENESIS',$3)`, id, corr, hash1); err == nil {
		t.Fatal("gap in card_event_seq accepted")
	}
	if _, err = p.Exec(ctx, `INSERT INTO station.gpu_scheduler_events(station_id,node_uid,gpu_uuid,card_event_seq,correlation_event_uuid,entity_type,entity_id,to_state,prev_hash,event_hash) VALUES($1,'node-5090','GPU-0',1,$2,'card','GPU-0','ready',$3,$4)`, id, corr, "WRONG_PREV", hash1); err == nil {
		t.Fatal("mismatched prev_hash accepted")
	}
	if _, err = p.Exec(ctx, `INSERT INTO station.gpu_scheduler_events(station_id,node_uid,gpu_uuid,card_event_seq,correlation_event_uuid,entity_type,entity_id,to_state,prev_hash,event_hash) VALUES($1,'node-5090','GPU-0',1,$2,'card','GPU-0','ready','GENESIS',$3)`, id, corr, hash1); err != nil {
		t.Fatalf("valid seq=1 event rejected: %v", err)
	}
	if _, err = p.Exec(ctx, `INSERT INTO station.gpu_scheduler_events(station_id,node_uid,gpu_uuid,card_event_seq,correlation_event_uuid,entity_type,entity_id,to_state,prev_hash,event_hash) VALUES($1,'node-5090','GPU-0',2,$2,'card','GPU-0','ready',$3,$4)`, id, corr, hash1, hash2); err != nil {
		t.Fatalf("valid seq=2 event rejected: %v", err)
	}
	var tailSeq int64
	var tailHash string
	if err = p.QueryRow(ctx, `SELECT last_event_seq,last_event_hash FROM station.gpu_physical_cards WHERE station_id=$1 AND node_uid='node-5090' AND gpu_uuid='GPU-0'`, id).Scan(&tailSeq, &tailHash); err != nil {
		t.Fatal(err)
	}
	if tailSeq != 2 || tailHash != hash2 {
		t.Fatalf("unexpected card chain tail: seq=%d hash=%s", tailSeq, tailHash)
	}

	// Verify direct chain-head tampering on gpu_physical_cards is forbidden.
	if _, err = p.Exec(ctx, `UPDATE station.gpu_physical_cards SET last_event_seq=99 WHERE station_id=$1 AND node_uid='node-5090' AND gpu_uuid='GPU-0'`, id); err == nil {
		t.Fatal("direct last_event_seq tampering on gpu_physical_cards accepted")
	}
	if _, err = p.Exec(ctx, `UPDATE station.gpu_physical_cards SET last_event_hash=$2 WHERE station_id=$1 AND node_uid='node-5090' AND gpu_uuid='GPU-0'`, id, forgedHash); err == nil {
		t.Fatal("direct last_event_hash tampering on gpu_physical_cards accepted")
	}
	if _, err = p.Exec(ctx, `DELETE FROM station.gpu_physical_cards WHERE station_id=$1 AND node_uid='node-5090' AND gpu_uuid='GPU-0'`, id); err == nil {
		t.Fatal("DELETE on gpu_physical_cards with non-empty event chain accepted")
	}
	if _, err = p.Exec(ctx, `TRUNCATE station.gpu_physical_cards CASCADE`); err == nil {
		t.Fatal("TRUNCATE on gpu_physical_cards accepted")
	}

	if _, err = p.Exec(ctx, `UPDATE station.gpu_scheduler_events SET reason_code='tamper' WHERE card_event_seq=1`); err == nil {
		t.Fatal("UPDATE on gpu_scheduler_events accepted")
	}
	if _, err = p.Exec(ctx, `DELETE FROM station.gpu_scheduler_events WHERE card_event_seq=1`); err == nil {
		t.Fatal("DELETE on gpu_scheduler_events accepted")
	}
	if _, err = p.Exec(ctx, `TRUNCATE station.gpu_scheduler_events`); err == nil {
		t.Fatal("TRUNCATE on gpu_scheduler_events accepted")
	}

	// Mandatory G2 role privilege verification (zero conditional skips).
	var runnerHasSelect, runnerHasInsert, auditorHasSelect, auditorHasInsert, auditorHasUpdate, runtimeHasInsert, runtimeHasUpdate, runtimeHasDelete bool
	if err = p.QueryRow(ctx, `
		SELECT
			has_table_privilege('vf_t4_runner', 'station.gpu_scheduler_events', 'SELECT'),
			has_table_privilege('vf_t4_runner', 'station.gpu_scheduler_events', 'INSERT'),
			has_table_privilege('vf_t4_auditor', 'station.gpu_scheduler_events', 'SELECT'),
			has_table_privilege('vf_t4_auditor', 'station.gpu_scheduler_events', 'INSERT'),
			has_table_privilege('vf_t4_auditor', 'station.gpu_physical_cards', 'UPDATE'),
			has_table_privilege('vf_gpu_runtime', 'station.gpu_scheduler_events', 'INSERT'),
			has_table_privilege('vf_gpu_runtime', 'station.gpu_scheduler_events', 'UPDATE'),
			has_table_privilege('vf_gpu_runtime', 'station.gpu_scheduler_events', 'DELETE')
	`).Scan(&runnerHasSelect, &runnerHasInsert, &auditorHasSelect, &auditorHasInsert, &auditorHasUpdate, &runtimeHasInsert, &runtimeHasUpdate, &runtimeHasDelete); err != nil {
		t.Fatalf("mandatory G2 role query failed: %v", err)
	}
	if runnerHasSelect || runnerHasInsert || !auditorHasSelect || auditorHasInsert || auditorHasUpdate || !runtimeHasInsert || runtimeHasUpdate || runtimeHasDelete {
		t.Fatalf("G2 privilege separation violated: runnerSelect=%v runnerInsert=%v auditorSelect=%v auditorInsert=%v auditorUpdate=%v runtimeInsert=%v runtimeUpdate=%v runtimeDelete=%v",
			runnerHasSelect, runnerHasInsert, auditorHasSelect, auditorHasInsert, auditorHasUpdate, runtimeHasInsert, runtimeHasUpdate, runtimeHasDelete)
	}

	// Execute live SET LOCAL ROLE checks for all three G2 roles.
	rtx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = rtx.Exec(ctx, `SET LOCAL ROLE vf_t4_runner`); err != nil {
		_ = rtx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err = rtx.Exec(ctx, `SELECT 1 FROM station.gpu_scheduler_events LIMIT 1`); err == nil {
		_ = rtx.Rollback(ctx)
		t.Fatal("vf_t4_runner should not be able to SELECT station.gpu_scheduler_events")
	}
	_ = rtx.Rollback(ctx)

	atx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = atx.Exec(ctx, `SET LOCAL ROLE vf_t4_auditor`); err != nil {
		_ = atx.Rollback(ctx)
		t.Fatal(err)
	}
	var cnt int
	if err = atx.QueryRow(ctx, `SELECT count(*)::int FROM station.gpu_scheduler_events`).Scan(&cnt); err != nil || cnt != 2 {
		_ = atx.Rollback(ctx)
		t.Fatalf("vf_t4_auditor SELECT failed: cnt=%d err=%v", cnt, err)
	}
	if _, err = atx.Exec(ctx, `DELETE FROM station.gpu_scheduler_events`); err == nil {
		_ = atx.Rollback(ctx)
		t.Fatal("vf_t4_auditor should not be able to DELETE station.gpu_scheduler_events")
	}
	_ = atx.Rollback(ctx)

	gtx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = gtx.Exec(ctx, `SET LOCAL ROLE vf_gpu_runtime`); err != nil {
		_ = gtx.Rollback(ctx)
		t.Fatal(err)
	}
	for _, roTbl := range []string{
		"station.app_instance_workspaces",
		"station.app_instance_starts",
		"station.app_installations",
		"station.runtime_instance_workspaces",
		"station.runtime_instance_executions",
	} {
		var instCnt int
		if err = gtx.QueryRow(ctx, `SELECT count(*)::int FROM `+roTbl).Scan(&instCnt); err != nil {
			_ = gtx.Rollback(ctx)
			t.Fatalf("vf_gpu_runtime SELECT on %s failed: %v", roTbl, err)
		}
	}
	if _, err = gtx.Exec(ctx, `DELETE FROM station.gpu_resource_leases`); err == nil {
		_ = gtx.Rollback(ctx)
		t.Fatal("vf_gpu_runtime should not have DELETE on station.gpu_resource_leases")
	}
	_ = gtx.Rollback(ctx)
}

func TestUpgrade22To23To24ChainAndModelCacheGuards(t *testing.T) {
	ctx := context.Background()
	p := testdb.New(t)
	id := uuid.Must(uuid.NewV7()).String()
	all, err := load()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 24 || all[22].name != "0023_gpu_scheduler_leases_and_residency.sql" || all[23].name != "0024_comfyui_model_cache_and_catalog.sql" {
		t.Fatalf("unexpected migration tail: count=%d", len(all))
	}
	if _, err = p.Exec(ctx, `CREATE SCHEMA station; CREATE TABLE station.schema_migrations(version integer PRIMARY KEY,name text NOT NULL,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now());`); err != nil {
		t.Fatal(err)
	}
	for _, m := range all[:23] {
		if _, err = p.Exec(ctx, m.sql); err != nil {
			t.Fatalf("baseline migration %d failed: %v", m.version, err)
		}
		if _, err = p.Exec(ctx, "INSERT INTO station.schema_migrations(version,name,checksum) VALUES($1,$2,$3)", m.version, m.name, m.checksum); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = p.Exec(ctx, "INSERT INTO station.configuration(singleton,station_id,contracts_major) VALUES(true,$1,1)", id); err != nil {
		t.Fatal(err)
	}
	if Check(ctx, p, id) == nil {
		t.Fatal("version 23 accepted when version 24 required")
	}
	for range 2 {
		if err = Apply(ctx, p, id); err != nil {
			t.Fatalf("upgrade 23->24 failed: %v", err)
		}
	}
	if err = Check(ctx, p, id); err != nil {
		t.Fatal(err)
	}

	// Simulated Version=23 binary must reject database already migrated to Version=24.
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = history(ctx, tx, all[:23]); err == nil {
		_ = tx.Rollback(ctx)
		t.Fatal("old Version=23 binary accepted database at Version=24")
	}
	_ = tx.Rollback(ctx)

	sha := "31e35c80fc4829d14f90153f4c74cd59c90b779f6afe05a74cd6120b893f7e5b"
	paidSha := "aaaaaaaaaaaa29d14f90153f4c74cd59c90b779f6afe05a74cd6120b893f7e5b"
	if _, err = p.Exec(ctx, `INSERT INTO station.model_catalog_entries(station_id,model_id,catalog_code,display_name,category,filename,sha256,size_bytes,vram_footprint_mib,license) VALUES($1,'sdxl-base-1_0','MDL-CKPT-SDXL-001','SDXL Base 1.0','CKPT','sd_xl_base_1.0.safetensors',$2,6938078334,6912,'openrail++')`, id, sha); err != nil {
		t.Fatalf("valid catalog entry rejected: %v", err)
	}
	// Paid catalog entries cannot be approved, and paid blobs can NEVER be inserted into model_cache_blobs.
	if _, err = p.Exec(ctx, `INSERT INTO station.model_catalog_entries(station_id,model_id,catalog_code,display_name,category,filename,sha256,size_bytes,vram_footprint_mib,license,paid_access,license_tier,approval_state) VALUES($1,'paid-test','MDL-LORA-PAID-001','Paid Test','LORA','paid_test.safetensors',$2,1024,512,'proprietary',true,'paid_forbidden','approved')`, id, paidSha); err == nil {
		t.Fatal("approved paid_access=true catalog entry accepted")
	}
	if _, err = p.Exec(ctx, `INSERT INTO station.model_cache_blobs(station_id,sha256,size_bytes,paid_access,status) VALUES($1,$2,1024,true,'queued')`, id, paidSha); err == nil {
		t.Fatal("paid_access=true blob accepted in model_cache_blobs")
	}

	// Verify 7-state lifecycle, monotonic download_fence, and no-DELETE / no-TRUNCATE guards.
	if _, err = p.Exec(ctx, `INSERT INTO station.model_cache_blobs(station_id,sha256,size_bytes,status,download_fence) VALUES($1,$2,6938078334,'ready',3)`, id, sha); err != nil {
		t.Fatalf("valid model_cache_blobs insert rejected: %v", err)
	}
	if _, err = p.Exec(ctx, `UPDATE station.model_cache_blobs SET download_fence=2 WHERE station_id=$1 AND sha256=$2`, id, sha); err == nil {
		t.Fatal("decrementing download_fence accepted")
	}
	if _, err = p.Exec(ctx, `DELETE FROM station.model_cache_blobs WHERE station_id=$1 AND sha256=$2`, id, sha); err == nil {
		t.Fatal("DELETE on model_cache_blobs accepted")
	}
	if _, err = p.Exec(ctx, `TRUNCATE station.model_cache_blobs CASCADE`); err == nil {
		t.Fatal("TRUNCATE on model_cache_blobs accepted")
	}
	// In-place eviction transition requires active_evict_id and preserves monotonic download_fence + evict_fence.
	evictID := uuid.Must(uuid.NewV7()).String()
	if _, err = p.Exec(ctx, `UPDATE station.model_cache_blobs SET status='evicting' WHERE station_id=$1 AND sha256=$2`, id, sha); err == nil {
		t.Fatal("evicting transition without active_evict_id accepted")
	}
	if _, err = p.Exec(ctx, `UPDATE station.model_cache_blobs SET status='evicting',evict_fence=2,active_evict_id=$3::uuid WHERE station_id=$1 AND sha256=$2`, id, sha, evictID); err != nil {
		t.Fatalf("evicting transition failed: %v", err)
	}
	if _, err = p.Exec(ctx, `UPDATE station.model_cache_blobs SET evict_fence=1 WHERE station_id=$1 AND sha256=$2`, id, sha); err == nil {
		t.Fatal("decrementing evict_fence accepted")
	}
	if _, err = p.Exec(ctx, `UPDATE station.model_cache_blobs SET status='not_installed',active_evict_id=NULL,bytes_downloaded=0,part_bytes_on_disk=0 WHERE station_id=$1 AND sha256=$2`, id, sha); err != nil {
		t.Fatalf("not_installed reset failed: %v", err)
	}

	// Mandatory G2 role privilege verification on 0024 tables.
	var runnerSelect, auditorSelect, auditorInsert, runtimeInsert, runtimeUpdate, runtimeDelete bool
	if err = p.QueryRow(ctx, `
		SELECT
			has_table_privilege('vf_t4_runner', 'station.model_cache_blobs', 'SELECT'),
			has_table_privilege('vf_t4_auditor', 'station.model_cache_blobs', 'SELECT'),
			has_table_privilege('vf_t4_auditor', 'station.model_cache_blobs', 'INSERT'),
			has_table_privilege('vf_gpu_runtime', 'station.model_ref_pins', 'INSERT'),
			has_table_privilege('vf_gpu_runtime', 'station.model_ref_pins', 'UPDATE'),
			has_table_privilege('vf_gpu_runtime', 'station.model_ref_pins', 'DELETE')
	`).Scan(&runnerSelect, &auditorSelect, &auditorInsert, &runtimeInsert, &runtimeUpdate, &runtimeDelete); err != nil {
		t.Fatalf("0024 G2 privilege check failed: %v", err)
	}
	if runnerSelect || !auditorSelect || auditorInsert || !runtimeInsert || !runtimeUpdate || runtimeDelete {
		t.Fatalf("0024 G2 privilege separation violated: runnerSelect=%v auditorSelect=%v auditorInsert=%v runtimeInsert=%v runtimeUpdate=%v runtimeDelete=%v",
			runnerSelect, auditorSelect, auditorInsert, runtimeInsert, runtimeUpdate, runtimeDelete)
	}
}
