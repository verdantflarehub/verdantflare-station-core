-- Station GPU per-card four-quadrant ledger, task leases, platform residency,
-- per-card linear hash chain events, and G2 three-role isolation grants.

CREATE TABLE station.gpu_physical_cards (
    station_id uuid NOT NULL REFERENCES station.configuration(station_id),
    node_uid text NOT NULL CHECK (length(node_uid) BETWEEN 1 AND 128),
    node_name text NOT NULL DEFAULT '',
    gpu_uuid text NOT NULL CHECK (length(gpu_uuid) BETWEEN 1 AND 128),
    gpu_index integer NOT NULL DEFAULT 0 CHECK (gpu_index >= 0),
    model_name text NOT NULL DEFAULT 'NVIDIA GeForce RTX 5090',
    total_vram_mib bigint NOT NULL CHECK (total_vram_mib > 0),
    driver_overhead_mib bigint NOT NULL DEFAULT 607 CHECK (driver_overhead_mib >= 0),
    measured_free_vram_mib bigint NOT NULL DEFAULT 0 CHECK (measured_free_vram_mib >= 0),
    measured_used_vram_mib bigint NOT NULL DEFAULT 0 CHECK (measured_used_vram_mib >= 0),
    telemetry_boot_id text NOT NULL DEFAULT '',
    telemetry_sample_id bigint NOT NULL DEFAULT 0 CHECK (telemetry_sample_id >= 0),
    telemetry_sampled_at timestamptz,
    attributed_processes jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(attributed_processes) = 'array'),
    fencing_epoch bigint NOT NULL DEFAULT 1 CHECK (fencing_epoch > 0),
    min_fencing_token bigint NOT NULL DEFAULT 1 CHECK (min_fencing_token > 0),
    reserved_by_lease_id uuid,
    quarantined_mib bigint NOT NULL DEFAULT 0 CHECK (quarantined_mib >= 0),
    time_slicing_slots integer NOT NULL DEFAULT 6 CHECK (time_slicing_slots >= 1),
    allocated_k8s_slots integer NOT NULL DEFAULT 0 CHECK (allocated_k8s_slots >= 0),
    scaling_up_k8s_slots integer NOT NULL DEFAULT 0 CHECK (scaling_up_k8s_slots >= 0),
    unscheduled_pending_k8s_slots integer NOT NULL DEFAULT 0 CHECK (unscheduled_pending_k8s_slots >= 0),
    active_gpu_pods jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(active_gpu_pods) = 'array'),
    admission_mode text NOT NULL DEFAULT 'drain_only' CHECK (admission_mode IN ('drain_only', 'active')),
    last_event_seq bigint NOT NULL DEFAULT 0 CHECK (last_event_seq >= 0),
    last_event_hash text NOT NULL DEFAULT 'GENESIS' CHECK (length(last_event_hash) >= 7),
    state text NOT NULL DEFAULT 'reconciling' CHECK (state IN ('ready', 'reconciling', 'quarantined')),
    state_reason text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, node_uid, gpu_uuid)
);

CREATE TABLE station.gpu_resource_leases (
    lease_id uuid PRIMARY KEY,
    station_id uuid NOT NULL REFERENCES station.configuration(station_id),
    node_uid text NOT NULL CHECK (length(node_uid) BETWEEN 1 AND 128),
    holder_app_id text NOT NULL CHECK (length(holder_app_id) BETWEEN 1 AND 128),
    holder_task_id text NOT NULL CHECK (length(holder_task_id) BETWEEN 1 AND 256),
    attempt_id text NOT NULL CHECK (length(attempt_id) BETWEEN 1 AND 256),
    stage_id text NOT NULL DEFAULT 'primary' CHECK (length(stage_id) BETWEEN 1 AND 128),
    pod_uid text NOT NULL DEFAULT '',
    container_id text NOT NULL DEFAULT '',
    boot_id text NOT NULL DEFAULT '',
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 256),
    request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    candidate_gpu_uuids text[] NOT NULL DEFAULT '{}',
    gpu_uuids text[] NOT NULL DEFAULT '{}',
    reserved_gpu_uuids text[] NOT NULL DEFAULT '{}',
    card_count integer NOT NULL DEFAULT 1 CHECK (card_count IN (1, 2)),
    k8s_slots_required integer NOT NULL DEFAULT 0 CHECK (k8s_slots_required >= 0),
    peak_budget_mib bigint NOT NULL CHECK (peak_budget_mib > 0),
    startup_free_min_mib bigint NOT NULL DEFAULT 0 CHECK (startup_free_min_mib >= 0),
    headroom_override_mib bigint CHECK (headroom_override_mib IS NULL OR headroom_override_mib >= 0),
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    legacy_lease_role text NOT NULL DEFAULT '',
    priority text NOT NULL CHECK (priority IN ('human_wake', 'shared_slice', 'exclusive_batch')),
    bypass_count integer NOT NULL DEFAULT 0 CHECK (bypass_count >= 0),
    state text NOT NULL CHECK (state IN (
        'preparing', 'legacy_acquired', 'queued', 'draining', 'leased', 'running',
        'releasing', 'released', 'expired', 'reconciling', 'reclaiming', 'quarantined'
    )),
    blocking_reason text NOT NULL DEFAULT 'none',
    blocking_holder_id text NOT NULL DEFAULT '',
    revoke_acknowledged boolean NOT NULL DEFAULT false,
    reclaim_proof jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(reclaim_proof) = 'object'),
    response_cache jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(response_cache) = 'object'),
    expires_at timestamptz NOT NULL,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (station_id, holder_app_id, idempotency_key)
);

CREATE INDEX gpu_resource_leases_active_idx
    ON station.gpu_resource_leases (station_id, node_uid, state, created_at)
    WHERE state NOT IN ('released', 'expired');

CREATE INDEX gpu_resource_leases_task_stage_idx
    ON station.gpu_resource_leases (station_id, holder_app_id, holder_task_id, attempt_id, stage_id);

CREATE TABLE station.gpu_instance_residency (
    station_id uuid NOT NULL REFERENCES station.configuration(station_id),
    instance_id text NOT NULL CHECK (length(instance_id) BETWEEN 1 AND 128),
    app_id text NOT NULL CHECK (length(app_id) BETWEEN 1 AND 128),
    node_uid text NOT NULL CHECK (length(node_uid) BETWEEN 1 AND 128),
    pod_uid text NOT NULL CHECK (length(pod_uid) BETWEEN 1 AND 128),
    container_name text NOT NULL DEFAULT 'worker',
    container_id text NOT NULL DEFAULT '',
    boot_id text NOT NULL CHECK (length(boot_id) BETWEEN 1 AND 128),
    gpu_epoch bigint NOT NULL DEFAULT 0 CHECK (gpu_epoch >= 0),
    residency_seq bigint NOT NULL DEFAULT 1 CHECK (residency_seq > 0),
    gpu_uuid text NOT NULL CHECK (length(gpu_uuid) BETWEEN 1 AND 128),
    residency_state text NOT NULL CHECK (residency_state IN (
        'active_compute', 'human_protected', 'human_dirty', 'idle_evictable',
        'zero_vram_standby', 'waking', 'sealing', 'draining_hard',
        'reconciling', 'quarantined'
    )),
    human_protected boolean NOT NULL DEFAULT false,
    gui_connection_alive boolean NOT NULL DEFAULT false,
    dirty boolean NOT NULL DEFAULT false,
    snapshot_in_progress boolean NOT NULL DEFAULT false,
    snapshot_required boolean NOT NULL DEFAULT false,
    blocking_reason text NOT NULL DEFAULT 'none',
    pinned_budget_mib bigint NOT NULL DEFAULT 0 CHECK (pinned_budget_mib >= 0),
    vram_allocated_mib bigint CHECK (vram_allocated_mib IS NULL OR vram_allocated_mib >= 0),
    idle_seconds bigint NOT NULL DEFAULT 0 CHECK (idle_seconds >= 0),
    callback_endpoint text NOT NULL DEFAULT '',
    template_upgraded boolean NOT NULL DEFAULT true,
    last_active_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, instance_id)
);

CREATE INDEX gpu_instance_residency_card_idx
    ON station.gpu_instance_residency (station_id, node_uid, gpu_uuid, residency_state);

CREATE TABLE station.gpu_scheduler_events (
    event_id bigserial PRIMARY KEY,
    station_id uuid NOT NULL REFERENCES station.configuration(station_id),
    node_uid text NOT NULL,
    gpu_uuid text NOT NULL,
    card_event_seq bigint NOT NULL CHECK (card_event_seq > 0),
    txid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    occurred_at timestamptz NOT NULL DEFAULT now(),
    run_nonce text NOT NULL DEFAULT '',
    correlation_event_uuid uuid NOT NULL,
    entity_type text NOT NULL CHECK (entity_type IN ('card', 'lease', 'residency', 'quota', 'reconcile', 'admin')),
    entity_id text NOT NULL CHECK (length(entity_id) BETWEEN 1 AND 256),
    from_state text NOT NULL DEFAULT '',
    to_state text NOT NULL CHECK (length(to_state) BETWEEN 1 AND 64),
    fencing_token bigint NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    residency_seq bigint NOT NULL DEFAULT 0 CHECK (residency_seq >= 0),
    gpu_epoch bigint NOT NULL DEFAULT 0 CHECK (gpu_epoch >= 0),
    budget_mib bigint NOT NULL DEFAULT 0 CHECK (budget_mib >= 0),
    reason_code text NOT NULL DEFAULT '',
    payload jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(payload) = 'object'),
    prev_hash text NOT NULL CHECK (length(prev_hash) >= 7),
    event_hash text NOT NULL CHECK (event_hash ~ '^[0-9a-f]{64}$'),
    UNIQUE (station_id, node_uid, gpu_uuid, card_event_seq),
    FOREIGN KEY (station_id, node_uid, gpu_uuid)
        REFERENCES station.gpu_physical_cards(station_id, node_uid, gpu_uuid)
);

CREATE INDEX gpu_scheduler_events_entity_idx
    ON station.gpu_scheduler_events (station_id, entity_type, entity_id, occurred_at);

CREATE INDEX gpu_scheduler_events_correlation_idx
    ON station.gpu_scheduler_events (station_id, correlation_event_uuid);

CREATE FUNCTION station.canonical_jsonb(j jsonb) RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE jsonb_typeof(j)
        WHEN 'object' THEN
            '{' || COALESCE((
                SELECT string_agg(to_json(e.key)::text || ':' || station.canonical_jsonb(e.value), ',' ORDER BY e.key COLLATE "C")
                FROM jsonb_each(j) AS e
            ), '') || '}'
        WHEN 'array' THEN
            '[' || COALESCE((
                SELECT string_agg(station.canonical_jsonb(a.value), ',' ORDER BY a.ord)
                FROM jsonb_array_elements(j) WITH ORDINALITY AS a(value, ord)
            ), '') || ']'
        ELSE j::text
    END;
$$;

CREATE FUNCTION station.canonical_gpu_event_hash(
    p_station_id uuid,
    p_node_uid text,
    p_gpu_uuid text,
    p_card_event_seq bigint,
    p_prev_hash text,
    p_correlation_event_uuid uuid,
    p_run_nonce text,
    p_entity_type text,
    p_entity_id text,
    p_from_state text,
    p_to_state text,
    p_fencing_token bigint,
    p_residency_seq bigint,
    p_gpu_epoch bigint,
    p_budget_mib bigint,
    p_reason_code text,
    p_payload jsonb
) RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT encode(
        sha256(
            convert_to(
                p_prev_hash || '|{'
                || '"station_id":' || to_json(p_station_id::text)::text
                || ',"node_uid":' || to_json(p_node_uid)::text
                || ',"gpu_uuid":' || to_json(p_gpu_uuid)::text
                || ',"card_event_seq":' || p_card_event_seq::text
                || ',"prev_hash":' || to_json(p_prev_hash)::text
                || ',"correlation_event_uuid":' || to_json(p_correlation_event_uuid::text)::text
                || ',"run_nonce":' || to_json(p_run_nonce)::text
                || ',"entity_type":' || to_json(p_entity_type)::text
                || ',"entity_id":' || to_json(p_entity_id)::text
                || ',"from_state":' || to_json(p_from_state)::text
                || ',"to_state":' || to_json(p_to_state)::text
                || ',"fencing_token":' || p_fencing_token::text
                || ',"residency_seq":' || p_residency_seq::text
                || ',"gpu_epoch":' || p_gpu_epoch::text
                || ',"budget_mib":' || p_budget_mib::text
                || ',"reason_code":' || to_json(p_reason_code)::text
                || ',"payload":[' || COALESCE((
                    SELECT string_agg('[' || to_json(e.key)::text || ',' || station.canonical_jsonb(e.value) || ']', ',' ORDER BY e.key COLLATE "C")
                    FROM jsonb_each(p_payload) AS e
                ), '') || ']}',
                'UTF8'
            )
        ),
        'hex'
    );
$$;

CREATE FUNCTION station.protect_gpu_card_chain_head() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    expected_token text;
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'gpu_physical_cards cannot be truncated';
    END IF;
    IF TG_OP = 'DELETE' THEN
        IF OLD.last_event_seq > 0 THEN
            RAISE EXCEPTION 'cannot delete gpu_physical_cards row with non-empty event chain';
        END IF;
        RETURN OLD;
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.last_event_seq <> 0 OR NEW.last_event_hash <> 'GENESIS' THEN
            RAISE EXCEPTION 'new gpu_physical_cards row must start at seq=0 hash=GENESIS';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.station_id IS DISTINCT FROM OLD.station_id
       OR NEW.node_uid IS DISTINCT FROM OLD.node_uid
       OR NEW.gpu_uuid IS DISTINCT FROM OLD.gpu_uuid THEN
        RAISE EXCEPTION 'gpu_physical_cards identity columns are immutable';
    END IF;
    IF NEW.last_event_seq IS DISTINCT FROM OLD.last_event_seq
       OR NEW.last_event_hash IS DISTINCT FROM OLD.last_event_hash THEN
        expected_token := NEW.station_id::text || '|' || NEW.node_uid || '|' || NEW.gpu_uuid
            || '|' || NEW.last_event_seq::text || '|' || NEW.last_event_hash;
        IF pg_trigger_depth() < 2
           OR current_setting('station.chain_advance_target', true) IS DISTINCT FROM expected_token
           OR NEW.last_event_seq <> OLD.last_event_seq + 1 THEN
            RAISE EXCEPTION 'direct mutation of gpu_physical_cards chain head is forbidden';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER gpu_physical_cards_protect_chain_head
BEFORE INSERT OR UPDATE OR DELETE ON station.gpu_physical_cards
FOR EACH ROW EXECUTE FUNCTION station.protect_gpu_card_chain_head();

CREATE TRIGGER gpu_physical_cards_forbid_truncate
BEFORE TRUNCATE ON station.gpu_physical_cards
FOR EACH STATEMENT EXECUTE FUNCTION station.protect_gpu_card_chain_head();

CREATE FUNCTION station.enforce_gpu_card_event_chain() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    expected_seq bigint;
    expected_prev text;
    expected_hash text;
    advance_token text;
BEGIN
    SELECT last_event_seq, last_event_hash
      INTO expected_seq, expected_prev
      FROM station.gpu_physical_cards
     WHERE station_id = NEW.station_id
       AND node_uid = NEW.node_uid
       AND gpu_uuid = NEW.gpu_uuid
       FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'gpu physical card missing for event chain';
    END IF;

    IF NEW.card_event_seq <> expected_seq + 1 THEN
        RAISE EXCEPTION 'gpu card_event_seq mismatch: got %, expected %', NEW.card_event_seq, expected_seq + 1;
    END IF;

    IF NEW.prev_hash <> expected_prev THEN
        RAISE EXCEPTION 'gpu prev_hash mismatch: got %, expected %', NEW.prev_hash, expected_prev;
    END IF;

    expected_hash := station.canonical_gpu_event_hash(
        NEW.station_id,
        NEW.node_uid,
        NEW.gpu_uuid,
        NEW.card_event_seq,
        NEW.prev_hash,
        NEW.correlation_event_uuid,
        NEW.run_nonce,
        NEW.entity_type,
        NEW.entity_id,
        NEW.from_state,
        NEW.to_state,
        NEW.fencing_token,
        NEW.residency_seq,
        NEW.gpu_epoch,
        NEW.budget_mib,
        NEW.reason_code,
        NEW.payload
    );

    IF NEW.event_hash <> expected_hash THEN
        RAISE EXCEPTION 'gpu event_hash cryptographic mismatch: got %, expected %', NEW.event_hash, expected_hash;
    END IF;

    advance_token := NEW.station_id::text || '|' || NEW.node_uid || '|' || NEW.gpu_uuid
        || '|' || NEW.card_event_seq::text || '|' || NEW.event_hash;
    PERFORM set_config('station.chain_advance_target', advance_token, true);

    UPDATE station.gpu_physical_cards
       SET last_event_seq = NEW.card_event_seq,
           last_event_hash = NEW.event_hash,
           updated_at = now()
     WHERE station_id = NEW.station_id
       AND node_uid = NEW.node_uid
       AND gpu_uuid = NEW.gpu_uuid;

    PERFORM set_config('station.chain_advance_target', '', true);

    RETURN NEW;
END;
$$;

CREATE TRIGGER gpu_scheduler_events_chain_advance
BEFORE INSERT ON station.gpu_scheduler_events
FOR EACH ROW EXECUTE FUNCTION station.enforce_gpu_card_event_chain();

CREATE FUNCTION station.reject_gpu_scheduler_event_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'gpu_scheduler_events are append-only';
END;
$$;

CREATE TRIGGER gpu_scheduler_events_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON station.gpu_scheduler_events
FOR EACH STATEMENT EXECUTE FUNCTION station.reject_gpu_scheduler_event_mutation();

DO $$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY['vf_t4_runner', 'vf_t4_auditor', 'vf_gpu_runtime'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            BEGIN
                EXECUTE format('CREATE ROLE %I NOLOGIN', role_name);
            EXCEPTION
                WHEN duplicate_object OR unique_violation THEN
                    NULL;
            END;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            RAISE EXCEPTION 'mandatory G2 role % does not exist', role_name;
        END IF;
    END LOOP;
END;
$$;

REVOKE ALL ON
    station.gpu_physical_cards,
    station.gpu_resource_leases,
    station.gpu_instance_residency,
    station.gpu_scheduler_events
FROM PUBLIC;

REVOKE ALL ON
    station.gpu_physical_cards,
    station.gpu_resource_leases,
    station.gpu_instance_residency,
    station.gpu_scheduler_events
FROM vf_t4_runner;
REVOKE ALL ON SEQUENCE station.gpu_scheduler_events_event_id_seq FROM vf_t4_runner;

REVOKE ALL ON
    station.gpu_physical_cards,
    station.gpu_resource_leases,
    station.gpu_instance_residency,
    station.gpu_scheduler_events
FROM vf_t4_auditor;
GRANT USAGE ON SCHEMA station TO vf_t4_auditor;
GRANT SELECT ON
    station.configuration,
    station.gpu_physical_cards,
    station.gpu_resource_leases,
    station.gpu_instance_residency,
    station.gpu_scheduler_events
TO vf_t4_auditor;

REVOKE ALL ON
    station.gpu_physical_cards,
    station.gpu_resource_leases,
    station.gpu_instance_residency,
    station.gpu_scheduler_events
FROM vf_gpu_runtime;
GRANT USAGE ON SCHEMA station TO vf_gpu_runtime;
GRANT SELECT ON
    station.configuration,
    station.app_installations,
    station.app_instance_workspaces,
    station.app_instance_starts,
    station.app_instance_stops,
    station.runtime_instance_workspaces,
    station.runtime_instance_executions
TO vf_gpu_runtime;
DO $$
BEGIN
    IF to_regclass('station.schema_migrations') IS NOT NULL THEN
        EXECUTE 'GRANT SELECT ON station.schema_migrations TO vf_gpu_runtime';
    END IF;
END;
$$;
GRANT SELECT, INSERT, UPDATE ON
    station.gpu_physical_cards,
    station.gpu_resource_leases,
    station.gpu_instance_residency
TO vf_gpu_runtime;
GRANT SELECT, INSERT ON station.gpu_scheduler_events TO vf_gpu_runtime;
GRANT USAGE, SELECT ON SEQUENCE station.gpu_scheduler_events_event_id_seq TO vf_gpu_runtime;
GRANT EXECUTE ON FUNCTION
    station.canonical_jsonb(jsonb),
    station.canonical_gpu_event_hash(uuid, text, text, bigint, text, uuid, text, text, text, text, text, bigint, bigint, bigint, bigint, text, jsonb)
TO vf_t4_auditor, vf_gpu_runtime;
