-- ComfyUI unified model catalog, 7-state singleflight CAS cache inventory,
-- monotonic download_fence protection, cross-DB two-phase model_ref_pins,
-- caller-scoped model.ensure idempotency, statvfs storage watermark,
-- and mandatory G2 three-role isolation grants.

CREATE TABLE station.model_catalog_entries (
    station_id uuid NOT NULL REFERENCES station.configuration(station_id),
    model_id text NOT NULL CHECK (model_id ~ '^[a-zA-Z0-9_-]{1,80}$'),
    catalog_code text NOT NULL CHECK (catalog_code ~ '^MDL-(CKPT|UNET|CLIP|VAE|LORA|CNET|UPSC|IPAD)-[A-Z0-9]{2,24}-[0-9]{3}$'),
    display_name text NOT NULL CHECK (length(btrim(display_name)) BETWEEN 2 AND 160),
    category text NOT NULL CHECK (category IN ('CKPT', 'UNET', 'CLIP', 'VAE', 'LORA', 'CNET', 'UPSC', 'IPAD')),
    filename text NOT NULL CHECK (filename ~ '^[A-Za-z0-9._-]{1,120}\.safetensors$' AND position('..' IN filename) = 0),
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    size_bytes bigint NOT NULL CHECK (size_bytes BETWEEN 1 AND 107374182400),
    vram_footprint_mib bigint NOT NULL CHECK (vram_footprint_mib BETWEEN 1 AND 29696),
    license text NOT NULL CHECK (length(btrim(license)) BETWEEN 1 AND 64),
    license_tier text NOT NULL DEFAULT 'commercial_free' CHECK (license_tier IN ('commercial_free', 'research_non_commercial', 'paid_forbidden')),
    paid_access boolean NOT NULL DEFAULT false,
    origin_kind text NOT NULL DEFAULT 'public' CHECK (origin_kind IN ('public', 'local')),
    approval_state text NOT NULL DEFAULT 'approved' CHECK (approval_state IN ('approved', 'pending_admin', 'rejected')),
    source_urls jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(source_urls) = 'array'),
    source_policy jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(source_policy) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, model_id),
    UNIQUE (station_id, catalog_code),
    CHECK (split_part(catalog_code, '-', 2) = category),
    CHECK (NOT (split_part(catalog_code, '-', 3) = 'LOCAL' AND origin_kind <> 'local')),
    CHECK (NOT paid_access OR (license_tier = 'paid_forbidden' AND approval_state <> 'approved'))
);

CREATE INDEX model_catalog_entries_sha256_idx
    ON station.model_catalog_entries (station_id, sha256);

CREATE INDEX model_catalog_entries_filter_idx
    ON station.model_catalog_entries (station_id, category, origin_kind, catalog_code);

CREATE TABLE station.model_cache_blobs (
    station_id uuid NOT NULL REFERENCES station.configuration(station_id),
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    size_bytes bigint NOT NULL CHECK (size_bytes > 0),
    paid_access boolean NOT NULL DEFAULT false CHECK (paid_access = false),
    status text NOT NULL DEFAULT 'not_installed' CHECK (status IN (
        'not_installed',
        'queued',
        'downloading',
        'verifying_sha256',
        'ready',
        'failed',
        'evicting'
    )),
    download_fence bigint NOT NULL DEFAULT 0 CHECK (download_fence >= 0),
    evict_fence bigint NOT NULL DEFAULT 0 CHECK (evict_fence >= 0),
    active_evict_id uuid,
    bytes_downloaded bigint NOT NULL DEFAULT 0 CHECK (bytes_downloaded >= 0 AND bytes_downloaded <= size_bytes),
    part_bytes_on_disk bigint NOT NULL DEFAULT 0 CHECK (part_bytes_on_disk >= 0 AND part_bytes_on_disk <= size_bytes),
    speed_bps bigint NOT NULL DEFAULT 0 CHECK (speed_bps >= 0),
    eta_seconds integer CHECK (eta_seconds IS NULL OR eta_seconds >= 0),
    etag text NOT NULL DEFAULT '',
    published_views jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(published_views) = 'array'),
    pending_views jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(pending_views) = 'array'),
    cas_inode bigint CHECK (cas_inode IS NULL OR cas_inode >= 0),
    verified_at timestamptz,
    last_error_code text NOT NULL DEFAULT '',
    last_error_message text NOT NULL DEFAULT '',
    lease_owner text NOT NULL DEFAULT '',
    lease_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, sha256),
    CHECK ((status = 'evicting') = (active_evict_id IS NOT NULL))
);

CREATE INDEX model_cache_blobs_status_idx
    ON station.model_cache_blobs (station_id, status);

CREATE FUNCTION station.protect_model_cache_blob_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'model_cache_blobs cannot be truncated';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'model_cache_blobs rows cannot be deleted; transition status to not_installed instead';
    END IF;
    IF NEW.station_id IS DISTINCT FROM OLD.station_id
       OR NEW.sha256 IS DISTINCT FROM OLD.sha256 THEN
        RAISE EXCEPTION 'model_cache_blobs identity columns are immutable';
    END IF;
    IF NEW.download_fence < OLD.download_fence THEN
        RAISE EXCEPTION 'model_cache_blobs download_fence must be monotonic: old=% new=%', OLD.download_fence, NEW.download_fence;
    END IF;
    IF NEW.evict_fence < OLD.evict_fence THEN
        RAISE EXCEPTION 'model_cache_blobs evict_fence must be monotonic: old=% new=%', OLD.evict_fence, NEW.evict_fence;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER model_cache_blobs_protect_fence
BEFORE UPDATE OR DELETE ON station.model_cache_blobs
FOR EACH ROW EXECUTE FUNCTION station.protect_model_cache_blob_fence();

CREATE TRIGGER model_cache_blobs_forbid_truncate
BEFORE TRUNCATE ON station.model_cache_blobs
FOR EACH STATEMENT EXECUTE FUNCTION station.protect_model_cache_blob_fence();

CREATE TABLE station.model_ref_pins (
    pin_id uuid PRIMARY KEY,
    station_id uuid NOT NULL REFERENCES station.configuration(station_id),
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    holder_app_id text NOT NULL DEFAULT 'comfyui' CHECK (length(btrim(holder_app_id)) BETWEEN 1 AND 64),
    reservation_id text NOT NULL CHECK (length(btrim(reservation_id)) BETWEEN 1 AND 128),
    operation_id text NOT NULL CHECK (length(btrim(operation_id)) BETWEEN 1 AND 128),
    scope_kind text NOT NULL CHECK (scope_kind IN ('task_execution', 'published_api')),
    owner_id text NOT NULL CHECK (length(btrim(owner_id)) BETWEEN 1 AND 256),
    organization_id text NOT NULL DEFAULT '',
    project_id text NOT NULL DEFAULT '',
    request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    pin_state text NOT NULL CHECK (pin_state IN ('reserved', 'active', 'released')),
    release_reason text NOT NULL DEFAULT '',
    expires_at timestamptz,
    committed_at timestamptz,
    released_at timestamptz,
    last_reconcile_checked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (station_id, holder_app_id, reservation_id, sha256),
    FOREIGN KEY (station_id, sha256)
        REFERENCES station.model_cache_blobs(station_id, sha256),
    CHECK (
        (pin_state = 'reserved' AND expires_at IS NOT NULL AND committed_at IS NULL AND released_at IS NULL)
        OR (pin_state = 'active' AND expires_at IS NULL AND committed_at IS NOT NULL AND released_at IS NULL)
        OR (pin_state = 'released' AND released_at IS NOT NULL)
    )
);

CREATE INDEX model_ref_pins_live_blob_idx
    ON station.model_ref_pins (station_id, sha256, pin_state)
    WHERE pin_state IN ('reserved', 'active');

CREATE INDEX model_ref_pins_expired_reserved_idx
    ON station.model_ref_pins (station_id, COALESCE(last_reconcile_checked_at, created_at), expires_at, reservation_id)
    WHERE pin_state = 'reserved';

CREATE INDEX model_ref_pins_active_task_idx
    ON station.model_ref_pins (station_id, COALESCE(last_reconcile_checked_at, committed_at, created_at), reservation_id)
    WHERE pin_state = 'active' AND scope_kind = 'task_execution';

CREATE TABLE station.model_ensure_idempotency (
    station_id uuid NOT NULL REFERENCES station.configuration(station_id),
    organization_id text NOT NULL CHECK (length(btrim(organization_id)) BETWEEN 1 AND 128),
    project_id text NOT NULL CHECK (length(btrim(project_id)) BETWEEN 1 AND 128),
    principal_id text NOT NULL CHECK (length(btrim(principal_id)) BETWEEN 1 AND 128),
    target_alias text NOT NULL DEFAULT 'default' CHECK (length(btrim(target_alias)) BETWEEN 1 AND 128),
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 8 AND 128 AND idempotency_key !~ '\s'),
    claim_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    state text NOT NULL DEFAULT 'completed' CHECK (state IN ('in_progress', 'completed')),
    request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    model_id text NOT NULL CHECK (model_id ~ '^[a-zA-Z0-9_-]{1,80}$'),
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    download_fence bigint NOT NULL DEFAULT 0 CHECK (download_fence >= 0),
    response_status integer NOT NULL CHECK (
        (state = 'in_progress' AND response_status = 0)
        OR (state = 'completed' AND response_status BETWEEN 200 AND 599)
    ),
    response_json jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(response_json) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL DEFAULT (now() + interval '24 hours'),
    PRIMARY KEY (station_id, organization_id, project_id, principal_id, target_alias, idempotency_key)
);

CREATE TABLE station.model_cache_storage_state (
    station_id uuid PRIMARY KEY REFERENCES station.configuration(station_id),
    filesystem_total_bytes bigint NOT NULL CHECK (filesystem_total_bytes >= 0),
    filesystem_free_bytes bigint NOT NULL CHECK (filesystem_free_bytes >= 0),
    reserved_headroom_bytes bigint NOT NULL DEFAULT 214748364800 CHECK (reserved_headroom_bytes >= 214748364800),
    max_auto_pull_model_bytes bigint NOT NULL DEFAULT 26843545600 CHECK (max_auto_pull_model_bytes BETWEEN 1 AND 26843545600),
    sampled_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

REVOKE ALL ON
    station.model_catalog_entries,
    station.model_cache_blobs,
    station.model_ref_pins,
    station.model_ensure_idempotency,
    station.model_cache_storage_state
FROM PUBLIC;

REVOKE ALL ON
    station.model_catalog_entries,
    station.model_cache_blobs,
    station.model_ref_pins,
    station.model_ensure_idempotency,
    station.model_cache_storage_state
FROM vf_t4_runner;

REVOKE ALL ON
    station.model_catalog_entries,
    station.model_cache_blobs,
    station.model_ref_pins,
    station.model_ensure_idempotency,
    station.model_cache_storage_state
FROM vf_t4_auditor;
GRANT USAGE ON SCHEMA station TO vf_t4_auditor;
GRANT SELECT ON
    station.model_catalog_entries,
    station.model_cache_blobs,
    station.model_ref_pins,
    station.model_ensure_idempotency,
    station.model_cache_storage_state
TO vf_t4_auditor;

REVOKE ALL ON
    station.model_catalog_entries,
    station.model_cache_blobs,
    station.model_ref_pins,
    station.model_ensure_idempotency,
    station.model_cache_storage_state
FROM vf_gpu_runtime;
GRANT USAGE ON SCHEMA station TO vf_gpu_runtime;
GRANT SELECT, INSERT, UPDATE ON
    station.model_catalog_entries,
    station.model_cache_blobs,
    station.model_ref_pins,
    station.model_ensure_idempotency,
    station.model_cache_storage_state
TO vf_gpu_runtime;
GRANT DELETE ON station.model_ensure_idempotency TO vf_gpu_runtime;
