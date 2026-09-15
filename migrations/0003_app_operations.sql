CREATE TABLE station.app_operations (
 operation_id uuid PRIMARY KEY,
 request_id text NOT NULL CHECK (length(request_id) BETWEEN 1 AND 128),
 user_id uuid NOT NULL REFERENCES station.users(user_id),
 organization_id uuid NOT NULL REFERENCES station.organizations(organization_id),
 station_id uuid NOT NULL REFERENCES station.configuration(station_id),
 app_id text NOT NULL, app_version text NOT NULL,
 action text NOT NULL CHECK (action IN ('install','start','stop','restart','delete')),
 status text NOT NULL CHECK (status IN ('accepted','running','succeeded','failed')),
 phase text NOT NULL CHECK (phase IN ('checking','downloading','verifying','installing','starting','warming','stopping','deleting','completed')),
 downloaded_bytes bigint NOT NULL DEFAULT 0 CHECK (downloaded_bytes >= 0),
 total_bytes bigint CHECK (total_bytes >= 0),
 error_code text, error_message text,
 idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 8 AND 128),
 request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
 updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(user_id,organization_id,station_id,idempotency_key)
);
CREATE INDEX app_operations_app ON station.app_operations(organization_id,app_id,updated_at DESC);
