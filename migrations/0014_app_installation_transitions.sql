-- Version transitions are separate from historical AppCommand operations.
CREATE TABLE station.app_installation_transitions (
 operation_id uuid PRIMARY KEY,
 installation_id uuid NOT NULL REFERENCES station.app_installations(installation_id),
 user_id uuid NOT NULL REFERENCES station.users(user_id),
 organization_id uuid NOT NULL REFERENCES station.organizations(organization_id),
 station_id uuid NOT NULL REFERENCES station.configuration(station_id),
 app_id text NOT NULL,
 request_id text NOT NULL,
 idempotency_key text NOT NULL,
 request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
 action text NOT NULL CHECK (action IN ('upgrade','rollback')),
 expected_revision bigint NOT NULL CHECK (expected_revision > 0),
 source_version text NOT NULL,
 target_version text NOT NULL,
 source_snapshot jsonb NOT NULL CHECK (jsonb_typeof(source_snapshot)='object'),
 target_snapshot jsonb NOT NULL CHECK (jsonb_typeof(target_snapshot)='object'),
 status text NOT NULL CHECK (status IN ('accepted','running','succeeded','failed','recovery_required')),
 phase text NOT NULL CHECK (phase IN ('accepted','checking','sealing','applying','verifying','publishing','releasing','restoring','recovery_required','completed')),
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(user_id,organization_id,station_id,idempotency_key),
 CHECK (source_version <> target_version),
 CHECK ((status IN ('succeeded','failed')) = (phase='completed')),
 CHECK ((status='accepted') = (phase='accepted')),
 CHECK ((status='recovery_required') = (phase='recovery_required'))
);
CREATE UNIQUE INDEX app_installation_transition_active ON station.app_installation_transitions(installation_id)
 WHERE status NOT IN ('succeeded','failed');
CREATE INDEX app_installation_transition_scope ON station.app_installation_transitions(station_id,organization_id,user_id,created_at);
