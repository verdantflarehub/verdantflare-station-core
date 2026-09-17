-- Core owns app_operations; only Runtime APIs write runtime_app_executions.
CREATE TABLE station.runtime_app_executions (
 operation_id uuid PRIMARY KEY,
 station_id uuid NOT NULL REFERENCES station.configuration(station_id),
 organization_id uuid NOT NULL REFERENCES station.organizations(organization_id),
 user_id uuid NOT NULL REFERENCES station.users(user_id),
 request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
 app_id text NOT NULL,
 app_version text NOT NULL,
 action text NOT NULL CHECK (action IN ('install','start','stop','restart','delete')),
 target_hash bytea NOT NULL CHECK (octet_length(target_hash)=32),
 status text NOT NULL CHECK (status IN ('running','succeeded','failed')),
 phase text NOT NULL,
 error_code text NOT NULL DEFAULT '',
 error_message text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX runtime_app_pending ON station.runtime_app_executions(station_id,app_id) WHERE status='running';
