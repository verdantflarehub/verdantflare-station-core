CREATE TABLE station.runtime_instance_executions (
 operation_id uuid PRIMARY KEY,
 instance_id uuid NOT NULL,
 station_id uuid NOT NULL REFERENCES station.configuration(station_id),
 organization_id uuid NOT NULL REFERENCES station.organizations(organization_id),
 user_id uuid NOT NULL REFERENCES station.users(user_id),
 project_id uuid NOT NULL,
 action text NOT NULL CHECK (action IN ('create','start','stop','destroy')),
 request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
 profile_hash bytea NOT NULL CHECK (octet_length(profile_hash)=32),
 input jsonb NOT NULL CHECK (jsonb_typeof(input)='object'),
 status text NOT NULL CHECK (status IN ('running','succeeded','failed')),
 phase text NOT NULL,
 state jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(state)='object'),
 error_code text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX runtime_instance_one_active ON station.runtime_instance_executions(instance_id) WHERE status='running';
CREATE UNIQUE INDEX runtime_instance_one_creation ON station.runtime_instance_executions(instance_id) WHERE action='create';
