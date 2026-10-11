-- Installation-bound App workers; legacy Project-bound Blender rows are retained.
CREATE TABLE station.app_instance_workspaces (
 instance_id uuid PRIMARY KEY,
 station_id uuid NOT NULL REFERENCES station.configuration(station_id),
 organization_id uuid NOT NULL REFERENCES station.organizations(organization_id),
 user_id uuid NOT NULL REFERENCES station.users(user_id),
 installation_id uuid NOT NULL REFERENCES station.app_installations(installation_id),
 profile_hash text NOT NULL CHECK (profile_hash ~ '^[0-9a-f]{64}$'),
 namespace text NOT NULL,
 pvc_name text NOT NULL,
 pvc_uid text NOT NULL UNIQUE,
 pv_name text NOT NULL,
 pv_uid text NOT NULL UNIQUE,
 binding jsonb NOT NULL CHECK (jsonb_typeof(binding)='object'),
 created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(station_id,namespace,pvc_name),
 UNIQUE(station_id,pv_name)
);
CREATE TABLE station.app_instance_starts (
 operation_id uuid PRIMARY KEY,
 instance_id uuid NOT NULL REFERENCES station.app_instance_workspaces(instance_id),
 generation bigint NOT NULL CHECK (generation > 0),
 installation_revision bigint NOT NULL CHECK (installation_revision > 0),
 profile_hash text NOT NULL CHECK (profile_hash ~ '^[0-9a-f]{64}$'),
 input jsonb NOT NULL CHECK (jsonb_typeof(input)='object'),
 phase text NOT NULL CHECK (phase IN ('accepted','launching','running','stopping','stopped')),
 observation jsonb NOT NULL CHECK (jsonb_typeof(observation)='object'),
 revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(instance_id,generation)
);
CREATE UNIQUE INDEX app_instance_one_active_start ON station.app_instance_starts(instance_id) WHERE phase <> 'stopped';
