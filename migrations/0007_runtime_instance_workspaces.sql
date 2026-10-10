-- Dedicated Blender instance storage. App installation executions do not own these leases.
-- Rows survive instance destruction; retained data continues consuming a pool slot.
CREATE TABLE station.runtime_instance_workspaces (
 instance_id uuid PRIMARY KEY,
 station_id uuid NOT NULL REFERENCES station.configuration(station_id),
 organization_id uuid NOT NULL REFERENCES station.organizations(organization_id),
 project_id uuid NOT NULL,
 create_operation_id uuid NOT NULL UNIQUE,
 profile_id text NOT NULL,
 profile_hash bytea NOT NULL CHECK (octet_length(profile_hash)=32),
 namespace text NOT NULL,
 pool_name text NOT NULL,
 pvc_name text NOT NULL,
 pvc_uid text NOT NULL UNIQUE,
 pv_name text NOT NULL,
 pv_uid text NOT NULL UNIQUE,
 node_name text NOT NULL,
 reserved_bytes bigint NOT NULL CHECK (reserved_bytes > 0),
 retained_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(station_id,namespace,pvc_name)
);
