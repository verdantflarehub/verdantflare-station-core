-- Core owns installation identities. Runtime execution and AppInstance IDs are separate.
CREATE TABLE station.app_installations (
 installation_id uuid PRIMARY KEY,
 created_by uuid NOT NULL REFERENCES station.users(user_id),
 organization_id uuid NOT NULL REFERENCES station.organizations(organization_id),
 station_id uuid NOT NULL REFERENCES station.configuration(station_id),
 app_id text NOT NULL CHECK (length(app_id) BETWEEN 1 AND 128),
 app_version text NOT NULL CHECK (app_version ~ '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'),
 state text NOT NULL CHECK (state IN ('pending','installed','failed','removed')),
 revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
 last_operation_id uuid REFERENCES station.app_operations(operation_id),
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX app_installations_current ON station.app_installations(station_id,app_id) WHERE state <> 'removed';
CREATE INDEX app_installations_scope ON station.app_installations(station_id,organization_id,created_at);
ALTER TABLE station.app_operations ADD COLUMN installation_id uuid REFERENCES station.app_installations(installation_id);
