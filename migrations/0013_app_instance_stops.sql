CREATE TABLE station.app_instance_stops (
 operation_id uuid PRIMARY KEY,
 start_operation_id uuid NOT NULL UNIQUE REFERENCES station.app_instance_starts(operation_id),
 boot_id uuid NOT NULL,
 phase text NOT NULL CHECK (phase IN ('accepted','sealed','pod_delete_requested','pod_deleted','service_delete_requested','stopped')),
 proof jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(proof)='object'),
 revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 CHECK (operation_id <> start_operation_id),
 CHECK ((phase='accepted' AND proof='{}') OR (phase<>'accepted' AND proof<>'{}'))
);
