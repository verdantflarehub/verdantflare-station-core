-- Fixed release evidence for opt-in preinstalled commands. Historical v1 rows
-- remain unbound; no current registry value is backfilled into old operations.
CREATE TABLE station.preinstalled_operation_bindings (
 operation_id uuid PRIMARY KEY REFERENCES station.app_operations(operation_id),
 binding jsonb NOT NULL CHECK (jsonb_typeof(binding) = 'object'),
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE station.preinstalled_installation_bindings (
 installation_id uuid PRIMARY KEY REFERENCES station.app_installations(installation_id),
 binding jsonb NOT NULL CHECK (jsonb_typeof(binding) = 'object'),
 created_at timestamptz NOT NULL DEFAULT now()
);
