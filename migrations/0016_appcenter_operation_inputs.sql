-- Original AppCenter inputs and verified execution identity are committed with
-- the operation. Historical operations and their request hashes are untouched.
CREATE TABLE station.appcenter_operation_inputs (
    operation_id uuid PRIMARY KEY REFERENCES station.app_operations(operation_id),
    prepared jsonb NOT NULL CHECK (jsonb_typeof(prepared) = 'object'),
    execution jsonb NOT NULL CHECK (jsonb_typeof(execution) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (octet_length(prepared::text) <= 16384),
    CHECK (octet_length(execution::text) <= 4096)
);
