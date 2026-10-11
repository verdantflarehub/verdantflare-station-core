-- Runtime preparation is not publication. Core owns the installation version
-- and terminal outcome; checkpoints are private recovery material.
CREATE TABLE station.runtime_management_transitions (
 operation_id uuid PRIMARY KEY REFERENCES station.app_installation_transitions(operation_id),
 station_id uuid NOT NULL REFERENCES station.configuration(station_id),
 app_id text NOT NULL,
 input_hash bytea NOT NULL CHECK (octet_length(input_hash)=32),
 phase text NOT NULL CHECK (phase IN ('checking','sealing','applying','verifying','prepared','restoring','restored','recovery_required')),
 checkpoint jsonb CHECK (jsonb_typeof(checkpoint)='object'),
 revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 CHECK (phase IN ('checking','restored') OR checkpoint IS NOT NULL)
);
CREATE INDEX runtime_management_transition_app ON station.runtime_management_transitions(station_id,app_id);
