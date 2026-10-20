-- Append-only recovery inputs preserve the failed transition's original snapshots.
CREATE TABLE station.app_installation_forward_recoveries (
 recovery_id uuid PRIMARY KEY,
 operation_id uuid NOT NULL REFERENCES station.app_installation_transitions(operation_id),
 user_id uuid NOT NULL REFERENCES station.users(user_id),
 organization_id uuid NOT NULL REFERENCES station.organizations(organization_id),
 station_id uuid NOT NULL REFERENCES station.configuration(station_id),
 request_id text NOT NULL,
 idempotency_key text NOT NULL,
 request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
 expected_revision bigint NOT NULL CHECK (expected_revision > 0),
 installed_version text NOT NULL,
 parent_input_digest text NOT NULL,
 target_snapshot jsonb NOT NULL CHECK(jsonb_typeof(target_snapshot)='object'),
 input_digest text NOT NULL,
 state text NOT NULL CHECK(state IN ('pending','succeeded','failed')),
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(user_id,organization_id,station_id,idempotency_key)
);
CREATE UNIQUE INDEX app_installation_forward_recovery_pending
 ON station.app_installation_forward_recoveries(operation_id) WHERE state='pending';
CREATE TABLE station.runtime_forward_recovery_proofs (
 recovery_id uuid PRIMARY KEY REFERENCES station.app_installation_forward_recoveries(recovery_id),
 input_digest text NOT NULL,
 proof jsonb NOT NULL CHECK(jsonb_typeof(proof)='object'),
 verified_at timestamptz NOT NULL DEFAULT now()
);
