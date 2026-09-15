CREATE TABLE station.configuration (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    station_id uuid NOT NULL UNIQUE,
    contracts_major integer NOT NULL CHECK (contracts_major = 1),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE station.users (
    user_id uuid PRIMARY KEY,
    username text NOT NULL UNIQUE CHECK (username = lower(username)),
    password_hash text NOT NULL,
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    failed_logins integer NOT NULL DEFAULT 0 CHECK (failed_logins >= 0),
    locked_until timestamptz,
    revocation_version bigint NOT NULL DEFAULT 0 CHECK (revocation_version >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE station.organizations (
    organization_id uuid PRIMARY KEY,
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 128),
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    policy_version bigint NOT NULL DEFAULT 1 CHECK (policy_version > 0),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE station.memberships (
    user_id uuid NOT NULL REFERENCES station.users(user_id),
    organization_id uuid NOT NULL REFERENCES station.organizations(organization_id),
    role text NOT NULL CHECK (role = 'admin'),
    active boolean NOT NULL DEFAULT true,
    PRIMARY KEY (user_id, organization_id)
);
CREATE TABLE station.sessions (
    session_id uuid PRIMARY KEY,
    user_id uuid NOT NULL,
    organization_id uuid NOT NULL,
    token_hash bytea NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
    issued_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (expires_at > issued_at),
    revocation_version bigint NOT NULL CHECK (revocation_version >= 0),
    revoked_at timestamptz,
    FOREIGN KEY (user_id, organization_id) REFERENCES station.memberships(user_id, organization_id)
);
CREATE INDEX sessions_user ON station.sessions(user_id);
CREATE INDEX sessions_expiry ON station.sessions(expires_at);
