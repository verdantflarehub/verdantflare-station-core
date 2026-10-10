CREATE TABLE station.egress_proxies (
    proxy_id uuid PRIMARY KEY,
    station_id uuid NOT NULL REFERENCES station.configuration(station_id),
    organization_id uuid NOT NULL REFERENCES station.organizations(organization_id),
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
    protocol text NOT NULL CHECK (protocol IN ('http','https','socks5','socks5h')),
    host text NOT NULL,
    port integer NOT NULL CHECK (port BETWEEN 1 AND 65535),
    credentials bytea NOT NULL,
    endpoint_key text NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','active','off')),
    tag text NOT NULL DEFAULT '',
    note text NOT NULL DEFAULT '',
    expires_at timestamptz,
    assigned_apps text[] NOT NULL DEFAULT '{}',
    revision bigint NOT NULL DEFAULT 1,
    probe jsonb,
    probe_token uuid,
    probe_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(station_id, organization_id, endpoint_key)
);
CREATE INDEX egress_proxies_owner ON station.egress_proxies(station_id, organization_id, created_at);
