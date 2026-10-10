ALTER TABLE station.sessions
    ADD COLUMN credential_kind text NOT NULL DEFAULT 'session'
    CHECK (credential_kind IN ('session', 'mcp'));
