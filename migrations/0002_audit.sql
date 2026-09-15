CREATE TABLE station.audit_events (
    event_id uuid PRIMARY KEY,
    request_id text NOT NULL CHECK (length(request_id) BETWEEN 1 AND 128),
    user_id uuid REFERENCES station.users(user_id),
    organization_id uuid REFERENCES station.organizations(organization_id),
    session_id uuid REFERENCES station.sessions(session_id),
    action text NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('succeeded', 'denied')),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX audit_request ON station.audit_events(request_id);
CREATE FUNCTION station.reject_audit_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit events are append-only';
END;
$$;
CREATE TRIGGER audit_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON station.audit_events
FOR EACH STATEMENT EXECUTE FUNCTION station.reject_audit_mutation();
