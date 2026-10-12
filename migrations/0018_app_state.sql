-- Core owns the durable application read model. etcd receives only revision hints.
CREATE TABLE station.app_state_scopes (
 station_id uuid NOT NULL REFERENCES station.configuration(station_id),
 organization_id uuid NOT NULL REFERENCES station.organizations(organization_id),
 revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
 published_revision bigint NOT NULL DEFAULT 0 CHECK (published_revision BETWEEN 0 AND revision),
 changed_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(station_id,organization_id)
);
CREATE TABLE station.app_state_items (
 station_id uuid NOT NULL,
 organization_id uuid NOT NULL,
 app_id text NOT NULL CHECK (length(app_id) BETWEEN 1 AND 128),
 revision bigint NOT NULL CHECK (revision > 0),
 PRIMARY KEY(station_id,organization_id,app_id),
 FOREIGN KEY(station_id,organization_id) REFERENCES station.app_state_scopes(station_id,organization_id)
);
CREATE TABLE station.app_state_catalog (
 station_id uuid NOT NULL REFERENCES station.configuration(station_id),
 app_id text NOT NULL CHECK (app_id ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$' AND length(app_id)<=63),
 PRIMARY KEY(station_id,app_id)
);
CREATE TABLE station.app_state_observations (
 installation_id uuid PRIMARY KEY REFERENCES station.app_installations(installation_id),
 workload_uid text,
 state text NOT NULL CHECK (state IN ('unknown','absent','starting','ready','degraded','stopping','stopped')),
 reason_code text NOT NULL CHECK (reason_code ~ '^[A-Z][A-Z0-9_]{0,63}$'),
 received_at timestamptz NOT NULL,
 expires_at timestamptz NOT NULL CHECK (expires_at > received_at),
 expired boolean NOT NULL DEFAULT false,
 CHECK (workload_uid IS NULL OR length(workload_uid) BETWEEN 1 AND 128),
 CHECK (state NOT IN ('starting','ready','degraded','stopping','stopped') OR workload_uid IS NOT NULL)
);
CREATE INDEX app_state_observations_expiry ON station.app_state_observations(expires_at) WHERE NOT expired;

CREATE FUNCTION station.mark_app_state(s uuid, o uuid, a text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE v bigint;
BEGIN
 INSERT INTO station.app_state_scopes(station_id,organization_id,revision) VALUES(s,o,1)
 ON CONFLICT(station_id,organization_id) DO UPDATE SET revision=station.app_state_scopes.revision+1,changed_at=clock_timestamp()
 RETURNING revision INTO v;
 INSERT INTO station.app_state_items(station_id,organization_id,app_id,revision) VALUES(s,o,a,v)
 ON CONFLICT(station_id,organization_id,app_id) DO UPDATE SET revision=EXCLUDED.revision;
END $$;

CREATE FUNCTION station.app_state_business_changed() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE n jsonb; p jsonb; f text[];
BEGIN
 IF TG_OP='DELETE' THEN n:=to_jsonb(OLD); ELSE n:=to_jsonb(NEW); END IF;
 IF TG_OP='UPDATE' THEN
  p:=to_jsonb(OLD);
  IF TG_TABLE_NAME='app_installations' THEN
   f:=ARRAY['station_id','organization_id','app_id','installation_id','app_version','state','revision','last_operation_id'];
  ELSE
   f:=ARRAY['station_id','organization_id','app_id','installation_id','operation_id','user_id','status','phase'];
  END IF;
  -- Poll timestamps and byte counters do not invalidate the state snapshot.
  IF (SELECT jsonb_object_agg(key,value) FROM jsonb_each(n) WHERE key=ANY(f))
     IS NOT DISTINCT FROM (SELECT jsonb_object_agg(key,value) FROM jsonb_each(p) WHERE key=ANY(f)) THEN
   RETURN NEW;
  END IF;
  IF (n->>'station_id',n->>'organization_id',n->>'app_id') IS DISTINCT FROM
     (p->>'station_id',p->>'organization_id',p->>'app_id') THEN
   RAISE EXCEPTION 'immutable application state scope';
  END IF;
 END IF;
 PERFORM station.mark_app_state((n->>'station_id')::uuid,(n->>'organization_id')::uuid,n->>'app_id');
 RETURN NULL;
END $$;
CREATE TRIGGER app_state_installation_changed AFTER INSERT OR UPDATE OR DELETE ON station.app_installations
 FOR EACH ROW EXECUTE FUNCTION station.app_state_business_changed();
CREATE TRIGGER app_state_operation_changed AFTER INSERT OR UPDATE OR DELETE ON station.app_operations
 FOR EACH ROW EXECUTE FUNCTION station.app_state_business_changed();
CREATE TRIGGER app_state_transition_changed AFTER INSERT OR UPDATE OR DELETE ON station.app_installation_transitions
 FOR EACH ROW EXECUTE FUNCTION station.app_state_business_changed();

CREATE FUNCTION station.app_state_observation_changed() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE i station.app_installations%ROWTYPE;
BEGIN
 IF TG_OP='DELETE' THEN
  SELECT * INTO STRICT i FROM station.app_installations WHERE installation_id=OLD.installation_id;
  PERFORM station.mark_app_state(i.station_id,i.organization_id,i.app_id);
  RETURN OLD;
 END IF;
 IF TG_OP='UPDATE' THEN
  IF NEW.installation_id<>OLD.installation_id THEN RAISE EXCEPTION 'immutable observation identity'; END IF;
  IF (NEW.workload_uid,NEW.state,NEW.reason_code,NEW.expired) IS NOT DISTINCT FROM
     (OLD.workload_uid,OLD.state,OLD.reason_code,OLD.expired)
     AND NOT (OLD.expires_at<=clock_timestamp() AND NEW.expires_at>clock_timestamp()) THEN RETURN NEW; END IF;
 END IF;
 SELECT * INTO STRICT i FROM station.app_installations WHERE installation_id=NEW.installation_id;
 PERFORM station.mark_app_state(i.station_id,i.organization_id,i.app_id);
 RETURN NEW;
END $$;
CREATE TRIGGER app_state_observation_changed AFTER INSERT OR UPDATE OR DELETE ON station.app_state_observations
 FOR EACH ROW EXECUTE FUNCTION station.app_state_observation_changed();

-- Backfill revisions, never invented observations or installation facts.
DO $$
DECLARE x record;
BEGIN
 FOR x IN SELECT DISTINCT station_id,organization_id,app_id FROM (
  SELECT station_id,organization_id,app_id FROM station.app_installations
  UNION SELECT station_id,organization_id,app_id FROM station.app_operations
  UNION SELECT station_id,organization_id,app_id FROM station.app_installation_transitions
 ) q ORDER BY station_id,organization_id,app_id LOOP
  PERFORM station.mark_app_state(x.station_id,x.organization_id,x.app_id);
 END LOOP;
END $$;
