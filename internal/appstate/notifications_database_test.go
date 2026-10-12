package appstate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func TestNotificationDatabaseTracksStateWithoutWriting(t *testing.T) {
	s, u, id := fixture(t)
	revisions, err := s.NotificationRevisions(t.Context())
	if err != nil || !strings.HasPrefix(revisions[u.OrganizationID], "hint:") || revisions[u.OrganizationID] == snapshot(t, s, u, id).Revision {
		t.Fatal("missing distinct hint token", err)
	}
	before := persistentRevision(t, s, u.OrganizationID)
	sink := &recordingHints{fail: true}
	p := Publisher{station: s.StationID, source: s.NotificationRevisions, sink: sink}
	if p.step(t.Context()) == nil {
		t.Fatal("sink failure missing")
	}
	if v := snapshot(t, s, u, id); v.Items[0].Installation.State != "installed" || persistentRevision(t, s, u.OrganizationID) != before {
		t.Fatal("notification failure changed persistent facts")
	}
	query(t, s, `UPDATE station.app_installations SET revision=revision+1 WHERE installation_id=$1`, id)
	after, err := s.NotificationRevisions(t.Context())
	if err != nil || after[u.OrganizationID] == revisions[u.OrganizationID] {
		t.Fatal("business revision did not trigger hint", err)
	}
}

// Requires the documented 5090 etcd and a disposable PostgreSQL database.
func TestNotificationEtcdLeaseAndLostHints(t *testing.T) {
	endpoint := os.Getenv("STATION_TEST_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("5090 etcd required")
	}
	s, u, id := fixture(t)
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal("etcd client unavailable")
	}
	defer client.Close()
	key := NotificationPrefix + s.StationID + "/" + u.OrganizationID
	defer func() { _, _ = client.Delete(t.Context(), key) }()
	p := Publisher{station: s.StationID, source: s.NotificationRevisions, sink: etcdNotificationSink{client}}
	if p.step(t.Context()) != nil {
		t.Fatal("publish failed")
	}
	result, err := client.Get(t.Context(), key)
	if err != nil || len(result.Kvs) != 1 || result.Kvs[0].Lease == 0 {
		t.Fatal("notification lacks bounded lease")
	}
	ttl, err := client.TimeToLive(t.Context(), clientv3.LeaseID(result.Kvs[0].Lease))
	if err != nil || ttl.TTL <= 0 || ttl.TTL > 120 {
		t.Fatal("unexpected notification TTL")
	}
	var hint map[string]any
	revisions, revisionErr := s.NotificationRevisions(t.Context())
	if revisionErr != nil || json.Unmarshal(result.Kvs[0].Value, &hint) != nil || len(hint) != 3 || hint["revision"] != revisions[u.OrganizationID] {
		t.Fatal("notification contains state or wrong revision")
	}
	if _, err := client.Delete(t.Context(), key); err != nil {
		t.Fatal("test hint deletion failed")
	}
	if snapshot(t, s, u, id).Items[0].Installation.State != "installed" {
		t.Fatal("lost hint removed installation")
	}
	query(t, s, `UPDATE station.app_installations SET revision=revision+1 WHERE installation_id=$1`, id)
	if p.step(t.Context()) != nil {
		t.Fatal("publisher did not recover after hint loss")
	}
	result, err = client.Get(t.Context(), key)
	if err != nil || len(result.Kvs) != 1 {
		t.Fatal("next change did not republish")
	}
}

func notificationCatalog(t *testing.T, s *Service) {
	t.Helper()
	e := catalogEntry(ResourceObservation{AppID: "fixture-app", Namespace: "apps", Name: "fixture-app"})
	e.Source = "deploys/fixture"
	raw, err := json.Marshal([]catalog.Entry{e})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "catalog.json")
	if os.WriteFile(path, raw, 0600) != nil {
		t.Fatal("catalog fixture unavailable")
	}
	s.Catalog, err = catalog.Load(path, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func notificationToken(t *testing.T, s *Service, organization string) string {
	t.Helper()
	revisions, err := s.NotificationRevisions(t.Context())
	if err != nil || revisions[organization] == "" {
		t.Fatal("notification revision unavailable", err)
	}
	return revisions[organization]
}

func TestNotificationDatabaseForeignExclusionAndRedaction(t *testing.T) {
	s, owner, installation := fixture(t)
	notificationCatalog(t, s)
	observer := owner
	observer.OrganizationID = uuid.Must(uuid.NewV7()).String()
	query(t, s, `INSERT INTO station.organizations(organization_id,name) VALUES($1,'Notification observer')`, observer.OrganizationID)
	if err := s.SyncCatalog(t.Context(), []string{"fixture-app"}); err != nil {
		t.Fatal(err)
	}
	before := notificationToken(t, s, observer.OrganizationID)
	apiBefore := snapshot(t, s, observer, "").Revision
	op := uuid.Must(uuid.NewV7()).String()
	query(t, s, `INSERT INTO station.app_operations(operation_id,installation_id,request_id,user_id,organization_id,station_id,app_id,app_version,action,status,phase,idempotency_key,request_hash) VALUES($1,$2,'fixture',$3,$4,$5,'fixture-app','1.0.0','start','running','starting','notification-operation',decode(repeat('01',32),'hex'))`, op, installation, owner.UserID, owner.OrganizationID, owner.StationID)
	active := notificationToken(t, s, observer.OrganizationID)
	if active == before || snapshot(t, s, observer, "").Revision != apiBefore {
		t.Fatal("foreign blocker did not independently trigger hint")
	}
	for _, private := range []string{owner.OrganizationID, owner.UserID, installation, op} {
		if strings.Contains(active, private) {
			t.Fatal("private identity leaked in hint")
		}
	}
	query(t, s, `UPDATE station.app_operations SET phase='warming',updated_at=clock_timestamp() WHERE operation_id=$1`, op)
	if notificationToken(t, s, observer.OrganizationID) != active {
		t.Fatal("foreign private progress triggered hint")
	}
	query(t, s, `UPDATE station.app_operations SET status='succeeded',phase='completed' WHERE operation_id=$1`, op)
	if notificationToken(t, s, observer.OrganizationID) != before {
		t.Fatal("released blocker did not restore availability")
	}
	transition := uuid.Must(uuid.NewV7()).String()
	query(t, s, `INSERT INTO station.app_installation_transitions(operation_id,installation_id,user_id,organization_id,station_id,app_id,request_id,idempotency_key,request_hash,action,expected_revision,source_version,target_version,source_snapshot,target_snapshot,status,phase) VALUES($1,$2,$3,$4,$5,'fixture-app','fixture','notification-transition',decode(repeat('02',32),'hex'),'upgrade',1,'1.0.0','2.0.0','{}','{}','accepted','accepted')`, transition, installation, owner.UserID, owner.OrganizationID, owner.StationID)
	if notificationToken(t, s, observer.OrganizationID) == before {
		t.Fatal("foreign transition did not block")
	}
	query(t, s, `UPDATE station.app_installation_transitions SET status='succeeded',phase='completed' WHERE operation_id=$1`, transition)
	if notificationToken(t, s, observer.OrganizationID) != before {
		t.Fatal("transition completion did not unblock")
	}
	query(t, s, `UPDATE station.app_installations SET state='removed' WHERE installation_id=$1`, installation)
	available := notificationToken(t, s, observer.OrganizationID)
	if available == before {
		t.Fatal("foreign installation removal did not trigger hint")
	}
	query(t, s, `INSERT INTO station.app_installations(installation_id,created_by,organization_id,station_id,app_id,app_version,state) VALUES($1,$2,$3,$4,'private-app','1.0.0','installed')`, uuid.Must(uuid.NewV7()).String(), owner.UserID, owner.OrganizationID, owner.StationID)
	if notificationToken(t, s, observer.OrganizationID) != available {
		t.Fatal("invisible foreign application triggered hint")
	}
	restarted := &Service{Pool: s.Pool, StationID: s.StationID, Catalog: s.Catalog}
	if notificationToken(t, restarted, observer.OrganizationID) == available {
		t.Fatal("Core restart reused hint token")
	}
}

func TestNotificationDatabaseCatalogLifecycleWithoutInstallation(t *testing.T) {
	s, u, id := fixture(t)
	notificationCatalog(t, s)
	query(t, s, `UPDATE station.app_installations SET state='removed' WHERE installation_id=$1`, id)
	m, stream, _, o, now := memoryFixture(t, false)
	m.station = s.StationID
	s.Observations = m
	before := notificationToken(t, s, u.OrganizationID)
	persistent := persistentRevision(t, s, u.OrganizationID)
	memoryFull(t, stream)
	absent := notificationToken(t, s, u.OrganizationID)
	if absent == before {
		t.Fatal("complete empty inventory did not trigger hint")
	}
	*now = now.Add(10 * time.Second)
	if stream.Heartbeat() != nil {
		t.Fatal("heartbeat failed")
	}
	if notificationToken(t, s, u.OrganizationID) != absent {
		t.Fatal("heartbeat created semantic notification")
	}
	o.StationID, o.OrganizationID, o.InstallationID = "", "", ""
	if stream.Upsert(o) != nil {
		t.Fatal("unbound upsert failed")
	}
	occupied := notificationToken(t, s, u.OrganizationID)
	if occupied == absent {
		t.Fatal("unknown occupant did not trigger hint")
	}
	o.UID = uuid.NewString()
	if stream.Upsert(o) != nil || notificationToken(t, s, u.OrganizationID) != occupied {
		t.Fatal("unowned identity changed public hint")
	}
	if stream.Delete(o.Namespace, o.Name, o.UID) != nil {
		t.Fatal("delete failed")
	}
	if notificationToken(t, s, u.OrganizationID) != absent {
		t.Fatal("position release did not trigger hint")
	}
	*now = now.Add(31 * time.Second)
	if notificationToken(t, s, u.OrganizationID) != before {
		t.Fatal("expired empty inventory did not return unknown")
	}
	if persistentRevision(t, s, u.OrganizationID) != persistent {
		t.Fatal("runtime observations changed persistent revision")
	}
}
