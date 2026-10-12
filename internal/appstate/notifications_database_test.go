package appstate

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func TestNotificationDatabaseUsesSnapshotRevision(t *testing.T) {
	s, u, id := fixture(t)
	revisions, err := s.NotificationRevisions(t.Context())
	if err != nil || revisions[u.OrganizationID] != snapshot(t, s, u, id).Revision {
		t.Fatal("publication token differs from state API", err)
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
	if json.Unmarshal(result.Kvs[0].Value, &hint) != nil || len(hint) != 3 || hint["revision"] != snapshot(t, s, u, id).Revision {
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
