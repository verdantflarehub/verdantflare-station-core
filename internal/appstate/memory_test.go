package appstate

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func memoryFixture(t *testing.T, health bool) (*Memory, *StreamSession, Binding, ResourceObservation, *time.Time) {
	t.Helper()
	station := uuid.NewString()
	c := Collector{ID: "runtime", Namespaces: []string{"apps"}}
	if health {
		c.HealthRequiredApps = []string{"fixture-app"}
	}
	m, err := NewMemory(station, []Collector{c})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }
	s, err := m.Open("runtime")
	if err != nil {
		t.Fatal(err)
	}
	b := Binding{uuid.NewString(), uuid.NewString(), "fixture-app"}
	o := ResourceObservation{Namespace: "apps", Name: "fixture-app", UID: uuid.NewString(), ResourceVersion: "opaque",
		StationID: station, OrganizationID: b.OrganizationID, InstallationID: b.InstallationID, AppID: b.AppID,
		Generation: 1, ObservedGeneration: 1, Desired: 1, Replicas: 1, Ready: 1, Updated: 1, Available: 1, Health: "not_checked"}
	return m, s, b, o, &now
}

func memoryView(t *testing.T, m *Memory, b Binding) MemoryView {
	t.Helper()
	views, _, err := m.Views([]Binding{b})
	if err != nil {
		t.Fatal(err)
	}
	return views[b.InstallationID]
}

func memoryFull(t *testing.T, s *StreamSession, objects ...ResourceObservation) {
	t.Helper()
	id := uuid.NewString()
	if err := s.Begin(id); err != nil {
		t.Fatal(err)
	}
	if len(objects) > 0 {
		if err := s.Objects(id, objects); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.End(id, len(objects)); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryAtomicSnapshotAndDisconnect(t *testing.T) {
	m, s, b, o, _ := memoryFixture(t, false)
	if got := memoryView(t, m, b).Runtime; got.State != "unknown" || got.ReasonCode != "NOT_SYNCED" {
		t.Fatal(got)
	}
	memoryFull(t, s, o)
	before := memoryView(t, m, b)
	if before.Runtime.State != "ready" {
		t.Fatal(before)
	}
	id := uuid.NewString()
	if err := s.Begin(id); err != nil {
		t.Fatal(err)
	}
	o.Ready = 0
	if err := s.Objects(id, []ResourceObservation{o}); err != nil {
		t.Fatal(err)
	}
	if got := memoryView(t, m, b); got.Revision != before.Revision || got.Runtime.State != "ready" {
		t.Fatal("partial inventory published")
	}
	s.Close()
	if got := memoryView(t, m, b); got.Runtime.State != "ready" {
		t.Fatal("disconnect discarded valid evidence")
	}
	next, err := m.Open("runtime")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.End(id, 1); !errors.Is(err, ErrStreamClosed) {
		t.Fatal("old session published", err)
	}
	memoryFull(t, next, o)
	if got := memoryView(t, m, b); got.Runtime.State != "starting" || got.Revision == before.Revision {
		t.Fatal(got)
	}
}

func TestMemoryBusyTimeoutAndStaleWriter(t *testing.T) {
	m, s, b, o, now := memoryFixture(t, false)
	memoryFull(t, s, o)
	memoryView(t, m, b)
	if _, err := m.Open("runtime"); !errors.Is(err, ErrCollectorBusy) {
		t.Fatal(err)
	}
	*now = now.Add(30 * time.Second)
	next, err := m.Open("runtime")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Heartbeat(); !errors.Is(err, ErrStreamClosed) {
		t.Fatal("old session still alive")
	}
	if got := memoryView(t, m, b).Runtime; got.State != "unknown" || got.ReasonCode != "OBSERVATION_EXPIRED" {
		t.Fatal(got)
	}
	memoryFull(t, next, o)
	s.Close()
	if err = next.Heartbeat(); err != nil {
		t.Fatal("old Close revoked replacement", err)
	}
	if got := memoryView(t, m, b).Runtime.State; got != "ready" {
		t.Fatal(got)
	}
}

func TestMemoryInvalidFullSyncNeverPublishes(t *testing.T) {
	for _, kind := range []string{"count", "duplicate", "timeout", "namespace", "before_snapshot", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			m, s, b, o, now := memoryFixture(t, false)
			memoryFull(t, s, o)
			before := memoryView(t, m, b)
			id := uuid.NewString()
			if err := s.Begin(id); err != nil {
				t.Fatal(err)
			}
			var err error
			switch kind {
			case "count":
				err = s.End(id, 1)
			case "duplicate":
				err = s.Objects(id, []ResourceObservation{o, o})
			case "timeout":
				*now = now.Add(30 * time.Second)
				err = s.End(id, 0)
			case "namespace":
				o.Namespace = "foreign"
				err = s.Objects(id, []ResourceObservation{o})
			case "before_snapshot":
				err = s.Upsert(o)
			case "oversize":
				err = s.Objects(id, make([]ResourceObservation, 501))
			}
			if err == nil {
				t.Fatal("invalid synchronization accepted")
			}
			if len(s.collector.objects) != 1 || s.staging != nil {
				t.Fatal("inventory changed or staging retained")
			}
			if got := memoryView(t, m, b); kind != "timeout" && got.Revision != before.Revision {
				t.Fatal("invalid sync changed state")
			}
		})
	}
}

func TestMemoryReplacementDeletionAndAbsentFreshness(t *testing.T) {
	m, s, b, o, now := memoryFixture(t, false)
	memoryFull(t, s, o)
	memoryView(t, m, b)
	oldUID := o.UID
	o.UID = uuid.NewString()
	if err := s.Upsert(o); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(o.Namespace, o.Name, oldUID); err != nil {
		t.Fatal(err)
	}
	if got := memoryView(t, m, b).Runtime.State; got != "ready" {
		t.Fatal("stale delete removed replacement", got)
	}
	if err := s.Delete(o.Namespace, o.Name, o.UID); err != nil {
		t.Fatal(err)
	}
	if got := memoryView(t, m, b).Runtime.State; got != "absent" {
		t.Fatal(got)
	}
	*now = now.Add(20 * time.Second)
	if err := s.Heartbeat(); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(11 * time.Second)
	if got := memoryView(t, m, b).Runtime.State; got != "absent" {
		t.Fatal("confirmed absence not refreshed", got)
	}
	*now = now.Add(20 * time.Second)
	m.Expire()
	if got := memoryView(t, m, b).Runtime.State; got != "unknown" {
		t.Fatal("expired absence still known", got)
	}
}

func TestMemoryHeartbeatDoesNotRenewHealthOrBusinessRevision(t *testing.T) {
	m, s, b, o, now := memoryFixture(t, true)
	o.Health = "ready"
	memoryFull(t, s, o)
	before := memoryView(t, m, b)
	*now = now.Add(20 * time.Second)
	if err := s.Heartbeat(); err != nil {
		t.Fatal(err)
	}
	after := memoryView(t, m, b)
	if before.Revision != after.Revision || !after.Runtime.ObservedAt.After(*before.Runtime.ObservedAt) {
		t.Fatal("heartbeat advanced business revision or did not confirm freshness")
	}
	*now = now.Add(11 * time.Second)
	m.Expire()
	after = memoryView(t, m, b)
	if after.Runtime.State != "unknown" || after.Runtime.ReasonCode != "HEALTH_NOT_OBSERVED" || after.Revision == before.Revision {
		t.Fatal("heartbeat extended health proof", after)
	}
	if err := s.Heartbeat(); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(29 * time.Second)
	if err := s.Heartbeat(); !errors.Is(err, ErrStreamFrame) {
		t.Fatal("heartbeat accepted after full List deadline", err)
	}
}

func TestMemoryIdentityAndInstanceBoundaries(t *testing.T) {
	m, s, b, o, _ := memoryFixture(t, false)
	foreign := o
	foreign.Name = "foreign"
	foreign.OrganizationID = uuid.NewString()
	worker := o
	worker.Name = "worker"
	worker.InstanceID = uuid.NewString()
	memoryFull(t, s, o, foreign, worker)
	if got := memoryView(t, m, b).Runtime.State; got != "ready" {
		t.Fatal("foreign or worker poisoned management state", got)
	}
	second := o
	second.Name = "second"
	if err := s.Upsert(second); err != nil {
		t.Fatal(err)
	}
	if got := memoryView(t, m, b).Runtime; got.State != "unknown" || got.ReasonCode != "IDENTITY_CONFLICT" {
		t.Fatal("duplicate ownership selected arbitrarily", got)
	}
	if err := s.Delete(second.Namespace, second.Name, second.UID); err != nil {
		t.Fatal(err)
	}
	o.OrganizationID = uuid.NewString()
	if err := s.Upsert(o); err != nil {
		t.Fatal(err)
	}
	if got := memoryView(t, m, b).Runtime.ReasonCode; got != "IDENTITY_CONFLICT" {
		t.Fatal("location ownership change ignored", got)
	}
	never := Binding{uuid.NewString(), b.OrganizationID, "never-seen"}
	memoryFull(t, s)
	if got := memoryView(t, m, never).Runtime.State; got != "unknown" {
		t.Fatal("unobserved installation declared absent", got)
	}
	if got := memoryView(t, m, b).Runtime.State; got != "absent" {
		t.Fatal("complete empty inventory not applied", got)
	}
}

func TestMemoryCoreRestartAndSnapshotCopy(t *testing.T) {
	m, s, b, o, _ := memoryFixture(t, false)
	memoryFull(t, s, o)
	before := memoryView(t, m, b)
	*before.Runtime.ObservedAt = time.Time{}
	*before.Runtime.LastKnownState = "bad"
	if got := memoryView(t, m, b).Runtime; got.ObservedAt.IsZero() || *got.LastKnownState != "ready" {
		t.Fatal("caller mutated live cache")
	}
	restarted, err := NewMemory(m.station, []Collector{s.collector.config})
	if err != nil {
		t.Fatal(err)
	}
	after := memoryView(t, restarted, b)
	if after.Runtime.State != "unknown" || after.Runtime.ReasonCode != "NOT_SYNCED" || after.Revision == before.Revision {
		t.Fatal("restart reused state or token", after)
	}
	if after.Runtime.LastKnownState != nil {
		t.Fatal("restart fabricated last observation")
	}
}

func TestMemorySnapshotPreservesChunkReceiptTime(t *testing.T) {
	m, s, b, o, now := memoryFixture(t, false)
	id := uuid.NewString()
	if err := s.Begin(id); err != nil {
		t.Fatal(err)
	}
	if err := s.Objects(id, []ResourceObservation{o}); err != nil {
		t.Fatal(err)
	}
	received := *now
	*now = now.Add(20 * time.Second)
	if err := s.End(id, 1); err != nil {
		t.Fatal(err)
	}
	if got := memoryView(t, m, b).Runtime.ObservedAt; got == nil || !got.Equal(received) {
		t.Fatal("end renewed old data", got)
	}
	*now = now.Add(11 * time.Second)
	if got := memoryView(t, m, b).Runtime.State; got != "unknown" {
		t.Fatal("stale chunk still ready", got)
	}
}

func TestMemoryConcurrentSessionSingleWinner(t *testing.T) {
	m, s, _, _, _ := memoryFixture(t, false)
	s.Close()
	var wg sync.WaitGroup
	winners := make(chan *StreamSession, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := m.Open("runtime")
			if err == nil {
				winners <- s
			} else if !errors.Is(err, ErrCollectorBusy) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	close(winners)
	if len(winners) != 1 {
		t.Fatal("multiple active sessions", len(winners))
	}
}

func TestMemoryReclaimsRetiredInstallationBindings(t *testing.T) {
	m, _, b, _, _ := memoryFixture(t, false)
	bindings := make([]Binding, maxBindings)
	for i := range bindings {
		bindings[i] = Binding{uuid.NewString(), b.OrganizationID, "fixture-" + strconv.Itoa(i)}
	}
	if _, _, err := m.Views(bindings); err != nil {
		t.Fatal(err)
	}
	retired := bindings[0].InstallationID
	bindings[0].InstallationID = uuid.NewString()
	if _, _, err := m.Views(bindings); err != nil {
		t.Fatal("reinstall exhausted active installation budget", err)
	}
	if len(m.bindings) != maxBindings || m.bindings[retired] != nil {
		t.Fatal("retired binding not reclaimed")
	}
	if _, _, err := m.Views(append(bindings, b)); !errors.Is(err, ErrLimit) {
		t.Fatal("active installation limit not enforced")
	}
	if len(m.bindings) != maxBindings {
		t.Fatal("rejected registry partially applied")
	}
}

func TestMemoryNamespaceMigrationAndResetAppBindings(t *testing.T) {
	station := uuid.NewString()
	c := Collector{ID: "runtime", Namespaces: []string{"verdantflare-comfyui", "verdantflare-app"}}
	m, err := NewMemory(station, []Collector{c})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }
	s, err := m.Open("runtime")
	if err != nil {
		t.Fatal(err)
	}
	b := Binding{InstallationID: uuid.NewString(), OrganizationID: uuid.NewString(), AppID: "comfyui"}
	oldObs := ResourceObservation{
		Namespace: "verdantflare-comfyui", Name: "comfyui-manager", UID: uuid.NewString(), ResourceVersion: "1",
		StationID: station, OrganizationID: b.OrganizationID, InstallationID: b.InstallationID, AppID: b.AppID,
		Generation: 1, ObservedGeneration: 1, Desired: 1, Replicas: 1, Ready: 1, Updated: 1, Available: 1, Health: "not_checked",
	}
	memoryFull(t, s, oldObs)
	if got := memoryView(t, m, b).Runtime; got.State != "ready" {
		t.Fatalf("expected initial ready state, got %+v", got)
	}

	// Delete old workload in legacy namespace and upsert new workload in verdantflare-app.
	if err := s.Delete(oldObs.Namespace, oldObs.Name, oldObs.UID); err != nil {
		t.Fatal(err)
	}
	newObs := oldObs
	newObs.Namespace = "verdantflare-app"
	newObs.UID = uuid.NewString()
	newObs.ResourceVersion = "2"
	if err := s.Upsert(newObs); err != nil {
		t.Fatal(err)
	}
	got := memoryView(t, m, b)
	if got.Runtime.State != "ready" || got.Runtime.ReasonCode == "IDENTITY_CONFLICT" {
		t.Fatalf("expected migrated workload in verdantflare-app to be ready without IDENTITY_CONFLICT, got %+v", got.Runtime)
	}
	if got.Resource == nil || got.Resource.Namespace != "verdantflare-app" || got.Resource.UID != newObs.UID {
		t.Fatalf("expected resource bound to verdantflare-app, got %+v", got.Resource)
	}

	// Verify ResetAppBindings clears stale legacy observations if both briefly existed before delete.
	if err := s.Upsert(oldObs); err != nil {
		t.Fatal(err)
	}
	if conflict := memoryView(t, m, b).Runtime; conflict.ReasonCode != "IDENTITY_CONFLICT" {
		t.Fatalf("expected conflict when both namespaces have active workloads, got %+v", conflict)
	}
	svc := &Service{StationID: station, Observations: m}
	svc.ResetAppBindings("comfyui", "verdantflare-app")
	afterReset := memoryView(t, m, b)
	if afterReset.Runtime.State != "ready" || afterReset.Resource == nil || afterReset.Resource.Namespace != "verdantflare-app" {
		t.Fatalf("expected ResetAppBindings to clear non-target namespace observation and restore ready state, got %+v", afterReset)
	}
}
