package appstate

import (
	"encoding/json"
	"testing"
	"time"
)

func healthFrame(o ResourceObservation, state string) Health {
	return Health{1, "health", o.Namespace, o.Name, o.UID, o.ResourceVersion, state}
}

func TestMemoryHealthIndependentFreshness(t *testing.T) {
	m, s, b, o, now := memoryFixture(t, true)
	memoryFull(t, s, o)
	if got := memoryView(t, m, b).Runtime; got.ReasonCode != "HEALTH_NOT_OBSERVED" {
		t.Fatal(got)
	}
	raw, _ := json.Marshal(healthFrame(o, "ready"))
	if err := s.Receive(raw); err != nil {
		t.Fatal(err)
	}
	before := memoryView(t, m, b)
	if before.Runtime.State != "ready" {
		t.Fatal(before)
	}
	*now = now.Add(20 * time.Second)
	memoryFull(t, s, o)
	if err := s.Upsert(o); err != nil {
		t.Fatal(err)
	}
	if err := s.Heartbeat(); err != nil {
		t.Fatal(err)
	}
	if got := memoryView(t, m, b); got.Runtime.State != "ready" || got.Revision != before.Revision {
		t.Fatal("unchanged Kubernetes facts discarded health", got)
	}
	*now = now.Add(11 * time.Second)
	if got := memoryView(t, m, b); got.Runtime.ReasonCode != "HEALTH_NOT_OBSERVED" || got.Revision == before.Revision {
		t.Fatal("List/Watch/heartbeat renewed old HTTP result", got)
	}
	if err := s.Health(healthFrame(o, "failed")); err != nil {
		t.Fatal(err)
	}
	if got := memoryView(t, m, b).Runtime; got.ReasonCode != "HEALTH_CHECK_FAILED" {
		t.Fatal(got)
	}
	*now = now.Add(20 * time.Second)
	if err := s.Health(healthFrame(o, "ready")); err != nil {
		t.Fatal(err)
	}
	if got := memoryView(t, m, b).Runtime; got.State != "unknown" || got.ReasonCode == "HEALTH_NOT_OBSERVED" {
		t.Fatal("HTTP result renewed expired Kubernetes facts", got)
	}
}

func TestMemoryHealthStaleResultAndSession(t *testing.T) {
	for _, change := range []string{"version", "uid", "facts", "delete", "reconnect"} {
		t.Run(change, func(t *testing.T) {
			m, s, b, o, _ := memoryFixture(t, true)
			memoryFull(t, s, o)
			if err := s.Health(healthFrame(o, "ready")); err != nil {
				t.Fatal(err)
			}
			memoryView(t, m, b)
			old := healthFrame(o, "failed")
			switch change {
			case "version":
				o.ResourceVersion = "replacement-version"
			case "uid":
				o.UID = "replacement-uid"
			case "facts":
				o.Ready = 0
			case "delete":
				if err := s.Delete(o.Namespace, o.Name, o.UID); err != nil {
					t.Fatal(err)
				}
			case "reconnect":
				s.Close()
				next, err := m.Open("runtime")
				if err != nil {
					t.Fatal(err)
				}
				memoryFull(t, next, o)
				if err = s.Health(old); err == nil {
					t.Fatal("old session accepted health")
				}
				if memoryView(t, m, b).Runtime.ReasonCode != "HEALTH_NOT_OBSERVED" {
					t.Fatal("health survived reconnection")
				}
				return
			}
			if change != "delete" {
				if err := s.Upsert(o); err != nil {
					t.Fatal(err)
				}
			}
			if change != "facts" {
				if err := s.Health(old); err != nil {
					t.Fatal(err)
				}
			}
			got := memoryView(t, m, b).Runtime
			if got.State == "ready" || got.ReasonCode == "HEALTH_CHECK_FAILED" {
				t.Fatal("stale health retained", got)
			}
		})
	}
}

func TestMemoryHealthStrictFrames(t *testing.T) {
	for _, modify := range []func(map[string]any){
		func(f map[string]any) { delete(f, "resourceVersion") },
		func(f map[string]any) { f["health"] = "not_checked" },
		func(f map[string]any) { f["namespace"] = "outside" },
		func(f map[string]any) { f["url"] = "http://untrusted.invalid" },
	} {
		_, s, _, o, _ := memoryFixture(t, false)
		memoryFull(t, s, o)
		raw, _ := json.Marshal(healthFrame(o, "ready"))
		var frame map[string]any
		_ = json.Unmarshal(raw, &frame)
		modify(frame)
		raw, _ = json.Marshal(frame)
		if err := s.Receive(raw); err == nil {
			t.Fatal("bad health accepted")
		}
		if err := s.Heartbeat(); err == nil {
			t.Fatal("invalid frame did not close session")
		}
	}
}
