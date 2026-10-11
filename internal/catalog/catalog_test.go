package catalog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeploymentStates(t *testing.T) {
	for _, tc := range []struct {
		name                                         string
		status                                       int
		generation, observed                         int64
		desired, replicas, updated, ready, available int32
		failed                                       bool
		want, reason                                 string
	}{
		{name: "ready", status: 200, generation: 2, observed: 2, desired: 1, replicas: 1, updated: 1, ready: 1, available: 1, want: "ready", reason: "replicas_ready"},
		{name: "old generation", status: 200, generation: 2, observed: 1, desired: 1, replicas: 1, updated: 1, ready: 1, available: 1, want: "starting", reason: "reconciling"},
		{name: "rolling update", status: 200, generation: 2, observed: 2, desired: 1, replicas: 2, updated: 1, ready: 1, available: 1, want: "starting", reason: "reconciling"},
		{name: "stopped", status: 200, generation: 2, observed: 2, want: "stopped", reason: "scaled_to_zero"},
		{name: "stopping", status: 200, generation: 2, observed: 2, replicas: 1, want: "stopping", reason: "scaling_down"},
		{name: "failed", status: 200, generation: 2, observed: 2, desired: 1, failed: true, want: "degraded", reason: "deployment_failed"},
		{name: "missing", status: 404, want: "not_installed", reason: "not_found"},
		{name: "forbidden", status: 403, want: "unknown", reason: "forbidden"},
		{name: "unavailable", status: 503, want: "unknown", reason: "unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/apis/apps/v1/namespaces/verdantflare-video/deployments/video-mcp-server" {
					t.Error("unexpected cluster operation")
				}
				w.WriteHeader(tc.status)
				conditions := []map[string]string{}
				if tc.failed {
					conditions = append(conditions, map[string]string{"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded"})
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"generation": tc.generation}, "spec": map[string]any{"replicas": tc.desired, "template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"name": "server", "image": "example.invalid/app:1.0.0", "env": []map[string]string{{"name": "PASSWORD", "value": "must-not-leak"}}}}}}}, "status": map[string]any{"observedGeneration": tc.observed, "replicas": tc.replicas, "updatedReplicas": tc.updated, "readyReplicas": tc.ready, "availableReplicas": tc.available, "conditions": conditions}})
			}))
			defer server.Close()
			reader := &Kubernetes{server.URL, server.Client()}
			obs := reader.Observe(context.Background(), Entry{Namespace: "verdantflare-video", WorkloadName: "video-mcp-server"})
			if obs.State != tc.want || obs.Reason != tc.reason {
				t.Fatalf("got %s/%s", obs.State, obs.Reason)
			}
			b, _ := json.Marshal(obs)
			if strings.Contains(string(b), "must-not-leak") {
				t.Fatal("deployment environment leaked")
			}
			if tc.status != 200 && (obs.Desired != nil || obs.Ready != nil) {
				t.Fatal("unknown replicas fabricated")
			}
		})
	}
}
func TestCatalogConfigAndModelStatus(t *testing.T) {
	entry := Entry{AppID: "app", DisplayName: "App", GroupID: "video", Brand: "vf", Models: []string{"model"}, Source: "deploys/app.yaml", Namespace: "verdantflare-video", WorkloadName: "app", Version: "1.0.0", Images: []Image{{"app", "example.invalid/app:1.0.0"}}}
	path := filepath.Join(t.TempDir(), "catalog.json")
	save := func(entries []Entry) {
		t.Helper()
		b, _ := json.Marshal(entries)
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	save([]Entry{entry})
	s, err := Load(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := s.List(context.Background(), "video")
	if len(rows) != 1 || rows[0].ModelStatus != "unknown" || rows[0].Deployment.State != "unknown" {
		t.Fatal("invented readiness")
	}
	if len(s.List(context.Background(), "image")) != 0 {
		t.Fatal("incorrect group filter")
	}
	if _, ok := s.Get(context.Background(), "unknown"); ok {
		t.Fatal("unknown app accepted")
	}
	save([]Entry{entry, entry})
	if _, err = Load(path, nil); err == nil {
		t.Fatal("duplicates accepted")
	}
	entry.Namespace = "../secrets"
	save([]Entry{entry})
	if _, err = Load(path, nil); err == nil {
		t.Fatal("unsafe namespace accepted")
	}
}

func TestCatalogSystemApplications(t *testing.T) {
	entries := []Entry{}
	for _, id := range []string{"project", "world", "network", "resources"} {
		entries = append(entries, Entry{AppID: id, DisplayName: id, GroupID: "system", Brand: "vf", Models: []string{}, Source: "deploys/apps.yaml", Namespace: "verdantflare-station", WorkloadName: "app-" + id, Version: "0.1.0", Images: []Image{{"app", "example.invalid/" + id + ":0.1.0"}}})
	}
	path := filepath.Join(t.TempDir(), "catalog.json")
	write := func() {
		t.Helper()
		data, err := json.Marshal(entries)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	s, err := Load(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.List(context.Background(), "system")) != 4 || len(s.List(context.Background(), "desktop")) != 0 {
		t.Fatal("system catalog filtering failed")
	}
	for _, entry := range entries {
		item, found := s.Get(context.Background(), entry.AppID)
		if !found || item.GroupID != "system" || item.ModelStatus != "not_required" || item.Deployment.State != "unknown" {
			t.Fatal("system catalog identity or unobserved state changed")
		}
	}
	for _, group := range []string{"System", "unknown", ""} {
		entries[0].GroupID = group
		write()
		if _, err := Load(path, nil); err == nil {
			t.Fatalf("unsupported group %q accepted", group)
		}
	}
}
