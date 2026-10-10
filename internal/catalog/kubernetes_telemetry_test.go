package catalog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTelemetryPreservesContainerIdentity(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/pods" {
			_, _ = w.Write([]byte(`{"items":[{"metadata":{"uid":"new-uid","name":"blender-a","namespace":"verdantflare-blender","creationTimestamp":"2026-10-10T01:00:00Z"},"spec":{"containers":[{"name":"sidecar"},{"name":"blender","resources":{"requests":{"cpu":"2"},"limits":{"memory":"16Gi"}}}]},"status":{"phase":"Running","containerStatuses":[{"name":"blender","ready":true,"restartCount":3},{"name":"sidecar","ready":false,"restartCount":1}]}}]}`))
		} else {
			_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"blender-a","namespace":"verdantflare-blender"},"timestamp":"2026-10-10T02:00:00Z","window":"15s","containers":[{"name":"blender","usage":{"cpu":"0","memory":"1Gi"}},{"name":"sidecar","usage":{"cpu":"5m","memory":"2Mi"}}]}]}`))
		}
	}))
	defer api.Close()
	k := &Kubernetes{baseURL: api.URL, client: api.Client()}
	pods, err := k.ListPods(context.Background())
	if err != nil || len(pods) != 1 {
		t.Fatal(pods, err)
	}
	p := pods[0]
	if p.UID != "new-uid" || p.CreatedAt == "" || len(p.Containers) != 2 || p.Containers[0].Ready || !p.Containers[1].Ready || p.Containers[1].RestartCount != 3 || p.Containers[1].CPUReq != "2" {
		t.Fatal(p)
	}
	metrics, err := k.ListPodMetrics(context.Background())
	if err != nil || len(metrics) != 2 || metrics[0].Container != "blender" || metrics[0].CPUUsage != "0" || metrics[0].Window != "15s" || metrics[0].Timestamp == "" {
		t.Fatal(metrics, err)
	}
}
