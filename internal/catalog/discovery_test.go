package catalog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestDynamicDiscovery_ClusterWide(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/apis/apps/v1/deployments" {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":       "image-mcp-server",
							"namespace":  "verdantflare-image",
							"generation": 1,
							"uid":        "uid-img-001",
							"labels": map[string]string{
								"app.kubernetes.io/name":        "image-mcp-server",
								"app.kubernetes.io/version":     "0.1.21",
								"apps.verdantflare.com/managed": "true",
								"apps.verdantflare.com/group":   "image",
								"apps.verdantflare.com/brand":   "vf",
							},
							"annotations": map[string]string{
								"apps.verdantflare.com/display-name": "Image MCP",
								"apps.verdantflare.com/models":       "",
								"apps.verdantflare.com/source":       "deploys/k8s/image.yaml",
							},
						},
						"spec": map[string]any{
							"replicas": 1,
							"template": map[string]any{
								"spec": map[string]any{
									"containers": []map[string]string{
										{"name": "api", "image": "example.invalid/image:0.1.21"},
									},
								},
							},
						},
						"status": map[string]any{
							"observedGeneration": 1,
							"replicas":           1,
							"readyReplicas":      1,
							"availableReplicas":  1,
							"updatedReplicas":    1,
						},
					},
					{
						// Unmanaged workload: should be ignored
						"metadata": map[string]any{
							"name":       "unmanaged-helper",
							"namespace":  "default",
							"generation": 1,
							"labels": map[string]string{
								"app.kubernetes.io/name": "unmanaged-helper",
							},
						},
						"spec": map[string]any{
							"replicas": 1,
							"template": map[string]any{
								"spec": map[string]any{
									"containers": []map[string]string{
										{"name": "helper", "image": "example.invalid/helper:1.0.0"},
									},
								},
							},
						},
						"status": map[string]any{
							"observedGeneration": 1,
							"readyReplicas":      1,
						},
					},
				},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	reader := &Kubernetes{server.URL, server.Client()}
	items, err := reader.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected discover error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 managed item, got %d", len(items))
	}
	item := items[0]
	if item.AppID != "image-mcp-server" || item.DisplayName != "Image MCP" || item.GroupID != "image" || item.Brand != "vf" {
		t.Fatalf("metadata mismatch: %+v", item)
	}
	if item.Deployment.State != "ready" || item.ModelStatus != "not_required" {
		t.Fatalf("status mismatch: state=%s, model=%s", item.Deployment.State, item.ModelStatus)
	}
}

func TestDynamicDiscovery_FallbackToNamespaces(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/apis/apps/v1/deployments" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/apis/apps/v1/namespaces/verdantflare-image/deployments" {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":       "image-mcp-server",
							"namespace":  "verdantflare-image",
							"generation": 1,
							"uid":        "uid-img-002",
							"labels": map[string]string{
								"app.kubernetes.io/name":        "image-mcp-server",
								"apps.verdantflare.com/managed": "true",
								"apps.verdantflare.com/group":   "image",
							},
						},
						"spec": map[string]any{
							"replicas": 1,
							"template": map[string]any{
								"spec": map[string]any{
									"containers": []map[string]string{
										{"name": "api", "image": "example.invalid/image:1.0.0"},
									},
								},
							},
						},
						"status": map[string]any{
							"observedGeneration": 1,
							"readyReplicas":      1,
							"availableReplicas":  1,
							"updatedReplicas":    1,
							"replicas":           1,
						},
					},
				},
			})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
	}))
	defer server.Close()

	reader := &Kubernetes{server.URL, server.Client()}
	items, err := reader.Discover(context.Background())
	if err != nil {
		t.Fatalf("discover failed: %v", err)
	}
	if len(items) != 1 || items[0].AppID != "image-mcp-server" {
		t.Fatalf("expected fallback to find image-mcp-server, got: %v", items)
	}
}

func TestDynamicDiscovery_AggregationWithStaticCatalog(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/apis/apps/v1/deployments" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":       "image-mcp-server",
							"namespace":  "verdantflare-image",
							"generation": 1,
							"uid":        "uid-discovered",
							"labels": map[string]string{
								"app.kubernetes.io/name":        "image-mcp-server",
								"apps.verdantflare.com/managed": "true",
								"apps.verdantflare.com/group":   "image",
							},
						},
						"spec": map[string]any{
							"replicas": 1,
							"template": map[string]any{
								"spec": map[string]any{
									"containers": []map[string]string{
										{"name": "api", "image": "example.invalid/image:1.0.0"},
									},
								},
							},
						},
						"status": map[string]any{
							"observedGeneration": 1,
							"readyReplicas":      1,
							"availableReplicas":  1,
							"updatedReplicas":    1,
							"replicas":           1,
						},
					},
				},
			})
			return
		}
		if r.URL.Path == "/apis/apps/v1/namespaces/verdantflare-video/deployments/video-mcp-server" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"generation": 1, "uid": "uid-video"},
				"spec":     map[string]any{"replicas": 1, "template": map[string]any{"spec": map[string]any{"containers": []map[string]string{{"name": "api", "image": "example.invalid/video:1.0.0"}}}}},
				"status":   map[string]any{"observedGeneration": 1, "readyReplicas": 1, "availableReplicas": 1, "updatedReplicas": 1, "replicas": 1},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	reader := &Kubernetes{server.URL, server.Client()}

	// Static catalog with only video-mcp-server
	staticEntry := Entry{
		AppID:        "video-mcp-server",
		DisplayName:  "Video MCP",
		GroupID:      "video",
		Brand:        "vf",
		Models:       []string{},
		Source:       "deploys/video.yaml",
		Namespace:    "verdantflare-video",
		WorkloadName: "video-mcp-server",
		Version:      "1.0.0",
		Images:       []Image{{"api", "example.invalid/video:1.0.0"}},
	}
	catalogPath := filepath.Join(t.TempDir(), "catalog.json")
	raw, _ := json.Marshal([]Entry{staticEntry})
	_ = os.WriteFile(catalogPath, raw, 0600)

	svc, err := Load(catalogPath, reader)
	if err != nil {
		t.Fatalf("failed to load catalog: %v", err)
	}

	// 1. List all: should aggregate static (video) + dynamic (image)
	all := svc.List(context.Background(), "")
	if len(all) != 2 {
		t.Fatalf("expected 2 apps in aggregated list, got %d", len(all))
	}

	// 2. List image: only dynamically discovered image-mcp-server
	images := svc.List(context.Background(), "image")
	if len(images) != 1 || images[0].AppID != "image-mcp-server" {
		t.Fatalf("expected only image-mcp-server, got %v", images)
	}

	// 3. Get dynamically discovered image-mcp-server
	img, found := svc.Get(context.Background(), "image-mcp-server")
	if !found || img.AppID != "image-mcp-server" {
		t.Fatalf("expected to get image-mcp-server, found=%v", found)
	}
}
