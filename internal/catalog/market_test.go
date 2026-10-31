package catalog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestLoadMarketCatalogAndVersionResolution(t *testing.T) {
	var yamlHits atomic.Int32
	var versionHits atomic.Int32
	manifestDigest := "sha256:" + strings.Repeat("a", 64)
	packageDigest := "sha256:" + strings.Repeat("b", 64)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-market-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/api/control/station/v2/apps.yaml":
			yamlHits.Add(1)
			etag := `"catalog-v2-test"`
			w.Header().Set("ETag", etag)
			if r.Header.Get("If-None-Match") == etag {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
			_, _ = w.Write([]byte(`schemaVersion: 2
generatedAt: "2026-10-19T10:00:00Z"
revision: sha256:` + strings.Repeat("c", 64) + `
items:
  - appId: blender
    displayName: Blender
    sortOrder: 10
    groupId: desktop
    brand: blender
    iconRef: brand:blender
    version: 0.1.12
    description: Blender 3D 创作工具
    models: []
    capabilities:
      - scene.get
    images:
      - component: app
        image: registry.cn-qingdao.aliyuncs.com/wod/verdantflare-app:blender-v0.1.12
    chart: oci://registry.verdantflare.ai/charts/blender
    chartVersion: 0.1.0
  - appId: google
    displayName: Google AI Studio
    sortOrder: 12
    groupId: channel
    brand: vf
    iconRef: brand:google
    version: 0.1.11
    description: 账号、调用日志。
    models: []
    capabilities:
      - google.channel.manage
    images:
      - component: gateway
        image: registry.cn-qingdao.aliyuncs.com/wod/verdantflare-app:google-channel-v0.1.11
    chart: oci://registry.verdantflare.ai/charts/google
    chartVersion: 0.1.0
  - appId: image-face-fusion-api
    displayName: Face Fusion
    sortOrder: 50
    groupId: image
    brand: github
    iconRef: brand:github
    description: 本地换脸能力
    models:
      - InsightFace
    capabilities:
      - image.faceswap
    images:
      - component: api
        image: registry.cn-qingdao.aliyuncs.com/wod/verdantflare-app:image-face-fusion-api-v0.1.2
    disabledReason: 当前人脸融合模型服务按需调度于 GPU 节点，未在 5090 dev 常驻。
`))
		case "/api/control/station/v2/apps/blender/versions/0.1.12":
			versionHits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"appId":   "blender",
				"version": "0.1.12",
				"chart": map[string]any{
					"ociRef":           "oci://registry.verdantflare.ai/charts/blender",
					"name":             "blender",
					"version":          "0.1.0",
					"manifestDigest":   manifestDigest,
					"packageDigest":    packageDigest,
					"packageSizeBytes": 4096,
				},
			})
		case "/api/control/station/v2/apps/google/versions/0.1.11":
			versionHits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"appId":   "google",
				"version": "0.1.11",
				"chart": map[string]any{
					"ociRef":           "oci://registry.verdantflare.ai/charts/google",
					"name":             "google",
					"version":          "0.1.0",
					"manifestDigest":   manifestDigest,
					"packageDigest":    packageDigest,
					"packageSizeBytes": 2048,
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cacheFile := filepath.Join(t.TempDir(), "market-cache.json")
	svc, err := LoadMarket(context.Background(), MarketConfig{
		BaseURL:    server.URL,
		Token:      "test-market-token",
		CacheFile:  cacheFile,
		HTTPClient: server.Client(),
	}, nil)
	if err != nil {
		t.Fatalf("LoadMarket failed: %v", err)
	}
	entries := svc.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 verified entries, got %d", len(entries))
	}
	if _, ok := svc.Entry("image-face-fusion-api"); ok {
		t.Fatal("unverified release was ingested into the Station catalog")
	}
	blender, ok := svc.Entry("blender")
	if !ok || blender.WorkloadName != "blender-manager" || blender.Namespace != "verdantflare-app" || blender.Helm == nil || blender.Helm.ManifestDigest != manifestDigest || blender.Helm.ValuesSource != "deploys/k8s.dev.verdantflarehub.com/verdantflare-app/blender.yaml" {
		t.Fatalf("unexpected blender entry: %+v", blender)
	}
	// Second refresh should hit 304 Not Modified and not re-fetch versions.
	if err := svc.RefreshMarket(context.Background()); err != nil {
		t.Fatalf("RefreshMarket 304 failed: %v", err)
	}
	if versionHits.Load() != 2 {
		t.Fatalf("expected 2 version fetches, got %d", versionHits.Load())
	}

	// LKG cache fallback when remote server is closed.
	server.Close()
	cachedSvc, err := LoadMarket(context.Background(), MarketConfig{
		BaseURL:    server.URL,
		Token:      "test-market-token",
		CacheFile:  cacheFile,
		HTTPClient: server.Client(),
	}, nil)
	if err != nil || len(cachedSvc.Entries()) != 2 {
		t.Fatalf("expected LKG cache fallback to succeed with verified entries, err=%v", err)
	}
	if _, ok := cachedSvc.Entry("image-face-fusion-api"); ok {
		t.Fatal("cached unverified release was restored into the Station catalog")
	}
}

func TestMarketVersionLookupFailuresAreCached(t *testing.T) {
	var hits atomic.Int32
	digest := "sha256:" + strings.Repeat("d", 64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		switch r.URL.Path {
		case "/api/control/station/v2/apps/known/versions/1.0.0":
			_ = json.NewEncoder(w).Encode(map[string]any{"chart": map[string]any{"name": "known", "ociRef": "oci://registry.invalid/charts/known", "version": "1.0.0", "manifestDigest": digest, "packageDigest": digest}})
		case "/api/control/station/v2/apps/broken/versions/1.0.0":
			w.WriteHeader(http.StatusBadGateway)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	s := &Service{marketCfg: &MarketConfig{BaseURL: server.URL, HTTPClient: server.Client()}}
	for range 3 {
		if _, ok := s.ResolveHelm(context.Background(), "absent", "1.0.0"); ok {
			t.Fatal("absent version resolved")
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("absent version requested %d times", hits.Load())
	}
	for range 3 {
		s.ResolveHelm(context.Background(), "broken", "1.0.0")
	}
	if hits.Load() != 2 {
		t.Fatalf("failing version requested %d times", hits.Load())
	}
	for range 3 {
		if _, ok := s.ResolveHelm(context.Background(), "known", "1.0.0"); !ok {
			t.Fatal("known version not resolved")
		}
	}
	if hits.Load() != 3 {
		t.Fatalf("known version requested %d times", hits.Load()-2)
	}
	s.mu.Lock()
	s.versionMiss["absent@1.0.0"] = s.versionMiss["absent@1.0.0"].Add(-marketVersionMissTTL)
	s.mu.Unlock()
	s.ResolveHelm(context.Background(), "absent", "1.0.0")
	if hits.Load() != 4 {
		t.Fatal("expired miss not retried")
	}
}
