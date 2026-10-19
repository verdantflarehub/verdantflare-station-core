package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

type MarketConfig struct {
	BaseURL      string
	Token        string
	CacheFile    string
	FallbackFile string
	HTTPClient   *http.Client
}

type marketAppsYAML struct {
	SchemaVersion int              `json:"schemaVersion"`
	GeneratedAt   string           `json:"generatedAt"`
	Revision      string           `json:"revision"`
	Apps          []marketYAMLItem `json:"apps"`
	Items         []marketYAMLItem `json:"items"`
}

type marketYAMLItem struct {
	AppID          string   `json:"appId"`
	DisplayName    string   `json:"displayName"`
	SortOrder      *int     `json:"sortOrder,omitempty"`
	GroupID        string   `json:"groupId"`
	Brand          string   `json:"brand"`
	IconRef        string   `json:"iconRef"`
	Version        string   `json:"version,omitempty"`
	Description    string   `json:"description"`
	Models         []string `json:"models"`
	Capabilities   []string `json:"capabilities"`
	Images         []Image  `json:"images"`
	Chart          string   `json:"chart,omitempty"`
	ChartVersion   string   `json:"chartVersion,omitempty"`
	DisabledReason string   `json:"disabledReason,omitempty"`
}

type marketAppVersion struct {
	AppID   string `json:"appId"`
	Version string `json:"version"`
	Chart   *struct {
		OCIRef           string  `json:"ociRef"`
		Name             string  `json:"name"`
		Version          string  `json:"version"`
		AppVersion       *string `json:"appVersion,omitempty"`
		ManifestDigest   string  `json:"manifestDigest"`
		PackageDigest    string  `json:"packageDigest"`
		PackageSizeBytes int64   `json:"packageSizeBytes"`
	} `json:"chart"`
	Images       []Image  `json:"images"`
	Models       []string `json:"models"`
	Capabilities []string `json:"capabilities"`
}

func defaultWorkloadName(appID string) string {
	switch appID {
	case "image-mcp", "video-mcp", "music-mcp", "speech-mcp":
		return appID + "-server"
	case "blender", "comfyui":
		return appID + "-manager"
	case "project", "world", "resources", "network":
		return "app-" + appID
	case "higgsfield", "fal", "google", "chatgpt", "grok":
		return appID + "-channel"
	default:
		return appID
	}
}

func defaultValuesSource(appID string) string {
	return "deploys/k8s.dev.verdantflarehub.com/verdantflare-app/" + appID + ".yaml"
}

func LoadMarket(ctx context.Context, cfg MarketConfig, reader Reader) (*Service, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("market base URL required")
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	cfg.BaseURL = baseURL
	cfg.HTTPClient = client

	var disc Discoverer
	if d, ok := reader.(Discoverer); ok {
		disc = d
	}
	svc := &Service{
		reader:        reader,
		discoverer:    disc,
		marketCfg:     &cfg,
		versionCache:  map[string]marketAppVersion{},
	}

	if err := svc.RefreshMarket(ctx); err != nil {
		if cfg.CacheFile != "" {
			if cached, cacheErr := loadEntriesFile(cfg.CacheFile); cacheErr == nil {
				svc.setEntries(cached)
				return svc, nil
			}
		}
		if cfg.FallbackFile != "" {
			if fallback, fbErr := Load(cfg.FallbackFile, reader); fbErr == nil {
				svc.setEntries(fallback.Entries())
				return svc, nil
			}
		}
		return nil, err
	}
	return svc, nil
}

func (s *Service) RefreshMarket(ctx context.Context) error {
	if s == nil || s.marketCfg == nil {
		return errors.New("market catalog not configured")
	}
	cfg := s.marketCfg
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.BaseURL+"/api/control/station/v2/apps.yaml", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/yaml")
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}
	s.mu.RLock()
	etag := s.marketETag
	hasEntries := len(s.entries) > 0
	s.mu.RUnlock()
	if etag != "" && hasEntries {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified && hasEntries {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("market catalog returned status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return err
	}
	var doc marketAppsYAML
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("invalid market apps.yaml: %w", err)
	}
	rawItems := doc.Apps
	if len(rawItems) == 0 {
		rawItems = doc.Items
	}
	if doc.SchemaVersion != 2 || len(rawItems) == 0 || len(rawItems) > 200 {
		return errors.New("invalid market apps.yaml schema or item count")
	}

	entries := make([]Entry, 0, len(rawItems))
	ids := map[string]bool{}
	targets := map[string]bool{}
	for idx, item := range rawItems {
		appID := strings.TrimSpace(item.AppID)
		if !dnsName.MatchString(appID) || len(appID) > 63 || ids[appID] {
			return fmt.Errorf("invalid or duplicate market appId %q", appID)
		}
		groupID := strings.TrimSpace(item.GroupID)
		if !ValidGroup(groupID) {
			return fmt.Errorf("unsupported market groupId %q for %s", groupID, appID)
		}
		brand := strings.TrimSpace(item.Brand)
		if !ValidBrand(brand) {
			return fmt.Errorf("unsupported market brand %q for %s", brand, appID)
		}
		sortOrder := 100 + idx
		if item.SortOrder != nil && *item.SortOrder >= 0 && *item.SortOrder <= 1000000 {
			sortOrder = *item.SortOrder
		}
		ns := "verdantflare-app"
		workloadName := defaultWorkloadName(appID)
		target := ns + "/" + workloadName
		if targets[target] {
			return fmt.Errorf("duplicate target workload %s", target)
		}
		models := item.Models
		if models == nil {
			models = []string{}
		}
		capabilities := item.Capabilities
		if capabilities == nil {
			capabilities = []string{}
		}
		images := item.Images
		if len(images) == 0 {
			images = []Image{{Component: "app", Image: "registry.verdantflare.ai/verdantflare/" + appID + ":unreleased"}}
		}
		ver := strings.TrimSpace(item.Version)
		disabledReason := strings.TrimSpace(item.DisabledReason)
		var helmMeta *Helm
		if ver != "" && disabledReason == "" {
			if !version.MatchString(ver) {
				return fmt.Errorf("invalid version %q for %s", ver, appID)
			}
			appVer, vErr := s.fetchMarketVersion(ctx, appID, ver)
			if vErr != nil {
				return fmt.Errorf("fetch market version %s@%s: %w", appID, ver, vErr)
			}
			if appVer.Chart != nil && appVer.Chart.OCIRef != "" && appVer.Chart.Version != "" &&
				digestPattern.MatchString(appVer.Chart.ManifestDigest) && digestPattern.MatchString(appVer.Chart.PackageDigest) {
				chartName := appVer.Chart.Name
				if chartName == "" {
					chartName = appID
				}
				helmMeta = &Helm{
					OCIRef:           appVer.Chart.OCIRef,
					ChartName:        chartName,
					ChartVersion:     appVer.Chart.Version,
					ManifestDigest:   appVer.Chart.ManifestDigest,
					PackageDigest:    appVer.Chart.PackageDigest,
					PackageSizeBytes: appVer.Chart.PackageSizeBytes,
					TargetNamespace:  ns,
					ReleaseName:      appID,
					ValuesSource:     defaultValuesSource(appID),
				}
			}
		} else {
			if ver != "" && !version.MatchString(ver) {
				ver = ""
			}
			if ver == "" && len(images) > 0 {
				if idx := strings.LastIndex(images[0].Image, "-v"); idx >= 0 {
					if cand := strings.TrimSpace(images[0].Image[idx+2:]); version.MatchString(cand) {
						ver = cand
					}
				}
			}
			if disabledReason == "" {
				disabledReason = "尚未发布可安装的 Chart OCI 版本"
			}
		}
		entry := Entry{
			AppID:          appID,
			DisplayName:    strings.TrimSpace(item.DisplayName),
			GroupID:        groupID,
			Brand:          brand,
			SortOrder:      sortOrder,
			IconRef:        strings.TrimSpace(item.IconRef),
			Description:    strings.TrimSpace(item.Description),
			Capabilities:   capabilities,
			DisabledReason: disabledReason,
			Helm:           helmMeta,
			Models:         models,
			Source:         defaultValuesSource(appID),
			Namespace:      ns,
			WorkloadName:   workloadName,
			Version:        ver,
			Images:         images,
		}
		if entry.DisplayName == "" {
			entry.DisplayName = appID
		}
		ids[appID] = true
		targets[target] = true
		entries = append(entries, entry)
	}

	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].SortOrder != entries[j].SortOrder {
			return entries[i].SortOrder < entries[j].SortOrder
		}
		return entries[i].AppID < entries[j].AppID
	})

	s.mu.Lock()
	s.entries = entries
	if newETag := strings.TrimSpace(resp.Header.Get("ETag")); newETag != "" {
		s.marketETag = newETag
	}
	s.mu.Unlock()

	if cfg.CacheFile != "" {
		_ = saveEntriesFile(cfg.CacheFile, entries)
	}
	return nil
}

func (s *Service) fetchMarketVersion(ctx context.Context, appID, ver string) (marketAppVersion, error) {
	key := appID + "@" + ver
	s.mu.RLock()
	if cached, ok := s.versionCache[key]; ok {
		s.mu.RUnlock()
		return cached, nil
	}
	s.mu.RUnlock()

	cfg := s.marketCfg
	url := fmt.Sprintf("%s/api/control/station/v2/apps/%s/versions/%s", cfg.BaseURL, appID, ver)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return marketAppVersion{}, err
	}
	req.Header.Set("Accept", "application/json")
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}
	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return marketAppVersion{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return marketAppVersion{}, fmt.Errorf("version endpoint returned %d", resp.StatusCode)
	}
	var out marketAppVersion
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return marketAppVersion{}, err
	}
	s.mu.Lock()
	if s.versionCache == nil {
		s.versionCache = map[string]marketAppVersion{}
	}
	s.versionCache[key] = out
	s.mu.Unlock()
	return out, nil
}

func (s *Service) ResolveHelm(ctx context.Context, appID, appVersion string) (*Helm, bool) {
	if s == nil || appID == "" || appVersion == "" {
		return nil, false
	}
	s.mu.RLock()
	for _, entry := range s.entries {
		if entry.AppID == appID && entry.Version == appVersion && entry.Helm != nil &&
			digestPattern.MatchString(entry.Helm.ManifestDigest) && digestPattern.MatchString(entry.Helm.PackageDigest) {
			clone := *entry.Helm
			s.mu.RUnlock()
			return &clone, true
		}
	}
	hasMarketCfg := s.marketCfg != nil
	s.mu.RUnlock()
	if !hasMarketCfg {
		return nil, false
	}
	appVer, err := s.fetchMarketVersion(ctx, appID, appVersion)
	if err != nil || appVer.Chart == nil || appVer.Chart.OCIRef == "" || appVer.Chart.Version == "" ||
		!digestPattern.MatchString(appVer.Chart.ManifestDigest) || !digestPattern.MatchString(appVer.Chart.PackageDigest) {
		return nil, false
	}
	chartName := appVer.Chart.Name
	if chartName == "" {
		chartName = appID
	}
	return &Helm{
		OCIRef:           appVer.Chart.OCIRef,
		ChartName:        chartName,
		ChartVersion:     appVer.Chart.Version,
		ManifestDigest:   appVer.Chart.ManifestDigest,
		PackageDigest:    appVer.Chart.PackageDigest,
		PackageSizeBytes: appVer.Chart.PackageSizeBytes,
		TargetNamespace:  "verdantflare-app",
		ReleaseName:      appID,
		ValuesSource:     defaultValuesSource(appID),
	}, true
}

func loadEntriesFile(path string) ([]Entry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var entries []Entry
	if err := json.Unmarshal(raw, &entries); err != nil || len(entries) == 0 {
		return nil, errors.New("invalid cached catalog")
	}
	return entries, nil
}

func saveEntriesFile(path string, entries []Entry) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	raw, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

