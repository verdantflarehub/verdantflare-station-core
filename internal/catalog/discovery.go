package catalog

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Discoverer dynamically locates installed and managed applications in the cluster.
type Discoverer interface {
	Discover(ctx context.Context) ([]Item, error)
}

type deploymentList struct {
	Items []deployment `json:"items"`
}

func itemFromDeployment(d *deployment) (Item, bool) {
	if d.Metadata.Labels == nil || d.Metadata.Labels["apps.verdantflare.com/managed"] != "true" {
		return Item{}, false
	}
	appID := d.Metadata.Labels["app.kubernetes.io/name"]
	if appID == "" {
		appID = d.Metadata.Name
	}
	if !dnsName.MatchString(appID) || len(appID) > 63 {
		return Item{}, false
	}
	groupID := d.Metadata.Labels["apps.verdantflare.com/group"]
	if !ValidGroup(groupID) {
		return Item{}, false
	}
	brand := d.Metadata.Labels["apps.verdantflare.com/brand"]
	if brand != "vf" && brand != "minimax" && brand != "github" {
		brand = "github"
	}
	ver := d.Metadata.Labels["app.kubernetes.io/version"]
	if !version.MatchString(ver) {
		ver = "0.1.0"
	}
	displayName := ""
	if d.Metadata.Annotations != nil {
		displayName = d.Metadata.Annotations["apps.verdantflare.com/display-name"]
	}
	if displayName == "" {
		displayName = appID
	}
	var models []string
	if d.Metadata.Annotations != nil {
		rawModels := d.Metadata.Annotations["apps.verdantflare.com/models"]
		if rawModels != "" {
			parts := strings.Split(rawModels, ",")
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					models = append(models, p)
				}
			}
		}
	}
	if models == nil {
		models = []string{}
	}
	source := ""
	if d.Metadata.Annotations != nil {
		source = d.Metadata.Annotations["apps.verdantflare.com/source"]
	}
	if source == "" || strings.Contains(source, "..") || !strings.HasPrefix(source, "deploys/") {
		source = "deploys/k8s/discovered.yaml"
	}
	if !dnsName.MatchString(d.Metadata.Namespace) || len(d.Metadata.Namespace) > 63 ||
		!dnsName.MatchString(d.Metadata.Name) || len(d.Metadata.Name) > 63 {
		return Item{}, false
	}
	images := []Image{}
	for _, c := range d.Spec.Template.Spec.Containers {
		if c.Name != "" && c.Image != "" {
			images = append(images, Image{Component: c.Name, Image: c.Image})
		}
	}
	if len(images) == 0 {
		return Item{}, false
	}
	entry := Entry{
		AppID:        appID,
		DisplayName:  displayName,
		GroupID:      groupID,
		Brand:        brand,
		Models:       models,
		Source:       source,
		Namespace:    d.Metadata.Namespace,
		WorkloadName: d.Metadata.Name,
		Version:      ver,
		Images:       images,
	}
	obs := observationFromDeployment(d)
	modelStatus := "unknown"
	if len(models) == 0 {
		modelStatus = "not_required"
	}
	return Item{
		Entry:       entry,
		ModelStatus: modelStatus,
		Deployment:  obs,
	}, true
}

func (k *Kubernetes) Discover(ctx context.Context) ([]Item, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	labelSelector := url.QueryEscape("apps.verdantflare.com/managed=true")

	// 1. Try cluster-wide list
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.baseURL+"/apis/apps/v1/deployments?labelSelector="+labelSelector, nil)
	if err != nil {
		return nil, err
	}
	resp, err := k.client.Do(req)
	if err == nil && resp.StatusCode == http.StatusOK {
		defer resp.Body.Close()
		return decodeDeploymentList(resp.Body)
	}
	if resp != nil {
		resp.Body.Close()
	}

	// 2. Fallback to known namespaces if cluster-wide is forbidden (403) or not found
	knownNamespaces := []string{"verdantflare-video", "verdantflare-image", "verdantflare-music", "verdantflare-station"}
	var allItems []Item
	seen := make(map[string]bool)
	for _, ns := range knownNamespaces {
		nsReq, nsErr := http.NewRequestWithContext(ctx, http.MethodGet, k.baseURL+"/apis/apps/v1/namespaces/"+ns+"/deployments?labelSelector="+labelSelector, nil)
		if nsErr != nil {
			continue
		}
		nsResp, nsDoErr := k.client.Do(nsReq)
		if nsDoErr != nil || nsResp.StatusCode != http.StatusOK {
			if nsResp != nil {
				nsResp.Body.Close()
			}
			continue
		}
		items, decodeErr := decodeDeploymentList(nsResp.Body)
		nsResp.Body.Close()
		if decodeErr == nil {
			for _, it := range items {
				if !seen[it.AppID] {
					seen[it.AppID] = true
					allItems = append(allItems, it)
				}
			}
		}
	}
	return allItems, nil
}

func decodeDeploymentList(r io.Reader) ([]Item, error) {
	var dl deploymentList
	decoder := json.NewDecoder(io.LimitReader(r, 8<<20))
	if err := decoder.Decode(&dl); err != nil {
		return nil, err
	}
	var items []Item
	seen := make(map[string]bool)
	for i := range dl.Items {
		if item, ok := itemFromDeployment(&dl.Items[i]); ok {
			if !seen[item.AppID] {
				seen[item.AppID] = true
				items = append(items, item)
			}
		}
	}
	return items, nil
}
