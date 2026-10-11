package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"
)

// SnapshotItem preserves the distinction between a registered release and a
// discovered workload. Discovery must never make an application installable.
type SnapshotItem struct {
	Item
	Registered bool
}

// Snapshot is the strict, lossless reader for AppCenter. The historical List
// endpoint keeps its compatibility semantics. Unlike List, this reader does not
// swallow discovery failures or collapse different workloads by application ID.
func (s *Service) Snapshot(ctx context.Context) ([]SnapshotItem, error) {
	if s == nil {
		return nil, errors.New("catalog unavailable")
	}
	items := make([]SnapshotItem, len(s.entries))
	jobs := make(chan int)
	var workers sync.WaitGroup
	for n := 0; n < 4; n++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				items[index] = SnapshotItem{s.observe(ctx, s.entries[index]), true}
			}
		}()
	}
	for index := range s.entries {
		select {
		case jobs <- index:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return nil, ctx.Err()
		}
	}
	close(jobs)
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.discoverer != nil {
		discover := s.discoverer.Discover
		if strict, ok := s.discoverer.(interface {
			DiscoverSnapshot(context.Context) ([]Item, error)
		}); ok {
			discover = strict.DiscoverSnapshot
		}
		discovered, err := discover(ctx)
		if err != nil {
			return nil, err
		}
		known := map[string]int{}
		for i, item := range items {
			known[item.Namespace+"/"+item.WorkloadName] = i
		}
		for _, item := range discovered {
			key := item.Namespace + "/" + item.WorkloadName
			if i, ok := known[key]; ok {
				// A registered object is observed directly; never replace that
				// observation with a second, potentially conflicting list read.
				if items[i].AppID != item.AppID {
					return nil, errors.New("catalog workload identity conflict")
				}
				continue
			}
			known[key] = len(items)
			items = append(items, SnapshotItem{item, false})
		}
	}
	if len(items) > 1000 {
		return nil, errors.New("catalog snapshot limit exceeded")
	}
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i], items[j]
		return a.AppID+"/"+a.Namespace+"/"+a.WorkloadName < b.AppID+"/"+b.Namespace+"/"+b.WorkloadName
	})
	return items, ctx.Err()
}

// DiscoverSnapshot requires the complete authorized list. It neither falls
// back to a hardcoded namespace subset nor deduplicates by application name.
// Existing Discover keeps its historical behavior for old clients.
func (k *Kubernetes) DiscoverSnapshot(ctx context.Context) ([]Item, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.baseURL+"/apis/apps/v1/deployments?labelSelector="+url.QueryEscape("apps.verdantflare.com/managed=true"), nil)
	if err != nil {
		return nil, err
	}
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, errors.New("workload discovery unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("workload discovery unavailable")
	}
	limited := &io.LimitedReader{R: resp.Body, N: (8 << 20) + 1}
	decoder := json.NewDecoder(limited)
	var list struct {
		Items    *[]deployment `json:"items"`
		Metadata struct {
			Continue string `json:"continue"`
		} `json:"metadata"`
	}
	if err := decoder.Decode(&list); err != nil {
		return nil, errors.New("invalid workload snapshot")
	}
	if decoder.Decode(new(any)) != io.EOF || limited.N == 0 || list.Items == nil || list.Metadata.Continue != "" || len(*list.Items) > 1000 {
		return nil, errors.New("incomplete workload snapshot")
	}
	items := []Item{}
	seen := map[string]bool{}
	for _, d := range *list.Items {
		if item, ok := itemFromDeployment(&d); ok {
			key := item.Namespace + "/" + item.WorkloadName
			if seen[key] {
				return nil, errors.New("duplicate workload identity")
			}
			seen[key] = true
			// Discovery metadata is not a registered release. Do not retain
			// the legacy synthetic version when the label is absent/invalid.
			item.Version = d.Metadata.Labels["app.kubernetes.io/version"]
			if !version.MatchString(item.Version) {
				item.Version = ""
			}
			items = append(items, item)
		}
	}
	return items, nil
}
