// Package catalog exposes registered applications and read-only Deployment observations.
package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

type Image struct {
	Component string `json:"component"`
	Image     string `json:"image"`
}
type Entry struct {
	AppID        string   `json:"app_id"`
	DisplayName  string   `json:"display_name"`
	GroupID      string   `json:"group_id"`
	Brand        string   `json:"brand"`
	Models       []string `json:"models"`
	Source       string   `json:"source"`
	Namespace    string   `json:"namespace"`
	WorkloadName string   `json:"workload_name"`
	Version      string   `json:"version"`
	Images       []Image  `json:"images"`
}
type Observation struct {
	State      string    `json:"state"`
	Reason     string    `json:"reason"`
	ObservedAt time.Time `json:"observed_at"`
	Desired    *int32    `json:"desired_replicas"`
	Ready      *int32    `json:"ready_replicas"`
	Images     []Image   `json:"images"`
}
type Item struct {
	Entry
	ModelStatus string      `json:"model_status"`
	Deployment  Observation `json:"deployment"`
}
type List struct {
	RequestID  string  `json:"request_id"`
	StationID  string  `json:"station_id"`
	Items      []Item  `json:"items"`
	NextCursor *string `json:"next_cursor"`
}
type Detail struct {
	RequestID string `json:"request_id"`
	StationID string `json:"station_id"`
	App       Item   `json:"app"`
}
type Reader interface {
	Observe(context.Context, Entry) Observation
}
type Service struct {
	entries []Entry
	reader  Reader
}

var dnsName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
var version = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func Load(path string, reader Reader) (*Service, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("catalog file unavailable")
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 2<<20))
	d.DisallowUnknownFields()
	var entries []Entry
	if d.Decode(&entries) != nil || len(entries) == 0 || len(entries) > 100 {
		return nil, errors.New("invalid catalog")
	}
	if d.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid catalog trailing data")
	}
	ids, targets := map[string]bool{}, map[string]bool{}
	for _, e := range entries {
		target := e.Namespace + "/" + e.WorkloadName
		if !dnsName.MatchString(e.AppID) || len(e.AppID) > 63 || !dnsName.MatchString(e.Namespace) || len(e.Namespace) > 63 || !dnsName.MatchString(e.WorkloadName) || len(e.WorkloadName) > 63 || e.DisplayName == "" || !ValidGroup(e.GroupID) || !version.MatchString(e.Version) || (e.Brand != "vf" && e.Brand != "minimax" && e.Brand != "github") || e.Models == nil || len(e.Images) == 0 || !strings.HasPrefix(e.Source, "deploys/") || strings.Contains(e.Source, "..") || ids[e.AppID] || targets[target] {
			return nil, errors.New("invalid catalog entry")
		}
		for _, im := range e.Images {
			if im.Component == "" || im.Image == "" {
				return nil, errors.New("invalid catalog image")
			}
		}
		ids[e.AppID] = true
		targets[target] = true
	}
	return &Service{entries, reader}, nil
}
func ValidGroup(group string) bool { return group == "image" || group == "music" || group == "video" }
func (s *Service) observe(ctx context.Context, e Entry) Item {
	obs := Observation{State: "unknown", Reason: "unavailable", ObservedAt: time.Now().UTC(), Images: []Image{}}
	if s.reader != nil {
		obs = s.reader.Observe(ctx, e)
	}
	model := "unknown"
	if len(e.Models) == 0 {
		model = "not_required"
	}
	return Item{e, model, obs}
}
func (s *Service) List(ctx context.Context, group string) []Item {
	entries := []Entry{}
	for _, e := range s.entries {
		if group == "" || e.GroupID == group {
			entries = append(entries, e)
		}
	}
	out := make([]Item, len(entries))
	var wg sync.WaitGroup
	jobs := make(chan int)
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				out[i] = s.observe(ctx, entries[i])
			}
		}()
	}
	for i := range entries {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return out
}
func (s *Service) Get(ctx context.Context, id string) (Item, bool) {
	for _, e := range s.entries {
		if e.AppID == id {
			return s.observe(ctx, e), true
		}
	}
	return Item{}, false
}
