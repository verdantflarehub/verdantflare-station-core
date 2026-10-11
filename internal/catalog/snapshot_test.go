package catalog

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type snapshotReader struct {
	items []Item
	err   error
}

type snapshotTransport func(*http.Request) (*http.Response, error)

func (f snapshotTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSnapshotStrictDiscoveryRejectsPartialResults(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		valid      bool
	}{
		{"empty", `{"items":[]}`, 200, true},
		{"missing", `{}`, 200, false},
		{"null", `{"items":null}`, 200, false},
		{"paginated", `{"metadata":{"continue":"next"},"items":[]}`, 200, false},
		{"trailing", `{"items":[]} {}`, 200, false},
		{"forbidden", `{"items":[]}`, 403, false},
		{"unavailable", `{"items":[]}`, 503, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			k := &Kubernetes{baseURL: "https://cluster.invalid", client: &http.Client{Transport: snapshotTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body)), Header: http.Header{}}, nil
			})}}
			s := &Service{reader: k, discoverer: k}
			_, err := s.Snapshot(context.Background())
			if (err == nil) != tc.valid || calls != 1 {
				t.Fatal("partial response accepted or silent namespace fallback", err, calls)
			}
		})
	}
}

func (r snapshotReader) Observe(context.Context, Entry) Observation {
	return Observation{State: "unknown"}
}
func (r snapshotReader) Discover(context.Context) ([]Item, error) { return r.items, r.err }

func TestSnapshotPreservesWorkloads(t *testing.T) {
	a := Entry{AppID: "app", Namespace: "apps", WorkloadName: "a"}
	b := a
	b.WorkloadName = "b"
	r := snapshotReader{items: []Item{{Entry: a}, {Entry: b}}}
	s := &Service{entries: []Entry{a}, reader: r, discoverer: r}
	items, err := s.Snapshot(context.Background())
	if err != nil || len(items) != 2 || !items[0].Registered || items[1].Registered {
		t.Fatal("workloads collapsed or discovery made installable", err)
	}
	r.items[0].AppID = "different-app"
	s.discoverer = r
	if _, err = s.Snapshot(context.Background()); err == nil {
		t.Fatal("conflicting catalog identity accepted")
	}
}
func TestSnapshotPropagatesFailureAndCancellation(t *testing.T) {
	failure := errors.New("discovery failed")
	r := snapshotReader{err: failure}
	s := &Service{reader: r, discoverer: r}
	if _, err := s.Snapshot(context.Background()); !errors.Is(err, failure) {
		t.Fatal("discovery failure swallowed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Snapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation swallowed")
	}
	s = &Service{}
	items, err := s.Snapshot(context.Background())
	if err != nil || items == nil || len(items) != 0 {
		t.Fatal("empty successful snapshot not preserved")
	}
}
