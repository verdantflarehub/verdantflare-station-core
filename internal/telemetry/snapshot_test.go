package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type snapshotTransport func(*http.Request) (*http.Response, error)

func (f snapshotTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func snapshotRow(name string, at time.Time, value string, labels map[string]string) map[string]any {
	metric := map[string]string{"__name__": name, "instance": "fixture:9100"}
	for k, v := range labels {
		metric[k] = v
	}
	return map[string]any{"metric": metric, "values": [][]any{{float64(at.UnixMilli()) / 1000, value}}}
}

func snapshotService(rows []any) *Service {
	return &Service{k8s: workloadFixture(), baseURL: "http://telemetry.invalid", httpClient: &http.Client{Transport: snapshotTransport(func(r *http.Request) (*http.Response, error) {
		body, _ := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": rows}})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
	})}}
}

func TestSnapshotPreservesRealZeroAndMissingHardware(t *testing.T) {
	now := time.Now().UTC().Add(-time.Second)
	labels := map[string]string{"UUID": "GPU-fixture", "gpu": "0", "device": "nvidia0", "modelName": "Fixture GPU"}
	s := snapshotService([]any{snapshotRow("DCGM_FI_DEV_FB_FREE", now, "32768", labels), snapshotRow("DCGM_FI_DEV_FB_USED", now, "0", labels), snapshotRow("node_memory_MemTotal_bytes", now, "17179869184", nil)})
	out, err := s.GetSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.SchemaVersion != 1 || out.HardwareState != "fresh" || len(out.GPUs) != 1 || *out.GPUs[0].UsedVRAMMB != 0 || *out.GPUs[0].TotalVRAMMB != 32768 {
		t.Fatal(out)
	}
	if out.GPUs[0].TemperatureC != nil || out.Node.MemUsedBytes != nil || out.Node.CPUCores != nil {
		t.Fatal("missing observation became a default")
	}
	if out.GPUs[0].SampledAt == out.ObservedAt {
		t.Fatal("scrape time replaced by request time")
	}
	encoded, _ := json.Marshal(out.Workloads[0])
	for _, field := range []string{`"summary"`, `"display_name"`, `"type"`, `"age"`} {
		if strings.Contains(string(encoded), field) {
			t.Fatal("dashboard projection remains in raw facts", field)
		}
	}
	if out.Workloads[0].Containers[1].Memory.Value == nil || *out.Workloads[0].Containers[1].Memory.Value != 0 {
		t.Fatal("workload zero lost")
	}
}

func TestSnapshotRejectsStaleDuplicateAndUnknownNodeIdentity(t *testing.T) {
	for _, kind := range []string{"stale", "future", "duplicate", "multiple_hosts", "missing_instance"} {
		t.Run(kind, func(t *testing.T) {
			now := time.Now().UTC().Add(-time.Second)
			if kind == "stale" {
				now = now.Add(-31 * time.Second)
			}
			if kind == "future" {
				now = now.Add(time.Minute)
			}
			row := snapshotRow("node_memory_MemTotal_bytes", now, "1024", nil)
			rows := []any{row}
			if kind == "duplicate" {
				rows = append(rows, row)
			}
			if kind == "multiple_hosts" {
				rows = append(rows, snapshotRow("node_memory_MemAvailable_bytes", now, "512", map[string]string{"instance": "other:9100"}))
			}
			if kind == "missing_instance" {
				row["metric"].(map[string]string)["instance"] = ""
			}
			out, err := snapshotService(rows).GetSnapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if out.Node != nil {
				t.Fatal("ambiguous/stale source became a host")
			}
			if kind == "stale" && out.HardwareState != "stale" {
				t.Fatal(out.HardwareState)
			}
		})
	}
}

func TestSnapshotCPUUsesFreshScrapeCounterDelta(t *testing.T) {
	now := time.Now().UTC().Add(-time.Second)
	row := snapshotRow("node_cpu_seconds_total", now, "120", map[string]string{"cpu": "0", "mode": "idle"})
	row["values"] = [][]any{{float64(now.Add(-10*time.Second).UnixMilli()) / 1000, "115"}, {float64(now.UnixMilli()) / 1000, "120"}}
	out, err := snapshotService([]any{row}).GetSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Node == nil || *out.Node.CPUCores != 1 || *out.Node.CPUUtilizationPercent != 50 {
		t.Fatal("CPU counter interpretation incorrect", out.Node)
	}
	row["values"] = [][]any{{float64(now.Add(-10*time.Second).UnixMilli()) / 1000, "121"}, {float64(now.UnixMilli()) / 1000, "120"}}
	out, err = snapshotService([]any{row}).GetSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Node.CPUUtilizationPercent != nil {
		t.Fatal("counter reset accepted as CPU usage")
	}
}

func TestSnapshotPrometheusFailureDoesNotReuseLegacyCache(t *testing.T) {
	s := snapshotService(nil)
	s.cachedGPUs = []GPUMetric{{TotalVRAMMB: 32768}}
	s.cachedNode = &NodeMetric{CPUCores: 24}
	s.lastGPUAt = time.Now()
	s.lastNodeAt = time.Now()
	s.httpClient = &http.Client{Transport: snapshotTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("unavailable"))}, nil
	})}
	out, err := s.GetSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.HardwareState != "unavailable" || out.GPUs != nil || out.Node != nil || len(out.Workloads) != 1 {
		t.Fatal("legacy cached measurements leaked into raw snapshot")
	}
}
