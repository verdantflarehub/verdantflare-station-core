package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"strings"
	"testing"
	"time"
)

type workloadReader struct {
	pods              []catalog.RawPodItem
	samples           []catalog.RawPodMetricItem
	podErr, metricErr error
}

func (r workloadReader) ListPods(context.Context) ([]catalog.RawPodItem, error) {
	return r.pods, r.podErr
}
func (r workloadReader) ListPodMetrics(context.Context) ([]catalog.RawPodMetricItem, error) {
	return r.samples, r.metricErr
}

func workloadFixture() workloadReader {
	return workloadReader{pods: []catalog.RawPodItem{{UID: "uid-new", Name: "blender-a", Namespace: "verdantflare-blender", Phase: "Running",
		CreatedAt: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano), Labels: map[string]string{"verdantflare.com/instance": "blenderA"},
		Containers: []catalog.RawContainer{{Name: "sidecar"}, {Name: "blender", CPUReq: "2", CPULim: "8", MemReq: "4Gi", MemLim: "16Gi", GPUReq: "1"}}}},
		samples: []catalog.RawPodMetricItem{{Name: "blender-a", Namespace: "verdantflare-blender", Container: "blender", Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Window: "15s", CPUUsage: "500000n", MemUsage: "0"}}}
}

func TestWorkloadsIdentityAndRealZero(t *testing.T) {
	r := workloadFixture()
	s := &Service{k8s: r}
	result, err := s.GetWorkloads(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.SchemaVersion != 2 || len(result.Workloads) != 1 || result.Workloads[0].InstanceAlias != "blenderA" {
		t.Fatal(result)
	}
	containers := result.Workloads[0].Containers
	if containers[0].CPU.Value != nil {
		t.Fatal("sidecar inherited Blender sample")
	}
	cpu, mem := containers[1].CPU, containers[1].Memory
	if cpu.Value == nil || *cpu.Value != .0005 || *cpu.Request != 2 || *mem.Value != 0 || *mem.Limit != 16*1024*1024*1024 {
		t.Fatal(cpu, mem)
	}
	if result.Summary.TotalGPURequested != 1 {
		t.Fatal(result.Summary)
	}
	data, _ := json.Marshal(result)
	if strings.Contains(string(data), "gpu_index") || strings.Contains(string(data), "gpu_vram") {
		t.Fatal("guessed GPU binding")
	}
}

func TestWorkloadUnusableSamplesNeverBecomeZero(t *testing.T) {
	for _, name := range []string{"metrics_error", "missing", "duplicate", "wrong_uid", "old_pod", "overlap_window", "wrong_container", "stale", "future", "invalid_cpu", "invalid_window"} {
		t.Run(name, func(t *testing.T) {
			r := workloadFixture()
			switch name {
			case "metrics_error":
				r.metricErr = errors.New("offline")
			case "missing":
				r.samples = nil
			case "duplicate":
				r.samples = append(r.samples, r.samples[0])
			case "wrong_uid":
				r.samples[0].UID = "old"
			case "old_pod":
				r.pods[0].CreatedAt = time.Now().Add(time.Second).UTC().Format(time.RFC3339Nano)
			case "overlap_window":
				r.pods[0].CreatedAt = time.Now().Add(-5 * time.Second).UTC().Format(time.RFC3339Nano)
			case "wrong_container":
				r.samples[0].Container = "other"
			case "stale":
				r.samples[0].Timestamp = time.Now().Add(-31 * time.Second).UTC().Format(time.RFC3339Nano)
			case "future":
				r.samples[0].Timestamp = time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)
			case "invalid_cpu":
				r.samples[0].CPUUsage = "-1"
			case "invalid_window":
				r.samples[0].Window = "?"
			}
			result, err := (&Service{k8s: r}).GetWorkloads(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			cpu := result.Workloads[0].Containers[1].CPU
			if cpu.Value != nil || cpu.Quality == "fresh" || cpu.Request == nil || *cpu.Request != 2 {
				t.Fatal(cpu)
			}
			if name == "stale" && (cpu.Quality != "stale" || cpu.SampledAt == nil) {
				t.Fatal(cpu)
			}
		})
	}
}

func TestWorkloadsEmptyAndFailure(t *testing.T) {
	for _, r := range []workloadReader{{}, {podErr: errors.New("offline")}} {
		result, err := (&Service{k8s: r}).GetWorkloads(context.Background())
		if r.podErr != nil {
			if err == nil {
				t.Fatal("pod listing error hidden")
			}
			continue
		}
		if err != nil || result.Workloads == nil || len(result.Workloads) != 0 || result.Summary.TotalPods != 0 {
			t.Fatal(result, err)
		}
	}
	if _, err := (&Service{}).GetWorkloads(context.Background()); err == nil {
		t.Fatal("missing reader accepted")
	}
}
