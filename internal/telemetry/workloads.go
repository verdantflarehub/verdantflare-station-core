package telemetry

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"k8s.io/apimachinery/pkg/api/resource"
)

func quantity(raw string) *float64 {
	q, err := resource.ParseQuantity(raw)
	if err != nil {
		return nil
	}
	value := q.AsApproximateFloat64()
	if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return nil
	}
	return &value
}

func workloadSample(req, limit, unit, raw string, sample *catalog.RawPodMetricItem, pod catalog.RawPodItem, now time.Time) ResourceMetric {
	m := ResourceMetric{Request: quantity(req), Limit: quantity(limit), Unit: unit, Scope: "instance_container", Quality: "unavailable", Reason: "METRICS_UNAVAILABLE"}
	if sample == nil {
		return m
	}
	at, err := time.Parse(time.RFC3339Nano, sample.Timestamp)
	created, createdErr := time.Parse(time.RFC3339Nano, pod.CreatedAt)
	window, windowErr := time.ParseDuration(sample.Window)
	if err != nil || createdErr != nil || windowErr != nil || window < 0 || pod.UID == "" ||
		(sample.UID != "" && sample.UID != pod.UID) || at.Add(-window).Before(created) || at.After(now.Add(5*time.Second)) {
		m.Reason = "SAMPLE_IDENTITY_UNVERIFIED"
		return m
	}
	stamp := at.UTC().Format(time.RFC3339Nano)
	m.SampledAt = &stamp
	if now.Sub(at) > 30*time.Second {
		m.Quality = "stale"
		m.Reason = "SAMPLE_STALE"
		return m
	}
	if m.Value = quantity(raw); m.Value == nil {
		m.Reason = "SAMPLE_INVALID"
		return m
	}
	m.Quality, m.Reason = "fresh", ""
	return m
}

// GetWorkloads never fabricates workload records or derives GPU ownership from names.
func (s *Service) GetWorkloads(ctx context.Context) (*WorkloadsResponse, error) {
	// Avoid caching identity-sensitive samples: a restarted Pod must not inherit old usage.
	if s.k8s == nil {
		return nil, fmt.Errorf("workload reader unavailable")
	}
	pods, err := s.k8s.ListPods(ctx)
	if err != nil {
		return nil, err
	}
	observed := time.Now().UTC()
	samples, metricsErr := s.k8s.ListPodMetrics(ctx)
	byContainer := map[string][]catalog.RawPodMetricItem{}
	if metricsErr == nil {
		for _, sample := range samples {
			key := sample.Namespace + "/" + sample.Name + "/" + sample.Container
			byContainer[key] = append(byContainer[key], sample)
		}
	}
	now := time.Now().UTC()
	out := &WorkloadsResponse{SchemaVersion: 2, Workloads: []WorkloadMetric{}, UpdatedAt: observed.Format(time.RFC3339Nano)}
	for _, pod := range pods {
		if !strings.HasPrefix(pod.Namespace, "verdantflare-") {
			continue
		}
		w := WorkloadMetric{Name: pod.Name, PodName: pod.Name, PodUID: pod.UID, Namespace: pod.Namespace,
			InstanceAlias: pod.Labels["verdantflare.com/instance"], Status: pod.Phase, Age: formatAge(pod.StartTime),
			Type: "infra", DisplayName: pod.Labels["app.kubernetes.io/name"], CreatedAt: pod.CreatedAt, Containers: []ContainerMetric{}}
		for _, c := range pod.Containers {
			var sample *catalog.RawPodMetricItem
			matches := byContainer[pod.Namespace+"/"+pod.Name+"/"+c.Name]
			if len(matches) == 1 {
				sample = &matches[0]
			}
			cpu, memory := "", ""
			if sample != nil {
				cpu, memory = sample.CPUUsage, sample.MemUsage
			}
			metric := ContainerMetric{Name: c.Name, Ready: c.Ready, Restarts: c.RestartCount,
				CPU:        workloadSample(c.CPUReq, c.CPULim, "cores", cpu, sample, pod, now),
				Memory:     workloadSample(c.MemReq, c.MemLim, "bytes", memory, sample, pod, now),
				GPURequest: quantity(c.GPUReq), GPULimit: quantity(c.GPULim)}
			if metric.GPURequest != nil && *metric.GPURequest > 0 {
				w.Type = "gpu"
			}
			if metric.GPULimit != nil && *metric.GPULimit > 0 {
				w.Type = "gpu"
			}
			// Summary covers active Pod configuration requests, not actual device allocations.
			if pod.Phase == "Running" || pod.Phase == "Pending" {
				if metric.CPU.Request != nil {
					out.Summary.TotalCPURequested += *metric.CPU.Request
				}
				if metric.Memory.Request != nil {
					out.Summary.TotalMemoryRequested += *metric.Memory.Request
				}
				if metric.GPURequest != nil {
					out.Summary.TotalGPURequested += *metric.GPURequest
				}
			}
			w.Containers = append(w.Containers, metric)
		}
		out.Summary.TotalPods++
		if w.Type == "gpu" {
			out.Summary.GPUPods++
		} else {
			out.Summary.InfraPods++
		}
		out.Workloads = append(out.Workloads, w)
	}
	return out, nil
}
