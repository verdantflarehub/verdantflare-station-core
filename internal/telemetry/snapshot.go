package telemetry

import (
	"context"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
)

// Snapshot is the versioned collection boundary consumed by Resources.
// It contains no dashboard totals, health thresholds, workload grouping or age.
type Snapshot struct {
	SchemaVersion int                `json:"schema_version"`
	ObservedAt    string             `json:"observed_at"`
	HardwareState string             `json:"hardware_state"`
	GPUs          []SnapshotGPU      `json:"gpus"`
	Node          *SnapshotNode      `json:"node"`
	Workloads     []SnapshotWorkload `json:"workloads"`
}
type SnapshotGPU struct {
	Index        int      `json:"index"`
	UUID         string   `json:"uuid"`
	Device       string   `json:"device"`
	ModelName    string   `json:"model_name"`
	SampledAt    string   `json:"sampled_at"`
	Quality      string   `json:"quality"`
	FreeVRAMMB   *float64 `json:"free_vram_mb"`
	TotalVRAMMB  *float64 `json:"total_vram_mb"`
	UsedVRAMMB   *float64 `json:"used_vram_mb"`
	Utilization  *float64 `json:"utilization"`
	TemperatureC *float64 `json:"temperature_c"`
	PowerWatts   *float64 `json:"power_watts"`
}
type SnapshotDisk struct {
	Mountpoint  string   `json:"mountpoint"`
	Device      string   `json:"device"`
	FSType      string   `json:"fstype"`
	TotalBytes  *float64 `json:"total_bytes"`
	AvailBytes  *float64 `json:"avail_bytes"`
	UsedBytes   *float64 `json:"used_bytes"`
	UsedPercent *float64 `json:"used_percent"`
}
type SnapshotNode struct {
	SampledAt             string         `json:"sampled_at"`
	Quality               string         `json:"quality"`
	CPUCores              *float64       `json:"cpu_cores"`
	CPUUtilizationPercent *float64       `json:"cpu_utilization_percent"`
	CPULoad1              *float64       `json:"cpu_load1"`
	CPULoad5              *float64       `json:"cpu_load5"`
	CPULoad15             *float64       `json:"cpu_load15"`
	MemTotalBytes         *float64       `json:"mem_total_bytes"`
	MemAvailableBytes     *float64       `json:"mem_available_bytes"`
	MemUsedBytes          *float64       `json:"mem_used_bytes"`
	MemBuffersBytes       *float64       `json:"mem_buffers_bytes"`
	MemCachedBytes        *float64       `json:"mem_cached_bytes"`
	MemUsedPercent        *float64       `json:"mem_used_percent"`
	StorageDisks          []SnapshotDisk `json:"storage_disks"`
}
type SnapshotWorkload struct {
	Name          string            `json:"name"`
	Namespace     string            `json:"namespace"`
	PodName       string            `json:"pod_name"`
	PodUID        string            `json:"pod_uid"`
	InstanceAlias string            `json:"instance_alias"`
	Status        string            `json:"status"`
	CreatedAt     string            `json:"created_at"`
	Labels        map[string]string `json:"labels"`
	Containers    []ContainerMetric `json:"containers"`
}

// A range vector carries scrape times. Instant query evaluation time cannot
// turn a stale exporter observation into a new hardware measurement.
const snapshotQuery = `{__name__=~"DCGM_FI_DEV_(FB_FREE|FB_USED|FB_TOTAL|GPU_UTIL|GPU_TEMP|POWER_USAGE)|node_(cpu_seconds_total|load1|load5|load15|memory_(MemTotal|MemAvailable|Buffers|Cached)_bytes|filesystem_(size|avail)_bytes)"}[35s]`

type snapshotSeries struct {
	labels     map[string]string
	value      *float64
	previous   *float64
	at, before time.Time
}

func snapshotNumber(v float64) *float64 { return &v }
func snapshotSample(value []interface{}) (*float64, time.Time) {
	if len(value) != 2 {
		return nil, time.Time{}
	}
	timestamp, ok := value[0].(float64)
	n, err := parseValue(value)
	if !ok || err != nil || timestamp < 0 || timestamp > math.MaxInt64/1000 || math.IsNaN(timestamp) || math.IsInf(timestamp, 0) || math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
		return nil, time.Time{}
	}
	return &n, time.UnixMilli(int64(timestamp * 1000)).UTC()
}
func oldest(current, candidate time.Time) time.Time {
	if current.IsZero() || candidate.Before(current) {
		return candidate
	}
	return current
}

func (s *Service) GetSnapshot(ctx context.Context) (*Snapshot, error) {
	workloads, err := s.snapshotWorkloads(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := &Snapshot{SchemaVersion: 1, ObservedAt: now.Format(time.RFC3339Nano), HardwareState: "unavailable", Workloads: workloads}
	if s.httpClient == nil || s.baseURL == "" {
		return out, nil
	}
	response, err := s.queryVector(ctx, snapshotQuery)
	if err != nil || response.Data.ResultType != "matrix" {
		return out, nil
	}
	now = time.Now().UTC()
	series := map[string][]snapshotSeries{}
	stale := false
	for _, row := range response.Data.Result {
		if len(row.Values) == 0 {
			continue
		}
		v, at := snapshotSample(row.Values[len(row.Values)-1])
		if v == nil || at.After(now.Add(5*time.Second)) {
			continue
		}
		if now.Sub(at) > 30*time.Second {
			stale = true
			continue
		}
		item := snapshotSeries{labels: row.Metric, value: v, at: at}
		if len(row.Values) > 1 {
			item.previous, item.before = snapshotSample(row.Values[len(row.Values)-2])
		}
		series[row.Metric["__name__"]] = append(series[row.Metric["__name__"]], item)
	}
	out.GPUs = snapshotGPUs(series)
	out.Node = snapshotHost(series)
	if len(out.GPUs) > 0 || out.Node != nil {
		out.HardwareState = "fresh"
	} else if stale {
		out.HardwareState = "stale"
	}
	return out, nil
}

func snapshotGPUs(series map[string][]snapshotSeries) []SnapshotGPU {
	byID := map[string]map[string][]snapshotSeries{}
	for name, rows := range series {
		if !strings.HasPrefix(name, "DCGM_") {
			continue
		}
		for _, r := range rows {
			id := r.labels["UUID"]
			if id == "" {
				continue
			}
			if byID[id] == nil {
				byID[id] = map[string][]snapshotSeries{}
			}
			byID[id][name] = append(byID[id][name], r)
		}
	}
	var out []SnapshotGPU
	for id, fields := range byID {
		g := SnapshotGPU{UUID: id, Quality: "fresh"}
		at := time.Time{}
		identity := false
		get := func(name string) *float64 {
			rows := fields["DCGM_FI_DEV_"+name]
			if len(rows) != 1 {
				return nil
			}
			r := rows[0]
			index, err := strconv.Atoi(r.labels["gpu"])
			if err != nil || index < 0 {
				return nil
			}
			if identity && (index != g.Index || r.labels["device"] != g.Device || r.labels["modelName"] != g.ModelName) {
				return nil
			}
			identity = true
			g.Index, g.Device, g.ModelName = index, r.labels["device"], r.labels["modelName"]
			at = oldest(at, r.at)
			return r.value
		}
		g.FreeVRAMMB, g.UsedVRAMMB, g.TotalVRAMMB = get("FB_FREE"), get("FB_USED"), get("FB_TOTAL")
		g.Utilization, g.TemperatureC, g.PowerWatts = get("GPU_UTIL"), get("GPU_TEMP"), get("POWER_USAGE")
		if g.Utilization != nil && *g.Utilization > 100 {
			g.Utilization = nil
		}
		if g.TotalVRAMMB == nil && g.FreeVRAMMB != nil && g.UsedVRAMMB != nil {
			g.TotalVRAMMB = snapshotNumber(*g.FreeVRAMMB + *g.UsedVRAMMB)
		}
		if !at.IsZero() {
			g.SampledAt = at.Format(time.RFC3339Nano)
			out = append(out, g)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UUID < out[j].UUID })
	return out
}

func snapshotHost(series map[string][]snapshotSeries) *SnapshotNode {
	// Existing node contract is singular. Multiple exporter identities must not
	// be silently merged into a fictional host; multi-node presentation needs a
	// separately versioned list contract.
	instances := map[string]bool{}
	for name, rows := range series {
		if strings.HasPrefix(name, "node_") {
			for _, r := range rows {
				instances[r.labels["instance"]] = true
			}
		}
	}
	if len(instances) != 1 || instances[""] {
		return nil
	}
	n := &SnapshotNode{Quality: "fresh"}
	at := time.Time{}
	get := func(name string) *float64 {
		rows := series[name]
		if len(rows) != 1 {
			return nil
		}
		at = oldest(at, rows[0].at)
		return rows[0].value
	}
	n.CPULoad1, n.CPULoad5, n.CPULoad15 = get("node_load1"), get("node_load5"), get("node_load15")
	n.MemTotalBytes, n.MemAvailableBytes = get("node_memory_MemTotal_bytes"), get("node_memory_MemAvailable_bytes")
	n.MemBuffersBytes, n.MemCachedBytes = get("node_memory_Buffers_bytes"), get("node_memory_Cached_bytes")
	if n.MemTotalBytes != nil && n.MemAvailableBytes != nil && *n.MemAvailableBytes <= *n.MemTotalBytes {
		n.MemUsedBytes = snapshotNumber(*n.MemTotalBytes - *n.MemAvailableBytes)
		if *n.MemTotalBytes > 0 {
			n.MemUsedPercent = snapshotNumber(*n.MemUsedBytes / *n.MemTotalBytes * 100)
		}
	}
	cpuRows := map[string][]snapshotSeries{}
	for _, row := range series["node_cpu_seconds_total"] {
		if row.labels["mode"] == "idle" {
			cpuRows[row.labels["cpu"]] = append(cpuRows[row.labels["cpu"]], row)
		}
	}
	if len(cpuRows) > 0 {
		idle := 0.0
		rates, identities := true, true
		for cpu, rows := range cpuRows {
			if cpu == "" || len(rows) != 1 {
				identities = false
				break
			}
			r := rows[0]
			at = oldest(at, r.at)
			delta := r.at.Sub(r.before).Seconds()
			if r.previous == nil || delta <= 0 || delta > 30 || *r.value < *r.previous {
				rates = false
				continue
			}
			rate := (*r.value - *r.previous) / delta
			if rate < 0 || rate > 1.05 {
				rates = false
				continue
			}
			idle += math.Min(rate, 1)
		}
		if identities {
			n.CPUCores = snapshotNumber(float64(len(cpuRows)))
			if rates {
				n.CPUUtilizationPercent = snapshotNumber((1 - idle/float64(len(cpuRows))) * 100)
			}
		}
	}
	disks := map[string]map[string][]snapshotSeries{}
	for _, name := range []string{"node_filesystem_size_bytes", "node_filesystem_avail_bytes"} {
		for _, r := range series[name] {
			mount := r.labels["mountpoint"]
			if mount != "/" && mount != "/data" {
				continue
			}
			key := mount + "\x00" + r.labels["device"] + "\x00" + r.labels["fstype"]
			if disks[key] == nil {
				disks[key] = map[string][]snapshotSeries{}
			}
			disks[key][name] = append(disks[key][name], r)
		}
	}
	for _, fields := range disks {
		size, avail := fields["node_filesystem_size_bytes"], fields["node_filesystem_avail_bytes"]
		if len(size) != 1 {
			continue
		}
		r := size[0]
		at = oldest(at, r.at)
		d := SnapshotDisk{Mountpoint: r.labels["mountpoint"], Device: r.labels["device"], FSType: r.labels["fstype"], TotalBytes: r.value}
		if len(avail) == 1 && *avail[0].value <= *r.value {
			d.AvailBytes = avail[0].value
			at = oldest(at, avail[0].at)
			d.UsedBytes = snapshotNumber(*r.value - *d.AvailBytes)
			if *r.value > 0 {
				d.UsedPercent = snapshotNumber(*d.UsedBytes / *r.value * 100)
			}
		}
		n.StorageDisks = append(n.StorageDisks, d)
	}
	sort.Slice(n.StorageDisks, func(i, j int) bool { return n.StorageDisks[i].Mountpoint < n.StorageDisks[j].Mountpoint })
	if at.IsZero() {
		return nil
	}
	n.SampledAt = at.Format(time.RFC3339Nano)
	return n
}

func (s *Service) snapshotWorkloads(ctx context.Context) ([]SnapshotWorkload, error) {
	s.mu.RLock()
	reader := s.k8s
	s.mu.RUnlock()
	if reader == nil {
		return nil, errors.New("workload reader unavailable")
	}
	pods, err := reader.ListPods(ctx)
	if err != nil {
		return nil, err
	}
	samples, metricsErr := reader.ListPodMetrics(ctx)
	byContainer := map[string][]catalog.RawPodMetricItem{}
	if metricsErr == nil {
		for _, sample := range samples {
			key := sample.Namespace + "/" + sample.Name + "/" + sample.Container
			byContainer[key] = append(byContainer[key], sample)
		}
	}
	now := time.Now().UTC()
	out := []SnapshotWorkload{}
	for _, pod := range pods {
		if !strings.HasPrefix(pod.Namespace, "verdantflare-") {
			continue
		}
		w := SnapshotWorkload{Name: pod.Name, PodName: pod.Name, PodUID: pod.UID, Namespace: pod.Namespace, InstanceAlias: pod.Labels["verdantflare.com/instance"], Status: pod.Phase, CreatedAt: pod.CreatedAt, Labels: map[string]string{}, Containers: []ContainerMetric{}}
		if name := pod.Labels["app.kubernetes.io/name"]; name != "" {
			w.Labels["app.kubernetes.io/name"] = name
		}
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
			w.Containers = append(w.Containers, ContainerMetric{Name: c.Name, Ready: c.Ready, Restarts: c.RestartCount, CPU: workloadSample(c.CPUReq, c.CPULim, "cores", cpu, sample, pod, now), Memory: workloadSample(c.MemReq, c.MemLim, "bytes", memory, sample, pod, now), GPURequest: quantity(c.GPUReq), GPULimit: quantity(c.GPULim)})
		}
		out = append(out, w)
	}
	return out, nil
}
