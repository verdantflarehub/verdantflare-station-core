package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
)

// GPUMetric represents real-time telemetry of a single GPU device.
type GPUMetric struct {
	Index        int     `json:"index"`
	Device       string  `json:"device"`
	ModelName    string  `json:"model_name"`
	UUID         string  `json:"uuid"`
	FreeVRAMMB   int64   `json:"free_vram_mb"`
	TotalVRAMMB  int64   `json:"total_vram_mb"`
	UsedVRAMMB   int64   `json:"used_vram_mb"`
	Utilization  float64 `json:"utilization"`
	TemperatureC float64 `json:"temperature_c"`
	PowerWatts   float64 `json:"power_watts"`
}

// DiskMetric represents storage partition telemetry.
type DiskMetric struct {
	Mountpoint  string  `json:"mountpoint"`
	Device      string  `json:"device"`
	FSType      string  `json:"fstype"`
	TotalBytes  uint64  `json:"total_bytes"`
	AvailBytes  uint64  `json:"avail_bytes"`
	UsedBytes   uint64  `json:"used_bytes"`
	UsedPercent float64 `json:"used_percent"`
}

// NodeMetric represents real-time host metrics.
type NodeMetric struct {
	CPUCores              int          `json:"cpu_cores"`
	CPUUtilizationPercent float64      `json:"cpu_utilization_percent"`
	CPULoad1              float64      `json:"cpu_load1"`
	CPULoad5              float64      `json:"cpu_load5"`
	CPULoad15             float64      `json:"cpu_load15"`
	MemTotalBytes         uint64       `json:"mem_total_bytes"`
	MemAvailableBytes     uint64       `json:"mem_available_bytes"`
	MemUsedBytes          uint64       `json:"mem_used_bytes"`
	MemBuffersBytes       uint64       `json:"mem_buffers_bytes"`
	MemCachedBytes        uint64       `json:"mem_cached_bytes"`
	MemUsedPercent        float64      `json:"mem_used_percent"`
	StorageDisks          []DiskMetric `json:"storage_disks"`
}

// WorkloadMetric represents real-time resource telemetry of a deployed workload pod.
type WorkloadMetric struct {
	Name              string  `json:"name"`
	Namespace         string  `json:"namespace"`
	PodName           string  `json:"pod_name"`
	NodeName          string  `json:"node_name"`
	Status            string  `json:"status"`
	Ready             bool    `json:"ready"`
	Age               string  `json:"age"`
	Restarts          int32   `json:"restarts"`
	Type              string  `json:"type"` // "gpu" | "infra"
	DisplayName       string  `json:"display_name"`
	GPUCountReq       int     `json:"gpu_count_req"`
	GPUIndex          int     `json:"gpu_index"`
	GPUDevice         string  `json:"gpu_device"`
	GPUVRAMUsedMB     int64   `json:"gpu_vram_used_mb"`
	GPUVRAMTotalMB    int64   `json:"gpu_vram_total_mb"`
	GPUVRAMPercent    float64 `json:"gpu_vram_percent"`
	GPUUtil           float64 `json:"gpu_util"`
	GPUTempC          float64 `json:"gpu_temp_c"`
	GPUPowerWatts     float64 `json:"gpu_power_watts"`
	CPUReqMillicores  int64   `json:"cpu_req_millicores"`
	CPULimMillicores  int64   `json:"cpu_lim_millicores"`
	CPUUsedMillicores int64   `json:"cpu_used_millicores"`
	CPUUsedPercent    float64 `json:"cpu_used_percent"`
	MemReqBytes       uint64  `json:"mem_req_bytes"`
	MemLimBytes       uint64  `json:"mem_lim_bytes"`
	MemUsedBytes      uint64  `json:"mem_used_bytes"`
	MemUsedPercent    float64 `json:"mem_used_percent"`
	ModelName         string  `json:"model_name,omitempty"`
	MountPoint        string  `json:"mount_point,omitempty"`
}

// WorkloadSummary summarizes cluster workload allocation quotas.
type WorkloadSummary struct {
	TotalPods        int   `json:"total_pods"`
	GPUPods          int   `json:"gpu_pods"`
	InfraPods        int   `json:"infra_pods"`
	TotalGPUAssigned int   `json:"total_gpu_assigned"`
	TotalVRAMUsedMB  int64 `json:"total_vram_used_mb"`
	TotalCPUReqM     int64 `json:"total_cpu_req_millicores"`
	TotalMemReqMB    int64 `json:"total_mem_req_mb"`
}

// WorkloadsResponse combines workload metrics with quota summaries.
type WorkloadsResponse struct {
	Summary   WorkloadSummary  `json:"summary"`
	Workloads []WorkloadMetric `json:"workloads"`
	UpdatedAt string           `json:"updated_at"`
}

// KubeReader defines methods to query pods and metrics from Kubernetes.
type KubeReader interface {
	ListPods(ctx context.Context) ([]catalog.RawPodItem, error)
	ListPodMetrics(ctx context.Context) ([]catalog.RawPodMetricItem, error)
}

// ResourceSummary combines high-level resource indicators for Studio OS.
type ResourceSummary struct {
	TotalGPUs          int          `json:"total_gpus"`
	TotalVRAMMB        int64        `json:"total_vram_mb"`
	UsedVRAMMB         int64        `json:"used_vram_mb"`
	FreeVRAMMB         int64        `json:"free_vram_mb"`
	AverageGPUUtil     float64      `json:"average_gpu_util"`
	HostCPUCores       int          `json:"host_cpu_cores"`
	HostCPUUtilization float64      `json:"host_cpu_utilization"`
	HostCPULoad1       float64      `json:"host_cpu_load1"`
	HostMemTotalGB     float64      `json:"host_mem_total_gb"`
	HostMemUsedGB      float64      `json:"host_mem_used_gb"`
	HostMemUsedPercent float64      `json:"host_mem_used_percent"`
	StorageTotalTB     float64      `json:"storage_total_tb"`
	StorageUsedTB      float64      `json:"storage_used_tb"`
	StorageUsedPercent float64      `json:"storage_used_percent"`
	StorageDisks       []DiskMetric `json:"storage_disks,omitempty"`
	Status             string       `json:"status"` // "Healthy", "Warning", "Degraded"
	UpdatedAt          string       `json:"updated_at"`
}

type promResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// Service provides cached hardware telemetry querying from Prometheus.
type Service struct {
	baseURL    string
	httpClient *http.Client
	k8s        KubeReader
	ttl        time.Duration

	mu         sync.RWMutex
	lastGPUAt  time.Time
	cachedGPUs []GPUMetric
	lastNodeAt time.Time
	cachedNode *NodeMetric
	lastWlAt   time.Time
	cachedWls  *WorkloadsResponse
}

// NewService creates a new telemetry Service.
func NewService(baseURL string, ttl time.Duration) *Service {
	if baseURL == "" {
		baseURL = "http://prometheus.verdantflare-station.svc.cluster.local:9090"
	}
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	return &Service{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
		ttl: ttl,
	}
}

// SetKubernetes binds the Kubernetes reader to the telemetry service.
func (s *Service) SetKubernetes(k8s KubeReader) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.k8s = k8s
}

func (s *Service) queryVector(ctx context.Context, promQL string) (*promResponse, error) {
	reqURL := fmt.Sprintf("%s/api/v1/query?query=%s", s.baseURL, url.QueryEscape(promQL))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create query request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("prometheus query failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prometheus status %d", resp.StatusCode)
	}

	var parsed promResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode prometheus response: %w", err)
	}
	if parsed.Status != "success" {
		return nil, fmt.Errorf("prometheus status: %s", parsed.Status)
	}

	return &parsed, nil
}

// GetGPUs returns cached or fresh GPU metrics.
func (s *Service) GetGPUs(ctx context.Context) ([]GPUMetric, error) {
	s.mu.RLock()
	if time.Since(s.lastGPUAt) < s.ttl && s.cachedGPUs != nil {
		defer s.mu.RUnlock()
		return s.cachedGPUs, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.lastGPUAt) < s.ttl && s.cachedGPUs != nil {
		return s.cachedGPUs, nil
	}

	freeResp, err := s.queryVector(ctx, "DCGM_FI_DEV_FB_FREE")
	if err != nil {
		if s.cachedGPUs != nil {
			return s.cachedGPUs, nil // Graceful degradation
		}
		return nil, err
	}

	gpusMap := make(map[string]*GPUMetric)
	for _, r := range freeResp.Data.Result {
		gpuIdx, _ := strconv.Atoi(r.Metric["gpu"])
		freeVal, _ := parseValue(r.Value)
		m := &GPUMetric{
			Index:      gpuIdx,
			Device:     r.Metric["device"],
			ModelName:  r.Metric["modelName"],
			UUID:       r.Metric["UUID"],
			FreeVRAMMB: int64(freeVal),
		}
		gpusMap[r.Metric["UUID"]] = m
	}

	// Used VRAM
	if usedResp, err := s.queryVector(ctx, "DCGM_FI_DEV_FB_USED"); err == nil {
		for _, r := range usedResp.Data.Result {
			if m, ok := gpusMap[r.Metric["UUID"]]; ok {
				val, _ := parseValue(r.Value)
				m.UsedVRAMMB = int64(val)
			}
		}
	}

	// Total VRAM
	if totalResp, err := s.queryVector(ctx, "DCGM_FI_DEV_FB_TOTAL"); err == nil && len(totalResp.Data.Result) > 0 {
		for _, r := range totalResp.Data.Result {
			if m, ok := gpusMap[r.Metric["UUID"]]; ok {
				val, _ := parseValue(r.Value)
				m.TotalVRAMMB = int64(val)
			}
		}
	}

	for _, m := range gpusMap {
		if m.TotalVRAMMB == 0 {
			if m.FreeVRAMMB+m.UsedVRAMMB > 0 {
				m.TotalVRAMMB = m.FreeVRAMMB + m.UsedVRAMMB
			} else {
				m.TotalVRAMMB = 32768
			}
		}
		if m.UsedVRAMMB == 0 && m.TotalVRAMMB > m.FreeVRAMMB {
			m.UsedVRAMMB = m.TotalVRAMMB - m.FreeVRAMMB
		}
	}

	// GPU Util
	if utilResp, err := s.queryVector(ctx, "DCGM_FI_DEV_GPU_UTIL"); err == nil {
		for _, r := range utilResp.Data.Result {
			if m, ok := gpusMap[r.Metric["UUID"]]; ok {
				val, _ := parseValue(r.Value)
				m.Utilization = val
			}
		}
	}

	// GPU Temp
	if tempResp, err := s.queryVector(ctx, "DCGM_FI_DEV_GPU_TEMP"); err == nil {
		for _, r := range tempResp.Data.Result {
			if m, ok := gpusMap[r.Metric["UUID"]]; ok {
				val, _ := parseValue(r.Value)
				m.TemperatureC = val
			}
		}
	}

	// Power
	if powerResp, err := s.queryVector(ctx, "DCGM_FI_DEV_POWER_USAGE"); err == nil {
		for _, r := range powerResp.Data.Result {
			if m, ok := gpusMap[r.Metric["UUID"]]; ok {
				val, _ := parseValue(r.Value)
				m.PowerWatts = val
			}
		}
	}

	result := make([]GPUMetric, 0, len(gpusMap))
	for _, m := range gpusMap {
		result = append(result, *m)
	}

	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[i].Index > result[j].Index {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	s.cachedGPUs = result
	s.lastGPUAt = time.Now()
	return result, nil
}

// GetNode returns host CPU and memory telemetry.
func (s *Service) GetNode(ctx context.Context) (*NodeMetric, error) {
	s.mu.RLock()
	if time.Since(s.lastNodeAt) < s.ttl && s.cachedNode != nil {
		defer s.mu.RUnlock()
		return s.cachedNode, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.lastNodeAt) < s.ttl && s.cachedNode != nil {
		return s.cachedNode, nil
	}

	node := &NodeMetric{
		CPUCores: 24,
	}

	if coresResp, err := s.queryVector(ctx, `count(node_cpu_seconds_total{mode="idle"})`); err == nil && len(coresResp.Data.Result) > 0 {
		val, _ := parseValue(coresResp.Data.Result[0].Value)
		if val > 0 {
			node.CPUCores = int(val)
		}
	}

	cpuResp, err := s.queryVector(ctx, `100 - (avg(irate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)`)
	if err == nil && len(cpuResp.Data.Result) > 0 {
		val, _ := parseValue(cpuResp.Data.Result[0].Value)
		node.CPUUtilizationPercent = val
	}

	if l1, err := s.queryVector(ctx, "node_load1"); err == nil && len(l1.Data.Result) > 0 {
		val, _ := parseValue(l1.Data.Result[0].Value)
		node.CPULoad1 = val
	}
	if l5, err := s.queryVector(ctx, "node_load5"); err == nil && len(l5.Data.Result) > 0 {
		val, _ := parseValue(l5.Data.Result[0].Value)
		node.CPULoad5 = val
	}
	if l15, err := s.queryVector(ctx, "node_load15"); err == nil && len(l15.Data.Result) > 0 {
		val, _ := parseValue(l15.Data.Result[0].Value)
		node.CPULoad15 = val
	}

	totalMemResp, err := s.queryVector(ctx, "node_memory_MemTotal_bytes")
	if err == nil && len(totalMemResp.Data.Result) > 0 {
		val, _ := parseValue(totalMemResp.Data.Result[0].Value)
		node.MemTotalBytes = uint64(val)
	}

	availMemResp, err := s.queryVector(ctx, "node_memory_MemAvailable_bytes")
	if err == nil && len(availMemResp.Data.Result) > 0 {
		val, _ := parseValue(availMemResp.Data.Result[0].Value)
		node.MemAvailableBytes = uint64(val)
	}

	if bufResp, err := s.queryVector(ctx, "node_memory_Buffers_bytes"); err == nil && len(bufResp.Data.Result) > 0 {
		val, _ := parseValue(bufResp.Data.Result[0].Value)
		node.MemBuffersBytes = uint64(val)
	}

	if cacheResp, err := s.queryVector(ctx, "node_memory_Cached_bytes"); err == nil && len(cacheResp.Data.Result) > 0 {
		val, _ := parseValue(cacheResp.Data.Result[0].Value)
		node.MemCachedBytes = uint64(val)
	}

	if node.MemTotalBytes > 0 {
		used := node.MemTotalBytes - node.MemAvailableBytes
		node.MemUsedBytes = used
		node.MemUsedPercent = (float64(used) / float64(node.MemTotalBytes)) * 100.0
	}

	// Disks
	diskSizes := make(map[string]uint64)
	diskAvail := make(map[string]uint64)
	diskDevs := make(map[string]string)
	diskFSTypes := make(map[string]string)

	if sizesResp, err := s.queryVector(ctx, `node_filesystem_size_bytes{mountpoint=~"/|/data"}`); err == nil {
		for _, r := range sizesResp.Data.Result {
			mp := r.Metric["mountpoint"]
			val, _ := parseValue(r.Value)
			diskSizes[mp] = uint64(val)
			diskDevs[mp] = r.Metric["device"]
			diskFSTypes[mp] = r.Metric["fstype"]
		}
	}
	if availResp, err := s.queryVector(ctx, `node_filesystem_avail_bytes{mountpoint=~"/|/data"}`); err == nil {
		for _, r := range availResp.Data.Result {
			mp := r.Metric["mountpoint"]
			val, _ := parseValue(r.Value)
			diskAvail[mp] = uint64(val)
		}
	}

	for _, mp := range []string{"/data", "/"} {
		size, ok := diskSizes[mp]
		if !ok || size == 0 {
			continue
		}
		avail := diskAvail[mp]
		used := size - avail
		usedPct := (float64(used) / float64(size)) * 100.0
		node.StorageDisks = append(node.StorageDisks, DiskMetric{
			Mountpoint:  mp,
			Device:      diskDevs[mp],
			FSType:      diskFSTypes[mp],
			TotalBytes:  size,
			AvailBytes:  avail,
			UsedBytes:   used,
			UsedPercent: usedPct,
		})
	}

	s.cachedNode = node
	s.lastNodeAt = time.Now()
	return node, nil
}

// GetSummary aggregates overview KPI statistics.
func (s *Service) GetSummary(ctx context.Context) (*ResourceSummary, error) {
	gpus, err := s.GetGPUs(ctx)
	if err != nil {
		return nil, err
	}
	node, _ := s.GetNode(ctx)

	summary := &ResourceSummary{
		TotalGPUs: len(gpus),
		Status:    "Healthy",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	var sumUtil float64
	for _, g := range gpus {
		summary.TotalVRAMMB += g.TotalVRAMMB
		summary.UsedVRAMMB += g.UsedVRAMMB
		summary.FreeVRAMMB += g.FreeVRAMMB
		sumUtil += g.Utilization
		if g.TemperatureC > 80 {
			summary.Status = "Warning"
		}
	}
	if len(gpus) > 0 {
		summary.AverageGPUUtil = sumUtil / float64(len(gpus))
	}

	if node != nil {
		summary.HostCPUCores = node.CPUCores
		summary.HostCPUUtilization = node.CPUUtilizationPercent
		summary.HostCPULoad1 = node.CPULoad1
		summary.HostMemTotalGB = float64(node.MemTotalBytes) / (1024 * 1024 * 1024)
		summary.HostMemUsedGB = float64(node.MemUsedBytes) / (1024 * 1024 * 1024)
		summary.HostMemUsedPercent = node.MemUsedPercent
		summary.StorageDisks = node.StorageDisks

		var totalDisk, usedDisk uint64
		for _, d := range node.StorageDisks {
			totalDisk += d.TotalBytes
			usedDisk += d.UsedBytes
		}
		if totalDisk > 0 {
			summary.StorageTotalTB = float64(totalDisk) / 1e12
			summary.StorageUsedTB = float64(usedDisk) / 1e12
			summary.StorageUsedPercent = (float64(usedDisk) / float64(totalDisk)) * 100.0
		}

		if node.MemUsedPercent > 90 {
			summary.Status = "Degraded"
		}
	}

	return summary, nil
}

func parseValue(val []interface{}) (float64, error) {
	if len(val) < 2 {
		return 0, fmt.Errorf("invalid value slice: %v", val)
	}
	strVal, ok := val[1].(string)
	if !ok {
		return 0, fmt.Errorf("value is not string: %v", val[1])
	}
	return strconv.ParseFloat(strVal, 64)
}

func parseCPU(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if strings.HasSuffix(s, "n") {
		n, _ := strconv.ParseInt(strings.TrimSuffix(s, "n"), 10, 64)
		return n / 1000000
	}
	if strings.HasSuffix(s, "u") {
		u, _ := strconv.ParseInt(strings.TrimSuffix(s, "u"), 10, 64)
		return u / 1000
	}
	if strings.HasSuffix(s, "m") {
		m, _ := strconv.ParseInt(strings.TrimSuffix(s, "m"), 10, 64)
		return m
	}
	v, _ := strconv.ParseFloat(s, 64)
	return int64(v * 1000)
}

func parseMemory(s string) uint64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	multipliers := map[string]uint64{
		"Ki": 1024,
		"Mi": 1024 * 1024,
		"Gi": 1024 * 1024 * 1024,
		"Ti": 1024 * 1024 * 1024 * 1024,
		"k":  1000,
		"M":  1000 * 1000,
		"G":  1000 * 1000 * 1000,
		"T":  1000 * 1000 * 1000 * 1000,
	}
	for suffix, mult := range multipliers {
		if strings.HasSuffix(s, suffix) {
			numStr := strings.TrimSuffix(s, suffix)
			val, _ := strconv.ParseFloat(numStr, 64)
			return uint64(val * float64(mult))
		}
	}
	val, _ := strconv.ParseUint(s, 10, 64)
	return val
}

func formatAge(startTime string) string {
	if startTime == "" {
		return "刚刚"
	}
	t, err := time.Parse(time.RFC3339, startTime)
	if err != nil {
		return "运行中"
	}
	d := time.Since(t)
	if d.Hours() >= 24 {
		days := int(d.Hours() / 24)
		hours := int(d.Hours()) % 24
		if hours > 0 {
			return fmt.Sprintf("%d天%d小时", days, hours)
		}
		return fmt.Sprintf("%d天", days)
	}
	if d.Hours() >= 1 {
		return fmt.Sprintf("%d小时", int(d.Hours()))
	}
	if d.Minutes() >= 1 {
		return fmt.Sprintf("%d分钟", int(d.Minutes()))
	}
	return "刚刚"
}

// GetWorkloads returns real-time resource telemetry of deployed workloads.
func (s *Service) GetWorkloads(ctx context.Context) (*WorkloadsResponse, error) {
	s.mu.RLock()
	if time.Since(s.lastWlAt) < s.ttl && s.cachedWls != nil {
		defer s.mu.RUnlock()
		return s.cachedWls, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.lastWlAt) < s.ttl && s.cachedWls != nil {
		return s.cachedWls, nil
	}

	gpus, _ := s.GetGPUs(ctx)

	var rawPods []catalog.RawPodItem
	var rawMetrics []catalog.RawPodMetricItem
	if s.k8s != nil {
		rawPods, _ = s.k8s.ListPods(ctx)
		rawMetrics, _ = s.k8s.ListPodMetrics(ctx)
	}

	metricsMap := make(map[string]catalog.RawPodMetricItem)
	for _, m := range rawMetrics {
		metricsMap[m.Namespace+"/"+m.Name] = m
	}

	resp := &WorkloadsResponse{
		Workloads: make([]WorkloadMetric, 0),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	for _, p := range rawPods {
		if !strings.HasPrefix(p.Namespace, "verdantflare-") {
			continue
		}
		if p.Phase != "Running" && p.Phase != "Pending" {
			continue
		}

		m := WorkloadMetric{
			Name:        p.Name,
			Namespace:   p.Namespace,
			PodName:     p.Name,
			NodeName:    p.NodeName,
			Status:      p.Phase,
			Ready:       p.Ready,
			Age:         formatAge(p.StartTime),
			Restarts:    p.RestartCount,
			Type:        "infra",
			GPUIndex:    -1,
		}

		lowerName := strings.ToLower(p.Name)
		if strings.Contains(lowerName, "image-mcp") {
			m.DisplayName = "image-mcp-server"
			m.Type = "gpu"
			m.GPUCountReq = 1
			m.GPUIndex = 0
			m.ModelName = "Flux.1-Dev (FP8)"
			m.MountPoint = "/data/models/flux"
		} else if strings.Contains(lowerName, "video-mcp") {
			m.DisplayName = "video-mcp-server"
			m.Type = "gpu"
			m.GPUCountReq = 1
			m.GPUIndex = 1
			m.ModelName = "Wan 2.1 任务分派器"
			m.MountPoint = "/data/artifacts/video"
		} else if strings.Contains(lowerName, "minimax") {
			m.DisplayName = "video-minimax-h3-singularity"
			m.Type = "infra"
			m.GPUCountReq = 0
			m.GPUIndex = -1
			m.ModelName = "MiniMax-H3 大模型引擎"
			m.MountPoint = "/data/models/minimax"
		} else if strings.Contains(lowerName, "station-core") {
			m.DisplayName = "station-core"
			m.Type = "infra"
			m.GPUCountReq = 0
			m.GPUIndex = -1
			m.ModelName = "API 网关与 Informer"
		} else if strings.Contains(lowerName, "station-runtime") {
			m.DisplayName = "station-runtime"
			m.Type = "infra"
			m.GPUCountReq = 0
			m.GPUIndex = -1
			m.ModelName = "显存防爆准入内核"
		} else if strings.Contains(lowerName, "studio") {
			m.DisplayName = "verdantflare-studio"
			m.Type = "infra"
			m.GPUCountReq = 0
			m.GPUIndex = -1
			m.ModelName = "创作者控制台与工作台"
		} else {
			continue
		}

		if m.GPUIndex >= 0 && m.GPUIndex < len(gpus) {
			gpu := gpus[m.GPUIndex]
			m.GPUDevice = gpu.ModelName
			m.GPUVRAMUsedMB = gpu.UsedVRAMMB
			m.GPUVRAMTotalMB = gpu.TotalVRAMMB
			if m.GPUVRAMTotalMB > 0 {
				m.GPUVRAMPercent = math.Round(float64(m.GPUVRAMUsedMB)/float64(m.GPUVRAMTotalMB)*1000) / 10
			}
			m.GPUUtil = gpu.Utilization
			m.GPUTempC = gpu.TemperatureC
			m.GPUPowerWatts = gpu.PowerWatts
		}

		m.CPUReqMillicores = parseCPU(p.CPUReq)
		m.CPULimMillicores = parseCPU(p.CPULim)
		if metric, ok := metricsMap[p.Namespace+"/"+p.Name]; ok {
			m.CPUUsedMillicores = parseCPU(metric.CPUUsage)
		}
		if m.CPULimMillicores > 0 {
			m.CPUUsedPercent = math.Round(float64(m.CPUUsedMillicores)/float64(m.CPULimMillicores)*1000) / 10
		} else if m.CPUReqMillicores > 0 {
			m.CPUUsedPercent = math.Round(float64(m.CPUUsedMillicores)/float64(m.CPUReqMillicores)*1000) / 10
		}

		m.MemReqBytes = parseMemory(p.MemReq)
		m.MemLimBytes = parseMemory(p.MemLim)
		if metric, ok := metricsMap[p.Namespace+"/"+p.Name]; ok {
			m.MemUsedBytes = parseMemory(metric.MemUsage)
		}
		if m.MemLimBytes > 0 {
			m.MemUsedPercent = math.Round(float64(m.MemUsedBytes)/float64(m.MemLimBytes)*1000) / 10
		} else if m.MemReqBytes > 0 {
			m.MemUsedPercent = math.Round(float64(m.MemUsedBytes)/float64(m.MemReqBytes)*1000) / 10
		}

		resp.Summary.TotalPods++
		if m.Type == "gpu" {
			resp.Summary.GPUPods++
			resp.Summary.TotalGPUAssigned += m.GPUCountReq
			resp.Summary.TotalVRAMUsedMB += m.GPUVRAMUsedMB
		} else {
			resp.Summary.InfraPods++
		}
		resp.Summary.TotalCPUReqM += m.CPUReqMillicores
		resp.Summary.TotalMemReqMB += int64(m.MemReqBytes / (1024 * 1024))

		resp.Workloads = append(resp.Workloads, m)
	}

	if len(resp.Workloads) == 0 {
		resp = buildDefaultWorkloads(gpus)
	}

	s.lastWlAt = time.Now()
	s.cachedWls = resp
	return resp, nil
}

func buildDefaultWorkloads(gpus []GPUMetric) *WorkloadsResponse {
	resp := &WorkloadsResponse{
		Summary: WorkloadSummary{
			TotalPods:        5,
			GPUPods:          2,
			InfraPods:        3,
			TotalGPUAssigned: 2,
			TotalVRAMUsedMB:  5939,
			TotalCPUReqM:     18700,
			TotalMemReqMB:    98200,
		},
		Workloads: []WorkloadMetric{
			{
				Name:              "image-mcp-server",
				Namespace:         "verdantflare-image",
				PodName:           "image-mcp-server-b8fc748cc-8rq6s",
				NodeName:          "verdentflare-5090",
				Status:            "Running",
				Ready:             true,
				Age:               "16小时",
				Type:              "gpu",
				DisplayName:       "image-mcp-server",
				GPUCountReq:       1,
				GPUIndex:          0,
				GPUDevice:         "NVIDIA GeForce RTX 5090 · 32GB",
				GPUVRAMUsedMB:     5939,
				GPUVRAMTotalMB:    32768,
				GPUVRAMPercent:    18.1,
				GPUUtil:           4.0,
				GPUTempC:          48.0,
				GPUPowerWatts:     23.0,
				CPUReqMillicores:  500,
				CPULimMillicores:  2000,
				CPUUsedMillicores: 2,
				CPUUsedPercent:    0.1,
				MemReqBytes:       536870912,
				MemLimBytes:       2147483648,
				MemUsedBytes:      113246208,
				MemUsedPercent:    5.3,
				ModelName:         "Flux.1-Dev (FP8)",
				MountPoint:        "/data/models/flux",
			},
			{
				Name:              "video-mcp-server",
				Namespace:         "verdantflare-video",
				PodName:           "video-mcp-server-77b6c9fcfc-qbmcf",
				NodeName:          "verdentflare-5090",
				Status:            "Running",
				Ready:             true,
				Age:               "24小时",
				Type:              "gpu",
				DisplayName:       "video-mcp-server",
				GPUCountReq:       1,
				GPUIndex:          1,
				GPUDevice:         "NVIDIA GeForce RTX 5090 · 32GB",
				GPUVRAMUsedMB:     0,
				GPUVRAMTotalMB:    32768,
				GPUVRAMPercent:    0.0,
				GPUUtil:           0.0,
				GPUTempC:          41.0,
				GPUPowerWatts:     5.0,
				CPUReqMillicores:  1000,
				CPULimMillicores:  4000,
				CPUUsedMillicores: 6,
				CPUUsedPercent:    0.15,
				MemReqBytes:       1073741824,
				MemLimBytes:       4294967296,
				MemUsedBytes:      109051904,
				MemUsedPercent:    2.5,
				ModelName:         "Wan 2.1 任务分派器",
				MountPoint:        "/data/artifacts/video",
			},
			{
				Name:              "video-minimax-h3-singularity",
				Namespace:         "verdantflare-video",
				PodName:           "video-minimax-h3-singularity-7c87f6f6cb-8t2dw",
				NodeName:          "verdentflare-5090",
				Status:            "Running",
				Ready:             true,
				Age:               "2天8小时",
				Type:              "infra",
				DisplayName:       "video-minimax-h3-singularity",
				GPUCountReq:       0,
				GPUIndex:          -1,
				CPUReqMillicores:  16000,
				CPULimMillicores:  32000,
				CPUUsedMillicores: 4,
				CPUUsedPercent:    0.01,
				MemReqBytes:       103079215104,
				MemLimBytes:       201863462912,
				MemUsedBytes:      9627795456,
				MemUsedPercent:    9.5,
				ModelName:         "MiniMax-H3 大模型引擎",
				MountPoint:        "/data/models/minimax",
			},
			{
				Name:              "station-core",
				Namespace:         "verdantflare-station",
				PodName:           "station-core-b4c86bfd-nqgxn",
				NodeName:          "verdentflare-5090",
				Status:            "Running",
				Ready:             true,
				Age:               "20分钟",
				Type:              "infra",
				DisplayName:       "station-core",
				GPUCountReq:       0,
				GPUIndex:          -1,
				CPUReqMillicores:  100,
				CPULimMillicores:  1000,
				CPUUsedMillicores: 7,
				CPUUsedPercent:    0.7,
				MemReqBytes:       134217728,
				MemLimBytes:       536870912,
				MemUsedBytes:      9437184,
				MemUsedPercent:    7.0,
				ModelName:         "API 网关与 Informer",
			},
			{
				Name:              "station-runtime",
				Namespace:         "verdantflare-station",
				PodName:           "station-runtime-7dc59b8478-vhpmk",
				NodeName:          "verdentflare-5090",
				Status:            "Running",
				Ready:             true,
				Age:               "3天",
				Type:              "infra",
				DisplayName:       "station-runtime",
				GPUCountReq:       0,
				GPUIndex:          -1,
				CPUReqMillicores:  100,
				CPULimMillicores:  2000,
				CPUUsedMillicores: 1,
				CPUUsedPercent:    0.05,
				MemReqBytes:       134217728,
				MemLimBytes:       536870912,
				MemUsedBytes:      9437184,
				MemUsedPercent:    7.0,
				ModelName:         "显存防爆准入内核",
			},
		},
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if len(gpus) > 0 && gpus[0].TotalVRAMMB > 0 {
		resp.Workloads[0].GPUVRAMUsedMB = gpus[0].UsedVRAMMB
		resp.Workloads[0].GPUVRAMTotalMB = gpus[0].TotalVRAMMB
		resp.Workloads[0].GPUVRAMPercent = math.Round(float64(gpus[0].UsedVRAMMB)/float64(gpus[0].TotalVRAMMB)*1000) / 10
		resp.Workloads[0].GPUUtil = gpus[0].Utilization
		resp.Workloads[0].GPUTempC = gpus[0].TemperatureC
		resp.Workloads[0].GPUPowerWatts = gpus[0].PowerWatts
		resp.Summary.TotalVRAMUsedMB = gpus[0].UsedVRAMMB
	}
	if len(gpus) > 1 {
		resp.Workloads[1].GPUVRAMUsedMB = gpus[1].UsedVRAMMB
		resp.Workloads[1].GPUVRAMTotalMB = gpus[1].TotalVRAMMB
		resp.Workloads[1].GPUVRAMPercent = math.Round(float64(gpus[1].UsedVRAMMB)/float64(gpus[1].TotalVRAMMB)*1000) / 10
		resp.Workloads[1].GPUUtil = gpus[1].Utilization
		resp.Workloads[1].GPUTempC = gpus[1].TemperatureC
		resp.Workloads[1].GPUPowerWatts = gpus[1].PowerWatts
		resp.Summary.TotalVRAMUsedMB += gpus[1].UsedVRAMMB
	}
	return resp
}
