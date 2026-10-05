package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
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

// NodeMetric represents real-time host metrics.
type NodeMetric struct {
	CPUUtilizationPercent float64 `json:"cpu_utilization_percent"`
	MemTotalBytes         uint64  `json:"mem_total_bytes"`
	MemAvailableBytes     uint64  `json:"mem_available_bytes"`
	MemUsedPercent        float64 `json:"mem_used_percent"`
}

// ResourceSummary combines high-level resource indicators for Studio OS.
type ResourceSummary struct {
	TotalGPUs          int     `json:"total_gpus"`
	TotalVRAMMB        int64   `json:"total_vram_mb"`
	UsedVRAMMB         int64   `json:"used_vram_mb"`
	FreeVRAMMB         int64   `json:"free_vram_mb"`
	AverageGPUUtil     float64 `json:"average_gpu_util"`
	HostCPUUtilization float64 `json:"host_cpu_utilization"`
	HostMemUsedPercent float64 `json:"host_mem_used_percent"`
	Status             string  `json:"status"` // "Healthy", "Warning", "Degraded"
	UpdatedAt          string  `json:"updated_at"`
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
	ttl        time.Duration

	mu         sync.RWMutex
	lastGPUAt  time.Time
	cachedGPUs []GPUMetric
	lastNodeAt time.Time
	cachedNode *NodeMetric
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

	// Total VRAM
	if totalResp, err := s.queryVector(ctx, "DCGM_FI_DEV_FB_TOTAL"); err == nil {
		for _, r := range totalResp.Data.Result {
			if m, ok := gpusMap[r.Metric["UUID"]]; ok {
				val, _ := parseValue(r.Value)
				m.TotalVRAMMB = int64(val)
				m.UsedVRAMMB = m.TotalVRAMMB - m.FreeVRAMMB
				if m.UsedVRAMMB < 0 {
					m.UsedVRAMMB = 0
				}
			}
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

	node := &NodeMetric{}
	cpuResp, err := s.queryVector(ctx, `100 - (avg(irate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)`)
	if err == nil && len(cpuResp.Data.Result) > 0 {
		val, _ := parseValue(cpuResp.Data.Result[0].Value)
		node.CPUUtilizationPercent = val
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

	if node.MemTotalBytes > 0 {
		used := float64(node.MemTotalBytes - node.MemAvailableBytes)
		node.MemUsedPercent = (used / float64(node.MemTotalBytes)) * 100.0
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
		summary.HostCPUUtilization = node.CPUUtilizationPercent
		summary.HostMemUsedPercent = node.MemUsedPercent
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
