package telemetry

import (
	"context"
	"math"
	"regexp"
	"time"
)

// UUID is an internal join key. Studio must match it to authenticated worker
// identity and whole-card allocation before publishing the usage value.
type GPUUsageSample struct {
	UUID   string         `json:"uuid"`
	Memory ResourceMetric `json:"memory"`
}

func (s *Service) gpuUsageSamples(ctx context.Context, now time.Time) []GPUUsageSample {
	if s.httpClient == nil || s.baseURL == "" {
		return nil
	}
	// A range vector retains the exporter sample timestamp. An instant query's
	// evaluation time must not make a stale scrape appear fresh.
	response, err := s.queryVector(ctx, "DCGM_FI_DEV_FB_USED[35s]")
	if err != nil || response.Data.ResultType != "matrix" {
		return nil
	}
	counts := map[string]int{}
	for _, row := range response.Data.Result {
		counts[row.Metric["UUID"]]++
	}
	var out []GPUUsageSample
	for _, row := range response.Data.Result {
		id := row.Metric["UUID"]
		if counts[id] != 1 || !regexp.MustCompile(`^GPU-[0-9a-fA-F-]{36}$`).MatchString(id) || len(row.Values) == 0 {
			continue
		}
		last := row.Values[len(row.Values)-1]
		if len(last) != 2 {
			continue
		}
		seconds, ok := last[0].(float64)
		value, e := parseValue(last)
		if !ok || e != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > math.MaxInt64/(1024*1024) {
			continue
		}
		at := time.UnixMilli(int64(seconds * 1000)).UTC()
		if at.After(now.Add(5 * time.Second)) {
			continue
		}
		stamp := at.Format(time.RFC3339Nano)
		metric := ResourceMetric{SampledAt: &stamp, Quality: "stale", Reason: "SAMPLE_STALE", Unit: "bytes", Scope: "exclusive_gpu"}
		if now.Sub(at) <= 30*time.Second {
			bytes := value * 1024 * 1024
			metric.Value = &bytes
			metric.Quality = "fresh"
			metric.Reason = ""
		}
		out = append(out, GPUUsageSample{UUID: id, Memory: metric})
	}
	return out
}
