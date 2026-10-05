package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestService_GetSummary(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		resp := promResponse{Status: "success"}
		resp.Data.ResultType = "vector"

		switch query {
		case "DCGM_FI_DEV_FB_FREE":
			resp.Data.Result = []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			}{
				{
					Metric: map[string]string{
						"gpu":       "0",
						"device":    "nvidia0",
						"modelName": "NVIDIA GeForce RTX 5090",
						"UUID":      "GPU-0",
					},
					Value: []interface{}{float64(1700000000), "26000"},
				},
			}
		case "DCGM_FI_DEV_FB_TOTAL":
			resp.Data.Result = []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			}{
				{
					Metric: map[string]string{"UUID": "GPU-0"},
					Value:  []interface{}{float64(1700000000), "32768"},
				},
			}
		case "DCGM_FI_DEV_GPU_UTIL":
			resp.Data.Result = []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			}{
				{
					Metric: map[string]string{"UUID": "GPU-0"},
					Value:  []interface{}{float64(1700000000), "12.5"},
				},
			}
		case "DCGM_FI_DEV_GPU_TEMP":
			resp.Data.Result = []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			}{
				{
					Metric: map[string]string{"UUID": "GPU-0"},
					Value:  []interface{}{float64(1700000000), "44"},
				},
			}
		case "DCGM_FI_DEV_POWER_USAGE":
			resp.Data.Result = []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			}{
				{
					Metric: map[string]string{"UUID": "GPU-0"},
					Value:  []interface{}{float64(1700000000), "110"},
				},
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	svc := NewService(ts.URL, time.Second)
	summary, err := svc.GetSummary(context.Background())
	if err != nil {
		t.Fatalf("GetSummary failed: %v", err)
	}

	if summary.TotalGPUs != 1 {
		t.Errorf("expected 1 GPU, got %d", summary.TotalGPUs)
	}
	if summary.TotalVRAMMB != 32768 || summary.FreeVRAMMB != 26000 {
		t.Errorf("unexpected VRAM stats: %+v", summary)
	}
	if summary.AverageGPUUtil != 12.5 {
		t.Errorf("expected 12.5%% util, got %f", summary.AverageGPUUtil)
	}
	if summary.Status != "Healthy" {
		t.Errorf("expected Healthy status, got %s", summary.Status)
	}
}
