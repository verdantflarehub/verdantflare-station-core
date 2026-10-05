package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"net/http"
	"strings"
	"time"
)

type Kubernetes struct {
	baseURL string
	client  *http.Client
}

func NewKubernetes(kubeconfig string) (*Kubernetes, error) {
	var cfg *rest.Config
	var err error
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}
	cfg.Timeout = 3 * time.Second
	client, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, err
	}
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &Kubernetes{strings.TrimRight(cfg.Host, "/"), client}, nil
}

type deployment struct {
	Metadata struct {
		Name        string            `json:"name"`
		Namespace   string            `json:"namespace"`
		Generation  int64             `json:"generation"`
		UID         string            `json:"uid"`
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
	Spec struct {
		Replicas *int32 `json:"replicas"`
		Template struct {
			Spec struct {
				Containers []ImageContainer `json:"containers"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
	Status struct {
		ObservedGeneration int64 `json:"observedGeneration"`
		Replicas           int32 `json:"replicas"`
		Ready              int32 `json:"readyReplicas"`
		Available          int32 `json:"availableReplicas"`
		Updated            int32 `json:"updatedReplicas"`
		Conditions         []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
			Reason string `json:"reason"`
		} `json:"conditions"`
	} `json:"status"`
}
type ImageContainer struct {
	Name  string `json:"name"`
	Image string `json:"image"`
}

func observationFromDeployment(d *deployment) Observation {
	out := Observation{State: "unknown", Reason: "unavailable", ObservedAt: time.Now().UTC(), Images: []Image{}}
	if d.Metadata.Generation < 1 || len(d.Spec.Template.Spec.Containers) == 0 {
		out.Reason = "invalid_response"
		return out
	}
	desired := int32(1)
	if d.Spec.Replicas != nil {
		desired = *d.Spec.Replicas
	}
	if desired < 0 {
		out.Reason = "invalid_response"
		return out
	}
	out.UID = d.Metadata.UID
	out.Ownership = d.Metadata.Annotations
	out.Desired = &desired
	out.Ready = &d.Status.Ready
	for _, c := range d.Spec.Template.Spec.Containers {
		out.Images = append(out.Images, Image{c.Name, c.Image})
	}
	out.State = "starting"
	out.Reason = "reconciling"
	if d.Status.ObservedGeneration < d.Metadata.Generation {
		return out
	}
	if desired == 0 {
		out.State = "stopping"
		out.Reason = "scaling_down"
		if d.Status.Replicas == 0 {
			out.State = "stopped"
			out.Reason = "scaled_to_zero"
		}
		return out
	}
	for _, c := range d.Status.Conditions {
		if (c.Type == "Progressing" && c.Status == "False" && c.Reason == "ProgressDeadlineExceeded") || (c.Type == "ReplicaFailure" && c.Status == "True") {
			out.State = "degraded"
			out.Reason = "deployment_failed"
			return out
		}
	}
	if d.Status.Replicas == desired && d.Status.Updated == desired && d.Status.Ready == desired && d.Status.Available == desired {
		out.State = "ready"
		out.Reason = "replicas_ready"
	}
	return out
}

func (k *Kubernetes) Observe(ctx context.Context, e Entry) Observation {
	out := Observation{State: "unknown", Reason: "unavailable", ObservedAt: time.Now().UTC(), Images: []Image{}}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.baseURL+"/apis/apps/v1/namespaces/"+e.Namespace+"/deployments/"+e.WorkloadName, nil)
	if err != nil {
		return out
	}
	resp, err := k.client.Do(req)
	if err != nil {
		return out
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		out.State = "not_installed"
		out.Reason = "not_found"
		return out
	}
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		out.Reason = "forbidden"
		return out
	}
	if resp.StatusCode != 200 {
		return out
	}
	var d deployment
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 4<<20))
	if decoder.Decode(&d) != nil || decoder.Decode(new(any)) != io.EOF {
		out.Reason = "invalid_response"
		return out
	}
	return observationFromDeployment(&d)
}

// RawPodItem represents minimal pod metadata, spec and status for workload telemetry.
type RawPodItem struct {
	Name         string            `json:"name"`
	Namespace    string            `json:"namespace"`
	NodeName     string            `json:"node_name"`
	Phase        string            `json:"phase"`
	StartTime    string            `json:"start_time"`
	Ready        bool              `json:"ready"`
	RestartCount int32             `json:"restart_count"`
	Labels       map[string]string `json:"labels"`
	CPUReq       string            `json:"cpu_req"`
	CPULim       string            `json:"cpu_lim"`
	MemReq       string            `json:"mem_req"`
	MemLim       string            `json:"mem_lim"`
	GPUReq       string            `json:"gpu_req"`
	GPULim       string            `json:"gpu_lim"`
	Image        string            `json:"image"`
}

// RawPodMetricItem represents real-time CPU & memory metrics from metrics-server.
type RawPodMetricItem struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	CPUUsage  string `json:"cpu_usage"`
	MemUsage  string `json:"mem_usage"`
}

// ListPods queries pods across all namespaces from Kubernetes API.
func (k *Kubernetes) ListPods(ctx context.Context) ([]RawPodItem, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.baseURL+"/api/v1/pods", nil)
	if err != nil {
		return nil, err
	}
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kubernetes pods status %d", resp.StatusCode)
	}

	var list struct {
		Items []struct {
			Metadata struct {
				Name      string            `json:"name"`
				Namespace string            `json:"namespace"`
				Labels    map[string]string `json:"labels"`
			} `json:"metadata"`
			Spec struct {
				NodeName   string `json:"nodeName"`
				Containers []struct {
					Name      string `json:"name"`
					Image     string `json:"image"`
					Resources struct {
						Requests map[string]string `json:"requests"`
						Limits   map[string]string `json:"limits"`
					} `json:"resources"`
				} `json:"containers"`
			} `json:"spec"`
			Status struct {
				Phase             string `json:"phase"`
				StartTime         string `json:"startTime"`
				ContainerStatuses []struct {
					Ready        bool  `json:"ready"`
					RestartCount int32 `json:"restartCount"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}

	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&list); err != nil {
		return nil, err
	}

	var results []RawPodItem
	for _, item := range list.Items {
		pod := RawPodItem{
			Name:         item.Metadata.Name,
			Namespace:    item.Metadata.Namespace,
			NodeName:     item.Spec.NodeName,
			Phase:        item.Status.Phase,
			StartTime:    item.Status.StartTime,
			Labels:       item.Metadata.Labels,
		}
		if len(item.Status.ContainerStatuses) > 0 {
			pod.Ready = item.Status.ContainerStatuses[0].Ready
			pod.RestartCount = item.Status.ContainerStatuses[0].RestartCount
		}
		if len(item.Spec.Containers) > 0 {
			c := item.Spec.Containers[0]
			pod.Image = c.Image
			if c.Resources.Requests != nil {
				pod.CPUReq = c.Resources.Requests["cpu"]
				pod.MemReq = c.Resources.Requests["memory"]
				pod.GPUReq = c.Resources.Requests["nvidia.com/gpu"]
			}
			if c.Resources.Limits != nil {
				pod.CPULim = c.Resources.Limits["cpu"]
				pod.MemLim = c.Resources.Limits["memory"]
				pod.GPULim = c.Resources.Limits["nvidia.com/gpu"]
			}
		}
		results = append(results, pod)
	}
	return results, nil
}

// ListPodMetrics queries real-time CPU & memory metrics from metrics.k8s.io.
func (k *Kubernetes) ListPodMetrics(ctx context.Context) ([]RawPodMetricItem, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.baseURL+"/apis/metrics.k8s.io/v1beta1/pods", nil)
	if err != nil {
		return nil, err
	}
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kubernetes pod metrics status %d", resp.StatusCode)
	}

	var list struct {
		Items []struct {
			Metadata struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
			Containers []struct {
				Usage struct {
					CPU    string `json:"cpu"`
					Memory string `json:"memory"`
				} `json:"usage"`
			} `json:"containers"`
		} `json:"items"`
	}

	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&list); err != nil {
		return nil, err
	}

	var results []RawPodMetricItem
	for _, item := range list.Items {
		metric := RawPodMetricItem{
			Name:      item.Metadata.Name,
			Namespace: item.Metadata.Namespace,
		}
		if len(item.Containers) > 0 {
			metric.CPUUsage = item.Containers[0].Usage.CPU
			metric.MemUsage = item.Containers[0].Usage.Memory
		}
		results = append(results, metric)
	}
	return results, nil
}
