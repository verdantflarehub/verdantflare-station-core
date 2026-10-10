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

// RawPodItem preserves workload identity and every regular container.
type RawContainer struct {
	Name                                           string
	CPUReq, CPULim, MemReq, MemLim, GPUReq, GPULim string
	Ready                                          bool
	RestartCount                                   int32
}
type RawPodItem struct {
	UID, Name, Namespace, NodeName, Phase, StartTime, CreatedAt string
	Labels                                                      map[string]string
	Containers                                                  []RawContainer
}
type RawPodMetricItem struct {
	UID, Name, Namespace, Container, Timestamp, Window, CPUUsage, MemUsage string
}

func (k *Kubernetes) telemetryList(ctx context.Context, path string, out any) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.baseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := k.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("kubernetes telemetry status %d", resp.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 8<<20))
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return fmt.Errorf("invalid telemetry response")
	}
	return nil
}

func (k *Kubernetes) ListPods(ctx context.Context) ([]RawPodItem, error) {
	var list struct {
		Items []struct {
			Metadata struct {
				UID, Name, Namespace string
				CreationTimestamp    string
				Labels               map[string]string
			}
			Spec struct {
				NodeName   string
				Containers []struct {
					Name      string
					Resources struct{ Requests, Limits map[string]string }
				}
			}
			Status struct {
				Phase, StartTime  string
				ContainerStatuses []struct {
					Name         string
					Ready        bool
					RestartCount int32
				}
			}
		}
	}
	if err := k.telemetryList(ctx, "/api/v1/pods", &list); err != nil {
		return nil, err
	}
	out := make([]RawPodItem, 0, len(list.Items))
	for _, item := range list.Items {
		pod := RawPodItem{UID: item.Metadata.UID, Name: item.Metadata.Name, Namespace: item.Metadata.Namespace,
			NodeName: item.Spec.NodeName, Phase: item.Status.Phase, StartTime: item.Status.StartTime,
			CreatedAt: item.Metadata.CreationTimestamp, Labels: item.Metadata.Labels, Containers: []RawContainer{}}
		for _, c := range item.Spec.Containers {
			rc := RawContainer{Name: c.Name, CPUReq: c.Resources.Requests["cpu"], CPULim: c.Resources.Limits["cpu"],
				MemReq: c.Resources.Requests["memory"], MemLim: c.Resources.Limits["memory"],
				GPUReq: c.Resources.Requests["nvidia.com/gpu"], GPULim: c.Resources.Limits["nvidia.com/gpu"]}
			for _, status := range item.Status.ContainerStatuses {
				if status.Name == c.Name {
					rc.Ready = status.Ready
					rc.RestartCount = status.RestartCount
				}
			}
			pod.Containers = append(pod.Containers, rc)
		}
		out = append(out, pod)
	}
	return out, nil
}

func (k *Kubernetes) ListPodMetrics(ctx context.Context) ([]RawPodMetricItem, error) {
	var list struct {
		Items []struct {
			Metadata          struct{ UID, Name, Namespace string }
			Timestamp, Window string
			Containers        []struct {
				Name  string
				Usage struct{ CPU, Memory string }
			}
		}
	}
	if err := k.telemetryList(ctx, "/apis/metrics.k8s.io/v1beta1/pods", &list); err != nil {
		return nil, err
	}
	out := []RawPodMetricItem{}
	for _, item := range list.Items {
		for _, c := range item.Containers {
			out = append(out, RawPodMetricItem{UID: item.Metadata.UID, Name: item.Metadata.Name, Namespace: item.Metadata.Namespace,
				Container: c.Name, Timestamp: item.Timestamp, Window: item.Window, CPUUsage: c.Usage.CPU, MemUsage: c.Usage.Memory})
		}
	}
	return out, nil
}
