package catalog

import (
	"context"
	"encoding/json"
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
		Generation int64 `json:"generation"`
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
	if decoder.Decode(&d) != nil || decoder.Decode(new(any)) != io.EOF || d.Metadata.Generation < 1 || len(d.Spec.Template.Spec.Containers) == 0 {
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
