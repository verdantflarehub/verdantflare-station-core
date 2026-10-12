package appstate

import (
	"testing"
)

func TestCollectorStateEvaluation(t *testing.T) {
	ready := ResourceObservation{Generation: 3, ObservedGeneration: 3, Desired: 1, Replicas: 1, Ready: 1, Updated: 1, Available: 1, Health: "not_checked"}
	for _, tc := range []struct {
		name     string
		change   func(*ResourceObservation)
		required bool
		state    string
	}{
		{"ready", func(o *ResourceObservation) {}, false, "ready"},
		{"zero", func(o *ResourceObservation) {
			o.Desired = 0
			o.Replicas = 0
			o.Ready = 0
			o.Updated = 0
			o.Available = 0
		}, false, "stopped"},
		{"draining", func(o *ResourceObservation) { o.Desired = 0 }, false, "stopping"},
		{"old_generation", func(o *ResourceObservation) { o.ObservedGeneration = 2 }, false, "starting"},
		{"rollout", func(o *ResourceObservation) { o.Updated = 0 }, false, "starting"},
		{"unavailable", func(o *ResourceObservation) { o.Available = 0 }, false, "starting"},
		{"failure", func(o *ResourceObservation) { o.ReplicaFailed = true }, false, "degraded"},
		{"health_required", func(o *ResourceObservation) {}, true, "unknown"},
		{"health_ready", func(o *ResourceObservation) { o.Health = "ready" }, true, "ready"},
		{"health_failed", func(o *ResourceObservation) { o.Health = "failed" }, true, "degraded"},
		{"deleting", func(o *ResourceObservation) { o.Terminating = true }, false, "stopping"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := ready
			tc.change(&v)
			if got, _ := evaluate(v, tc.required); got != tc.state {
				t.Fatal(got)
			}
		})
	}
}
