package appstate

import (
	"regexp"
)

// Collector is trusted server configuration, never accepted from a report body.
type Collector struct {
	ID                 string   `json:"id"`
	Namespaces         []string `json:"namespaces"`
	HealthRequiredApps []string `json:"healthRequiredApps"`
}

func dns(value string) bool {
	return len(value) > 0 && len(value) <= 63 && appIDPattern.MatchString(value)
}

var appIdentifier = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

func (c Collector) valid() bool {
	if !dns(c.ID) || len(c.Namespaces) == 0 || len(c.Namespaces) > 128 || len(c.HealthRequiredApps) > 1000 {
		return false
	}
	for index, values := range [][]string{c.Namespaces, c.HealthRequiredApps} {
		seen := map[string]bool{}
		for _, v := range values {
			if (index == 0 && !dns(v)) || (index == 1 && !appIdentifier.MatchString(v)) || seen[v] {
				return false
			}
			seen[v] = true
		}
	}
	return true
}
func evaluate(o ResourceObservation, healthRequired bool) (string, string) {
	if o.Deleted {
		return "absent", "RESOURCE_ABSENT"
	}
	if o.Terminating {
		return "stopping", "RESOURCE_TERMINATING"
	}
	if o.ObservedGeneration > o.Generation {
		return "unknown", "INVALID_OBSERVATION"
	}
	if o.Desired == 0 {
		if o.Replicas == 0 {
			return "stopped", "SCALED_TO_ZERO"
		}
		return "stopping", "REPLICAS_TERMINATING"
	}
	if o.ProgressFailed {
		return "degraded", "PROGRESS_DEADLINE_EXCEEDED"
	}
	if o.ReplicaFailed {
		return "degraded", "REPLICA_FAILURE"
	}
	if o.Health == "failed" {
		return "degraded", "HEALTH_CHECK_FAILED"
	}
	if o.Generation < 1 || o.ObservedGeneration < o.Generation || o.Replicas != o.Desired || o.Ready != o.Desired || o.Updated != o.Desired || o.Available != o.Desired {
		return "starting", "REPLICAS_NOT_READY"
	}
	if healthRequired && o.Health != "ready" {
		return "unknown", "HEALTH_NOT_OBSERVED"
	}
	return "ready", "READY"
}
