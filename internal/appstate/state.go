// Package appstate owns the Core application state read model.
package appstate

import (
	"errors"
	"time"
)

var (
	ErrUnavailable = errors.New("application state unavailable")
	ErrLimit       = errors.New("application state response limit exceeded")
	ErrNotFound    = errors.New("application installation not found")
)

type Scope struct {
	StationID      string `json:"stationId"`
	OrganizationID string `json:"organizationId"`
}
type Snapshot struct {
	SchemaVersion int       `json:"schemaVersion"`
	Scope         Scope     `json:"scope"`
	Revision      string    `json:"revision"`
	GeneratedAt   time.Time `json:"generatedAt"`
	Items         []Item    `json:"items"`
}
type Item struct {
	AppID          string        `json:"appId"`
	InstallationID *string       `json:"installationId"`
	StateRevision  string        `json:"stateRevision"`
	Installation   *Installation `json:"installation"`
	Runtime        Runtime       `json:"runtime"`
	Operation      *Operation    `json:"operation"`
}
type Installation struct {
	Revision        int64   `json:"revision"`
	AppVersion      string  `json:"appVersion"`
	State           string  `json:"state"`
	LastOperationID *string `json:"lastOperationId"`
}
type Runtime struct {
	State          string     `json:"state"`
	Quality        string     `json:"quality"`
	ReasonCode     string     `json:"reasonCode"`
	ObservedAt     *time.Time `json:"observedAt"`
	ExpiresAt      *time.Time `json:"expiresAt"`
	LastKnownState *string    `json:"lastKnownState"`
}
type Operation struct {
	OperationID string `json:"operationId"`
	Protocol    string `json:"protocol"`
	Status      string `json:"status"`
	Phase       string `json:"phase"`
	Blocking    bool   `json:"blocking"`
}

// RuntimeView never confuses missing/expired evidence with an absent workload.
// now is the Core database clock, not a client or collector timestamp.
func RuntimeView(now time.Time, state, reason *string, received, expires *time.Time, expired bool) Runtime {
	out := Runtime{State: "unknown", Quality: "unknown", ReasonCode: "NOT_OBSERVED", ObservedAt: received, ExpiresAt: expires}
	if state == nil || reason == nil || received == nil || expires == nil {
		return out
	}
	if !expires.After(*received) || received.After(now) {
		out.ReasonCode = "INVALID_OBSERVATION"
		return out
	}
	if *state != "unknown" {
		v := *state
		out.LastKnownState = &v
	}
	if expired || !now.Before(*expires) {
		out.ReasonCode = "OBSERVATION_EXPIRED"
		return out
	}
	out.State = *state
	out.ReasonCode = *reason
	if *state != "unknown" {
		out.Quality = "known"
	}
	return out
}
