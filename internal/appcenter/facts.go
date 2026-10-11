// Package appcenter projects authoritative installation and workload facts.
// It has no Kubernetes writes, command execution or application-name branches.
package appcenter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

var ErrUnavailable = errors.New("APPCENTER_DEPENDENCY_UNAVAILABLE")
var ErrLimit = errors.New("APPCENTER_LIMIT_EXCEEDED")
var ErrConflict = errors.New("APPCENTER_BINDING_CONFLICT")

type Scope struct {
	UserID             string `json:"userId"`
	OrganizationID     string `json:"organizationId"`
	StationID          string `json:"stationId"`
	AuthorizationEpoch string `json:"authorizationEpoch"`
}
type SourceStatus struct {
	Source     string     `json:"source"`
	Quality    string     `json:"quality"`
	Revision   *string    `json:"revision"`
	ObservedAt *time.Time `json:"observedAt"`
	ReasonCode *string    `json:"reasonCode"`
}
type Installation struct {
	InstallationID  string  `json:"installationId"`
	Revision        int64   `json:"revision"`
	AppVersion      string  `json:"appVersion"`
	State           string  `json:"state"`
	LastOperationID *string `json:"lastOperationId"`
}

// Record is private input from a scope-filtered database snapshot.
type Record struct {
	AppID string
	Installation
}
type Service struct {
	Quality        string     `json:"quality"`
	State          string     `json:"state"`
	ObservedAt     *time.Time `json:"observedAt"`
	ReasonCode     *string    `json:"reasonCode"`
	ObservationKey *string    `json:"observationKey"`
	WorkloadUID    *string    `json:"workloadUID"`
}
type Operation struct {
	Quality     string     `json:"quality"`
	Blocking    bool       `json:"blocking"`
	OperationID *string    `json:"operationId"`
	Protocol    *string    `json:"protocol"`
	Status      *string    `json:"status"`
	Phase       *string    `json:"phase"`
	CanRead     bool       `json:"canRead"`
	UpdatedAt   *time.Time `json:"updatedAt"`
}

// OperationRecord must not be marshaled. Only the original actor may receive
// its identity/status; others learn only that conflicting changes are blocked.
type OperationRecord struct {
	AppID, OperationID, UserID, OrganizationID, Protocol, Status, Phase string
	UpdatedAt                                                           time.Time
}
type Candidate struct {
	Kind                string  `json:"kind"`
	Enabled             bool    `json:"enabled"`
	ReasonCode          *string `json:"reasonCode"`
	AppVersion          string  `json:"appVersion"`
	ReleaseDigest       *string `json:"releaseDigest"`
	ExpectedWorkloadUID *string `json:"expectedWorkloadUID"`
}
type Lifecycle struct {
	PolicyRevision string      `json:"policyRevision"`
	Candidates     []Candidate `json:"candidates"`
}
type Fact struct {
	CardKey             string        `json:"cardKey"`
	AppID               string        `json:"appId"`
	Origin              string        `json:"origin"`
	Catalog             *catalog.Item `json:"catalog"`
	Installation        *Installation `json:"installation"`
	InstallationQuality string        `json:"installationQuality"`
	Service             Service       `json:"service"`
	Operation           Operation     `json:"operation"`
	Lifecycle           Lifecycle     `json:"lifecycle"`
}
type Facts struct {
	SchemaVersion int            `json:"schemaVersion"`
	RequestID     string         `json:"requestId"`
	Scope         Scope          `json:"scope"`
	Revision      string         `json:"revision"`
	GeneratedAt   time.Time      `json:"generatedAt"`
	Sources       []SourceStatus `json:"sources"`
	Items         []Fact         `json:"items"`
}

func Text(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
func Digest(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func ActorScope(user identity.IdentityContext) Scope {
	roles, scopes := slices.Clone(user.Roles), slices.Clone(user.Scopes)
	sort.Strings(roles)
	sort.Strings(scopes)
	// Keep the same session-bound authorization epoch as Studio's existing
	// Dashboard/navigation scope. A new session cannot inherit old bindings.
	epoch := Digest(struct {
		Session            string
		Revocation, Policy int64
		Roles, Scopes      []string
	}{user.SessionID, user.RevocationVersion, user.PolicyVersion, slices.Compact(roles), slices.Compact(scopes)})[7:]
	return Scope{user.UserID, user.OrganizationID, user.StationID, epoch}
}
func canRead(user identity.IdentityContext) bool {
	return user.UserID != "" && user.OrganizationID != "" && user.StationID != "" && slices.Contains(user.Roles, "admin") && slices.Contains(user.Scopes, "app:read")
}
func noService(reason string) Service {
	return Service{Quality: "unknown", State: "unknown", ReasonCode: Text(reason)}
}
func idle() Operation { return Operation{Quality: "known"} }
func observation(item catalog.Item) Service {
	d := item.Deployment
	quality, state := "known", d.State
	if state == "not_installed" {
		state = "absent"
	}
	if state == "unknown" || !slices.Contains([]string{"absent", "ready", "stopped", "starting", "stopping", "degraded"}, state) {
		quality, state = "unknown", "unknown"
	}
	var at *time.Time
	if !d.ObservedAt.IsZero() {
		v := d.ObservedAt
		at = &v
	} else {
		quality, state = "unknown", "unknown"
	}
	return Service{quality, state, at, Text(d.Reason), Text(observationKey(item)), Text(d.UID)}
}
func observationKey(item catalog.Item) string {
	if item.Deployment.UID == "" {
		return ""
	}
	return "obs:" + Digest([]string{item.Namespace, item.WorkloadName, item.Deployment.UID})[7:]
}
func scopedItem(item catalog.Item, user identity.IdentityContext) catalog.Item {
	item.ProjectManagement(user.StationID, user.OrganizationID)
	return item
}
func opFor(app string, records []OperationRecord, user identity.IdentityContext) Operation {
	var active, terminal *OperationRecord
	for _, r := range records {
		if r.AppID != app {
			continue
		}
		if r.Status != "succeeded" && r.Status != "failed" {
			if active != nil {
				return Operation{Quality: "conflict", Blocking: true}
			}
			copy := r
			active = &copy
		} else if r.UserID == user.UserID && r.OrganizationID == user.OrganizationID &&
			(terminal == nil || r.UpdatedAt.After(terminal.UpdatedAt) || (r.UpdatedAt.Equal(terminal.UpdatedAt) && r.OperationID < terminal.OperationID)) {
			copy := r
			terminal = &copy
		}
	}
	result := idle()
	r := terminal
	if active != nil {
		r = active
		result.Blocking = true
	}
	if r != nil && r.UserID == user.UserID && r.OrganizationID == user.OrganizationID {
		result.OperationID = Text(r.OperationID)
		result.Protocol = Text(r.Protocol)
		result.Status = Text(r.Status)
		result.Phase = Text(r.Phase)
		result.CanRead = true
		at := r.UpdatedAt
		result.UpdatedAt = &at
	}
	return result
}

// Registered catalog metadata is public, but the observed object may belong to
// another organization. Never serialize that object's identity or runtime data.
func publicCatalog(item catalog.Item) catalog.Item {
	if item.Deployment.ManagementStatus == "foreign" {
		item.Deployment = catalog.Observation{State: "unknown", Reason: "forbidden", Images: []catalog.Image{}, ManagementStatus: "foreign"}
	}
	return item
}

// Build consumes a complete, scope-filtered database snapshot. It does not infer
// an installation from an observed Deployment or collapse workload identities.
func Build(user identity.IdentityContext, request string, now time.Time, catalogItems []catalog.SnapshotItem, installations []Record, operations []OperationRecord) (Facts, error) {
	var empty Facts
	if !canRead(user) {
		return empty, identity.Denied
	}
	if len(catalogItems) > 1000 || len(installations) > 1000 || len(operations) > 1000 {
		return empty, ErrLimit
	}
	if now.IsZero() {
		return empty, ErrUnavailable
	}
	f := Facts{SchemaVersion: 1, RequestID: request, Scope: ActorScope(user), GeneratedAt: now.UTC(), Sources: []SourceStatus{}, Items: []Fact{}}
	used := map[int]bool{}
	installedApps := map[string]int{}
	ids := map[string]bool{}
	for _, r := range installations {
		if r.State == "removed" {
			continue
		}
		if r.InstallationID == "" || r.AppID == "" || r.Revision < 1 || !slices.Contains([]string{"pending", "installed", "failed"}, r.State) || ids[r.InstallationID] {
			return empty, ErrConflict
		}
		ids[r.InstallationID] = true
		installedApps[r.AppID]++
	}
	for _, r := range installations {
		if r.State == "removed" {
			continue
		}
		install := r.Installation
		// lastOperationId in an installation is not proof of permission to read
		// that operation. The scoped operation projection owns that decision.
		install.LastOperationID = nil
		item := Fact{CardKey: "installation:" + r.InstallationID, AppID: r.AppID, Origin: "installation", Installation: &install, InstallationQuality: "known", Service: noService("WORKLOAD_BINDING_MISSING"), Operation: opFor(r.AppID, operations, user)}
		matches := 0
		for index, source := range catalogItems {
			if source.AppID != r.AppID {
				continue
			}
			candidate := scopedItem(source.Item, user)
			if source.Registered && item.Catalog == nil {
				copy := publicCatalog(candidate)
				item.Catalog = &copy
			}
			owner := candidate.Deployment.Ownership
			if owner["station.verdantflare.com/installation-id"] != r.InstallationID {
				continue
			}
			used[index] = true
			matches++
			// Runtime binds the installation and template hash, not an
			// app-version annotation. The database owns the installed version;
			// this observation does not prove a release is safe to execute.
			if candidate.Deployment.ManagementStatus != "managed" {
				item.Service = Service{Quality: "conflict", State: "unknown", ReasonCode: Text("WORKLOAD_BINDING_CONFLICT")}
				continue
			}
			item.Service = observation(candidate)
		}
		if matches > 1 {
			item.Service = Service{Quality: "conflict", State: "unknown", ReasonCode: Text("MULTIPLE_WORKLOAD_BINDINGS")}
		}
		if installedApps[r.AppID] > 1 {
			item.InstallationQuality = "conflict"
			item.Service.Quality = "conflict"
			item.Service.ReasonCode = Text("MULTIPLE_INSTALLATIONS")
		}
		if item.Operation.CanRead && item.Operation.OperationID != nil && *item.Operation.OperationID == r.LastOperationIDValue() {
			item.Installation.LastOperationID = item.Operation.OperationID
		}
		f.Items = append(f.Items, item)
	}
	seenCatalog := map[string]bool{}
	for index, source := range catalogItems {
		if used[index] {
			continue
		}
		item := scopedItem(source.Item, user)
		if !source.Registered && item.Deployment.ManagementStatus == "foreign" {
			continue
		}
		if installedApps[item.AppID] > 0 {
			// A registered release is metadata for its existing installation,
			// not a second install button. Distinct live legacy objects remain
			// visible as observations with their own verified identity.
			if item.Deployment.UID == "" || item.Deployment.ManagementStatus == "foreign" {
				continue
			}
		}
		fact := Fact{CardKey: "catalog:" + item.AppID, AppID: item.AppID, Origin: "catalog", Catalog: &item, InstallationQuality: "known", Service: observation(item), Operation: opFor(item.AppID, operations, user)}
		if !source.Registered {
			// Dynamic discovery cannot supply trusted release metadata. Its
			// labels may be stale or have no version at all.
			fact.Catalog = nil
		}
		if item.Deployment.ManagementStatus == "foreign" {
			item = publicCatalog(item)
			fact.Service = Service{Quality: "conflict", State: "unknown", ReasonCode: Text("FOREIGN_WORKLOAD")}
		} else if item.Deployment.UID != "" {
			fact.CardKey = observationKey(item)
			fact.Origin = "observation"
		} else {
			if !source.Registered {
				continue
			}
			if seenCatalog[item.AppID] {
				return empty, ErrConflict
			}
			seenCatalog[item.AppID] = true
		}
		f.Items = append(f.Items, fact)
	}
	if len(f.Items) > 1000 {
		return empty, ErrLimit
	}
	for i := range f.Items {
		item := &f.Items[i]
		version := ""
		if item.Catalog != nil {
			version = item.Catalog.Version
		}
		if item.Installation != nil {
			version = item.Installation.AppVersion
		}
		// The new constrained submitter is a later delivery slice. Do not
		// advertise writes merely because historical AppCommand accepts them.
		reason := "ACTION_UNSUPPORTED"
		if !slices.Contains(user.Scopes, "app:manage") {
			reason = "PERMISSION_DENIED"
		}
		if item.Operation.Blocking {
			reason = "OPERATION_IN_PROGRESS"
		}
		item.Lifecycle = Lifecycle{PolicyRevision: Digest("appcenter-constrained-commands-pending-v1"), Candidates: []Candidate{}}
		if version == "" || item.Origin == "observation" {
			continue
		}
		for _, kind := range []string{"install", "start", "stop", "restart", "delete"} {
			item.Lifecycle.Candidates = append(item.Lifecycle.Candidates, Candidate{Kind: kind, ReasonCode: Text(reason), AppVersion: version})
		}
	}
	sort.Slice(f.Items, func(i, j int) bool { return f.Items[i].CardKey < f.Items[j].CardKey })
	for _, name := range []string{"catalog", "installations", "observations", "operations"} {
		quality := "known"
		var reason *string
		if name == "observations" {
			for _, item := range f.Items {
				if item.Service.Quality != "known" {
					quality = "unknown"
					reason = Text("OBSERVATIONS_PARTIAL")
					break
				}
			}
		}
		at := f.GeneratedAt
		f.Sources = append(f.Sources, SourceStatus{Source: name, Quality: quality, ObservedAt: &at, ReasonCode: reason})
	}
	f.Revision = semanticRevision(f)
	for i := range f.Sources {
		f.Sources[i].Revision = Text(f.Revision)
	}
	return f, nil
}
func (r Record) LastOperationIDValue() string {
	if r.LastOperationID == nil {
		return ""
	}
	return *r.LastOperationID
}
func semanticRevision(f Facts) string {
	raw, _ := json.Marshal(f)
	var copy Facts
	_ = json.Unmarshal(raw, &copy)
	copy.RequestID = ""
	copy.GeneratedAt = time.Time{}
	copy.Revision = ""
	for i := range copy.Sources {
		copy.Sources[i].ObservedAt = nil
		copy.Sources[i].Revision = nil
	}
	for i := range copy.Items {
		copy.Items[i].Service.ObservedAt = nil
		copy.Items[i].Operation.UpdatedAt = nil
		if copy.Items[i].Catalog != nil {
			copy.Items[i].Catalog.Deployment.ObservedAt = time.Time{}
		}
	}
	return Digest(copy)
}
