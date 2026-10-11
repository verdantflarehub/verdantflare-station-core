package operations

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func appCenterAdmissionReason(err error) string {
	switch {
	case errors.Is(err, identity.Denied):
		return "PERMISSION_DENIED"
	case errors.Is(err, ErrAppCenterUnsupported):
		return "ACTION_UNSUPPORTED"
	case errors.Is(err, ErrAppCenterRelease):
		return "RELEASE_UNVERIFIED"
	case errors.Is(err, ErrAppCenterDispatch):
		return "APPCENTER_EXECUTION_BINDING_UNSUPPORTED"
	case errors.Is(err, ErrAppCenterBinding):
		return "APPCENTER_BINDING_CHANGED"
	default:
		return "APPCENTER_DEPENDENCY_UNAVAILABLE"
	}
}

func (s *AppCenterAdmissionSource) projectCandidate(ctx context.Context, actor identity.IdentityContext, fact *appcenter.Fact, foreign bool, now time.Time) {
	if !slices.Contains(actor.Scopes, "app:manage") || fact.Operation.Blocking || fact.Operation.Quality != "known" || fact.InstallationQuality != "known" || fact.Origin == "observation" {
		return
	}
	if foreign {
		for i := range fact.Lifecycle.Candidates {
			fact.Lifecycle.Candidates[i].Enabled = false
			fact.Lifecycle.Candidates[i].ReasonCode = appcenter.Text("INSTALLATION_STATE_CONFLICT")
		}
		return
	}
	if fact.Service.Quality != "known" || fact.Service.ObservedAt == nil || fact.Service.ObservedAt.After(now.Add(2*time.Second)) || !now.Before(fact.Service.ObservedAt.Add(15*time.Second)) {
		return
	}
	var installation *Installation
	kind, version := "", ""
	if fact.Installation != nil && fact.Installation.State == "installed" && fact.Service.State == "stopped" {
		v := fact.Installation
		kind, version = "start", v.AppVersion
		installation = &Installation{InstallationID: v.InstallationID, Revision: v.Revision, AppID: fact.AppID, AppVersion: v.AppVersion, State: v.State, StationID: actor.StationID, OrganizationID: actor.OrganizationID}
	} else if fact.Origin == "catalog" && fact.Installation == nil && fact.Catalog != nil && fact.Service.State == "absent" {
		kind, version = "install", fact.Catalog.Version
	}
	if kind == "" {
		return
	}
	checkCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	result, err := s.Read(checkCtx, actor, Command{OrganizationID: actor.OrganizationID, StationID: actor.StationID, AppID: fact.AppID, AppVersion: version, Action: kind}, installation)
	reason := ""
	if err != nil {
		reason = appCenterAdmissionReason(err)
		fact.Lifecycle.PolicyRevision = appcenter.Digest([]string{"studio.appcenter.admission-unavailable.v1", fact.AppID, version, reason})
	} else {
		fact.Lifecycle.PolicyRevision = result.Execution.PolicyRevision
		if kind == "start" {
			if fact.Service.WorkloadUID == nil || *fact.Service.WorkloadUID != result.Execution.WorkloadUID {
				reason = "APPCENTER_BINDING_CHANGED"
			}
		} else if fact.Catalog.Namespace != result.Observation.Namespace || fact.Catalog.WorkloadName != result.Observation.WorkloadName {
			reason = "APPCENTER_BINDING_CHANGED"
		}
	}
	for i := range fact.Lifecycle.Candidates {
		candidate := &fact.Lifecycle.Candidates[i]
		if candidate.Kind != kind {
			continue
		}
		candidate.Enabled, candidate.ReasonCode = reason == "", appcenter.Text(reason)
		if err == nil {
			candidate.ReleaseDigest = appcenter.Text(result.Execution.ReleaseDigest)
			candidate.ExpectedWorkloadUID = appcenter.Text(result.Execution.WorkloadUID)
		}
	}
}

func (s *AppCenterAdmissionSource) project(ctx context.Context, actor identity.IdentityContext, facts *appcenter.Facts, foreign map[string]bool) {
	if s == nil {
		return
	}
	jobs := make(chan int)
	var workers sync.WaitGroup
	for n := 0; n < 4; n++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				s.projectCandidate(ctx, actor, &facts.Items[i], foreign[facts.Items[i].AppID], time.Now())
			}
		}()
	}
	for i := range facts.Items {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	facts.RecomputeRevision()
}
