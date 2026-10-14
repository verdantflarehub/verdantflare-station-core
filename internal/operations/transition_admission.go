package operations

import (
	"context"
	"errors"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

var ErrTransitionAdmissionDisabled = errors.New("TRANSITION_ADMISSION_DISABLED")

// ConfigureTransitionAdmission is startup-only. Recovery has its own lifetime
// and remains enabled when new acceptance is disabled.
func (s *Service) ConfigureTransitionAdmission(value string, runtime *HTTPTransitionRuntime) error {
	s.transitionAdmissionEnabled = false
	if value == "" || value == "false" {
		return nil
	}
	if value != "true" || runtime == nil || runtime.client == nil || runtime.endpoint == "" || runtime.token == "" ||
		!s.hasTransitionReleases() {
		return errors.New("invalid transition admission configuration")
	}
	s.transitionAdmissionEnabled = true
	return nil
}

// SubmitManagedTransition checks the startup gate only for new acceptance.
// SubmitTransition first handles authorized durable replay, even if materials
// have since been withdrawn or new acceptance is now disabled.
func (s *Service) SubmitManagedTransition(ctx context.Context, actor identity.IdentityContext, command TransitionCommand) (Transition, error) {
	return s.SubmitTransition(ctx, actor, command, func(ctx context.Context, actor identity.IdentityContext, installed Installation, ref string) (TransitionSnapshot, TransitionSnapshot, error) {
		if !s.transitionAdmissionEnabled {
			return TransitionSnapshot{}, TransitionSnapshot{}, ErrTransitionAdmissionDisabled
		}
		return s.transitionReleaseReader(ctx, actor, installed, ref)
	})
}

func (s *Service) hasTransitionReleases() bool {
	return (s.TransitionReleases != nil && s.TransitionReleases.Path != "") ||
		(s.AppCenterAdmissionSource != nil && s.AppCenterAdmissionSource.Path != "" && s.AppCenterAdmissionSource.Dashboard != nil && s.AppCenterAdmissionSource.Dashboard.Path != "")
}
