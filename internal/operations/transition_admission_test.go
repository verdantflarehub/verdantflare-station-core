package operations

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestTransitionAdmissionConfiguration(t *testing.T) {
	runtime, err := NewHTTPTransitionRuntime("http://runtime.test:5053", strings.Repeat("test-only-", 5))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", "false", "true", "TRUE", "1", " false "} {
		for _, configured := range []bool{false, true} {
			s := &Service{}
			client := runtime
			if configured {
				s.TransitionReleases = &TransitionReleases{Path: "controlled/registry.json"}
			} else {
				client = nil
			}
			err := s.ConfigureTransitionAdmission(value, client)
			want := value == "" || value == "false" || (value == "true" && configured)
			if (err == nil) != want || s.transitionAdmissionEnabled != (value == "true" && configured) {
				t.Fatal("invalid startup admission", value, configured, err)
			}
		}
	}
	for _, registry := range []*TransitionReleases{nil, {}} {
		s := &Service{TransitionReleases: registry}
		if s.ConfigureTransitionAdmission("true", runtime) == nil {
			t.Fatal("missing release registry enabled admission")
		}
	}
}

func TestTransitionAdmissionDatabaseReplaySurvivesDisabledGate(t *testing.T) {
	s, actor, command, reader := transitionDatabase(t)
	ctx := context.Background()
	if _, err := s.SubmitManagedTransition(ctx, actor, command); !errors.Is(err, ErrTransitionAdmissionDisabled) {
		t.Fatal(err)
	}
	accepted, err := s.SubmitTransition(ctx, actor, command, reader)
	if err != nil {
		t.Fatal(err)
	}
	command.RequestID = "retry-new-trace"
	for _, releases := range []*TransitionReleases{nil, {Path: "missing-materials"}} {
		s.TransitionReleases = releases
		out, err := s.SubmitManagedTransition(ctx, actor, command)
		if err != nil || out.OperationID != accepted.OperationID || out.RequestID != accepted.RequestID {
			t.Fatal("disabled gate lost replay", err)
		}
	}
}
