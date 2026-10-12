package appstate

import (
	"testing"
	"time"
)

func TestRuntimeEvidenceFreshness(t *testing.T) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	received := now.Add(-10 * time.Second)
	expires := now.Add(20 * time.Second)
	state, reason := "ready", "READY"
	if got := RuntimeView(now, nil, nil, nil, nil, false); got.State != "unknown" || got.ReasonCode != "NOT_OBSERVED" {
		t.Fatal("missing evidence manufactured state", got)
	}
	if got := RuntimeView(now, &state, &reason, &received, &expires, false); got.State != "ready" || got.Quality != "known" {
		t.Fatal(got)
	}
	for _, timepoint := range []time.Time{expires, expires.Add(time.Hour)} {
		got := RuntimeView(timepoint, &state, &reason, &received, &expires, false)
		if got.State != "unknown" || got.ReasonCode != "OBSERVATION_EXPIRED" || got.LastKnownState == nil || *got.LastKnownState != "ready" {
			t.Fatal("expired evidence remained ready", got)
		}
	}
	if got := RuntimeView(now, &state, &reason, &received, &expires, true); got.State != "unknown" {
		t.Fatal("persisted expiration ignored")
	}
	if got := RuntimeView(received.Add(-time.Second), &state, &reason, &received, &expires, false); got.ReasonCode != "INVALID_OBSERVATION" {
		t.Fatal("future observation accepted")
	}
	state = "absent"
	if got := RuntimeView(now, &state, &reason, &received, &expires, false); got.State != "absent" {
		t.Fatal("confirmed absent lost")
	}
}
