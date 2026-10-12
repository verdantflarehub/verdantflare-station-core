package appstate

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRuntimeViewUTCTimestamps(t *testing.T) {
	for _, offset := range []int{8 * 3600, -7 * 3600} {
		zone := time.FixedZone("test", offset)
		now := time.Date(2026, 10, 12, 12, 0, 0, 123, zone)
		received, expires := now.Add(-10*time.Second), now.Add(20*time.Second)
		beforeReceived, beforeExpires := received, expires
		state, reason := "ready", "READY"
		for _, tc := range []struct {
			name  string
			now   time.Time
			state *string
			want  string
		}{
			{"fresh", now, &state, "READY"},
			{"expired", expires.UTC(), &state, "OBSERVATION_EXPIRED"},
			{"invalid", received.Add(-time.Second), &state, "INVALID_OBSERVATION"},
			{"partial", now, nil, "NOT_OBSERVED"},
		} {
			t.Run(zone.String()+now.Format("Z07:00")+"/"+tc.name, func(t *testing.T) {
				got := RuntimeView(tc.now, tc.state, &reason, &received, &expires, false)
				if got.ReasonCode != tc.want || got.ObservedAt == nil || got.ExpiresAt == nil {
					t.Fatal("timestamp conversion changed state", got)
				}
				if !got.ObservedAt.Equal(received) || !got.ExpiresAt.Equal(expires) {
					t.Fatal("timestamp conversion changed instant")
				}
				for _, value := range []*time.Time{got.ObservedAt, got.ExpiresAt} {
					raw, err := json.Marshal(value)
					if err != nil || !strings.HasSuffix(string(raw), "Z\"") {
						t.Fatal("wire timestamp is not UTC", string(raw), err)
					}
					*value = value.Add(time.Hour)
				}
				if received != beforeReceived || expires != beforeExpires {
					t.Fatal("view mutated source evidence")
				}
			})
		}
	}
	got := RuntimeView(time.Now(), nil, nil, nil, nil, false)
	if got.ObservedAt != nil || got.ExpiresAt != nil {
		t.Fatal("missing timestamps manufactured")
	}
}

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
