package appcenter

import "testing"

func TestVersionNewer(t *testing.T) {
	for _, tc := range []struct {
		target, installed string
		want              bool
	}{
		{"0.1.28", "0.1.27", true},
		{"0.1.32", "0.1.35", false},
		{"0.1.35", "0.1.35", false},
		{"0.10.0", "0.9.9", true},
		{"1.0.0", "1.0.0-rc.1", true},
		{"1.0.0-rc.2", "1.0.0-rc.1", true},
		{"1.0.0-rc.1", "1.0.0", false},
		{"latest", "0.1.0", false},
		{"0.1.0", "", false},
	} {
		if got := VersionNewer(tc.target, tc.installed); got != tc.want {
			t.Errorf("VersionNewer(%q, %q) = %v, want %v", tc.target, tc.installed, got, tc.want)
		}
	}
}
