package operations

import "testing"

func TestLegacyVersionAdoptionRequiresTrustedInstalledUnbound(t *testing.T) {
	for _, trusted := range []bool{false, true} {
		for _, bound := range []bool{false, true} {
			for _, state := range []string{"pending", "installed", "failed", "removed"} {
				for _, action := range []string{"install", "start", "restart", "stop", "delete", "adopt"} {
					want := trusted && !bound && state == "installed" && action == "adopt"
					if got := legacyVersionAdoption(trusted, action, state, bound); got != want {
						t.Fatalf("trusted=%v bound=%v state=%s action=%s: %v", trusted, bound, state, action, got)
					}
				}
			}
		}
	}
}
