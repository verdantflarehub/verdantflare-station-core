package operations

import (
	"reflect"
	"testing"
)

func TestVideoSourceRecoveryRejectsOtherMaterialsAndTransitions(t *testing.T) {
	from, to := videoOriginalSource(), videoObservedSource()
	if !from.Valid() || !to.Valid() || !videoSourceRecovery(from, to, "adopt", "installed") {
		t.Fatal("historical recovery materials invalid")
	}
	for _, action := range []string{"install", "upgrade", "start", "restart", "delete", ""} {
		if videoSourceRecovery(from, to, action, "installed") {
			t.Fatalf("ordinary action %q admitted", action)
		}
	}
	for _, state := range []string{"pending", "failed", "removed", ""} {
		if videoSourceRecovery(from, to, "adopt", state) {
			t.Fatalf("installation state %q admitted", state)
		}
	}
	if videoSourceRecovery(to, from, "adopt", "installed") || videoSourceRecovery(from, from, "adopt", "installed") {
		t.Fatal("reverse or unchanged recovery admitted")
	}
	// Every binding field is part of authorization, including all three digests.
	for side := 0; side < 2; side++ {
		for field := 0; field < reflect.TypeOf(from).NumField(); field++ {
			a, b := from, to
			v := reflect.ValueOf(&a).Elem()
			if side == 1 {
				v = reflect.ValueOf(&b).Elem()
			}
			v.Field(field).SetString(v.Field(field).String() + "-changed")
			if videoSourceRecovery(a, b, "adopt", "installed") {
				t.Fatalf("changed binding side %d field %s admitted", side, v.Type().Field(field).Name)
			}
		}
	}
}
