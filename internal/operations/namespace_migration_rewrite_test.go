package operations

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRewriteExecutionStateMovesNamespaceAndEndpoint(t *testing.T) {
	raw := []byte(`{"helper":{"file_endpoint":"http://blender-prepare.verdantflare-blender.svc.cluster.local:48085"},"worker":{"namespace":"verdantflare-blender","gui_endpoint":"http://blender-run.verdantflare-blender.svc.cluster.local:48083"},"secret_uid":"old-secret"}`)
	out, changed, err := rewriteExecutionState(raw, "verdantflare-blender", "verdantflare-app", "new-secret")
	if err != nil || !changed {
		t.Fatalf("rewrite failed: changed=%v err=%v", changed, err)
	}
	text := string(out)
	for _, want := range []string{"verdantflare-app", "blender-prepare.verdantflare-app.svc.cluster.local", "new-secret"} {
		if !strings.Contains(text, want) {
			t.Fatalf("rewritten state missing %s: %s", want, text)
		}
	}
	if strings.Contains(text, "verdantflare-blender") {
		t.Fatalf("old namespace remained: %s", text)
	}
	again, changed, err := rewriteExecutionState(out, "verdantflare-blender", "verdantflare-app", "new-secret")
	if err != nil || changed || string(again) != string(out) {
		t.Fatalf("rewrite was not idempotent: changed=%v err=%v", changed, err)
	}
}

func TestRewriteExecutionStatePreservesEvidence(t *testing.T) {
	raw := []byte(`{"worker":{"namespace":"old"},"proof":{"startup":{"size":9007199254740993}},"source":{"sha256":"unchanged"},"sealed":false}`)
	out, changed, err := rewriteExecutionState(raw, "old", "new", "")
	if err != nil || !changed {
		t.Fatalf("rewrite failed: changed=%v err=%v", changed, err)
	}
	var state map[string]json.RawMessage
	if err := json.Unmarshal(out, &state); err != nil {
		t.Fatal(err)
	}
	if string(state["proof"]) != `{"startup":{"size":9007199254740993}}` || string(state["source"]) != `{"sha256":"unchanged"}` || string(state["sealed"]) != "false" {
		t.Fatalf("migration changed lifecycle evidence: %s", out)
	}
}

func TestRewriteExecutionStateUnchangedAndInvalid(t *testing.T) {
	for _, raw := range []string{`{}`, `null`, `{"worker":{"namespace":"other"},"secret_uid":"keep"}`, `{"namespace":"old","endpoint":"http://worker.old.svc.cluster.local"}`} {
		out, changed, err := rewriteExecutionState([]byte(raw), "old", "old", "")
		if err != nil || changed || string(out) != raw {
			t.Fatalf("unexpected rewrite: %s changed=%v err=%v", out, changed, err)
		}
	}
	if _, _, err := rewriteExecutionState([]byte(`{"worker":`), "old", "new", ""); err == nil {
		t.Fatal("malformed state accepted")
	}
}
