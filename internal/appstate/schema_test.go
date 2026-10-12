package appstate

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Generated from the central design contract by scripts/contracts/generate_app_state.py.
//
//go:embed schema.generated.json
var snapshotSchema []byte

func validateSnapshot(t *testing.T, value Snapshot) {
	t.Helper()
	var resource any
	if err := json.Unmarshal(snapshotSchema, &resource); err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	const id = "station://contracts/v1/app-state-snapshot"
	if err := c.AddResource(id, resource); err != nil {
		t.Fatal(err)
	}
	schema, err := c.Compile(id)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var instance any
	if err = decoder.Decode(&instance); err != nil {
		t.Fatal(err)
	}
	if err = schema.Validate(instance); err != nil {
		t.Fatal(err)
	}
	// Closed shape and scoped identities are part of the public contract.
	instance.(map[string]any)["internalEndpoint"] = "forbidden"
	if schema.Validate(instance) == nil {
		t.Fatal("unexpected transport field accepted")
	}
}
