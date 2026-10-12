package appstate

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Generated from the central design repository; these are not local contracts.
//
//go:embed collector.schema.generated.json
var collectorSchema []byte

//go:embed testdata/collector.generated.json
var collectorVectors []byte

func validateCollector(t *testing.T, definition string, value any) {
	t.Helper()
	var resource any
	if json.Unmarshal(collectorSchema, &resource) != nil {
		t.Fatal("invalid generated schema")
	}
	const id = "station://contracts/v1/app-state-collector"
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	if err := c.AddResource(id, resource); err != nil {
		t.Fatal(err)
	}
	schema, err := c.Compile(id + "#/$defs/" + definition)
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
}

func TestCollectorCentralContractVectors(t *testing.T) {
	var vectors struct {
		Evaluation []struct {
			Name              string
			Resource          ResourceObservation
			HealthRequired    bool
			State, ReasonCode string
		}
		Frames []json.RawMessage
	}
	if err := json.Unmarshal(collectorVectors, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, raw := range vectors.Frames {
		var frame map[string]any
		if json.Unmarshal(raw, &frame) != nil {
			t.Fatal("invalid frame vector")
		}
		validateCollector(t, frame["kind"].(string), frame)
	}
	for _, v := range vectors.Evaluation {
		t.Run(v.Name, func(t *testing.T) {
			validateCollector(t, "resource", v.Resource)
			if state, reason := evaluate(v.Resource, v.HealthRequired); state != v.State || reason != v.ReasonCode {
				t.Fatal("central evaluation differs", state, reason)
			}
		})
	}
}
