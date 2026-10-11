package appcenter

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

func TestCommandSchemaSharedVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/commands.generated.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name  string
		Valid bool
		Input json.RawMessage
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			var out any
			if err := Decode(bytes.NewReader(c.Input), "command-core", 16<<10, &out); (err == nil) != c.Valid {
				t.Fatalf("valid=%v, err=%v", c.Valid, err)
			}
		})
	}
}
