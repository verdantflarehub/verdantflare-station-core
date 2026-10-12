package appstate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func collectorConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "collector.json")
	if err := os.WriteFile(path, []byte(`{"schemaVersion":1,"collectors":[{"id":"runtime","namespaces":["apps"],"healthRequiredApps":[],"tokenEnv":"TEST_COLLECTOR_TOKEN"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestCollectorHTTPConfiguration(t *testing.T) {
	s := &Service{Pool: &pgxpool.Pool{}, StationID: uuid.NewString()}
	path := collectorConfig(t)
	lookup := func(string) string { return strings.Repeat("t", 40) }
	if h, err := LoadCollectors(path, lookup, s, nil); err != nil || h == nil {
		t.Fatal("valid registry refused", err)
	}
	if _, err := LoadCollectors(path, lookup, s, []string{lookup("")}); err == nil {
		t.Fatal("reused App credential accepted")
	}
	if _, err := LoadCollectors(path, func(string) string { return "" }, s, nil); err == nil {
		t.Fatal("missing service credential accepted")
	}
	if h, err := LoadCollectors("", nil, nil, nil); err != nil || h != nil {
		t.Fatal("disabled protocol not disabled")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		strings.Replace(string(raw), `"healthRequiredApps":[]`, `"healthRequiredApps":null`, 1),
		strings.Replace(string(raw), `"schemaVersion":1`, `"schemaVersion":1,"schemaVersion":1`, 1),
		strings.Replace(string(raw), `"apps"`, `"apps","apps"`, 1),
	} {
		if err = os.WriteFile(path, []byte(bad), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = LoadCollectors(path, lookup, s, nil); err == nil {
			t.Fatal("malformed registry accepted")
		}
	}
}
