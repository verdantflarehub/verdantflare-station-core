package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appstate"
)

// Run on 5090 dev: exercises the actual logging wrapper and bypass of the
// ordinary ten-second request deadline with an unfinished streaming upload.
func TestStreamGatewayRemainsDuplexBeyondRequestDeadline(t *testing.T) {
	service := &appstate.Service{Pool: &pgxpool.Pool{}, StationID: uuid.NewString()}
	path := filepath.Join(t.TempDir(), "collectors.json")
	if err := os.WriteFile(path, []byte(`{"schemaVersion":1,"collectors":[{"id":"runtime","namespaces":["apps"],"healthRequiredApps":[],"tokenEnv":"TEST_STREAM_TOKEN"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("s", 40)
	h, err := appstate.LoadCollectors(path, func(string) string { return token }, service, nil)
	if err != nil {
		t.Fatal(err)
	}
	base, cancelBase := context.WithCancel(t.Context())
	defer cancelBase()
	server := httptest.NewUnstartedServer(&Server{AppStateCollector: h, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	server.Config.BaseContext = func(net.Listener) context.Context { return base }
	server.Start()
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer writer.Close()
	r, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/internal/app-states/v1/stream", reader)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/x-ndjson")
	r.Header.Set("Authorization", "Bearer "+token)
	response, err := server.Client().Do(r)
	if err != nil {
		t.Fatal("gateway did not return streaming headers", err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.StatusCode)
	}
	scan := bufio.NewScanner(response.Body)
	if !scan.Scan() {
		t.Fatal("missing hello")
	}
	id := uuid.NewString()
	for _, frame := range []any{appstate.SnapshotBegin{SchemaVersion: 1, Kind: "snapshot-begin", SyncID: id}, appstate.SnapshotEnd{SchemaVersion: 1, Kind: "snapshot-end", SyncID: id, TotalCount: 0}} {
		if err = json.NewEncoder(writer).Encode(frame); err != nil {
			t.Fatal(err)
		}
	}
	started := time.Now()
	if !scan.Scan() {
		t.Fatal("stream terminated before ping", scan.Err())
	}
	var ping appstate.Ping
	if json.Unmarshal(scan.Bytes(), &ping) != nil || ping.Kind != "ping" || time.Since(started) < 9*time.Second {
		t.Fatal("unexpected control frame")
	}
	select {
	case <-ctx.Done():
		t.Fatal("stream context failed")
	case <-time.After(300 * time.Millisecond):
	}
	if err = json.NewEncoder(writer).Encode(appstate.Heartbeat{SchemaVersion: 1, Kind: "heartbeat"}); err != nil {
		t.Fatal("ordinary gateway deadline terminated stream", err)
	}
	cancelBase()
	if scan.Scan() {
		t.Fatal("shutdown left stream running")
	}
}
