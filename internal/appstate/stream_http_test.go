package appstate

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/strictjson"
)

func streamTestHandler(t *testing.T) (*CollectorHandler, Binding, ResourceObservation) {
	t.Helper()
	m, s, b, o, _ := memoryFixture(t, false)
	s.Close()
	m.now = time.Now
	h := &CollectorHandler{service: &Service{StationID: m.station, Observations: m}, credentials: []collectorCredential{{s.collector.config, sha256.Sum256([]byte(strings.Repeat("t", 40)))}}}
	return h, b, o
}

func TestStreamFrameStrictShape(t *testing.T) {
	for _, raw := range []string{
		`{"schemaVersion":1,"kind":"heartbeat","kind":"heartbeat"}`,
		`{"schemaVersion":1,"kind":"heartbeat","unknown":true}`,
		`{"schemaVersion":1,"kind":"snapshot-end","syncId":null,"totalCount":0}`,
		`{"schemaVersion":2,"kind":"heartbeat"}`,
		`{"schemaVersion":1,"kind":"snapshot-end","syncId":"0198abcd-1234-7000-8000-000000000001"}`,
		`{"schemaVersion":1,"kind":"ping"}`,
		strings.Repeat(" ", (1<<20)+1),
	} {
		_, s, _, _, _ := memoryFixture(t, false)
		if err := s.Receive([]byte(raw)); err != strictjson.ErrInvalid {
			t.Fatalf("invalid wire shape not rejected: %v", err)
		}
		if s.Live() {
			t.Fatal("invalid protocol left session active")
		}
	}
	_, s, _, o, _ := memoryFixture(t, false)
	id := uuid.NewString()
	for _, f := range []any{SnapshotBegin{1, "snapshot-begin", id}, SnapshotObjects{1, "snapshot-objects", id, []ResourceObservation{o}}, SnapshotEnd{1, "snapshot-end", id, 1}, Heartbeat{1, "heartbeat"}} {
		raw, _ := json.Marshal(f)
		if err := s.Receive(raw); err != nil {
			t.Fatal(err)
		}
		validateCollector(t, "upstream", f)
	}
}

func TestStreamHTTPGuards(t *testing.T) {
	h, _, _ := streamTestHandler(t)
	for _, tc := range []struct {
		path, method, media string
		auth                []string
		want                int
	}{
		{"/internal/app-states/v1/stream", "GET", "application/x-ndjson", nil, 405},
		{"/internal/app-states/v1/observations", "POST", "application/json", nil, 404},
		{"/internal/app-states/v1/stream?", "POST", "application/x-ndjson", nil, 400},
		{"/internal/app-states/v1/%73tream", "POST", "application/x-ndjson", nil, 400},
		{"/internal/app-states/v1/stream", "POST", "application/x-ndjson", nil, 401},
		{"/internal/app-states/v1/stream", "POST", "application/x-ndjson", []string{"Bearer user-token"}, 401},
		{"/internal/app-states/v1/stream", "POST", "application/x-ndjson", []string{"Bearer " + strings.Repeat("t", 40), "Bearer " + strings.Repeat("t", 40)}, 401},
		{"/internal/app-states/v1/stream", "POST", "application/json", []string{"Bearer " + strings.Repeat("t", 40)}, 400},
	} {
		r := httptest.NewRequest(tc.method, tc.path, bytes.NewReader(nil))
		r.Header.Set("Content-Type", tc.media)
		for _, v := range tc.auth {
			r.Header.Add("Authorization", v)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatal("unexpected guard status", w.Code, tc.want)
		}
		if strings.Contains(w.Body.String(), strings.Repeat("t", 40)) {
			t.Fatal("credential in response")
		}
	}
}

// A real HTTP server is intentional: ResponseRecorder cannot prove bidirectional
// request streaming. Execute this integration test on 5090 dev, not locally.
func TestStreamHTTPDuplexBusyAndReconnect(t *testing.T) {
	h, b, o := streamTestHandler(t)
	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	connect := func() (*http.Response, *io.PipeWriter) {
		reader, writer := io.Pipe()
		r, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/internal/app-states/v1/stream", reader)
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Content-Type", "application/x-ndjson")
		r.Header.Set("Authorization", "Bearer "+strings.Repeat("t", 40))
		response, err := server.Client().Do(r)
		if err != nil {
			writer.Close()
			t.Fatal("hello did not arrive while upload open", err)
		}
		return response, writer
	}
	response, writer := connect()
	defer response.Body.Close()
	defer writer.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.StatusCode)
	}
	scanner := bufio.NewScanner(response.Body)
	if !scanner.Scan() {
		t.Fatal("missing hello")
	}
	var hello Hello
	if strictjson.Decode(bytes.NewReader(scanner.Bytes()), 4096, &hello) != nil || hello.Kind != "hello" || hello.CoreBootID != h.service.Observations.BootID() {
		t.Fatal("invalid hello")
	}
	validateCollector(t, "downstream", hello)
	busy, busyWriter := connect()
	busyWriter.Close()
	busy.Body.Close()
	if busy.StatusCode != 409 || busy.Header.Get("Retry-After") != "2" {
		t.Fatal("healthy stream taken over", busy.StatusCode)
	}
	unknown := memoryView(t, h.service.Observations, b)
	if unknown.Runtime.State != "unknown" {
		t.Fatal("ready before snapshot")
	}
	id := uuid.NewString()
	encoder := json.NewEncoder(writer)
	for _, f := range []any{SnapshotBegin{1, "snapshot-begin", id}, SnapshotObjects{1, "snapshot-objects", id, []ResourceObservation{o}}, SnapshotEnd{1, "snapshot-end", id, 1}} {
		if err := encoder.Encode(f); err != nil {
			t.Fatal(err)
		}
	}
	waitState := func(state string) {
		t.Helper()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			if memoryView(t, h.service.Observations, b).Runtime.State == state {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal("state not converged", state)
			case <-ticker.C:
			}
		}
	}
	waitState("ready")
	o.Ready = 0
	if err := encoder.Encode(Upsert{1, "upsert", o}); err != nil {
		t.Fatal(err)
	}
	waitState("starting")
	writer.Close()
	response.Body.Close()
	// EOF of the upload releases the capability, without an acknowledgement.
	for {
		next, nextWriter := connect()
		if next.StatusCode == 200 {
			scan := bufio.NewScanner(next.Body)
			if !scan.Scan() {
				t.Fatal("missing reconnect hello")
			}
			var got Hello
			if strictjson.Decode(bytes.NewReader(scan.Bytes()), 4096, &got) != nil || got.SessionID == hello.SessionID {
				t.Fatal("reused connection identity")
			}
			nextWriter.Close()
			next.Body.Close()
			break
		}
		nextWriter.Close()
		next.Body.Close()
		if next.StatusCode != 409 {
			t.Fatal("unexpected reconnect status", next.StatusCode)
		}
		select {
		case <-ctx.Done():
			t.Fatal("disconnect did not release session")
		case <-time.After(time.Millisecond):
		}
	}
}
