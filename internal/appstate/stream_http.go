package appstate

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/verdantflarehub/verdantflare-station-core/internal/strictjson"
)

func (h *CollectorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	failure := func(status int, code string) {
		// Reject before reading an unbounded request body; HTTP/1 must not
		// drain that body before delivering busy/auth failures to the caller.
		if r.ProtoMajor == 1 {
			w.Header().Set("Connection", "close")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"code": code})
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		failure(405, "METHOD_NOT_ALLOWED")
		return
	}
	if r.URL.Path != "/internal/app-states/v1/stream" {
		failure(404, "NOT_FOUND")
		return
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" {
		failure(400, "INVALID_ARGUMENT")
		return
	}
	if h == nil || h.service == nil || h.service.Observations == nil {
		failure(503, "APP_STATE_UNAVAILABLE")
		return
	}
	auth := r.Header.Values("Authorization")
	if len(auth) != 1 || !strings.HasPrefix(auth[0], "Bearer ") || len(auth[0]) > 4103 {
		failure(401, "UNAUTHENTICATED")
		return
	}
	hash := sha256.Sum256([]byte(strings.TrimPrefix(auth[0], "Bearer ")))
	collector := ""
	for _, c := range h.credentials {
		if subtle.ConstantTimeCompare(hash[:], c.tokenHash[:]) == 1 {
			collector = c.collector.ID
		}
	}
	if collector == "" {
		failure(401, "UNAUTHENTICATED")
		return
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/x-ndjson" || len(r.Header.Values("Content-Type")) != 1 {
		failure(400, "INVALID_ARGUMENT")
		return
	}
	session, err := h.service.Observations.Open(collector)
	if errors.Is(err, ErrCollectorBusy) {
		w.Header().Set("Retry-After", "2")
		failure(409, "COLLECTOR_BUSY")
		return
	}
	if err != nil {
		failure(503, "APP_STATE_UNAVAILABLE")
		return
	}
	defer session.Close()
	controller := http.NewResponseController(w)
	if r.ProtoMajor == 1 {
		if err = controller.EnableFullDuplex(); err != nil {
			failure(503, "STREAM_UNSUPPORTED")
			return
		}
	}
	if controller.SetReadDeadline(time.Now().Add(observationTTL)) != nil || controller.SetWriteDeadline(time.Now().Add(5*time.Second)) != nil {
		failure(503, "STREAM_UNSUPPORTED")
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Accel-Buffering", "no")
	write := func(v any) error {
		if err := controller.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}
		if err := json.NewEncoder(w).Encode(v); err != nil {
			return err
		}
		return controller.Flush()
	}
	if write(Hello{1, "hello", h.service.Observations.BootID(), session.ID(), 10, 30}) != nil {
		return
	}
	type input struct {
		data []byte
		err  error
	}
	frames := make(chan input)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		defer close(frames)
		scanner := bufio.NewScanner(r.Body)
		scanner.Buffer(make([]byte, 4096), (1<<20)+1)
		for scanner.Scan() {
			data := bytes.Clone(scanner.Bytes())
			select {
			case frames <- input{data: data}:
			case <-stop:
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case frames <- input{err: err}:
			case <-stop:
			}
		}
	}()
	defer func() { close(stop); _ = controller.SetReadDeadline(time.Now()); _ = r.Body.Close(); <-done }()
	timer := time.NewTicker(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
			if !session.Live() {
				_ = write(Closing{1, "closing", "STREAM_TIMEOUT"})
				return
			}
			if write(Ping{1, "ping"}) != nil {
				return
			}
		case frame, ok := <-frames:
			if !ok {
				return
			}
			if frame.err != nil {
				_ = write(Closing{1, "closing", "RESYNC_REQUIRED"})
				return
			}
			if err := session.Receive(frame.data); err != nil {
				code := "RESYNC_REQUIRED"
				if errors.Is(err, strictjson.ErrInvalid) {
					code = "INVALID_ARGUMENT"
				}
				_ = write(Closing{1, "closing", code})
				return
			}
			if controller.SetReadDeadline(session.Deadline()) != nil {
				return
			}
		}
	}
}

func (s *StreamSession) Live() bool {
	s.memory.mu.Lock()
	defer s.memory.mu.Unlock()
	return s.live(s.memory.now())
}

func (s *StreamSession) Deadline() time.Time {
	s.memory.mu.Lock()
	defer s.memory.mu.Unlock()
	deadline := s.lastFrame.Add(observationTTL)
	if s.staging != nil && s.started.Add(observationTTL).Before(deadline) {
		deadline = s.started.Add(observationTTL)
	}
	return deadline
}

func (s *StreamSession) Receive(raw []byte) error {
	var discriminator struct {
		SchemaVersion int    `json:"schemaVersion"`
		Kind          string `json:"kind"`
	}
	if len(raw) > 1<<20 || json.Unmarshal(raw, &discriminator) != nil || discriminator.SchemaVersion != 1 {
		s.Close()
		return strictjson.ErrInvalid
	}
	decode := func(value any) error { return strictjson.Decode(bytes.NewReader(raw), 1<<20, value) }
	var err error
	switch discriminator.Kind {
	case "snapshot-begin":
		var f SnapshotBegin
		err = decode(&f)
		if err == nil {
			return s.Begin(f.SyncID)
		}
	case "snapshot-objects":
		var f SnapshotObjects
		err = decode(&f)
		if err == nil {
			return s.Objects(f.SyncID, f.Objects)
		}
	case "snapshot-end":
		var f SnapshotEnd
		err = decode(&f)
		if err == nil {
			return s.End(f.SyncID, f.TotalCount)
		}
	case "upsert":
		var f Upsert
		err = decode(&f)
		if err == nil {
			return s.Upsert(f.Object)
		}
	case "delete":
		var f Delete
		err = decode(&f)
		if err == nil {
			return s.Delete(f.Namespace, f.Name, f.UID)
		}
	case "heartbeat":
		var f Heartbeat
		err = decode(&f)
		if err == nil {
			return s.Heartbeat()
		}
	default:
		err = strictjson.ErrInvalid
	}
	s.Close()
	return err
}
