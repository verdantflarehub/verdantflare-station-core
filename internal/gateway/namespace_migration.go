package gateway

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/operations"
)

func (s *Server) requireBootstrapToken(w http.ResponseWriter, r *http.Request, requestID string) bool {
	if s.BootstrapToken == "" {
		failure(w, requestID, identity.Denied)
		return false
	}
	token := r.Header.Get("X-Bootstrap-Token")
	if token == "" {
		token = bearer(r)
	}
	expected := sha256.Sum256([]byte(s.BootstrapToken))
	actual := sha256.Sum256([]byte(token))
	if token == "" || subtle.ConstantTimeCompare(expected[:], actual[:]) != 1 {
		failure(w, requestID, identity.Denied)
		return false
	}
	return true
}

func decodeNamespaceMigrationRequest(w http.ResponseWriter, r *http.Request) (operations.NamespaceMigrationRequest, error) {
	var req operations.NamespaceMigrationRequest
	if r.URL.RawQuery != "" || r.URL.RawPath != "" || r.Header.Get("Content-Encoding") != "" {
		return req, identity.Invalid
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return req, identity.Invalid
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil || len(bytes.TrimSpace(data)) == 0 {
		return req, identity.Invalid
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&req); err != nil {
		return req, identity.Invalid
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return req, identity.Invalid
	}
	return req, nil
}

func (s *Server) migrateAppNamespace(w http.ResponseWriter, r *http.Request, requestID string) {
	if !s.requireBootstrapToken(w, r, requestID) {
		return
	}
	req, err := decodeNamespaceMigrationRequest(w, r)
	if err != nil {
		failure(w, requestID, err)
		return
	}
	if err := operations.ValidateNamespaceMigrationRequest(req); err != nil {
		failure(w, requestID, err)
		return
	}
	ops := s.Operations
	if ops == nil && s.AppStates != nil {
		ops = &operations.Service{AppStates: s.AppStates}
	}
	if ops == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Application operations are not configured", requestID})
		return
	}
	if ops.AppStates == nil && s.AppStates != nil {
		ops.AppStates = s.AppStates
	}
	result, err := ops.MigrateNamespace(r.Context(), req)
	if err != nil {
		failure(w, requestID, err)
		return
	}
	reply(w, 200, struct {
		operations.NamespaceMigrationResult
		RequestID string `json:"request_id"`
	}{result, requestID})
}
