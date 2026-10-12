package operations

import (
	"bytes"
	"context"
	"encoding/json"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/strictjson"
)

const transitionReconcilePath = "/internal/installation-transitions/v1/reconcile"

type HTTPTransitionRuntime struct {
	endpoint, token string
	client          *http.Client
}

func NewHTTPTransitionRuntime(origin, token string) (*HTTPTransitionRuntime, error) {
	if origin == "" && token == "" {
		return nil, nil
	}
	u, err := url.Parse(origin)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || strings.Contains(origin, "#") || u.RawPath != "" || (u.Path != "" && u.Path != "/") || len(token) < 32 || len(token) > 4096 {
		return nil, ErrTransitionProgress
	}
	for _, c := range []byte(token) {
		if c <= 32 || c > 126 {
			return nil, ErrTransitionProgress
		}
	}
	u.Path = transitionReconcilePath
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &HTTPTransitionRuntime{u.String(), token, &http.Client{Transport: transport, Timeout: 25 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func validTransitionStep(step TransitionStep) bool {
	id, err := uuid.Parse(step.RequestID)
	return canonicalInstallation(step.OperationID) && err == nil && id.Version() == 4 && id.Variant() == uuid.RFC4122 && id.String() == step.RequestID && transitionDigest.MatchString(step.InputDigest) &&
		slices.Contains([]string{"prepare", "restore", "release_source", "release_target"}, step.Mode)
}

func (c *HTTPTransitionRuntime) ReconcileTransition(ctx context.Context, step TransitionStep) (TransitionProgress, error) {
	var empty TransitionProgress
	if c == nil || c.client == nil || !validTransitionStep(step) {
		return empty, ErrTransitionProgress
	}
	raw, err := json.Marshal(step)
	if err != nil {
		return empty, ErrTransitionProgress
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(raw))
	if err != nil {
		return empty, ErrTransitionProgress
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+c.token)
	response, err := c.client.Do(r)
	if err != nil {
		if ctx.Err() != nil {
			return empty, ctx.Err()
		}
		return empty, ErrTransitionProgress
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return empty, ErrTransitionProgress
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(response.Header.Values("Content-Type")) != 1 || response.Header.Get("Content-Encoding") != "" {
		return empty, ErrTransitionProgress
	}
	var out TransitionProgress
	if strictjson.Decode(response.Body, 4096, &out) != nil || out.TransitionStep != step {
		return empty, ErrTransitionProgress
	}
	allowed := []string{"checking", "sealing", "applying", "verifying", "prepared", "recovery_required", "rejected"}
	if step.Mode == "restore" {
		allowed = []string{"restoring", "restored", "recovery_required"}
	} else if strings.HasPrefix(step.Mode, "release_") {
		allowed = []string{"releasing", "released"}
	}
	if !slices.Contains(allowed, out.Phase) {
		return empty, ErrTransitionProgress
	}
	return out, nil
}
