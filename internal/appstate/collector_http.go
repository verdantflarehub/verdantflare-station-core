package appstate

import (
	"crypto/sha256"
	"os"
	"regexp"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/strictjson"
)

type collectorCredential struct {
	collector Collector
	tokenHash [32]byte
}
type CollectorHandler struct {
	service     *Service
	credentials []collectorCredential
}

// LoadCollectors accepts only controlled configuration and environment-backed
// credentials. An absent path disables this internal protocol.
func LoadCollectors(path string, lookup func(string) string, s *Service, otherTokens []string) (*CollectorHandler, error) {
	if path == "" {
		return nil, nil
	}
	if lookup == nil || s == nil || s.Pool == nil || !validID(s.StationID) {
		return nil, identity.Invalid
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, identity.Invalid
	}
	defer f.Close()
	var registry struct {
		SchemaVersion int `json:"schemaVersion"`
		Collectors    []struct {
			ID                 string   `json:"id"`
			Namespaces         []string `json:"namespaces"`
			HealthRequiredApps []string `json:"healthRequiredApps"`
			TokenEnv           string   `json:"tokenEnv"`
		} `json:"collectors"`
	}
	if strictjson.Decode(f, 1<<20, &registry) != nil || registry.SchemaVersion != 1 || len(registry.Collectors) == 0 || len(registry.Collectors) > 128 {
		return nil, identity.Invalid
	}
	h := &CollectorHandler{service: s}
	seenID, seenNS := map[string]bool{}, map[string]bool{}
	seenToken := map[[32]byte]bool{}
	for _, token := range otherTokens {
		if token != "" {
			seenToken[sha256.Sum256([]byte(token))] = true
		}
	}
	for _, r := range registry.Collectors {
		c := Collector{r.ID, r.Namespaces, r.HealthRequiredApps}
		if !c.valid() || seenID[c.ID] || !regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`).MatchString(r.TokenEnv) {
			return nil, identity.Invalid
		}
		seenID[c.ID] = true
		for _, ns := range c.Namespaces {
			if seenNS[ns] {
				return nil, identity.Invalid
			}
			seenNS[ns] = true
		}
		token := lookup(r.TokenEnv)
		if len(token) < 32 || len(token) > 4096 {
			return nil, identity.Invalid
		}
		for _, ch := range token {
			if ch < 33 || ch > 126 {
				return nil, identity.Invalid
			}
		}
		hash := sha256.Sum256([]byte(token))
		if seenToken[hash] {
			return nil, identity.Invalid
		}
		seenToken[hash] = true
		h.credentials = append(h.credentials, collectorCredential{c, hash})
	}
	collectors := make([]Collector, 0, len(h.credentials))
	for _, credential := range h.credentials {
		collectors = append(collectors, credential.collector)
	}
	s.Observations, err = NewMemory(s.StationID, collectors)
	if err != nil {
		return nil, identity.Invalid
	}
	return h, nil
}
