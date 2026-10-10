// Package egress owns proxy management and fixed-target diagnostics, not application routing.
package egress

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

var (
	ErrNotFound = errors.New("PROXY_NOT_FOUND")
	ErrConflict = errors.New("PROXY_CONFLICT")
	ErrBusy     = errors.New("PROBE_BUSY")
	ErrDisabled = errors.New("PROXY_DISABLED_OR_EXPIRED")
	ErrEndpoint = errors.New("PROXY_ENDPOINT_NOT_ALLOWED")
)

type Input struct {
	Name      string  `json:"name"`
	Protocol  string  `json:"protocol"`
	Host      string  `json:"host"`
	Port      int     `json:"port"`
	Username  *string `json:"username,omitempty"`
	Password  *string `json:"password,omitempty"`
	ClearAuth bool    `json:"clear_auth,omitempty"`
	Status    string  `json:"status,omitempty"`
	Tag       string  `json:"tag"`
	Note      string  `json:"note"`
	ExpiresAt string  `json:"expires_at"`
	Revision  int64   `json:"revision,omitempty"`
}

type Endpoint struct {
	ID             string     `json:"proxy_id"`
	Name           string     `json:"name"`
	Protocol       string     `json:"protocol"`
	Host           string     `json:"host"`
	Port           int        `json:"port"`
	Status         string     `json:"status"`
	Tag            string     `json:"tag"`
	Note           string     `json:"note"`
	ExpiresAt      *time.Time `json:"expires_at"`
	AuthConfigured bool       `json:"auth_configured"`
	AssignedApps   []string   `json:"assigned_apps"`
	Revision       int64      `json:"revision"`
	Probe          *Result    `json:"probe"`
	Testing        bool       `json:"testing"`
	UpdatedAt      time.Time  `json:"updated_at"`
	credentials    []byte
}

type Credentials struct {
	Username string
	Password string
}
type Sample struct {
	At        time.Time `json:"at"`
	LatencyMS int64     `json:"latency_ms"`
	IP        string    `json:"ip,omitempty"`
	Error     string    `json:"error,omitempty"`
}
type Observation struct {
	Source       string    `json:"source"`
	URL          string    `json:"url"`
	At           time.Time `json:"at"`
	Status       string    `json:"status"`
	IP           string    `json:"ip"`
	Country      string    `json:"country,omitempty"`
	CountryCode  string    `json:"country_code,omitempty"`
	Region       string    `json:"region,omitempty"`
	City         string    `json:"city,omitempty"`
	ASN          string    `json:"asn,omitempty"`
	ISP          string    `json:"isp,omitempty"`
	Organization string    `json:"organization,omitempty"`
	ASNType      string    `json:"asn_type,omitempty"`
	Company      string    `json:"company,omitempty"`
	CompanyType  string    `json:"company_type,omitempty"`
	Datacenter   *bool     `json:"datacenter"`
	Proxy        *bool     `json:"proxy"`
	VPN          *bool     `json:"vpn"`
	Tor          *bool     `json:"tor"`
	Mobile       *bool     `json:"mobile"`
	RiskScore    *int      `json:"risk_score"`
	Error        string    `json:"error,omitempty"`
}
type Result struct {
	Status           string        `json:"status"`
	Target           string        `json:"target"`
	StartedAt        time.Time     `json:"started_at"`
	FinishedAt       time.Time     `json:"finished_at"`
	Samples          []Sample      `json:"samples"`
	Successes        int           `json:"successes"`
	Attempts         int           `json:"attempts"`
	SuccessRate      float64       `json:"success_rate"`
	MeanLatencyMS    int64         `json:"mean_latency_ms"`
	JitterMS         int64         `json:"jitter_ms"`
	ExitIP           string        `json:"exit_ip"`
	ExitChanged      bool          `json:"exit_changed"`
	Observations     []Observation `json:"observations"`
	LocationConflict bool          `json:"location_conflict"`
	BlacklistStatus  string        `json:"blacklist_status"`
}

var hostname = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$`)

func normalize(in *Input) (*time.Time, error) {
	in.Name = strings.TrimSpace(in.Name)
	in.Protocol = strings.ToLower(in.Protocol)
	in.Host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(in.Host)), ".")
	if len([]rune(in.Name)) < 1 || len([]rune(in.Name)) > 120 || len(in.Note) > 4000 || len(in.Tag) > 256 || in.Port < 1 || in.Port > 65535 || !slices.Contains([]string{"http", "https", "socks5", "socks5h"}, in.Protocol) {
		return nil, identity.Invalid
	}
	if net.ParseIP(in.Host) == nil && (!hostname.MatchString(in.Host) || strings.Contains(in.Host, "..")) {
		return nil, identity.Invalid
	}
	if strings.ContainsAny(in.Name+in.Tag, "\x00\r\n") || strings.ContainsRune(in.Note, 0) {
		return nil, identity.Invalid
	}
	if in.Status != "" && in.Status != "pending" && in.Status != "off" {
		return nil, identity.Invalid
	}
	if in.ClearAuth && (in.Username != nil || in.Password != nil) {
		return nil, identity.Invalid
	}
	if in.ExpiresAt == "" {
		return nil, nil
	}
	t, e := time.Parse(time.RFC3339, in.ExpiresAt)
	if e != nil {
		return nil, identity.Invalid
	}
	t = t.UTC()
	return &t, nil
}
func validCredentials(c Credentials) bool {
	return len(c.Username) <= 255 && len(c.Password) <= 255 && !strings.ContainsAny(c.Username+c.Password, "\x00\r\n") && (c.Username != "" || c.Password == "")
}
func authorize(u identity.IdentityContext, station string) error {
	if u.StationID != station || !slices.Contains(u.Roles, "admin") || !slices.Contains(u.Scopes, "app:manage") {
		return identity.Denied
	}
	if _, err := uuid.Parse(u.OrganizationID); err != nil {
		return identity.Denied
	}
	return nil
}
func newCipher(key []byte) (cipher.AEAD, error) {
	b, e := aes.NewCipher(key)
	if e != nil {
		return nil, e
	}
	return cipher.NewGCM(b)
}
func seal(a cipher.AEAD, c Credentials, aad string) ([]byte, error) {
	data, e := json.Marshal(c)
	if e != nil {
		return nil, e
	}
	n := make([]byte, a.NonceSize())
	if _, e = rand.Read(n); e != nil {
		return nil, e
	}
	return a.Seal(n, n, data, []byte(aad)), nil
}
func unseal(a cipher.AEAD, b []byte, aad string) (Credentials, error) {
	var c Credentials
	if len(b) < a.NonceSize() {
		return c, errors.New("invalid encrypted credentials")
	}
	data, e := a.Open(nil, b[:a.NonceSize()], b[a.NonceSize():], []byte(aad))
	if e != nil {
		return c, e
	}
	e = json.Unmarshal(data, &c)
	return c, e
}
func endpointKey(in Input, c Credentials) string {
	b, _ := json.Marshal([]any{in.Protocol, in.Host, in.Port, c.Username})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
