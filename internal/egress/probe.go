package egress

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	xproxy "golang.org/x/net/proxy"
)

const ProbeTarget = "https://api.ipify.org?format=json"

type Prober struct {
	AllowedInternal map[string]bool
	// Unexported overrides are only used by hermetic protocol tests.
	target, queryURL, geoURL, ipinfoURL string
	roots                               *x509.CertPool
	timeout                             time.Duration
}

func NewProber(allowed string) (*Prober, error) {
	p := &Prober{AllowedInternal: map[string]bool{}}
	for _, item := range strings.Split(allowed, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		host, port, e := net.SplitHostPort(item)
		n, pe := strconv.Atoi(port)
		if e != nil || pe != nil || n < 1 || n > 65535 || host == "" {
			return nil, errors.New("invalid internal proxy allowlist")
		}
		p.AllowedInternal[net.JoinHostPort(strings.ToLower(host), port)] = true
	}
	return p, nil
}
func publicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	for _, cidr := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001:db8::/32", "2001::/32", "2002::/16", "64:ff9b::/96"} {
		if netip.MustParsePrefix(cidr).Contains(ip) {
			return false
		}
	}
	return true
}
func (p *Prober) transport(ctx context.Context, e Endpoint, c Credentials) (*http.Transport, error) {
	endpoint := net.JoinHostPort(e.Host, strconv.Itoa(e.Port))
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", e.Host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, ErrEndpoint
	}
	for _, ip := range ips {
		if !publicIP(ip) && !p.AllowedInternal[endpoint] {
			return nil, ErrEndpoint
		}
	}
	pinned := net.JoinHostPort(ips[0].String(), strconv.Itoa(e.Port))
	dialer := &net.Dialer{Timeout: 8 * time.Second}
	direct := func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != endpoint {
			return nil, ErrEndpoint
		}
		return dialer.DialContext(ctx, network, pinned)
	}
	tr := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: p.roots}, DisableKeepAlives: true, ForceAttemptHTTP2: false, ResponseHeaderTimeout: 8 * time.Second, TLSHandshakeTimeout: 8 * time.Second}
	if e.Protocol == "http" || e.Protocol == "https" {
		u := &url.URL{Scheme: e.Protocol, Host: endpoint}
		if c.Username != "" {
			u.User = url.UserPassword(c.Username, c.Password)
		}
		tr.Proxy = http.ProxyURL(u)
		tr.DialContext = direct
		return tr, nil
	}
	var auth *xproxy.Auth
	if c.Username != "" {
		auth = &xproxy.Auth{User: c.Username, Password: c.Password}
	}
	socks, err := xproxy.SOCKS5("tcp", endpoint, auth, contextDialer{direct})
	if err != nil {
		return nil, err
	}
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if e.Protocol == "socks5" {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
			if err != nil {
				return nil, err
			}
			if len(addrs) == 0 {
				return nil, ErrEndpoint
			}
			address = net.JoinHostPort(addrs[0].String(), port)
		}
		return socks.(xproxy.ContextDialer).DialContext(ctx, network, address)
	}
	return tr, nil
}

type contextDialer struct {
	dial func(context.Context, string, string) (net.Conn, error)
}

func (d contextDialer) Dial(n, a string) (net.Conn, error) { return d.dial(context.Background(), n, a) }
func (d contextDialer) DialContext(ctx context.Context, n, a string) (net.Conn, error) {
	return d.dial(ctx, n, a)
}
func classify(err error) string {
	if errors.Is(err, ErrEndpoint) {
		return "endpoint_not_allowed"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	var cert *tls.CertificateVerificationError
	if errors.As(err, &cert) {
		return "tls_verification_failed"
	}
	text := strings.ToLower(err.Error())
	if strings.Contains(text, "authentication") || strings.Contains(text, "username/password") {
		return "proxy_auth_failed"
	}
	return "connection_failed"
}
func fetch(ctx context.Context, client *http.Client, target string, out any) error {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if e != nil {
		return e
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "VerdantFlare-Egress-Probe/1")
	response, e := client.Do(req)
	if e != nil {
		return e
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return fmt.Errorf("http_status_%d", response.StatusCode)
	}
	raw, e := io.ReadAll(io.LimitReader(response.Body, 128*1024+1))
	if e != nil {
		return e
	}
	if len(raw) > 128*1024 {
		return errors.New("response_too_large")
	}
	if json.Unmarshal(raw, out) != nil {
		return errors.New("invalid_response")
	}
	return nil
}
func infoError(e error) string {
	v := e.Error()
	if strings.HasPrefix(v, "http_status_") || v == "invalid_response" || v == "response_too_large" || v == "invalid_exit_ip" {
		return v
	}
	return classify(e)
}
func (p *Prober) Run(ctx context.Context, e Endpoint, c Credentials) Result {
	target := ProbeTarget
	if p.target != "" {
		target = p.target
	}
	result := Result{Status: "failed", Target: target, StartedAt: time.Now().UTC(), Samples: []Sample{}, Observations: []Observation{}, BlacklistStatus: "not_checked"}
	tr, err := p.transport(ctx, e, c)
	if err != nil {
		result.Samples = append(result.Samples, Sample{At: time.Now().UTC(), Error: classify(err)})
		result.Attempts = 1
		result.FinishedAt = time.Now().UTC()
		return result
	}
	defer tr.CloseIdleConnections()
	timeout := 8 * time.Second
	if p.timeout > 0 {
		timeout = p.timeout
	}
	client := &http.Client{Transport: tr, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var sum, min, max int64
	for i := 0; i < 3; i++ {
		start := time.Now()
		sample := Sample{At: start.UTC()}
		var v struct {
			IP string `json:"ip"`
		}
		err = fetch(ctx, client, target, &v)
		sample.LatencyMS = time.Since(start).Milliseconds()
		if err == nil {
			ip, pe := netip.ParseAddr(v.IP)
			if pe != nil || !publicIP(ip) {
				err = errors.New("invalid_exit_ip")
			} else {
				sample.IP = ip.String()
			}
		}
		if err != nil {
			sample.Error = infoError(err)
		} else {
			result.Successes++
			sum += sample.LatencyMS
			if result.Successes == 1 {
				min = sample.LatencyMS
			}
			if sample.LatencyMS < min {
				min = sample.LatencyMS
			}
			if sample.LatencyMS > max {
				max = sample.LatencyMS
			}
			if result.ExitIP == "" {
				result.ExitIP = sample.IP
			} else if result.ExitIP != sample.IP {
				result.ExitChanged = true
			}
		}
		result.Samples = append(result.Samples, sample)
		if ctx.Err() != nil {
			break
		}
	}
	result.Attempts = len(result.Samples)
	result.SuccessRate = float64(result.Successes) * 100 / float64(result.Attempts)
	if result.Successes > 0 {
		result.MeanLatencyMS = sum / int64(result.Successes)
		result.JitterMS = max - min
	}
	if result.Successes == 3 {
		result.Status = "passed"
	}
	if result.ExitIP != "" && ctx.Err() == nil {
		queryURL := "https://api.ipquery.io/" + url.PathEscape(result.ExitIP)
		if p.queryURL != "" {
			queryURL = p.queryURL
		}
		geoURL := "https://api.ipapi.is/?q=" + url.QueryEscape(result.ExitIP)
		if p.geoURL != "" {
			geoURL = p.geoURL
		}
		q := Observation{Source: "IPQuery", URL: queryURL, At: time.Now().UTC(), Status: "unavailable", IP: result.ExitIP}
		var raw struct {
			IP  string `json:"ip"`
			ISP struct {
				ASN string `json:"asn"`
				Org string `json:"org"`
				ISP string `json:"isp"`
			} `json:"isp"`
			Location struct {
				Country     string `json:"country"`
				CountryCode string `json:"country_code"`
				State       string `json:"state"`
				City        string `json:"city"`
			} `json:"location"`
			Risk struct {
				Mobile     *bool `json:"is_mobile"`
				VPN        *bool `json:"is_vpn"`
				Tor        *bool `json:"is_tor"`
				Proxy      *bool `json:"is_proxy"`
				Datacenter *bool `json:"is_datacenter"`
				Score      *int  `json:"risk_score"`
			} `json:"risk"`
		}
		if err := fetch(ctx, client, queryURL, &raw); err != nil {
			q.Error = infoError(err)
		} else if raw.IP != result.ExitIP {
			q.Error = "ip_mismatch"
		} else {
			q.Status = "available"
			q.Country = raw.Location.Country
			q.CountryCode = raw.Location.CountryCode
			q.Region = raw.Location.State
			q.City = raw.Location.City
			q.ASN = raw.ISP.ASN
			q.ISP = raw.ISP.ISP
			q.Organization = raw.ISP.Org
			q.Mobile = raw.Risk.Mobile
			q.VPN = raw.Risk.VPN
			q.Tor = raw.Risk.Tor
			q.Proxy = raw.Risk.Proxy
			q.Datacenter = raw.Risk.Datacenter
			if raw.Risk.Score != nil && *raw.Risk.Score >= 0 && *raw.Risk.Score <= 100 {
				q.RiskScore = raw.Risk.Score
			}
		}
		result.Observations = append(result.Observations, q)
		g := Observation{Source: "ipapi.is (anonymous)", URL: geoURL, At: time.Now().UTC(), Status: "unavailable", IP: result.ExitIP}
		var geo struct {
			IP      string `json:"ip"`
			Country string `json:"country"`
			Region  string `json:"region"`
			City    string `json:"city"`
			ASN     string `json:"asn"`
			Company string `json:"company"`
		}
		if err := fetch(ctx, client, geoURL, &geo); err != nil {
			g.Error = infoError(err)
		} else if geo.IP != result.ExitIP {
			g.Error = "ip_mismatch"
		} else {
			g.Status = "available"
			g.Country = geo.Country
			if len(geo.Country) == 2 {
				g.CountryCode = strings.ToUpper(geo.Country)
			}
			g.Region = geo.Region
			g.City = geo.City
			g.ASN = geo.ASN
			g.Organization = geo.Company
		}
		result.Observations = append(result.Observations, g)
		ipinfoURL := "https://ipinfo.io/" + url.PathEscape(result.ExitIP) + "/json"
		if p.ipinfoURL != "" {
			ipinfoURL = p.ipinfoURL
		}
		i := Observation{Source: "IPinfo", URL: ipinfoURL, At: time.Now().UTC(), Status: "unavailable", IP: result.ExitIP}
		var info struct {
			IP      string `json:"ip"`
			Country string `json:"country"`
			Region  string `json:"region"`
			City    string `json:"city"`
			Org     string `json:"org"`
		}
		if err := fetch(ctx, client, ipinfoURL, &info); err != nil {
			i.Error = infoError(err)
		} else if info.IP != result.ExitIP {
			i.Error = "ip_mismatch"
		} else {
			i.Status = "available"
			i.CountryCode = info.Country
			i.Country = info.Country
			i.Region = info.Region
			i.City = info.City
			i.Organization = info.Org
			if asn, org, ok := strings.Cut(info.Org, " "); ok && strings.HasPrefix(asn, "AS") {
				i.ASN = asn
				i.Organization = org
			}
		}
		result.Observations = append(result.Observations, i)
		result.LocationConflict = observationsConflict(result.Observations)
	}
	result.FinishedAt = time.Now().UTC()
	return result
}

func observationsConflict(observations []Observation) bool {
	for i, a := range observations {
		if a.Status != "available" {
			continue
		}
		for _, b := range observations[i+1:] {
			if b.Status != "available" {
				continue
			}
			if len(a.Country) > 2 && len(b.Country) > 2 && !strings.EqualFold(strings.TrimSpace(a.Country), strings.TrimSpace(b.Country)) {
				return true
			}
			// Compare country codes only: names and ISO codes are not interchangeable.
			for _, pair := range [][2]string{{a.CountryCode, b.CountryCode}, {a.Region, b.Region}, {a.City, b.City}, {a.ASN, b.ASN}} {
				if pair[0] != "" && pair[1] != "" && !strings.EqualFold(strings.TrimSpace(pair[0]), strings.TrimSpace(pair[1])) {
					return true
				}
			}
		}
	}
	return false
}
