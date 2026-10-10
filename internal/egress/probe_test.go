package egress

import (
	"bufio"
	"context"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func tunnel(a, b net.Conn) {
	defer a.Close()
	defer b.Close()
	done := make(chan struct{})
	go func() { _, _ = io.Copy(a, b); close(done) }()
	_, _ = io.Copy(b, a)
	a.Close()
	b.Close()
	<-done
}
func socksFixture(t *testing.T, required Credentials) (string, *atomic.Int32) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	count := &atomic.Int32{}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				b := bufio.NewReader(conn)
				head := make([]byte, 2)
				if _, e := io.ReadFull(b, head); e != nil {
					return
				}
				methods := make([]byte, int(head[1]))
				if _, e := io.ReadFull(b, methods); e != nil {
					return
				}
				if required.Username != "" {
					_, _ = conn.Write([]byte{5, 2})
					version, e := b.ReadByte()
					if e != nil || version != 1 {
						return
					}
					n, _ := b.ReadByte()
					user := make([]byte, int(n))
					io.ReadFull(b, user)
					n, _ = b.ReadByte()
					password := make([]byte, int(n))
					io.ReadFull(b, password)
					if string(user) != required.Username || string(password) != required.Password {
						conn.Write([]byte{1, 1})
						return
					}
					conn.Write([]byte{1, 0})
				} else {
					conn.Write([]byte{5, 0})
				}
				h := make([]byte, 4)
				if _, e := io.ReadFull(b, h); e != nil {
					return
				}
				host := ""
				switch h[3] {
				case 1:
					raw := make([]byte, 4)
					io.ReadFull(b, raw)
					host = net.IP(raw).String()
				case 4:
					raw := make([]byte, 16)
					io.ReadFull(b, raw)
					host = net.IP(raw).String()
				case 3:
					n, _ := b.ReadByte()
					raw := make([]byte, int(n))
					io.ReadFull(b, raw)
					host = string(raw)
				default:
					return
				}
				port := make([]byte, 2)
				if _, e := io.ReadFull(b, port); e != nil {
					return
				}
				up, e := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(port)))), time.Second)
				if e != nil {
					conn.Write([]byte{5, 5, 0, 1, 0, 0, 0, 0, 0, 0})
					return
				}
				count.Add(1)
				conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0})
				tunnel(conn, up)
			}()
		}
	}()
	return listener.Addr().String(), count
}
func endpoint(t *testing.T, addr, protocol string) Endpoint {
	t.Helper()
	host, port, e := net.SplitHostPort(addr)
	if e != nil {
		t.Fatal(e)
	}
	n, _ := strconv.Atoi(port)
	return Endpoint{Host: host, Port: n, Protocol: protocol}
}
func probeFixture(t *testing.T, protocol string, creds Credentials, handler http.HandlerFunc) (*Prober, Endpoint, *atomic.Int32) {
	t.Helper()
	target := httptest.NewTLSServer(handler)
	t.Cleanup(target.Close)
	roots := x509.NewCertPool()
	roots.AddCert(target.Certificate())
	count := &atomic.Int32{}
	addr := ""
	if strings.HasPrefix(protocol, "socks") {
		addr, count = socksFixture(t, creds)
	} else {
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "CONNECT" {
				http.Error(w, "CONNECT required", 400)
				return
			}
			if creds.Username != "" {
				u := &url.URL{User: url.UserPassword(creds.Username, creds.Password)}
				authReq := &http.Request{Header: http.Header{}}
				pw, _ := u.User.Password()
				authReq.SetBasicAuth(u.User.Username(), pw)
				if r.Header.Get("Proxy-Authorization") != authReq.Header.Get("Authorization") {
					w.WriteHeader(407)
					return
				}
			}
			up, e := net.DialTimeout("tcp", r.Host, time.Second)
			if e != nil {
				w.WriteHeader(502)
				return
			}
			client, _, e := w.(http.Hijacker).Hijack()
			if e != nil {
				up.Close()
				return
			}
			count.Add(1)
			_, _ = io.WriteString(client, "HTTP/1.1 200 Connection Established\r\n\r\n")
			go tunnel(client, up)
		})
		var proxy *httptest.Server
		if protocol == "https" {
			proxy = httptest.NewTLSServer(h)
			roots.AddCert(proxy.Certificate())
		} else {
			proxy = httptest.NewServer(h)
		}
		t.Cleanup(proxy.Close)
		u, _ := url.Parse(proxy.URL)
		addr = u.Host
	}
	p, _ := NewProber(addr)
	p.roots = roots
	p.target = target.URL + "/exit"
	p.queryURL = target.URL + "/quality"
	p.geoURL = target.URL + "/geo"
	p.ipinfoURL = target.URL + "/ipinfo"
	p.ipinfoTypeURL = target.URL + "/ipinfo-type"
	p.timeout = time.Second
	return p, endpoint(t, addr, protocol), count
}
func standardTarget(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/exit":
		io.WriteString(w, `{"ip":"8.8.8.8"}`)
	case "/quality":
		io.WriteString(w, `{"ip":"8.8.8.8","location":{"country":"A","city":"City A"},"isp":{"asn":"AS1","isp":"Network A"},"risk":{"is_proxy":false,"is_datacenter":true,"risk_score":25}}`)
	case "/geo":
		io.WriteString(w, `{"ip":"8.8.8.8","country":"A","city":"City B","asn":"AS2"}`)
	case "/ipinfo":
		io.WriteString(w, `{"ip":"8.8.8.8","country":"US","region":"California","city":"City C","org":"AS3 Network C"}`)
	case "/ipinfo-type":
		io.WriteString(w, `{"input":"8.8.8.8","data":{"ip":"8.8.8.8","asn":{"asn":"AS3","name":"Network C","type":"isp"},"company":{"name":"Company C","type":"business"},"is_hosting":false,"privacy":{"hosting":false}}}`)
	}
}
func TestProtocolsAndEvidence(t *testing.T) {
	for _, protocol := range []string{"socks5", "socks5h", "http", "https"} {
		t.Run(protocol, func(t *testing.T) {
			creds := Credentials{"test-user", "test-secret"}
			p, e, count := probeFixture(t, protocol, creds, standardTarget)
			r := p.Run(t.Context(), e, creds)
			if r.Status != "passed" || r.Successes != 3 || len(r.Samples) != 3 || r.ExitIP != "8.8.8.8" || count.Load() != 8 || len(r.Observations) != 4 || !r.LocationConflict {
				t.Fatalf("invalid evidence: %+v", r)
			}
			q := r.Observations[0]
			i := r.Observations[2]
			if i.CountryCode != "US" || i.Region != "California" || i.City != "City C" || i.ASN != "AS3" || i.Datacenter != nil {
				t.Fatal("IPinfo geography lost or classification invented")
			}
			if q.Proxy == nil || *q.Proxy || q.Datacenter == nil || !*q.Datacenter || q.VPN != nil || q.RiskScore == nil || *q.RiskScore != 25 {
				t.Fatal("unknown flags or source values lost")
			}
			raw, _ := json.Marshal(r)
			if strings.Contains(string(raw), creds.Password) || strings.Contains(string(raw), creds.Username) {
				t.Fatal("credential disclosure")
			}
			before := count.Load()
			bad := p.Run(t.Context(), e, Credentials{"test-user", "wrong"})
			if bad.Status == "passed" || count.Load() != before {
				t.Fatal("authentication failure bypassed proxy")
			}
		})
	}
}
func TestProbeFailuresRemainFailures(t *testing.T) {
	var hits atomic.Int32
	p, e, _ := probeFixture(t, "socks5h", Credentials{}, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.URL.Path == "/quality" {
			w.WriteHeader(429)
			return
		}
		standardTarget(w, r)
	})
	r := p.Run(t.Context(), e, Credentials{})
	if r.Status != "passed" || r.Observations[0].Error != "http_status_429" || r.Observations[0].Proxy != nil {
		t.Fatal("source error confused with connectivity", r)
	}
	p.AllowedInternal = map[string]bool{}
	before := hits.Load()
	r = p.Run(t.Context(), e, Credentials{})
	if r.Status == "passed" || r.Samples[0].Error != "endpoint_not_allowed" || hits.Load() != before {
		t.Fatal("internal endpoint policy bypass")
	}
	p.AllowedInternal[net.JoinHostPort(e.Host, strconv.Itoa(e.Port))] = true
	p.roots = x509.NewCertPool()
	r = p.Run(t.Context(), e, Credentials{})
	if r.Status == "passed" || hits.Load() != before {
		t.Fatal("TLS verification bypass")
	}
}
func TestInvalidAndChangingExit(t *testing.T) {
	var calls atomic.Int32
	p, e, _ := probeFixture(t, "http", Credentials{}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/exit" {
			if calls.Add(1) == 1 {
				io.WriteString(w, `{"ip":"8.8.8.8"}`)
			} else {
				io.WriteString(w, `{"ip":"1.1.1.1"}`)
			}
			return
		}
		standardTarget(w, r)
	})
	r := p.Run(t.Context(), e, Credentials{})
	if !r.ExitChanged || r.ExitIP != "8.8.8.8" {
		t.Fatal("changing IP not represented")
	}
	p2, e2, _ := probeFixture(t, "http", Credentials{}, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"ip":"127.0.0.1"}`) })
	r = p2.Run(t.Context(), e2, Credentials{})
	if r.Status == "passed" || r.Successes != 0 || len(r.Observations) != 0 {
		t.Fatal("invalid public IP accepted")
	}
}
func TestTimeoutAndRedirectNoFallback(t *testing.T) {
	var targetCalls atomic.Int32
	p, e, _ := probeFixture(t, "http", Credentials{}, func(w http.ResponseWriter, r *http.Request) { targetCalls.Add(1); <-r.Context().Done() })
	p.timeout = 20 * time.Millisecond
	r := p.Run(context.Background(), e, Credentials{})
	if r.Status == "passed" || r.Samples[0].Error != "timeout" || targetCalls.Load() != 3 {
		t.Fatal("timeout semantics", r)
	}
	p2, e2, _ := probeFixture(t, "http", Credentials{}, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "https://example.invalid/", 302) })
	r = p2.Run(t.Context(), e2, Credentials{})
	if r.Successes != 0 || r.Samples[0].Error != "http_status_302" {
		t.Fatal("redirect followed", r)
	}
}

func TestIPinfoFailuresDoNotInventEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, body, errorCode string
		status                int
	}{
		{"limited", "", "http_status_429", 429},
		{"mismatch", `{"ip":"1.1.1.1","country":"US","region":"California"}`, "ip_mismatch", 200},
		{"malformed", `invalid`, "invalid_response", 200},
		{"missing_ip", `{"country":"US"}`, "ip_mismatch", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, e, count := probeFixture(t, "socks5h", Credentials{}, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/ipinfo" {
					w.WriteHeader(tc.status)
					io.WriteString(w, tc.body)
					return
				}
				standardTarget(w, r)
			})
			r := p.Run(t.Context(), e, Credentials{})
			i := r.Observations[2]
			if r.Status != "passed" || count.Load() != 8 || i.Status != "unavailable" || i.Error != tc.errorCode || i.Region != "" || i.CountryCode != "" {
				t.Fatal("source failure corrupted probe or leaked unvalidated geography", r)
			}
		})
	}
}

func TestLocationConflictIncludesRegionAndNormalizesCase(t *testing.T) {
	a := Observation{Status: "available", CountryCode: "US", Country: "United States", Region: "California", City: "Los Angeles", ASN: "AS1"}
	b := a
	b.Country = "US"
	b.CountryCode = "us"
	b.City = "los angeles"
	if observationsConflict([]Observation{a, b}) {
		t.Fatal("equivalent geography marked conflicting")
	}
	b.Region = "Texas"
	if !observationsConflict([]Observation{a, b}) {
		t.Fatal("region conflict omitted")
	}
	b.Status = "unavailable"
	if observationsConflict([]Observation{a, b}) {
		t.Fatal("unavailable source used")
	}
}
