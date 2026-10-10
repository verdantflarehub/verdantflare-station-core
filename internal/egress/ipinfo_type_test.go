package egress

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func TestIPinfoTypeEvidenceThroughProxy(t *testing.T) {
	for _, tc := range []struct {
		name, body, wantA, wantC, wantError string
		status                              int
		hosting                             bool
	}{
		{"single", `{"input":"8.8.8.8","data":{"ip":"8.8.8.8","asn":{"type":" ISP "},"company":{"type":"business"},"is_hosting":false}}`, "isp", "business", "", 200, false},
		{"dual", `{"input":"8.8.8.8","data":{"ip":"8.8.8.8","asn":{"type":"isp"},"company":{"type":"ISP"}}}`, "isp", "isp", "", 200, false},
		{"hosting", `{"input":"8.8.8.8","data":{"ip":"8.8.8.8","asn":{"type":"hosting"},"company":{"type":"hosting"},"is_hosting":false,"privacy":{"hosting":true}}}`, "hosting", "hosting", "", 200, true},
		{"missing", `{"input":"8.8.8.8","data":{"ip":"8.8.8.8","asn":{"type":"isp"}}}`, "isp", "", "", 200, false},
		{"limited", ``, "", "", "http_status_429", 429, false},
		{"redirect", ``, "", "", "http_status_302", 302, false},
		{"malformed", `<html>challenge</html>`, "", "", "invalid_response", 200, false},
		{"oversized", strings.Repeat("x", 128*1024+1), "", "", "response_too_large", 200, false},
		{"wrong_input", `{"input":"1.1.1.1","data":{"ip":"8.8.8.8","asn":{"type":"isp"}}}`, "", "", "ip_mismatch", 200, false},
		{"wrong_ip", `{"input":"8.8.8.8","data":{"ip":"1.1.1.1","asn":{"type":"isp"}}}`, "", "", "ip_mismatch", 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, e, count := probeFixture(t, "socks5h", Credentials{}, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/ipinfo-type" {
					w.Header().Set("Location", "https://example.invalid/")
					w.WriteHeader(tc.status)
					io.WriteString(w, tc.body)
					return
				}
				standardTarget(w, r)
			})
			result := p.Run(t.Context(), e, Credentials{})
			o := result.Observations[3]
			wantCalls := int32(8)
			if tc.wantError != "" {
				wantCalls = 7
			}
			if result.Status != "passed" || count.Load() != wantCalls || o.Error != tc.wantError || o.ASNType != tc.wantA || o.CompanyType != tc.wantC || (o.Datacenter != nil && *o.Datacenter) != tc.hosting {
				t.Fatalf("unexpected classification evidence: %+v, calls=%d", o, count.Load())
			}
			if (o.Status == "available") != (tc.wantError == "") {
				t.Fatal("availability differs from validated evidence")
			}
		})
	}
}

func TestIPinfoTypeRejectsChangedExitAfterLookup(t *testing.T) {
	var exits atomic.Int32
	p, e, _ := probeFixture(t, "socks5h", Credentials{}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/exit" && exits.Add(1) > 3 {
			io.WriteString(w, `{"ip":"1.1.1.1"}`)
			return
		}
		standardTarget(w, r)
	})
	result := p.Run(t.Context(), e, Credentials{})
	o := result.Observations[3]
	if !result.ExitChanged || o.Status != "unavailable" || o.Error != "exit_changed" || o.ASNType != "" {
		t.Fatalf("changed exit accepted: %+v", result)
	}
}
