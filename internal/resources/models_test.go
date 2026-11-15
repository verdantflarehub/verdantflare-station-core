package resources

import (
	"strings"
	"testing"
	"time"
)

func TestParseModelListFilterValidation(t *testing.T) {
	f, err := ParseModelListFilter("")
	if err != nil || f.Limit != DefaultModelListLimit {
		t.Fatalf("empty query failed: %+v err=%v", f, err)
	}
	f, err = ParseModelListFilter("category=CKPT&status=ready&origin_kind=public&query=sdxl_1.0&cursor=MDL-CKPT-SDXL-001&limit=25")
	if err != nil || f.Category != "CKPT" || f.Status != "ready" || f.OriginKind != "public" || f.Query != "sdxl_1.0" || f.Cursor != "MDL-CKPT-SDXL-001" || f.Limit != 25 {
		t.Fatalf("valid filter parse mismatch: %+v err=%v", f, err)
	}
	f, err = ParseModelListFilter("category=all&status=all&origin_kind=all")
	if err != nil || f.Category != "" || f.Status != "" || f.OriginKind != "" {
		t.Fatalf("all-sentinel filter mismatch: %+v err=%v", f, err)
	}

	for _, bad := range []string{
		"admin=true",
		"category=CKPT&category=LORA",
		"category=UNKNOWN",
		"status=paused",
		"origin_kind=remote",
		"query=../etc/passwd",
		"cursor=bad-cursor",
		"limit=0",
		"limit=101",
		"limit=abc",
		"category=CKPT;status=ready",
		"category=",
	} {
		if _, err := ParseModelListFilter(bad); err == nil {
			t.Fatalf("expected invalid query %q to fail", bad)
		}
	}
}

func TestRelativeViewPathAndHostPathScrubbing(t *testing.T) {
	sha := "31e35c80fc4829d14f90153f4c74cd59c90b779f6afe05a74cd6120b893f7e5b"
	dir, view, err := RelativeViewPath("CKPT", sha, "sd_xl_base_1.0.safetensors")
	if err != nil || dir != "checkpoints" || view != "checkpoints/31e35c80fc48__sd_xl_base_1.0.safetensors" {
		t.Fatalf("unexpected relative view: dir=%q view=%q err=%v", dir, view, err)
	}
	if strings.HasPrefix(view, "/") || strings.Contains(view, "/data/models") {
		t.Fatalf("host path leaked in view: %q", view)
	}
	for _, tc := range [][3]string{
		{"CKPT", sha, "evil.ckpt"},
		{"CKPT", sha, "../sdxl.safetensors"},
		{"BAD", sha, "sdxl.safetensors"},
		{"CKPT", "UPPERCASE", "sdxl.safetensors"},
	} {
		if _, _, err := RelativeViewPath(tc[0], tc[1], tc[2]); err == nil {
			t.Fatalf("expected invalid view args %v to fail", tc)
		}
	}

	scrubbed := SanitizeErrorMessage("write failed at /data/models/comfyui/.staging/abc.part with Bearer secret-token-123 and checkpoints/31e35c80fc48__sd_xl_base_1.0.safetensors")
	if strings.Contains(scrubbed, "/data/models/comfyui") || strings.Contains(scrubbed, "secret-token-123") {
		t.Fatalf("failed to scrub host path or token: %q", scrubbed)
	}
	if !strings.Contains(scrubbed, "checkpoints/31e35c80fc48__sd_xl_base_1.0.safetensors") {
		t.Fatalf("controlled relative view path should be preserved: %q", scrubbed)
	}
}

func TestComputeWatermarkHeadroomGate(t *testing.T) {
	now := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	total := int64(3200) * 1024 * 1024 * 1024
	freeCompliant := int64(250) * 1024 * 1024 * 1024
	unmat := int64(20) * 1024 * 1024 * 1024
	wm := ComputeWatermark(&total, &freeCompliant, DefaultReservedHeadroomBytes, DefaultMaxAutoPullModelBytes, &now, 1024, unmat)
	if wm.HeadroomCompliant == nil || !*wm.HeadroomCompliant {
		t.Fatalf("expected 250GiB - 20GiB >= 200GiB to be compliant")
	}

	freeTight := int64(210) * 1024 * 1024 * 1024
	wm2 := ComputeWatermark(&total, &freeTight, DefaultReservedHeadroomBytes, DefaultMaxAutoPullModelBytes, &now, 1024, unmat)
	if wm2.HeadroomCompliant == nil || *wm2.HeadroomCompliant {
		t.Fatalf("expected 210GiB - 20GiB < 200GiB to be non-compliant")
	}

	wmNil := ComputeWatermark(nil, nil, 0, 0, nil, 0, 0)
	if wmNil.HeadroomCompliant != nil || wmNil.FilesystemFreeBytes != nil || wmNil.SampledAt != nil {
		t.Fatalf("unsampled watermark must preserve null pointers rather than faking zero: %+v", wmNil)
	}
}
