package resources

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ContainerModelsMountPath     = "/models"
	DefaultReservedHeadroomBytes = int64(200) * 1024 * 1024 * 1024 // 200 GiB
	DefaultMaxAutoPullModelBytes = int64(25) * 1024 * 1024 * 1024  // 25 GiB
	DefaultModelListLimit        = 50
	MaxModelListLimit            = 100
)

var (
	ErrInvalidFilter = errors.New("invalid model list filter")
	ErrInvalidModel  = errors.New("invalid model identifier")
)

var CategoryDirs = map[string]string{
	"CKPT": "checkpoints",
	"UNET": "unet",
	"CLIP": "clip",
	"VAE":  "vae",
	"LORA": "loras",
	"CNET": "controlnet",
	"UPSC": "upscale_models",
	"IPAD": "ipadapter",
}

var validLifecycleStates = map[string]bool{
	"not_installed":    true,
	"queued":           true,
	"downloading":      true,
	"verifying_sha256": true,
	"ready":            true,
	"failed":           true,
	"evicting":         true,
}

var (
	sha256HexPattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
	catalogCodePattern = regexp.MustCompile(`^MDL-(CKPT|UNET|CLIP|VAE|LORA|CNET|UPSC|IPAD)-[A-Z0-9]{2,24}-[0-9]{3}$`)
	filenamePattern    = regexp.MustCompile(`^[A-Za-z0-9._-]{1,120}\.safetensors$`)
	queryTextPattern   = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,80}$`)
	hostPathScrubRegex = regexp.MustCompile(`(?i)(/[a-z0-9._-]+/[a-z0-9._/-]+|[a-z]:\\[a-z0-9._\\-]+)`)
	secretScrubRegex   = regexp.MustCompile(`(?i)(bearer\s+\S+|postgres(?:ql)?://\S+|https?://[^\s?]+\?\S+)`)
)

type ModelListFilter struct {
	Category   string
	Status     string
	OriginKind string
	Query      string
	Cursor     string
	Limit      int
}

type StorageWatermark struct {
	FilesystemTotalBytes       *int64  `json:"filesystem_total_bytes"`
	FilesystemFreeBytes        *int64  `json:"filesystem_free_bytes"`
	CachedReadyBytes           int64   `json:"cached_ready_bytes"`
	UnmaterializedReservedByte int64   `json:"unmaterialized_reserved_bytes"`
	ReservedHeadroomBytes      int64   `json:"reserved_headroom_bytes"`
	MaxAutoPullModelBytes      int64   `json:"max_auto_pull_model_bytes"`
	HeadroomCompliant          *bool   `json:"headroom_compliant"`
	SampledAt                  *string `json:"sampled_at"`
}

type ModelCacheItem struct {
	CatalogCode      string   `json:"catalog_code"`
	ModelID          string   `json:"model_id"`
	DisplayName      string   `json:"display_name"`
	Category         string   `json:"category"`
	CategoryDir      string   `json:"category_dir"`
	Filename         string   `json:"filename"`
	RelativeViewPath string   `json:"relative_view_path"`
	SHA256           string   `json:"sha256"`
	SizeBytes        int64    `json:"size_bytes"`
	VRAMFootprintMiB int64    `json:"vram_footprint_mib"`
	License          string   `json:"license"`
	LicenseTier      string   `json:"license_tier"`
	PaidAccess       bool     `json:"paid_access"`
	OriginKind       string   `json:"origin_kind"`
	ApprovalState    string   `json:"approval_state"`
	Status           string   `json:"status"`
	DownloadFence    int64    `json:"download_fence"`
	BytesDownloaded  int64    `json:"bytes_downloaded"`
	PartBytesOnDisk  int64    `json:"part_bytes_on_disk"`
	SpeedBps         int64    `json:"speed_bps"`
	ETASeconds       *int     `json:"eta_seconds"`
	VerifiedAt       *string  `json:"verified_at"`
	ErrorCode        string   `json:"error_code,omitempty"`
	ErrorMessage     string   `json:"error_message,omitempty"`
	ActivePins       int      `json:"active_pins"`
	ReservedPins     int      `json:"reserved_pins"`
	PinScopes        []string `json:"pin_scopes"`
}

type ModelListPage struct {
	SchemaVersion int              `json:"schema_version"`
	MountPath     string           `json:"mount_path"`
	Watermark     StorageWatermark `json:"watermark"`
	Models        []ModelCacheItem `json:"models"`
	NextCursor    string           `json:"next_cursor,omitempty"`
	TotalCount    int              `json:"total_count"`
}

type ModelStore interface {
	ListModels(ctx context.Context, filter ModelListFilter) (ModelListPage, error)
}

func ValidSHA256(value string) bool {
	return sha256HexPattern.MatchString(value)
}

func RelativeViewPath(category, sha256Hex, filename string) (string, string, error) {
	dir, ok := CategoryDirs[category]
	if !ok || !ValidSHA256(sha256Hex) || !filenamePattern.MatchString(filename) || strings.Contains(filename, "..") {
		return "", "", ErrInvalidModel
	}
	return dir, fmt.Sprintf("%s/%s__%s", dir, sha256Hex[:12], filename), nil
}

func SanitizeErrorMessage(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	scrubbed := secretScrubRegex.ReplaceAllString(trimmed, "[REDACTED]")
	scrubbed = hostPathScrubRegex.ReplaceAllStringFunc(scrubbed, func(match string) string {
		// Preserve controlled relative view paths (<category_dir>/<12hex>__<file>.safetensors).
		for _, dir := range CategoryDirs {
			if strings.HasPrefix(match, dir+"/") {
				return match
			}
		}
		return "[REDACTED_PATH]"
	})
	if len(scrubbed) > 240 {
		scrubbed = scrubbed[:240]
	}
	return scrubbed
}

func ParseModelListFilter(rawQuery string) (ModelListFilter, error) {
	out := ModelListFilter{Limit: DefaultModelListLimit}
	if rawQuery == "" {
		return out, nil
	}
	if strings.Contains(rawQuery, ";") {
		return ModelListFilter{}, ErrInvalidFilter
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return ModelListFilter{}, ErrInvalidFilter
	}
	allowed := map[string]bool{
		"category":    true,
		"status":      true,
		"origin_kind": true,
		"query":       true,
		"cursor":      true,
		"limit":       true,
	}
	for key, list := range values {
		if !allowed[key] || len(list) != 1 {
			return ModelListFilter{}, ErrInvalidFilter
		}
		val := list[0]
		if val == "" {
			return ModelListFilter{}, ErrInvalidFilter
		}
		switch key {
		case "category":
			if val != "all" {
				if _, ok := CategoryDirs[val]; !ok {
					return ModelListFilter{}, ErrInvalidFilter
				}
				out.Category = val
			}
		case "status":
			if val != "all" {
				if !validLifecycleStates[val] {
					return ModelListFilter{}, ErrInvalidFilter
				}
				out.Status = val
			}
		case "origin_kind":
			if val != "all" {
				if val != "public" && val != "local" {
					return ModelListFilter{}, ErrInvalidFilter
				}
				out.OriginKind = val
			}
		case "query":
			if !queryTextPattern.MatchString(val) {
				return ModelListFilter{}, ErrInvalidFilter
			}
			out.Query = strings.ToLower(val)
		case "cursor":
			if !catalogCodePattern.MatchString(val) {
				return ModelListFilter{}, ErrInvalidFilter
			}
			out.Cursor = val
		case "limit":
			n, convErr := strconv.Atoi(val)
			if convErr != nil || n < 1 || n > MaxModelListLimit {
				return ModelListFilter{}, ErrInvalidFilter
			}
			out.Limit = n
		}
	}
	return out, nil
}

type PGModelStore struct {
	Pool      *pgxpool.Pool
	StationID string
}

func ComputeWatermark(
	fsTotal *int64,
	fsFree *int64,
	reservedHeadroom int64,
	maxAutoPull int64,
	sampledAt *time.Time,
	cachedReadyBytes int64,
	unmaterializedReservedBytes int64,
) StorageWatermark {
	if reservedHeadroom < DefaultReservedHeadroomBytes {
		reservedHeadroom = DefaultReservedHeadroomBytes
	}
	if maxAutoPull <= 0 || maxAutoPull > DefaultMaxAutoPullModelBytes {
		maxAutoPull = DefaultMaxAutoPullModelBytes
	}
	wm := StorageWatermark{
		FilesystemTotalBytes:       fsTotal,
		FilesystemFreeBytes:        fsFree,
		CachedReadyBytes:           cachedReadyBytes,
		UnmaterializedReservedByte: unmaterializedReservedBytes,
		ReservedHeadroomBytes:      reservedHeadroom,
		MaxAutoPullModelBytes:      maxAutoPull,
	}
	if fsFree != nil {
		ok := (*fsFree - unmaterializedReservedBytes) >= reservedHeadroom
		wm.HeadroomCompliant = &ok
	}
	if sampledAt != nil {
		ts := sampledAt.UTC().Format(time.RFC3339)
		wm.SampledAt = &ts
	}
	return wm
}

func (s *PGModelStore) ListModels(ctx context.Context, filter ModelListFilter) (ModelListPage, error) {
	if s == nil || s.Pool == nil || s.StationID == "" {
		return ModelListPage{}, errors.New("model store is not configured")
	}
	if filter.Limit < 1 || filter.Limit > MaxModelListLimit {
		filter.Limit = DefaultModelListLimit
	}

	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return ModelListPage{}, err
	}
	defer tx.Rollback(context.Background())

	var fsTotal, fsFree *int64
	reservedHeadroom := DefaultReservedHeadroomBytes
	maxAutoPull := DefaultMaxAutoPullModelBytes
	var sampledAt *time.Time

	var totalVal, freeVal int64
	var sampledVal time.Time
	err = tx.QueryRow(ctx, `
		SELECT filesystem_total_bytes, filesystem_free_bytes, reserved_headroom_bytes, max_auto_pull_model_bytes, sampled_at
		  FROM station.model_cache_storage_state
		 WHERE station_id = $1
	`, s.StationID).Scan(&totalVal, &freeVal, &reservedHeadroom, &maxAutoPull, &sampledVal)
	if err == nil {
		fsTotal = &totalVal
		fsFree = &freeVal
		sampledAt = &sampledVal
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return ModelListPage{}, err
	}

	var cachedReadyBytes, unmaterializedReservedBytes int64
	if err = tx.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN status = 'ready' THEN size_bytes ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status IN ('queued', 'downloading', 'verifying_sha256')
				THEN GREATEST(0, size_bytes - part_bytes_on_disk) ELSE 0 END), 0)
		  FROM station.model_cache_blobs
		 WHERE station_id = $1
	`, s.StationID).Scan(&cachedReadyBytes, &unmaterializedReservedBytes); err != nil {
		return ModelListPage{}, err
	}

	args := []any{s.StationID}
	whereParts := []string{"c.station_id = $1"}
	addArg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if filter.Category != "" {
		whereParts = append(whereParts, "c.category = "+addArg(filter.Category))
	}
	if filter.OriginKind != "" {
		whereParts = append(whereParts, "c.origin_kind = "+addArg(filter.OriginKind))
	}
	if filter.Status != "" {
		whereParts = append(whereParts, "COALESCE(b.status, 'not_installed') = "+addArg(filter.Status))
	}
	if filter.Query != "" {
		like := "%" + strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(filter.Query) + "%"
		p := addArg(like)
		whereParts = append(whereParts, fmt.Sprintf(
			"(LOWER(c.catalog_code) LIKE %s ESCAPE '\\' OR LOWER(c.model_id) LIKE %s ESCAPE '\\' OR LOWER(c.display_name) LIKE %s ESCAPE '\\' OR LOWER(c.filename) LIKE %s ESCAPE '\\' OR LOWER(c.sha256) LIKE %s ESCAPE '\\')",
			p, p, p, p, p,
		))
	}
	baseWhere := strings.Join(whereParts, " AND ")

	var totalCount int
	countSQL := `
		SELECT COUNT(*)::int
		  FROM station.model_catalog_entries c
		  LEFT JOIN station.model_cache_blobs b
		    ON b.station_id = c.station_id AND b.sha256 = c.sha256
		 WHERE ` + baseWhere
	if err = tx.QueryRow(ctx, countSQL, args...).Scan(&totalCount); err != nil {
		return ModelListPage{}, err
	}

	pageWhere := baseWhere
	if filter.Cursor != "" {
		pageWhere += " AND c.catalog_code > " + addArg(filter.Cursor)
	}
	limitPlaceholder := addArg(filter.Limit + 1)

	querySQL := `
		SELECT
			c.catalog_code,
			c.model_id,
			c.display_name,
			c.category,
			c.filename,
			c.sha256,
			c.size_bytes,
			c.vram_footprint_mib,
			c.license,
			c.license_tier,
			c.paid_access,
			c.origin_kind,
			c.approval_state,
			COALESCE(b.status, 'not_installed') AS cache_status,
			COALESCE(b.download_fence, 0),
			COALESCE(b.bytes_downloaded, 0),
			COALESCE(b.part_bytes_on_disk, 0),
			COALESCE(b.speed_bps, 0),
			b.eta_seconds,
			b.verified_at,
			COALESCE(b.last_error_code, ''),
			COALESCE(b.last_error_message, ''),
			COALESCE(p.active_pins, 0),
			COALESCE(p.reserved_pins, 0),
			COALESCE(p.pin_scopes, '{}'::text[])
		  FROM station.model_catalog_entries c
		  LEFT JOIN station.model_cache_blobs b
		    ON b.station_id = c.station_id AND b.sha256 = c.sha256
		  LEFT JOIN LATERAL (
		      SELECT
		          COUNT(*) FILTER (WHERE pin_state = 'active')::int AS active_pins,
		          COUNT(*) FILTER (WHERE pin_state = 'reserved')::int AS reserved_pins,
		           COALESCE(ARRAY_AGG(DISTINCT scope_kind) FILTER (WHERE pin_state IN ('active', 'reserved')), '{}'::text[]) AS pin_scopes
		        FROM station.model_ref_pins mp
		       WHERE mp.station_id = c.station_id AND mp.sha256 = c.sha256
		  ) p ON true
		 WHERE ` + pageWhere + `
		 ORDER BY c.catalog_code ASC
		 LIMIT ` + limitPlaceholder

	rows, err := tx.Query(ctx, querySQL, args...)
	if err != nil {
		return ModelListPage{}, err
	}
	defer rows.Close()

	items := make([]ModelCacheItem, 0, filter.Limit)
	for rows.Next() {
		var item ModelCacheItem
		var verifiedAt *time.Time
		var pinScopes []string
		if err = rows.Scan(
			&item.CatalogCode,
			&item.ModelID,
			&item.DisplayName,
			&item.Category,
			&item.Filename,
			&item.SHA256,
			&item.SizeBytes,
			&item.VRAMFootprintMiB,
			&item.License,
			&item.LicenseTier,
			&item.PaidAccess,
			&item.OriginKind,
			&item.ApprovalState,
			&item.Status,
			&item.DownloadFence,
			&item.BytesDownloaded,
			&item.PartBytesOnDisk,
			&item.SpeedBps,
			&item.ETASeconds,
			&verifiedAt,
			&item.ErrorCode,
			&item.ErrorMessage,
			&item.ActivePins,
			&item.ReservedPins,
			&pinScopes,
		); err != nil {
			return ModelListPage{}, err
		}
		catDir, relView, relErr := RelativeViewPath(item.Category, item.SHA256, item.Filename)
		if relErr != nil {
			return ModelListPage{}, relErr
		}
		item.CategoryDir = catDir
		item.RelativeViewPath = relView
		item.ErrorMessage = SanitizeErrorMessage(item.ErrorMessage)
		if verifiedAt != nil {
			ts := verifiedAt.UTC().Format(time.RFC3339)
			item.VerifiedAt = &ts
		}
		slices.Sort(pinScopes)
		if pinScopes == nil {
			pinScopes = []string{}
		}
		item.PinScopes = pinScopes
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return ModelListPage{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ModelListPage{}, err
	}

	nextCursor := ""
	if len(items) > filter.Limit {
		nextCursor = items[filter.Limit-1].CatalogCode
		items = items[:filter.Limit]
	}

	return ModelListPage{
		SchemaVersion: 1,
		MountPath:     ContainerModelsMountPath,
		Watermark: ComputeWatermark(
			fsTotal,
			fsFree,
			reservedHeadroom,
			maxAutoPull,
			sampledAt,
			cachedReadyBytes,
			unmaterializedReservedBytes,
		),
		Models:     items,
		NextCursor: nextCursor,
		TotalCount: totalCount,
	}, nil
}
