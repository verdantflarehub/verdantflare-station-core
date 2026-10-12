package operations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appstate"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/releasebinding"
)

type PreinstalledCommand struct {
	Command            Command                `json:"command"`
	Release            releasebinding.Binding `json:"release"`
	InventoryRevision  string                 `json:"inventoryRevision"`
	ReinstallRemovedID string                 `json:"reinstallRemovedId"`
}
type InstallationHistory struct {
	Installation Installation            `json:"installation"`
	Release      *releasebinding.Binding `json:"release"`
	Confirmed    bool                    `json:"confirmed"`
}
type PreinstalledInventory struct {
	AppID            string                `json:"appId"`
	Revision         string                `json:"revision"`
	Complete         bool                  `json:"complete"`
	WorkloadUID      string                `json:"workloadUid"`
	ManagementStatus string                `json:"managementStatus"`
	ObservedAt       time.Time             `json:"observedAt"`
	History          []InstallationHistory `json:"history"`
}

var preinstalledApp = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
var preinstalledDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

func (s *Service) SubmitPreinstalled(ctx context.Context, u identity.IdentityContext, in PreinstalledCommand) (Operation, error) {
	if !s.canReadInstallations(u) || !slices.Contains(u.Scopes, "app:manage") {
		return Operation{}, identity.Denied
	}
	if !in.Release.Valid() || in.Release.AppID != in.Command.AppID || in.Release.AppVersion != in.Command.AppVersion || !preinstalledDigest.MatchString(in.InventoryRevision) || !slices.Contains([]string{"install", "adopt"}, in.Command.Action) {
		return Operation{}, identity.Invalid
	}
	return s.submit(ctx, u, in.Command, &in, nil, nil)
}
func commandHash(c Command, preinstalled *PreinstalledCommand) [32]byte {
	c.RequestID = ""
	var raw []byte
	if preinstalled == nil {
		raw, _ = json.Marshal(c)
	} else {
		copy := *preinstalled
		copy.Command = c
		raw, _ = json.Marshal(copy)
	}
	return sha256.Sum256(raw)
}

type historyQuery interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func readPreinstalledHistory(ctx context.Context, db historyQuery, station, org, app string) ([]InstallationHistory, error) {
	rows, err := db.Query(ctx, `SELECT `+installationColumns+`,(SELECT binding FROM station.preinstalled_installation_bindings b WHERE b.installation_id=station.app_installations.installation_id),EXISTS(SELECT 1 FROM station.app_operations o JOIN station.preinstalled_operation_bindings b ON b.operation_id=o.operation_id JOIN station.preinstalled_installation_bindings i ON i.installation_id=o.installation_id AND i.binding=b.binding WHERE o.installation_id=station.app_installations.installation_id AND o.status='succeeded') FROM station.app_installations WHERE station_id=$1 AND organization_id=$2 AND app_id=$3 ORDER BY created_at,installation_id LIMIT 1001`, station, org, app)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []InstallationHistory{}
	for rows.Next() {
		var item InstallationHistory
		var raw []byte
		i := &item.Installation
		if err := rows.Scan(&i.InstallationID, &i.AppID, &i.AppVersion, &i.StationID, &i.OrganizationID, &i.State, &i.Revision, &i.LastOperationID, &i.CreatedAt, &i.UpdatedAt, &raw, &item.Confirmed); err != nil {
			return nil, err
		}
		if len(raw) > 0 {
			var binding releasebinding.Binding
			if json.Unmarshal(raw, &binding) != nil || !binding.Valid() {
				return nil, errors.New("invalid stored release binding")
			}
			item.Release = &binding
		}
		out = append(out, item)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	if len(out) > 1000 {
		return nil, errors.New("installation history limit exceeded")
	}
	return out, nil
}
func inventoryRevision(items []InstallationHistory) string {
	raw, _ := json.Marshal(items)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func (s *Service) PreinstalledInventory(ctx context.Context, u identity.IdentityContext, app string) (PreinstalledInventory, error) {
	out := PreinstalledInventory{AppID: app, History: []InstallationHistory{}}
	if !s.canReadInstallations(u) {
		return out, identity.Denied
	}
	if !preinstalledApp.MatchString(app) {
		return out, identity.Invalid
	}
	if s.Pool == nil || s.AppStates == nil {
		return out, appstate.ErrUnavailable
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return out, appstate.ErrUnavailable
	}
	defer tx.Rollback(ctx)
	items, err := readPreinstalledHistory(ctx, tx, s.StationID, u.OrganizationID, app)
	if err != nil {
		return out, err
	}
	out.History = items
	out.Revision = inventoryRevision(items)
	observation, err := s.AppStates.PreinstalledObservation(ctx, tx, u, app)
	if err != nil {
		return out, err
	}
	out.WorkloadUID = observation.UID
	out.ManagementStatus = observation.ManagementStatus
	out.ObservedAt = observation.ObservedAt
	out.Complete = observation.State != "unknown" && (observation.State == "not_installed" || out.WorkloadUID != "")
	return out, tx.Commit(ctx)
}
func (s *Service) validatePreinstalled(ctx context.Context, tx pgx.Tx, u identity.IdentityContext, in *PreinstalledCommand) error {
	if s.Releases == nil || s.Releases.Verify(in.Release) != nil {
		return releasebinding.ErrUntrusted
	}
	items, err := readPreinstalledHistory(ctx, tx, s.StationID, u.OrganizationID, in.Command.AppID)
	if err != nil {
		return err
	}
	if inventoryRevision(items) != in.InventoryRevision {
		return identity.Conflict
	}
	current := false
	removed := ""
	for _, item := range items {
		if item.Installation.State == "removed" {
			removed = item.Installation.InstallationID
		} else {
			current = true
			if item.Release != nil && *item.Release != in.Release {
				return identity.Conflict
			}
		}
	}
	if !current && removed != "" && in.ReinstallRemovedID != removed {
		return identity.Conflict
	}
	if in.ReinstallRemovedID != "" && (current || in.ReinstallRemovedID != removed) {
		return identity.Invalid
	}
	return nil
}
func bindPreinstalled(ctx context.Context, tx pgx.Tx, operation, installation string, binding releasebinding.Binding) error {
	raw, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO station.preinstalled_operation_bindings(operation_id,binding) VALUES($1,$2)`, operation, raw); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `INSERT INTO station.preinstalled_installation_bindings(installation_id,binding) VALUES($1,$2) ON CONFLICT(installation_id) DO UPDATE SET binding=EXCLUDED.binding WHERE station.preinstalled_installation_bindings.binding=EXCLUDED.binding`, installation, raw)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return identity.Conflict
	}
	return nil
}
