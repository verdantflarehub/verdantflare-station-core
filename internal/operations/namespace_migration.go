package operations

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

var (
	namespaceDNSLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	hexHashPattern    = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type NamespaceMigrationWorkspace struct {
	PVCName        string `json:"pvc_name"`
	NewPVCUID      string `json:"new_pvc_uid,omitempty"`
	NewProfileHash string `json:"new_profile_hash,omitempty"`
	NewSecretUID   string `json:"new_secret_uid,omitempty"`
}

type NamespaceMigrationRequest struct {
	AppID           string                        `json:"app_id"`
	InstallationID  string                        `json:"installation_id,omitempty"`
	SourceNamespace string                        `json:"source_namespace"`
	TargetNamespace string                        `json:"target_namespace"`
	Workspaces      []NamespaceMigrationWorkspace `json:"workspaces,omitempty"`
}

type MigratedWorkspaceSummary struct {
	Table       string `json:"table"`
	InstanceID  string `json:"instance_id"`
	Namespace   string `json:"namespace"`
	PVCName     string `json:"pvc_name"`
	PVCUID      string `json:"pvc_uid"`
	PVName      string `json:"pv_name"`
	PVUID       string `json:"pv_uid"`
	ProfileHash string `json:"profile_hash"`
}

type NamespaceMigrationResult struct {
	AppID                            string                     `json:"app_id"`
	InstallationID                   string                     `json:"installation_id,omitempty"`
	InstallationState                string                     `json:"installation_state,omitempty"`
	InstallationRevision             int64                      `json:"installation_revision,omitempty"`
	SourceNamespace                  string                     `json:"source_namespace"`
	TargetNamespace                  string                     `json:"target_namespace"`
	UpdatedAppInstanceWorkspaces     int64                      `json:"updated_app_instance_workspaces"`
	UpdatedAppInstanceStarts         int64                      `json:"updated_app_instance_starts"`
	UpdatedRuntimeInstanceWorkspaces int64                      `json:"updated_runtime_instance_workspaces"`
	UpdatedRuntimeInstanceExecutions int64                      `json:"updated_runtime_instance_executions"`
	Workspaces                       []MigratedWorkspaceSummary `json:"workspaces"`
}

func validDNSLabel(v string) bool {
	return len(v) > 0 && len(v) <= 63 && namespaceDNSLabel.MatchString(v)
}

func validUUIDString(v string) bool {
	id, err := uuid.Parse(v)
	return err == nil && id != uuid.Nil && id.String() == v
}

func normalizeHexProfileHash(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	trimmed := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(raw), "sha256:"))
	if !hexHashPattern.MatchString(trimmed) {
		return "", identity.Invalid
	}
	return trimmed, nil
}

func ValidateNamespaceMigrationRequest(req NamespaceMigrationRequest) error {
	if !validDNSLabel(req.AppID) || !validDNSLabel(req.SourceNamespace) || !validDNSLabel(req.TargetNamespace) {
		return identity.Invalid
	}
	if req.InstallationID != "" && !validUUIDString(req.InstallationID) {
		return identity.Invalid
	}
	if len(req.Workspaces) > 128 {
		return identity.Invalid
	}
	seenPVC := make(map[string]bool, len(req.Workspaces))
	for _, w := range req.Workspaces {
		if !validDNSLabel(w.PVCName) || seenPVC[w.PVCName] {
			return identity.Invalid
		}
		seenPVC[w.PVCName] = true
		if w.NewPVCUID != "" && !validUUIDString(w.NewPVCUID) {
			return identity.Invalid
		}
		if w.NewSecretUID != "" && !validUUIDString(w.NewSecretUID) {
			return identity.Invalid
		}
		if _, err := normalizeHexProfileHash(w.NewProfileHash); err != nil {
			return err
		}
	}
	return nil
}

// MigrateNamespace performs an idempotent, transactional migration of workspace
// and instance execution metadata for an application moving to TargetNamespace,
// and resets stale in-memory app-state bindings outside TargetNamespace.
func (s *Service) MigrateNamespace(ctx context.Context, req NamespaceMigrationRequest) (NamespaceMigrationResult, error) {
	out := NamespaceMigrationResult{
		AppID:           req.AppID,
		InstallationID:  req.InstallationID,
		SourceNamespace: req.SourceNamespace,
		TargetNamespace: req.TargetNamespace,
		Workspaces:      []MigratedWorkspaceSummary{},
	}
	if s == nil {
		return out, errors.New("operations service unavailable")
	}
	if err := ValidateNamespaceMigrationRequest(req); err != nil {
		return out, err
	}

	if s.Pool != nil {
		tx, err := s.Pool.Begin(ctx)
		if err != nil {
			return out, err
		}
		defer tx.Rollback(context.Background())

		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "runtime-workspaces:"+s.StationID); err != nil {
			return out, err
		}
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, s.StationID+":"+req.AppID); err != nil {
			return out, err
		}

		// Inspect active installation record if table exists.
		var hasInstallations bool
		if err = tx.QueryRow(ctx, `SELECT to_regclass('station.app_installations') IS NOT NULL`).Scan(&hasInstallations); err != nil {
			return out, err
		}
		if hasInstallations {
			var instID, orgID, state string
			var rev int64
			err = tx.QueryRow(ctx, `SELECT installation_id::text, organization_id::text, state, revision FROM station.app_installations WHERE station_id=$1 AND app_id=$2 AND state<>'removed' LIMIT 1`, s.StationID, req.AppID).Scan(&instID, &orgID, &state, &rev)
			if err == nil {
				if req.InstallationID != "" && req.InstallationID != instID {
					return out, identity.Conflict
				}
				out.InstallationID = instID
				out.InstallationState = state
				out.InstallationRevision = rev
				var hasMarkFn bool
				if tx.QueryRow(ctx, `SELECT to_regprocedure('station.mark_app_state(uuid,uuid,text)') IS NOT NULL`).Scan(&hasMarkFn) == nil && hasMarkFn {
					_, _ = tx.Exec(ctx, `SELECT station.mark_app_state($1::uuid, $2::uuid, $3)`, s.StationID, orgID, req.AppID)
				}
			} else if !errors.Is(err, pgx.ErrNoRows) {
				return out, err
			}
		}

		var hasAppWorkspaces, hasAppStarts, hasRuntimeWorkspaces, hasRuntimeExecutions bool
		_ = tx.QueryRow(ctx, `SELECT to_regclass('station.app_instance_workspaces') IS NOT NULL, to_regclass('station.app_instance_starts') IS NOT NULL, to_regclass('station.runtime_instance_workspaces') IS NOT NULL, to_regclass('station.runtime_instance_executions') IS NOT NULL`).Scan(&hasAppWorkspaces, &hasAppStarts, &hasRuntimeWorkspaces, &hasRuntimeExecutions)

		var hasBindingMatchConstraint, hasAppWorkspaceUpdatedAt, hasAppWorkspaceHexCheck, hasAppStartsHexCheck bool
		if hasAppWorkspaces {
			_ = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='station.app_instance_workspaces'::regclass AND conname='app_instance_workspace_binding_match'), EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='station' AND table_name='app_instance_workspaces' AND column_name='updated_at'), EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='station.app_instance_workspaces'::regclass AND conname='app_instance_workspaces_profile_hash_check')`).Scan(&hasBindingMatchConstraint, &hasAppWorkspaceUpdatedAt, &hasAppWorkspaceHexCheck)
		}
		if hasAppStarts {
			_ = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='station.app_instance_starts'::regclass AND conname='app_instance_starts_profile_hash_check')`).Scan(&hasAppStartsHexCheck)
		}

		for _, w := range req.Workspaces {
			hexHash, err := normalizeHexProfileHash(w.NewProfileHash)
			if err != nil {
				return out, err
			}

			if hasAppWorkspaces {
				type appWsRow struct {
					instanceID  string
					pvcUID      string
					pvName      string
					pvUID       string
					profileHash string
					binding     []byte
				}
				rows, err := tx.Query(ctx, `SELECT instance_id::text, pvc_uid::text, pv_name, pv_uid::text, profile_hash, binding FROM station.app_instance_workspaces WHERE ($1='' OR station_id::text=$1) AND pvc_name=$2 AND namespace IN ($3, $4) AND ($5='' OR installation_id::text=$5) ORDER BY instance_id FOR UPDATE`, s.StationID, w.PVCName, req.SourceNamespace, req.TargetNamespace, req.InstallationID)
				if err != nil {
					return out, err
				}
				var matched []appWsRow
				for rows.Next() {
					var r appWsRow
					if err = rows.Scan(&r.instanceID, &r.pvcUID, &r.pvName, &r.pvUID, &r.profileHash, &r.binding); err != nil {
						rows.Close()
						return out, err
					}
					matched = append(matched, r)
				}
				if err = rows.Err(); err != nil {
					rows.Close()
					return out, err
				}
				rows.Close()

				for _, r := range matched {
					effectivePVCUID := r.pvcUID
					if w.NewPVCUID != "" {
						effectivePVCUID = w.NewPVCUID
					}
					effectiveProfileHash := r.profileHash
					if hexHash != "" {
						if !hasAppWorkspaceHexCheck && strings.HasPrefix(w.NewProfileHash, "sha256:") && (hasBindingMatchConstraint || strings.HasPrefix(r.profileHash, "sha256:")) {
							effectiveProfileHash = w.NewProfileHash
						} else {
							effectiveProfileHash = hexHash
						}
					}
					bindingMap := map[string]any{}
					if len(r.binding) > 0 {
						_ = json.Unmarshal(r.binding, &bindingMap)
					}
					bindingMap["pvc_uid"] = effectivePVCUID
					bindingMap["profile_hash"] = effectiveProfileHash
					if _, hasNS := bindingMap["namespace"]; hasNS || hasBindingMatchConstraint {
						bindingMap["namespace"] = req.TargetNamespace
					}
					if hasBindingMatchConstraint {
						bindingMap["pvc_name"] = w.PVCName
						bindingMap["pv_name"] = r.pvName
					}
					newBinding, err := json.Marshal(bindingMap)
					if err != nil {
						return out, err
					}
					updateSQL := `UPDATE station.app_instance_workspaces SET namespace=$1, pvc_uid=$2, profile_hash=$3, binding=$4::jsonb WHERE instance_id=$5::uuid`
					if hasAppWorkspaceUpdatedAt {
						updateSQL = `UPDATE station.app_instance_workspaces SET namespace=$1, pvc_uid=$2, profile_hash=$3, binding=$4::jsonb, updated_at=now() WHERE instance_id=$5::uuid`
					}
					tag, err := tx.Exec(ctx, updateSQL, req.TargetNamespace, effectivePVCUID, effectiveProfileHash, string(newBinding), r.instanceID)
					if err != nil {
						return out, err
					}
					out.UpdatedAppInstanceWorkspaces += tag.RowsAffected()
					out.Workspaces = append(out.Workspaces, MigratedWorkspaceSummary{
						Table:       "station.app_instance_workspaces",
						InstanceID:  r.instanceID,
						Namespace:   req.TargetNamespace,
						PVCName:     w.PVCName,
						PVCUID:      effectivePVCUID,
						PVName:      r.pvName,
						PVUID:       r.pvUID,
						ProfileHash: effectiveProfileHash,
					})

					if hasAppStarts && (hexHash != "" || w.NewSecretUID != "") {
						startHash := hexHash
						if hexHash != "" && !hasAppStartsHexCheck && strings.HasPrefix(w.NewProfileHash, "sha256:") && hasBindingMatchConstraint {
							startHash = w.NewProfileHash
						}
						sTag, err := tx.Exec(ctx, `UPDATE station.app_instance_starts SET profile_hash = CASE WHEN $2 <> '' THEN $2 ELSE profile_hash END, observation = CASE WHEN $3 <> '' AND observation ? 'secret_uid' THEN jsonb_set(observation, '{secret_uid}', to_jsonb($3::text), true) ELSE observation END, updated_at = now() WHERE instance_id = $1::uuid`, r.instanceID, startHash, w.NewSecretUID)
						if err != nil {
							return out, err
						}
						out.UpdatedAppInstanceStarts += sTag.RowsAffected()
					}
				}
			}

			if hasRuntimeWorkspaces {
				type runtimeWsRow struct {
					instanceID string
					pvcUID     string
					pvName     string
					pvUID      string
					hexHash    string
				}
				rows, err := tx.Query(ctx, `SELECT instance_id::text, pvc_uid::text, pv_name, pv_uid::text, encode(profile_hash, 'hex') FROM station.runtime_instance_workspaces WHERE ($1='' OR station_id::text=$1) AND pvc_name=$2 AND namespace IN ($3, $4) ORDER BY instance_id FOR UPDATE`, s.StationID, w.PVCName, req.SourceNamespace, req.TargetNamespace)
				if err != nil {
					return out, err
				}
				var matched []runtimeWsRow
				for rows.Next() {
					var r runtimeWsRow
					if err = rows.Scan(&r.instanceID, &r.pvcUID, &r.pvName, &r.pvUID, &r.hexHash); err != nil {
						rows.Close()
						return out, err
					}
					matched = append(matched, r)
				}
				if err = rows.Err(); err != nil {
					rows.Close()
					return out, err
				}
				rows.Close()

				for _, r := range matched {
					effectivePVCUID := r.pvcUID
					if w.NewPVCUID != "" {
						effectivePVCUID = w.NewPVCUID
					}
					effectiveHexHash := r.hexHash
					if hexHash != "" {
						effectiveHexHash = hexHash
					}
					hashBytes, err := hex.DecodeString(effectiveHexHash)
					if err != nil {
						return out, err
					}
					tag, err := tx.Exec(ctx, `UPDATE station.runtime_instance_workspaces SET namespace=$1, pvc_uid=$2, profile_hash=$3 WHERE instance_id=$4::uuid`, req.TargetNamespace, effectivePVCUID, hashBytes, r.instanceID)
					if err != nil {
						return out, err
					}
					out.UpdatedRuntimeInstanceWorkspaces += tag.RowsAffected()
					out.Workspaces = append(out.Workspaces, MigratedWorkspaceSummary{
						Table:       "station.runtime_instance_workspaces",
						InstanceID:  r.instanceID,
						Namespace:   req.TargetNamespace,
						PVCName:     w.PVCName,
						PVCUID:      effectivePVCUID,
						PVName:      r.pvName,
						PVUID:       r.pvUID,
						ProfileHash: effectiveHexHash,
					})

					if hasRuntimeExecutions && (hexHash != "" || w.NewSecretUID != "") {
						var newHashBytes []byte
						if hexHash != "" {
							newHashBytes = hashBytes
						}
						eTag, err := tx.Exec(ctx, `UPDATE station.runtime_instance_executions SET profile_hash = COALESCE($2, profile_hash), state = CASE WHEN $3 <> '' THEN CASE WHEN (state ? 'secret_uid') AND (state #> '{execution,secret_uid}' IS NOT NULL) THEN jsonb_set(jsonb_set(state, '{secret_uid}', to_jsonb($3::text), true), '{execution,secret_uid}', to_jsonb($3::text), true) WHEN state ? 'secret_uid' THEN jsonb_set(state, '{secret_uid}', to_jsonb($3::text), true) WHEN state #> '{execution,secret_uid}' IS NOT NULL THEN jsonb_set(state, '{execution,secret_uid}', to_jsonb($3::text), true) ELSE state END ELSE state END, updated_at = now() WHERE instance_id = $1::uuid`, r.instanceID, newHashBytes, w.NewSecretUID)
						if err != nil {
							return out, err
						}
						out.UpdatedRuntimeInstanceExecutions += eTag.RowsAffected()
					}
				}
			}
		}

		if err = tx.Commit(ctx); err != nil {
			return out, err
		}
	}

	if s.AppStates != nil && req.AppID != "" && req.TargetNamespace != "" {
		s.AppStates.ResetAppBindings(req.AppID, req.TargetNamespace)
	}
	return out, nil
}
