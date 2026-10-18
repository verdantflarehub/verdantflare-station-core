package operations

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appstate"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
)

func TestValidateNamespaceMigrationRequest(t *testing.T) {
	validReq := NamespaceMigrationRequest{
		AppID:           "comfyui",
		InstallationID:  uuid.NewString(),
		SourceNamespace: "verdantflare-comfyui",
		TargetNamespace: "verdantflare-app",
		Workspaces: []NamespaceMigrationWorkspace{
			{
				PVCName:        "comfyui-managed-a",
				NewPVCUID:      uuid.NewString(),
				NewProfileHash: "sha256:" + strings.Repeat("a", 64),
				NewSecretUID:   uuid.NewString(),
			},
		},
	}
	if err := ValidateNamespaceMigrationRequest(validReq); err != nil {
		t.Fatalf("expected valid request, got %v", err)
	}
	badApp := validReq
	badApp.AppID = "INVALID"
	if err := ValidateNamespaceMigrationRequest(badApp); err == nil {
		t.Fatal("expected invalid app_id error")
	}
	badHash := validReq
	badHash.Workspaces = []NamespaceMigrationWorkspace{{PVCName: "comfyui-managed-a", NewProfileHash: "sha256:not-hex"}}
	if err := ValidateNamespaceMigrationRequest(badHash); err == nil {
		t.Fatal("expected invalid profile_hash error")
	}
}

func TestMigrateNamespaceDatabaseIdempotency(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	stationID := uuid.Must(uuid.NewV7()).String()
	if err := migrations.Apply(ctx, pool, stationID); err != nil {
		t.Fatal(err)
	}
	userID := uuid.Must(uuid.NewV7()).String()
	orgID := uuid.Must(uuid.NewV7()).String()
	instID := uuid.Must(uuid.NewV7()).String()
	if _, err := pool.Exec(ctx, `INSERT INTO station.users(user_id,username,password_hash) VALUES($1,'admin','hash')`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO station.organizations(organization_id,name) VALUES($1,'org')`, orgID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO station.app_installations(installation_id,created_by,organization_id,station_id,app_id,app_version,state,revision) VALUES($1,$2,$3,$4,'comfyui','0.1.15','installed',3)`, instID, userID, orgID, stationID); err != nil {
		t.Fatal(err)
	}

	appWsInstanceID := uuid.Must(uuid.NewV7()).String()
	oldHash := strings.Repeat("1", 64)
	newHash := strings.Repeat("2", 64)
	oldPVCUID := uuid.NewString()
	newPVCUID := uuid.NewString()
	pvUID := uuid.NewString()
	oldSecretUID := uuid.NewString()
	newSecretUID := uuid.NewString()
	startOpID := uuid.Must(uuid.NewV7()).String()

	bindingJSON := `{"station_id":"` + stationID + `","organization_id":"` + orgID + `","user_id":"` + userID + `","installation_id":"` + instID + `","instance_id":"` + appWsInstanceID + `","profile_hash":"` + oldHash + `","pvc_name":"comfyui-managed-a","pvc_uid":"` + oldPVCUID + `","pv_name":"pv-comfyui-managed-a","pv_uid":"` + pvUID + `"}`
	if _, err := pool.Exec(ctx, `INSERT INTO station.app_instance_workspaces(instance_id,station_id,organization_id,user_id,installation_id,profile_hash,namespace,pvc_name,pvc_uid,pv_name,pv_uid,binding) VALUES($1,$2,$3,$4,$5,$6,'verdantflare-comfyui','comfyui-managed-a',$7,'pv-comfyui-managed-a',$8,$9::jsonb)`,
		appWsInstanceID, stationID, orgID, userID, instID, oldHash, oldPVCUID, pvUID, bindingJSON); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO station.app_instance_starts(operation_id,instance_id,generation,installation_revision,profile_hash,input,phase,observation) VALUES($1,$2,1,3,$3,'{}'::jsonb,'stopped',$4::jsonb)`,
		startOpID, appWsInstanceID, oldHash, `{"secret_uid":"`+oldSecretUID+`","pod_ready":true}`); err != nil {
		t.Fatal(err)
	}

	blenderInstanceID := uuid.Must(uuid.NewV7()).String()
	blenderCreateOpID := uuid.Must(uuid.NewV7()).String()
	blenderProjectID := uuid.Must(uuid.NewV7()).String()
	blenderOldPVCUID := uuid.NewString()
	blenderNewPVCUID := uuid.NewString()
	blenderPVUID := uuid.NewString()
	oldBytes, _ := hex.DecodeString(oldHash)
	if _, err := pool.Exec(ctx, `INSERT INTO station.runtime_instance_workspaces(instance_id,station_id,organization_id,project_id,create_operation_id,profile_id,profile_hash,namespace,pool_name,pvc_name,pvc_uid,pv_name,pv_uid,node_name,reserved_bytes) VALUES($1,$2,$3,$4,$5,'blender-a-v2',$6,'verdantflare-blender','blender-local-v2','blender-pool-003',$7,'pv-blender-pool-003',$8,'verdentflare-5090',1024)`,
		blenderInstanceID, stationID, orgID, blenderProjectID, blenderCreateOpID, oldBytes, blenderOldPVCUID, blenderPVUID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO station.runtime_instance_executions(operation_id,instance_id,station_id,organization_id,user_id,project_id,action,request_hash,profile_hash,input,status,phase,state) VALUES($1,$2,$3,$4,$5,$6,'create',$7,$7,'{}'::jsonb,'succeeded','stopped',$8::jsonb)`,
		blenderCreateOpID, blenderInstanceID, stationID, orgID, userID, blenderProjectID, oldBytes, `{"secret_uid":"`+oldSecretUID+`"}`); err != nil {
		t.Fatal(err)
	}

	svc := &Service{Pool: pool, StationID: stationID, AppStates: &appstate.Service{Pool: pool, StationID: stationID}}
	req := NamespaceMigrationRequest{
		AppID:           "comfyui",
		InstallationID:  instID,
		SourceNamespace: "verdantflare-comfyui",
		TargetNamespace: "verdantflare-app",
		Workspaces: []NamespaceMigrationWorkspace{
			{
				PVCName:        "comfyui-managed-a",
				NewPVCUID:      newPVCUID,
				NewProfileHash: "sha256:" + newHash,
				NewSecretUID:   newSecretUID,
			},
		},
	}
	for i := 0; i < 2; i++ {
		res, err := svc.MigrateNamespace(ctx, req)
		if err != nil {
			t.Fatalf("attempt %d failed: %v", i, err)
		}
		if res.UpdatedAppInstanceWorkspaces != 1 || res.UpdatedAppInstanceStarts != 1 || len(res.Workspaces) != 1 || res.Workspaces[0].Namespace != "verdantflare-app" {
			t.Fatalf("unexpected comfyui migration result on attempt %d: %+v", i, res)
		}
	}

	blenderReq := NamespaceMigrationRequest{
		AppID:           "blender",
		SourceNamespace: "verdantflare-blender",
		TargetNamespace: "verdantflare-app",
		Workspaces: []NamespaceMigrationWorkspace{
			{
				PVCName:        "blender-pool-003",
				NewPVCUID:      blenderNewPVCUID,
				NewProfileHash: "sha256:" + newHash,
				NewSecretUID:   newSecretUID,
			},
		},
	}
	for i := 0; i < 2; i++ {
		res, err := svc.MigrateNamespace(ctx, blenderReq)
		if err != nil {
			t.Fatalf("blender attempt %d failed: %v", i, err)
		}
		if res.UpdatedRuntimeInstanceWorkspaces != 1 || res.UpdatedRuntimeInstanceExecutions != 1 || len(res.Workspaces) != 1 || res.Workspaces[0].Namespace != "verdantflare-app" {
			t.Fatalf("unexpected blender migration result on attempt %d: %+v", i, res)
		}
	}
}
