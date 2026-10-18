package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/verdantflarehub/verdantflare-station-core/internal/runtimev1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/verdantflarehub/verdantflare-station-core/internal/appstate"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/config"
	"github.com/verdantflarehub/verdantflare-station-core/internal/egress"
	"github.com/verdantflarehub/verdantflare-station-core/internal/gateway"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/operations"
	"github.com/verdantflarehub/verdantflare-station-core/internal/releasebinding"
	"github.com/verdantflarehub/verdantflare-station-core/internal/telemetry"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
)

func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	c, err := config.Load()
	if err != nil {
		logger.Error("configuration_invalid", "reason", err.Error())
		return 1
	}
	var level slog.Level
	_ = level.UnmarshalText([]byte(c.LogLevel))
	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	mode := "serve"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	if len(os.Args) > 2 || (mode != "serve" && mode != "migrate" && mode != "bind-mcp-token" && mode != "migrate-namespace") {
		logger.Error("usage", "command", "station-core [serve|migrate|bind-mcp-token|migrate-namespace]")
		return 1
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	poolConfig, err := pgxpool.ParseConfig(c.DatabaseURL)
	if err != nil {
		logger.Error("database_configuration_invalid")
		return 1
	}
	poolConfig.MaxConns = 8
	poolConfig.ConnConfig.ConnectTimeout = 3 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		logger.Error("database_connection_failed")
		return 1
	}
	defer pool.Close()
	checkCtx, stop := context.WithTimeout(ctx, 15*time.Second)
	defer stop()
	var serverVersion int
	if err = pool.QueryRow(checkCtx, "SELECT current_setting('server_version_num')::integer").Scan(&serverVersion); err != nil || serverVersion < 150000 {
		logger.Error("database_unavailable_or_unsupported", "minimum_major", 15)
		return 1
	}
	if mode == "migrate" {
		if err = migrations.Apply(checkCtx, pool, c.StationID); err != nil {
			logger.Error("migration_failed", "target_version", migrations.Version)
			return 1
		}
		logger.Info("migration_complete", "version", migrations.Version, "contracts_major", migrations.ContractsMajor)
		return 0
	}
	if err = migrations.Check(checkCtx, pool, c.StationID); err != nil {
		logger.Error("database_not_ready", "required_migration", migrations.Version)
		return 1
	}
	service, err := identity.New(pool, c.StationID, c.SessionTTL)
	if err != nil {
		logger.Error("identity_initialization_failed")
		return 1
	}
	if mode == "bind-mcp-token" {
		expires, err := time.Parse(time.RFC3339, os.Getenv("STATION_MCP_TOKEN_EXPIRES_AT"))
		if err != nil {
			logger.Error("mcp_token_configuration_invalid", "field", "STATION_MCP_TOKEN_EXPIRES_AT")
			return 1
		}
		bound, err := service.BindMCPToken(checkCtx, "operator-bind-mcp-token", os.Getenv("STATION_MCP_TOKEN"), os.Getenv("STATION_MCP_USER_ID"), os.Getenv("STATION_MCP_ORGANIZATION_ID"), expires)
		if err != nil {
			// Database error text may contain parameters. Never log it.
			logger.Error("mcp_token_binding_failed")
			return 1
		}
		logger.Info("mcp_token_bound", "session_id", bound.SessionID, "expires_at", bound.ExpiresAt)
		return 0
	}
	if mode == "migrate-namespace" {
		raw := []byte(os.Getenv("STATION_NAMESPACE_MIGRATION_JSON"))
		if len(bytes.TrimSpace(raw)) == 0 {
			raw, err = io.ReadAll(io.LimitReader(os.Stdin, 64<<10))
			if err != nil {
				logger.Error("namespace_migration_input_invalid")
				return 1
			}
		}
		var req operations.NamespaceMigrationRequest
		d := json.NewDecoder(bytes.NewReader(raw))
		d.DisallowUnknownFields()
		if err = d.Decode(&req); err != nil || d.Decode(new(any)) != io.EOF {
			logger.Error("namespace_migration_input_invalid")
			return 1
		}
		ops := &operations.Service{Pool: pool, StationID: c.StationID}
		res, err := ops.MigrateNamespace(checkCtx, req)
		if err != nil {
			logger.Error("namespace_migration_failed")
			return 1
		}
		logger.Info("namespace_migration_complete", "app_id", res.AppID, "target_namespace", res.TargetNamespace, "updated_app_instance_workspaces", res.UpdatedAppInstanceWorkspaces, "updated_runtime_instance_workspaces", res.UpdatedRuntimeInstanceWorkspaces)
		return 0
	}
	var appCatalog *catalog.Service
	if path := os.Getenv("STATION_CATALOG_FILE"); path != "" {
		var e error
		appCatalog, e = catalog.Load(path, nil)
		if e != nil {
			logger.Error("catalog_configuration_invalid")
			return 1
		}
	}
	// Kubernetes is retained only for Pod/metrics telemetry, never app state.
	k8sReader, _ := catalog.NewKubernetes(os.Getenv("STATION_KUBECONFIG"))
	ops := &operations.Service{Pool: pool, Catalog: appCatalog, StationID: c.StationID}
	appStates := &appstate.Service{Pool: pool, StationID: c.StationID, Catalog: appCatalog}
	ops.AppStates = appStates
	appStateCollector, err := appstate.LoadCollectors(os.Getenv("STATION_APP_STATE_COLLECTOR_REGISTRY"), os.Getenv, appStates, []string{
		c.BootstrapToken, os.Getenv("STATION_APP_TRANSITION_CONTROL_TOKEN"), os.Getenv("STATION_INSTANCE_CONTROL_TOKEN"), os.Getenv("STATION_BLENDER_CONTROL_TOKEN"), os.Getenv("STATION_APP_INSTALLATION_COMFYUI_TOKEN"), os.Getenv("STATION_NETWORK_TOKEN"), os.Getenv("STATION_RESOURCES_TOKEN"),
	})
	if err != nil {
		logger.Error("app_state_collector_configuration_invalid")
		return 1
	}
	if appCatalog != nil {
		seedCtx, finish := context.WithTimeout(ctx, 10*time.Second)
		err := appStates.SyncCatalog(seedCtx, appCatalog.RegisteredAppIDs())
		finish()
		if err != nil {
			logger.Error("app_state_catalog_unavailable")
			return 1
		}
	}
	legacyCtx, finishLegacy := context.WithTimeout(ctx, 10*time.Second)
	err = appStates.LoadLegacy(legacyCtx, os.Getenv("STATION_APP_STATE_LEGACY_REGISTRY"))
	finishLegacy()
	if err != nil {
		logger.Error("app_state_legacy_configuration_invalid")
		return 1
	}
	stateCtx, stopState := context.WithCancel(ctx)
	stateDone := make(chan struct{})
	go func() { defer close(stateDone); appStates.RunExpiry(stateCtx, logger) }()
	defer func() { stopState(); <-stateDone }()
	statePublisher, closePublisher, err := appstate.NewPublisher(appStates, os.Getenv)
	if err != nil {
		logger.Error("app_state_notification_configuration_invalid")
		return 1
	}
	defer closePublisher()
	if statePublisher != nil {
		publishCtx, stopPublisher := context.WithCancel(ctx)
		publishDone := make(chan struct{})
		go func() { defer close(publishDone); statePublisher.Run(publishCtx, logger) }()
		defer func() { stopPublisher(); <-publishDone }()
	}
	ops.Releases = &releasebinding.Source{Path: os.Getenv("STATION_PREINSTALLED_RELEASE_REGISTRY")}
	if path := os.Getenv("STATION_APP_TRANSITION_RELEASE_REGISTRY"); path != "" {
		ops.TransitionReleases = &operations.TransitionReleases{Path: path}
	}
	if target := os.Getenv("STATION_RUNTIME_TARGET"); target != "" {
		conn, e := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if e != nil {
			logger.Error("runtime_configuration_invalid")
			return 1
		}
		defer conn.Close()
		ops.AppCenterRuntime = pb.NewAppCenterRuntimeClient(conn)
		workerCtx, stopWorker := context.WithCancel(ctx)
		workerDone := make(chan struct{})
		go func() { defer close(workerDone); ops.Run(workerCtx, pb.NewAppRuntimeClient(conn), logger) }()
		defer func() { stopWorker(); <-workerDone }()
	}
	if path := os.Getenv("STATION_APPCENTER_COMMAND_REGISTRY"); path != "" {
		ops.AppCenterAdmissionSource = &operations.AppCenterAdmissionSource{Path: path, Signed: ops.Releases, Management: ops.TransitionReleases,
			Dashboard: &operations.DashboardReleases{Path: os.Getenv("STATION_APPCENTER_DASHBOARD_RELEASE_REGISTRY")}, Runtime: ops.AppCenterRuntime}
	}
	transitionRuntime, err := operations.NewHTTPTransitionRuntime(os.Getenv("STATION_APP_TRANSITION_RUNTIME_ORIGIN"), os.Getenv("STATION_APP_TRANSITION_CONTROL_TOKEN"))
	if err != nil {
		logger.Error("invalid_transition_runtime_configuration")
		return 1
	}
	if err := ops.ConfigureTransitionAdmission(os.Getenv("STATION_APP_TRANSITION_ACCEPT_COMMANDS"), transitionRuntime); err != nil {
		logger.Error("invalid_transition_admission_configuration")
		return 1
	}
	if transitionRuntime != nil {
		transitionCtx, stopTransitions := context.WithCancel(ctx)
		transitionDone := make(chan struct{})
		go func() { defer close(transitionDone); ops.RunTransitions(transitionCtx, transitionRuntime, logger) }()
		defer func() { stopTransitions(); <-transitionDone }()
	}

	telemetryURL := os.Getenv("STATION_PROMETHEUS_URL")
	if telemetryURL == "" {
		telemetryURL = "http://prometheus.verdantflare-station.svc.cluster.local:9090"
	}
	telemetryService := telemetry.NewService(telemetryURL, 2*time.Second)
	if k8sReader != nil {
		telemetryService.SetKubernetes(k8sReader)
	}

	blenderControl, err := gateway.NewBlenderControl(os.Getenv("STATION_BLENDER_CONTROL_TOKEN"), os.Getenv("STATION_INSTANCE_RUNTIME_URL"), os.Getenv("STATION_INSTANCE_CONTROL_TOKEN"))
	if err != nil {
		logger.Error("invalid_blender_control_configuration")
		return 1
	}
	var egressService *egress.Service
	if encoded := os.Getenv("STATION_EGRESS_ENCRYPTION_KEY"); encoded != "" {
		key, decodeErr := base64.StdEncoding.DecodeString(encoded)
		prober, configErr := egress.NewProber(os.Getenv("STATION_EGRESS_ALLOWED_INTERNAL"))
		if decodeErr != nil || configErr != nil {
			logger.Error("egress_configuration_invalid")
			return 1
		}
		egressService, err = egress.New(pool, c.StationID, key, prober)
		if err != nil {
			logger.Error("egress_configuration_invalid")
			return 1
		}
	}
	appAccess, err := gateway.NewAppAccess(os.Getenv("STATION_NETWORK_TOKEN"), os.Getenv("STATION_RESOURCES_TOKEN"), service)
	if err != nil {
		logger.Error("invalid_app_delegation_configuration")
		return 1
	}
	installationAccess, err := gateway.LoadInstallationAccess(os.Getenv("STATION_APP_INSTALLATION_ACCESS_REGISTRY"), os.Getenv, service, ops)
	if err != nil {
		logger.Error("invalid_installation_access_configuration")
		return 1
	}
	server := &http.Server{Addr: c.Listen, Handler: &gateway.Server{AppStates: appStates, AppStateCollector: appStateCollector, Identity: service, BootstrapToken: c.BootstrapToken, Logger: logger, Catalog: appCatalog, Operations: ops, Telemetry: telemetryService, BlenderControl: blenderControl, Egress: egressService, AppAccess: appAccess, InstallationAccess: installationAccess}, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 65 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	server.BaseContext = func(net.Listener) context.Context { return ctx }
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	logger.Info("station_core_starting", "version", gateway.Version, "station_id", c.StationID, "listen", c.Listen, "contracts_major", migrations.ContractsMajor, "migration_version", migrations.Version, "session_ttl", c.SessionTTL.String(), "login_failure_limit", 5, "account_lock_duration", "15m", "log_level", c.LogLevel)
	select {
	case err = <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http_server_failed")
			return 1
		}
	case <-ctx.Done():
		shutdownCtx, finish := context.WithTimeout(context.Background(), 10*time.Second)
		defer finish()
		if server.Shutdown(shutdownCtx) != nil {
			_ = server.Close()
			logger.Error("shutdown_timeout")
			return 1
		}
	}
	logger.Info("station_core_stopped")
	return 0
}
func main() { os.Exit(run()) }
