package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/config"
	"github.com/verdantflarehub/verdantflare-station-core/internal/gateway"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/operations"
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
	if len(os.Args) > 2 || (mode != "serve" && mode != "migrate") {
		logger.Error("usage", "command", "station-core [serve|migrate]")
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
	var appCatalog *catalog.Service
	if path := os.Getenv("STATION_CATALOG_FILE"); path != "" {
		reader, e := catalog.NewKubernetes(os.Getenv("STATION_KUBECONFIG"))
		if e != nil {
			logger.Error("catalog_kubernetes_configuration_invalid")
			return 1
		}
		appCatalog, e = catalog.Load(path, reader)
		if e != nil {
			logger.Error("catalog_configuration_invalid")
			return 1
		}
	}
	ops := &operations.Service{Pool: pool, Catalog: appCatalog, StationID: c.StationID}
	server := &http.Server{Addr: c.Listen, Handler: &gateway.Server{Identity: service, BootstrapToken: c.BootstrapToken, Logger: logger, Catalog: appCatalog, Operations: ops}, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
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
