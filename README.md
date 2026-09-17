# VerdantFlare Station Core

Go implementation of the Station Core foundation. Product and API source of truth:
[central Core design](../docs/design/station/07-core-foundation-implementation.md).

Requires Go 1.25+ and PostgreSQL 15+. Set `STATION_DATABASE_URL` and a persistent
UUIDv7 `STATION_ID` in the process environment. Optionally set a securely generated
`STATION_BOOTSTRAP_TOKEN` (at least 32 bytes) to enable administrator initialization.
Do not commit credentials. Configuration and API details live in the central design.

```sh
go build -o bin/station-core ./cmd/station-core
./bin/station-core migrate
./bin/station-core serve
```

Migrations are explicit and transactional. Serve refuses missing, changed or newer
migration history and mismatched Station identity. Default bind: `127.0.0.1:5050`.

```sh
go test -race ./...
go vet ./...
```

PostgreSQL integration tests run when `STATION_TEST_DATABASE_URL` points to a
**local test server** whose role can create databases. They create uniquely named
disposable databases and drop only those databases on completion. Without this
variable, integration tests are skipped. The central
`scripts/contracts/test_core_runtime.py` additionally verifies the compiled process,
HTTP schemas, process restarts and database outage recovery in its own temporary
PostgreSQL cluster; see `--help` for required binary paths.

## GitHub Actions

- `main`: primary branch; push/PR compiles and builds the container image.
- `dev`: development branch; push/PR compiles and builds the container image.
- `release`: publication branch; only push builds and publishes the image. PR and manual runs build only.

The Dockerfile compiles the executable and packages a linux/amd64 image. CI does
not run formatting checks, static analysis, unit/integration tests or container
smoke tests. Release runs are serialized and are not cancelled by newer pushes.

Configure repository or organization Actions secrets `REGISTRY_ENDPOINT_ALIYUN`
(registry host without URL scheme), `REGISTRY_USER_ALIYUN`, and
`REGISTRY_PASSWORD_ALIYUN`. Image configuration is visible at the top of the workflow:

```yaml
env:
  IMAGE_NAME: wod/verdantflare-station
  SERVICE: station-core
  VERSION: 0.3.0
```

The published reference is `<registry>/<IMAGE_NAME>:<SERVICE>-v<VERSION>`.
Increase `VERSION` for each release and keep the runtime `gateway.Version` aligned. Existing tags are rejected, and
registry/authentication errors stop publication. No moving `latest`, branch or SHA
tags are published. The container runs as a non-root user.

Integrate development on `dev`, then fast-forward approved, tested commits to
`release`. Keep `main` as the primary baseline through reviewed updates. Actions
neither auto-merges branches nor runs production migrations/deployments; deployment
manifests remain in the central design repository.

Commit and push the workflow to activate it. GitHub repository settings control
branch protection and organization-secret visibility; YAML cannot enable them.
The workflow job is `Build image and publish release`. Prohibit force pushes on persistent branches;
release rules must preserve the authorized fast-forward promotion route.

## Read-only app catalog

Set `STATION_CATALOG_FILE` to the generated central `catalog.json` snapshot to
serve authenticated `GET /catalog/apps` and `GET /catalog/apps/{app_id}`.
`group_id=image|music|video` is the optional list filter. Configuration and response
contracts live in [the central catalog design](../docs/design/station/08-app-catalog-implementation.md).

Kubernetes access uses the Pod ServiceAccount by default; local runs explicitly set
`STATION_KUBECONFIG` to a controlled kubeconfig. Only registered Deployment objects
are read. The central deployment directory supplies scoped get-only RBAC and a
catalog ConfigMap; Core's future deployment must mount that ConfigMap and select
its ServiceAccount. Unconfigured catalog returns 503. Kubernetes query failures
produce per-app unknown state, and Deployment readiness does not establish model
readiness. This slice does not implement install/start/stop/restart/delete.

Local live-catalog integration can set `STATION_TEST_CATALOG_FILE` and
`STATION_TEST_KUBECONFIG` alongside the existing isolated test PostgreSQL URL.
CI remains build-and-package only, as requested.

Runtime dispatch: set `STATION_RUNTIME_TARGET` to the trusted internal Runtime gRPC endpoint (for example `station-runtime:5052`). Apply migration 4 before starting Core or Runtime. Without this setting, commands remain queued; it does not indicate execution has begun.
