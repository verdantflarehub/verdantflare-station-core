package operations

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	pb "github.com/verdantflarehub/verdantflare-station-core/internal/runtimev1"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type runtimeTestServer struct {
	pb.UnimplementedAppRuntimeServer
	calls int
	id    string
}

func (s *runtimeTestServer) Reconcile(ctx context.Context, r *pb.AppRequest) (*pb.AppProgress, error) {
	s.calls++
	if s.id == "" {
		s.id = r.OperationId
	}
	if s.id != r.OperationId {
		return nil, status.Error(codes.AlreadyExists, "new operation during recovery")
	}
	if s.calls == 1 {
		return nil, status.Error(codes.Unavailable, "lost response")
	}
	return &pb.AppProgress{OperationId: r.OperationId, Status: "succeeded", Phase: "completed"}, nil
}
func TestDispatchGRPCRecovery(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	station := uuid.NewString()
	if e := migrations.Apply(ctx, pool, station); e != nil {
		t.Fatal(e)
	}
	auth, e := identity.New(pool, station, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	session, e := auth.Bootstrap(ctx, "test", identity.BootstrapRequest{Username: "admin", Password: "Test-password-strong-439!", OrganizationName: "Test"})
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "catalog.json")
	if e = os.WriteFile(path, []byte(`[{"app_id":"test-app","display_name":"Test","group_id":"video","brand":"vf","models":[],"source":"deploys/test.yaml","namespace":"test","workload_name":"test-app","version":"1.0.0","images":[{"component":"server","image":"example.invalid/server:1.0.0"}]}]`), 0600); e != nil {
		t.Fatal(e)
	}
	apps, e := catalog.Load(path, nil)
	if e != nil {
		t.Fatal(e)
	}
	s := &Service{Pool: pool, Catalog: apps, StationID: station}
	original, e := s.Submit(ctx, session.IdentityContext, Command{RequestID: "dispatch-test", IdempotencyKey: "dispatch-key", OrganizationID: session.OrganizationID, StationID: station, AppID: "test-app", AppVersion: "1.0.0", Action: "install"})
	if e != nil {
		t.Fatal(e)
	}
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	backend := &runtimeTestServer{}
	pb.RegisterAppRuntimeServer(server, backend)
	go server.Serve(lis)
	defer server.Stop()
	conn, e := grpc.NewClient("passthrough:///runtime", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	client := pb.NewAppRuntimeClient(conn)
	if found, e := s.DispatchOne(ctx, client); !found || status.Code(e) != codes.Unavailable {
		t.Fatalf("lost response: %v %v", found, e)
	}
	current, e := s.Get(ctx, session.IdentityContext, original.OperationID)
	if e != nil || current.Status != "accepted" {
		t.Fatal(current, e)
	}
	if found, e := s.DispatchOne(ctx, client); !found || e != nil {
		t.Fatal(found, e)
	}
	current, e = s.Get(ctx, session.IdentityContext, original.OperationID)
	if e != nil || current.Status != "succeeded" || current.Phase != "completed" {
		t.Fatal(current, e)
	}
	if found, e := s.DispatchOne(ctx, client); found || e != nil {
		t.Fatal("terminal operation dispatched", found, e)
	}
	if backend.calls != 2 {
		t.Fatal(backend.calls)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	s.Run(cancelled, client, slog.New(slog.NewTextHandler(io.Discard, nil)))
}
func TestRejectMalformedRuntimeProgress(t *testing.T) {
	total := int64(10)
	for _, p := range []*pb.AppProgress{{Status: "succeeded", Phase: "starting"}, {Status: "running", Phase: "completed"}, {Status: "failed", Phase: "checking"}, {Status: "running", Phase: "downloading", DownloadedBytes: 11, TotalBytes: &total}} {
		if validProgress(p) {
			t.Fatal("accepted invalid progress", p)
		}
	}
	if !validProgress(&pb.AppProgress{Status: "running", Phase: "downloading"}) {
		t.Fatal(errors.New("unknown total rejected"))
	}
}
