package appstate

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// NotificationRevisions reads only persistent revision/identity facts and a
// synchronized volatile view. It neither stores an outbox nor calls etcd.
func (s *Service) NotificationRevisions(ctx context.Context) (map[string]string, error) {
	if s == nil || s.Pool == nil || !validID(s.StationID) {
		return nil, ErrUnavailable
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rollback(tx)
	rows, err := tx.Query(ctx, `SELECT o.organization_id::text,COALESCE(s.revision,0)
 FROM station.organizations o LEFT JOIN station.app_state_scopes s ON s.organization_id=o.organization_id AND s.station_id=$1
 ORDER BY o.organization_id LIMIT 1001`, s.StationID)
	if err != nil {
		return nil, ErrUnavailable
	}
	revisions := map[string]string{}
	for rows.Next() {
		var organization string
		var revision int64
		if rows.Scan(&organization, &revision) != nil {
			rows.Close()
			return nil, ErrUnavailable
		}
		revisions[organization] = strconv.FormatInt(revision, 10)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, ErrUnavailable
	}
	if len(revisions) > 1000 {
		return nil, ErrLimit
	}
	bindings, err := s.readBindings(ctx, tx, "")
	if err != nil {
		return nil, err
	}
	memory := s.memory()
	_, scopes, err := memory.Views(bindings)
	if err != nil {
		return nil, err
	}
	for organization, revision := range revisions {
		volatile := scopes[organization]
		if volatile == "" {
			volatile = memory.token(0)
		}
		revisions[organization] = revision + ":" + volatile
	}
	if tx.Commit(ctx) != nil {
		return nil, ErrUnavailable
	}
	return revisions, nil
}

type notificationSink interface {
	Publish(context.Context, string, Notification) error
}

// One Publisher belongs to one process. last is intentionally disposable; a
// restart republishes current revisions instead of recovering acknowledgments.
type Publisher struct {
	station string
	source  func(context.Context) (map[string]string, error)
	sink    notificationSink
	last    map[string]string
}

func (p *Publisher) step(ctx context.Context) error {
	revisions, err := p.source(ctx)
	if err != nil {
		return err
	}
	if p.last == nil {
		p.last = map[string]string{}
	}
	organizations := make([]string, 0, len(revisions))
	for organization := range revisions {
		organizations = append(organizations, organization)
	}
	sort.Strings(organizations)
	for _, organization := range organizations {
		revision := revisions[organization]
		if p.last[organization] == revision {
			continue
		}
		hint := Notification{SchemaVersion: 1, Revision: revision, ChangedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		if publishErr := p.sink.Publish(ctx, NotificationPrefix+p.station+"/"+organization, hint); publishErr != nil {
			err = ErrUnavailable
		} else {
			p.last[organization] = revision
		}
	}
	for organization := range p.last {
		if _, present := revisions[organization]; !present {
			delete(p.last, organization)
		}
	}
	return err
}

func (p *Publisher) Run(ctx context.Context, logger *slog.Logger) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for ctx.Err() == nil {
		step, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := p.step(step)
		cancel()
		if err != nil && ctx.Err() == nil && logger != nil {
			logger.Warn("app_state_notification_unavailable")
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

type etcdNotificationSink struct{ client *clientv3.Client }

func (s etcdNotificationSink) Publish(ctx context.Context, key string, value Notification) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return ErrUnavailable
	}
	lease, err := s.client.Grant(ctx, 120)
	if err != nil {
		return ErrUnavailable
	}
	// Failed/ambiguous writes are retried on the next pass; a possibly unused
	// lease expires itself. There is never an unleased fallback or a CAS.
	_, err = s.client.Put(ctx, key, string(raw), clientv3.WithLease(lease.ID))
	return err
}

// NewPublisher validates startup-only operator configuration without waiting
// for etcd availability. Neither endpoint nor credentials enter public output.
func NewPublisher(s *Service, lookup func(string) string) (*Publisher, func(), error) {
	noop := func() {}
	if s == nil || !validID(s.StationID) || lookup == nil {
		return nil, noop, ErrUnavailable
	}
	raw := lookup("STATION_APP_STATE_ETCD_ENDPOINTS")
	username, password := lookup("STATION_APP_STATE_ETCD_USERNAME"), lookup("STATION_APP_STATE_ETCD_PASSWORD")
	if raw == "" {
		if username != "" || password != "" {
			return nil, noop, ErrStreamFrame
		}
		return nil, noop, nil
	}
	endpoints := strings.Split(raw, ",")
	if len(endpoints) > 8 || (username == "") != (password == "") {
		return nil, noop, ErrStreamFrame
	}
	seen := map[string]bool{}
	for i, endpoint := range endpoints {
		endpoint = strings.TrimSpace(endpoint)
		u, err := url.Parse(endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || seen[endpoint] {
			return nil, noop, ErrStreamFrame
		}
		seen[endpoint], endpoints[i] = true, endpoint
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, Username: username, Password: password, DialTimeout: 3 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		return nil, noop, ErrUnavailable
	}
	return &Publisher{station: s.StationID, source: s.NotificationRevisions, sink: etcdNotificationSink{client}}, func() { _ = client.Close() }, nil
}
