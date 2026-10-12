package appstate

import (
	"context"
	"errors"
	"testing"
)

type recordingHints struct {
	fail   bool
	values []Notification
	keys   []string
}

func (s *recordingHints) Publish(_ context.Context, key string, value Notification) error {
	if s.fail {
		return errors.New("unavailable")
	}
	s.keys = append(s.keys, key)
	s.values = append(s.values, value)
	return nil
}

func TestNotificationPublisherRetriesAndOpaqueTokens(t *testing.T) {
	sink := &recordingHints{}
	revision := "boot-z:10"
	readErr := false
	p := Publisher{station: "station", sink: sink, source: func(context.Context) (map[string]string, error) {
		if readErr {
			return nil, ErrUnavailable
		}
		return map[string]string{"organization": revision}, nil
	}}
	if p.step(t.Context()) != nil || len(sink.values) != 1 {
		t.Fatal("startup did not publish")
	}
	if p.step(t.Context()) != nil || len(sink.values) != 1 {
		t.Fatal("unchanged state republished")
	}
	sink.fail, revision = true, "boot-a:1"
	if p.step(t.Context()) == nil || p.last["organization"] != "boot-z:10" {
		t.Fatal("failed publish was acknowledged")
	}
	sink.fail = false
	if p.step(t.Context()) != nil || len(sink.values) != 2 || sink.values[1].Revision != revision {
		t.Fatal("opaque lower token was suppressed")
	}
	readErr = true
	if p.step(t.Context()) == nil || len(sink.values) != 2 {
		t.Fatal("source failure published invented data")
	}
	readErr = false
	restarted := Publisher{station: p.station, sink: sink, source: p.source}
	if restarted.step(t.Context()) != nil || len(sink.values) != 3 {
		t.Fatal("restart required durable acknowledgment")
	}
	if sink.keys[0] != NotificationPrefix+"station/organization" {
		t.Fatal("wrong notification namespace")
	}
}

func TestNotificationConfigurationRejectsUnsafeOrPartialValues(t *testing.T) {
	s, _, _, _, _ := memoryFixture(t, false)
	service := &Service{StationID: s.station}
	p, close, err := NewPublisher(service, func(string) string { return "" })
	close()
	if err != nil || p != nil {
		t.Fatal("absent configuration should disable publishing")
	}
	for _, cfg := range []map[string]string{
		{"STATION_APP_STATE_ETCD_PASSWORD": "test"},
		{"STATION_APP_STATE_ETCD_ENDPOINTS": "http://user:password@etcd.invalid:2379"},
		{"STATION_APP_STATE_ETCD_ENDPOINTS": "http://etcd.invalid:2379/prefix"},
		{"STATION_APP_STATE_ETCD_ENDPOINTS": "http://etcd.invalid:2379?scope=other"},
		{"STATION_APP_STATE_ETCD_ENDPOINTS": "http://etcd.invalid:2379,"},
		{"STATION_APP_STATE_ETCD_ENDPOINTS": "http://etcd.invalid:2379", "STATION_APP_STATE_ETCD_USERNAME": "test"},
	} {
		p, close, err := NewPublisher(service, func(key string) string { return cfg[key] })
		close()
		if err == nil || p != nil {
			t.Fatal("unsafe configuration accepted")
		}
	}
}
