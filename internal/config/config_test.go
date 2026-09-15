package config

import (
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestConfig(t *testing.T) {
	for _, key := range []string{"STATION_DATABASE_URL", "STATION_ID", "STATION_LISTEN_ADDR", "STATION_BOOTSTRAP_TOKEN", "STATION_LOG_LEVEL", "STATION_SESSION_TTL", "STATION_CONTRACTS_MAJOR"} {
		t.Setenv(key, "")
	}
	t.Setenv("STATION_DATABASE_URL", "postgres://localhost/station")
	t.Setenv("STATION_ID", uuid.Must(uuid.NewV7()).String())
	c, err := Load()
	if err != nil || c.SessionTTL != 8*time.Hour || c.Listen != "127.0.0.1:5050" {
		t.Fatal("defaults invalid")
	}
	for _, tc := range []struct{ key, value string }{{"STATION_DATABASE_URL", "https://localhost/db"}, {"STATION_ID", uuid.NewString()}, {"STATION_LISTEN_ADDR", ":0"}, {"STATION_SESSION_TTL", "25h"}, {"STATION_BOOTSTRAP_TOKEN", "short"}, {"STATION_LOG_LEVEL", "trace"}, {"STATION_CONTRACTS_MAJOR", "2"}} {
		t.Run(tc.key, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil {
				t.Fatal("invalid setting accepted")
			}
		})
	}
}
