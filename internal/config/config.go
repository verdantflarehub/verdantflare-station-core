package config

import (
	"errors"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Config struct {
	DatabaseURL, StationID, Listen, BootstrapToken, LogLevel string
	SessionTTL                                               time.Duration
}

func Load() (Config, error) {
	c := Config{DatabaseURL: os.Getenv("STATION_DATABASE_URL"), StationID: os.Getenv("STATION_ID"), Listen: os.Getenv("STATION_LISTEN_ADDR"), BootstrapToken: os.Getenv("STATION_BOOTSTRAP_TOKEN"), LogLevel: os.Getenv("STATION_LOG_LEVEL"), SessionTTL: 8 * time.Hour}
	u, e := url.Parse(c.DatabaseURL)
	if e != nil || u.Host == "" || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return c, errors.New("STATION_DATABASE_URL must be a PostgreSQL URL")
	}
	id, e := uuid.Parse(c.StationID)
	if e != nil || id.Version() != 7 || id.String() != c.StationID {
		return c, errors.New("STATION_ID must be a lowercase UUIDv7")
	}
	if c.Listen == "" {
		c.Listen = "127.0.0.1:5050"
	}
	host, port, e := net.SplitHostPort(c.Listen)
	if e != nil || host == "" {
		return c, errors.New("STATION_LISTEN_ADDR must specify a host and port")
	}
	n, e := strconv.Atoi(port)
	if e != nil || n < 1 || n > 65535 {
		return c, errors.New("STATION_LISTEN_ADDR has an invalid port")
	}
	if v := os.Getenv("STATION_CONTRACTS_MAJOR"); v != "" && v != "1" {
		return c, errors.New("STATION_CONTRACTS_MAJOR must be 1")
	}
	if v := os.Getenv("STATION_SESSION_TTL"); v != "" {
		c.SessionTTL, e = time.ParseDuration(v)
		if e != nil || c.SessionTTL < time.Minute || c.SessionTTL > 24*time.Hour {
			return c, errors.New("STATION_SESSION_TTL must be between 1m and 24h")
		}
	}
	if c.BootstrapToken != "" && len(c.BootstrapToken) < 32 {
		return c, errors.New("STATION_BOOTSTRAP_TOKEN must contain at least 32 bytes")
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if !strings.Contains("|debug|info|warn|error|", "|"+c.LogLevel+"|") {
		return c, errors.New("STATION_LOG_LEVEL is invalid")
	}
	return c, nil
}
