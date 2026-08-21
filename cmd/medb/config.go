package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"time"
)

const (
	defaultListen          = "127.0.0.1:8080"
	defaultMaxDocSize      = 16 << 20
	defaultFlushBytes      = 64 << 20
	defaultFlushInterval   = 5 * time.Second
	defaultMaxIDSize       = 64 << 10
	defaultMaxRequestSize  = 17 << 20
	defaultShutdownTimeout = 10 * time.Second
)

type serveConfig struct {
	dir             string
	listen          string
	maxDocSize      int
	flushBytes      int64
	flushInterval   time.Duration
	maxIDSize       int
	maxRequestSize  int64
	shutdownTimeout time.Duration
}

type recoverConfig struct {
	dir  string
	name string
}

func parseServeConfig(args []string, stderr io.Writer, getenv envLookup) (serveConfig, error) {
	var cfg serveConfig
	fs := flag.NewFlagSet("medb serve", flag.ContinueOnError)
	fs.SetOutput(stderr)

	dirDefault := envString(getenv, "MEDB_DIR", "")
	listenDefault := envString(getenv, "MEDB_LISTEN", defaultListen)
	maxDocDefault := envString(getenv, "MEDB_MAX_DOC_SIZE", strconv.Itoa(defaultMaxDocSize))
	flushBytesDefault := envString(getenv, "MEDB_FLUSH_BYTES", strconv.FormatInt(defaultFlushBytes, 10))
	flushIntervalDefault := envString(getenv, "MEDB_FLUSH_INTERVAL", defaultFlushInterval.String())
	maxIDDefault := envString(getenv, "MEDB_MAX_ID_SIZE", strconv.Itoa(defaultMaxIDSize))
	maxRequestDefault := envString(getenv, "MEDB_MAX_REQUEST_SIZE", strconv.FormatInt(defaultMaxRequestSize, 10))
	shutdownDefault := envString(getenv, "MEDB_SHUTDOWN_TIMEOUT", defaultShutdownTimeout.String())

	var maxDoc, flushBytes, flushInterval, maxID, maxRequest, shutdown string
	fs.StringVar(&cfg.dir, "dir", dirDefault, "MeDB database directory")
	fs.StringVar(&cfg.listen, "listen", listenDefault, "HTTP listen address")
	fs.StringVar(&maxDoc, "max-doc-size", maxDocDefault, "maximum encoded document bytes")
	fs.StringVar(&flushBytes, "flush-bytes", flushBytesDefault, "WAL bytes which trigger a snapshot")
	fs.StringVar(&flushInterval, "flush-interval", flushIntervalDefault, "snapshot interval")
	fs.StringVar(&maxID, "max-id-size", maxIDDefault, "maximum UTF-8 encoded ID bytes")
	fs.StringVar(&maxRequest, "max-request-size", maxRequestDefault, "maximum request body bytes")
	fs.StringVar(&shutdown, "shutdown-timeout", shutdownDefault, "graceful shutdown timeout")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 {
		return cfg, fmt.Errorf("medb: unexpected arguments: %v", fs.Args())
	}
	if cfg.dir == "" {
		return cfg, errors.New("medb: --dir or MEDB_DIR is required")
	}
	if cfg.listen == "" {
		return cfg, errors.New("medb: listen address must not be empty")
	}

	var err error
	if cfg.maxDocSize, err = parsePositiveInt("max document size", maxDoc); err != nil {
		return cfg, err
	}
	if cfg.flushBytes, err = parsePositiveInt64("flush bytes", flushBytes); err != nil {
		return cfg, err
	}
	if cfg.flushInterval, err = parsePositiveDuration("flush interval", flushInterval); err != nil {
		return cfg, err
	}
	if cfg.maxIDSize, err = parsePositiveInt("max ID size", maxID); err != nil {
		return cfg, err
	}
	if cfg.maxRequestSize, err = parsePositiveInt64("max request size", maxRequest); err != nil {
		return cfg, err
	}
	if cfg.shutdownTimeout, err = parsePositiveDuration("shutdown timeout", shutdown); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func parseRecoverConfig(args []string, stderr io.Writer) (recoverConfig, error) {
	var cfg recoverConfig
	fs := flag.NewFlagSet("medb auth recover", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&cfg.dir, "dir", "", "MeDB database directory")
	fs.StringVar(&cfg.name, "name", "", "administrator display name")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 {
		return cfg, fmt.Errorf("medb: unexpected arguments: %v", fs.Args())
	}
	if cfg.dir == "" {
		return cfg, errors.New("medb: --dir is required")
	}
	if err := validateLabel(cfg.name); err != nil {
		return cfg, fmt.Errorf("medb: invalid administrator name: %w", err)
	}
	return cfg, nil
}

func envString(getenv envLookup, name, fallback string) string {
	if value, ok := getenv(name); ok && value != "" {
		return value
	}
	return fallback
}

func parsePositiveInt(name, value string) (int, error) {
	n, err := strconv.ParseInt(value, 10, 0)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("medb: %s must be a positive decimal byte count, got %q", name, value)
	}
	return int(n), nil
}

func parsePositiveInt64(name, value string) (int64, error) {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("medb: %s must be a positive decimal byte count, got %q", name, value)
	}
	return n, nil
}

func parsePositiveDuration(name, value string) (time.Duration, error) {
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("medb: %s must be a positive Go duration, got %q", name, value)
	}
	return d, nil
}
