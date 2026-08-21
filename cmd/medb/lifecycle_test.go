package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/antonmedv/medb"
)

// syncBuffer collects log output written from the server goroutine while the
// test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

var addressPattern = regexp.MustCompile(`address=(\S+)`)

func waitForAddress(t *testing.T, logs *syncBuffer, done <-chan error) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if match := addressPattern.FindStringSubmatch(logs.String()); match != nil {
			return strings.Trim(match[1], `"`)
		}
		select {
		case err := <-done:
			t.Fatalf("serve returned before listening: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("server never reported a listen address: %q", logs.String())
	return ""
}

func TestServeInitializesListensAndShutsDown(t *testing.T) {
	dir := t.TempDir()
	token, err := newToken()
	if err != nil {
		t.Fatal(err)
	}
	cfg := testServeConfig(dir)
	logs := &syncBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, cfg, newLogger(logs), envMap{"MEDB_INIT_ADMIN_TOKEN": token}.lookup)
	}()
	address := waitForAddress(t, logs, done)

	response, err := http.Get("http://" + address + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("health: status %d", response.StatusCode)
	}

	request, err := http.NewRequest(http.MethodGet, "http://"+address+"/v1/collections", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("collections: status %d", response.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("serve did not return after cancellation")
	}

	// A clean shutdown releases the database lock.
	db, err := medb.Open(dir)
	if err != nil {
		t.Fatalf("directory still locked after shutdown: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestServeFailures(t *testing.T) {
	t.Run("uninitialized without a token", func(t *testing.T) {
		cfg := testServeConfig(t.TempDir())
		err := serve(context.Background(), cfg, newLogger(io.Discard), envMap{}.lookup)
		if err == nil || !strings.Contains(err.Error(), "MEDB_INIT_ADMIN_TOKEN") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("unusable listen address", func(t *testing.T) {
		dir := t.TempDir()
		token, err := newToken()
		if err != nil {
			t.Fatal(err)
		}
		cfg := testServeConfig(dir)
		cfg.listen = "256.256.256.256:8080"
		err = serve(context.Background(), cfg, newLogger(io.Discard),
			envMap{"MEDB_INIT_ADMIN_TOKEN": token}.lookup)
		if err == nil || !strings.Contains(err.Error(), "listen on") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("directory already locked", func(t *testing.T) {
		dir := t.TempDir()
		db, err := medb.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		cfg := testServeConfig(dir)
		if err := serve(context.Background(), cfg, newLogger(io.Discard), envMap{}.lookup); err == nil {
			t.Fatal("serve opened a locked directory")
		}
	})
}

func TestRunCommandDispatch(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "no command", wantErr: "command required"},
		{name: "unknown command", args: []string{"frobnicate"}, wantErr: "unknown command"},
		{name: "token without subcommand", args: []string{"token"}, wantErr: "medb token generate"},
		{name: "token with wrong subcommand", args: []string{"token", "revoke"}, wantErr: "medb token generate"},
		{name: "auth without subcommand", args: []string{"auth"}, wantErr: "medb auth recover"},
		{name: "serve without a directory", args: []string{"serve"}, wantErr: "--dir or MEDB_DIR is required"},
		{name: "recover without a directory", args: []string{"auth", "recover"}, wantErr: "--dir is required"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(context.Background(), test.args, &stdout, &stderr, envMap{}.lookup)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("err = %v, want %q", err, test.wantErr)
			}
			if stdout.Len() != 0 {
				t.Fatalf("wrote %q to standard output", stdout.String())
			}
		})
	}

	// Usage goes to standard error, never standard output.
	var stdout, stderr bytes.Buffer
	_ = run(context.Background(), []string{"frobnicate"}, &stdout, &stderr, envMap{}.lookup)
	if !strings.Contains(stderr.String(), "medb serve [options]") {
		t.Fatalf("usage missing from standard error: %q", stderr.String())
	}

	// -h is reported as flag.ErrHelp so main can exit quietly.
	stderr.Reset()
	err := run(context.Background(), []string{"serve", "-h"}, &stdout, &stderr, envMap{}.lookup)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("serve -h returned %v", err)
	}
}

func TestRunAuthRecover(t *testing.T) {
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(),
		[]string{"auth", "recover", "--dir", dir, "--name", "operator"},
		&stdout, &stderr, envMap{}.lookup); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "offline administrator credential") {
		t.Fatalf("missing recovery warning: %q", stderr.String())
	}
	fields := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(stdout.String()), "\n") {
		key, value, _ := strings.Cut(line, "=")
		fields[key] = value
	}
	if !validToken(fields["token"]) {
		t.Fatalf("invalid recovery output: %q", stdout.String())
	}

	// The recovered credential is enough for the server to start.
	db, err := medb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := initializeAuth(db, newAuthStore(db), envMap{}.lookup, newLogger(io.Discard)); err != nil {
		t.Fatalf("recovered database does not start: %v", err)
	}
	actor, ok := newAuthStore(db).authenticate(fields["token"], time.Now())
	if !ok || actor != roleAdmin {
		t.Fatalf("recovery token grants %q, ok = %v", actor, ok)
	}
}

func TestRecoverConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "no directory", args: nil, wantErr: "--dir is required"},
		{name: "no name", args: []string{"--dir", "/data"}, wantErr: "invalid administrator name"},
		{name: "extra argument", args: []string{"--dir", "/data", "--name", "a", "extra"}, wantErr: "unexpected arguments"},
		{name: "unknown flag", args: []string{"--dir", "/data", "--bogus"}, wantErr: "flag provided but not defined"},
		{name: "oversized name", args: []string{"--dir", "/data", "--name", strings.Repeat("n", 257)}, wantErr: "invalid administrator name"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseRecoverConfig(test.args, io.Discard)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("err = %v, want %q", err, test.wantErr)
			}
		})
	}
	cfg, err := parseRecoverConfig([]string{"--dir", "/data", "--name", "operator"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.dir != "/data" || cfg.name != "operator" {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestServeConfigRejectsIncoherentSizes(t *testing.T) {
	_, err := parseServeConfig([]string{
		"--dir", "/data", "--max-doc-size", "1000", "--max-request-size", "1000",
	}, io.Discard, envMap{}.lookup)
	if err == nil || !strings.Contains(err.Error(), "must exceed max document size") {
		t.Fatalf("err = %v", err)
	}

	// The documented defaults are coherent.
	cfg, err := parseServeConfig([]string{"--dir", "/data"}, io.Discard, envMap{}.lookup)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.maxRequestSize <= int64(cfg.maxDocSize) {
		t.Fatalf("default request size %d does not exceed document size %d",
			cfg.maxRequestSize, cfg.maxDocSize)
	}

	_, err = parseServeConfig([]string{"--dir", "/data", "extra"}, io.Discard, envMap{}.lookup)
	if err == nil || !strings.Contains(err.Error(), "unexpected arguments") {
		t.Fatalf("err = %v", err)
	}
	_, err = parseServeConfig([]string{"--dir", "/data", "--listen", ""}, io.Discard, envMap{}.lookup)
	if err == nil || !strings.Contains(err.Error(), "listen address") {
		t.Fatalf("err = %v", err)
	}
	for _, bad := range [][]string{
		{"--flush-bytes", "0"},
		{"--flush-interval", "nope"},
		{"--max-id-size", "-1"},
		{"--shutdown-timeout", "0s"},
	} {
		args := append([]string{"--dir", "/data"}, bad...)
		if _, err := parseServeConfig(args, io.Discard, envMap{}.lookup); err == nil {
			t.Fatalf("%v was accepted", bad)
		}
	}
}

func TestMutationErrorClassification(t *testing.T) {
	api, _, _ := newTestAPI(t)
	var logs bytes.Buffer
	api.log = newLogger(&logs)

	recorder := httptest.NewRecorder()
	api.mutationError(recorder, nil)
	if recorder.Body.Len() != 0 {
		t.Fatalf("nil error wrote %q", recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	api.mutationError(recorder, fmt.Errorf("wrapped: %w", medb.ErrTooLarge))
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("too large: status %d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	api.mutationError(recorder, fmt.Errorf("wrapped: %w", medb.ErrClosed))
	if recorder.Code != http.StatusServiceUnavailable || !api.unavailable.Load() {
		t.Fatalf("closed: status %d, unavailable %v", recorder.Code, api.unavailable.Load())
	}

	api.unavailable.Store(false)
	recorder = httptest.NewRecorder()
	storage := errors.New("no space left on device")
	api.mutationError(recorder, storage)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("storage: status %d", recorder.Code)
	}
	if !api.unavailable.Load() {
		t.Fatal("storage failure did not mark the server unavailable")
	}
	if !strings.Contains(logs.String(), "storage failure") {
		t.Fatalf("storage failure was not logged: %q", logs.String())
	}
	select {
	case err := <-api.failureCh:
		if !errors.Is(err, storage) {
			t.Fatalf("failure channel carried %v", err)
		}
	default:
		t.Fatal("storage failure was not reported to the shutdown path")
	}

	// Only the first failure is reported; later ones must not block.
	api.mutationError(httptest.NewRecorder(), errors.New("another failure"))
}

// A closed database makes data endpoints report unavailable rather than a
// storage fault.
func TestClosedDatabaseReportsUnavailable(t *testing.T) {
	dir := t.TempDir()
	db, err := medb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testServeConfig(dir)
	cfg.noAuth = true
	api := newAPIServer(db, cfg, newLogger(io.Discard))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	response := callRaw(t, api, "", http.MethodPost, "/v1/set",
		`{"collection":"docs","id":"x","document":1}`)
	if response.Code != http.StatusServiceUnavailable || responseCode(t, response) != "unavailable" {
		t.Fatalf("set on a closed database: status %d, body %s", response.Code, response.Body.String())
	}
	response = callRaw(t, api, "", http.MethodPost, "/v1/get",
		`{"collection":"docs","id":"x"}`)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("get on a closed database: status %d, body %s", response.Code, response.Body.String())
	}
}

func TestInitialTokenSources(t *testing.T) {
	token, err := newToken()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := dir + "/token"

	if _, err := initialToken(envMap{}.lookup); err == nil ||
		!strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("err = %v", err)
	}
	if _, err := initialToken(envMap{
		"MEDB_INIT_ADMIN_TOKEN": token, "MEDB_INIT_ADMIN_TOKEN_FILE": path,
	}.lookup); err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("err = %v", err)
	}
	if _, err := initialToken(envMap{"MEDB_INIT_ADMIN_TOKEN": "short"}.lookup); err == nil ||
		!strings.Contains(err.Error(), "invalid format") {
		t.Fatalf("err = %v", err)
	}
	if _, err := initialToken(envMap{"MEDB_INIT_ADMIN_TOKEN_FILE": path}.lookup); err == nil ||
		!strings.Contains(err.Error(), "read MEDB_INIT_ADMIN_TOKEN_FILE") {
		t.Fatalf("err = %v", err)
	}

	// One trailing LF or CRLF is ignored; anything else is part of the token.
	for _, ending := range []string{"", "\n", "\r\n"} {
		if err := os.WriteFile(path, []byte(token+ending), 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := initialToken(envMap{"MEDB_INIT_ADMIN_TOKEN_FILE": path}.lookup)
		if err != nil || got != token {
			t.Fatalf("ending %q: got %q, err %v", ending, got, err)
		}
	}
	if err := os.WriteFile(path, []byte(" "+token), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := initialToken(envMap{"MEDB_INIT_ADMIN_TOKEN_FILE": path}.lookup); err == nil {
		t.Fatal("leading whitespace was accepted")
	}
}
