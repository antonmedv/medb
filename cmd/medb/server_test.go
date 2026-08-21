package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/antonmedv/medb"
)

type envMap map[string]string

func (e envMap) lookup(name string) (string, bool) {
	value, ok := e[name]
	return value, ok
}

func testServeConfig(dir string) serveConfig {
	return serveConfig{
		dir:             dir,
		listen:          "127.0.0.1:0",
		maxDocSize:      defaultMaxDocSize,
		flushBytes:      defaultFlushBytes,
		flushInterval:   defaultFlushInterval,
		maxIDSize:       defaultMaxIDSize,
		maxRequestSize:  defaultMaxRequestSize,
		shutdownTimeout: defaultShutdownTimeout,
	}
}

func newTestAPI(t *testing.T) (*apiServer, *medb.DB, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := medb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil && !errors.Is(err, medb.ErrClosed) {
			t.Errorf("close database: %v", err)
		}
	})
	token, err := newToken()
	if err != nil {
		t.Fatal(err)
	}
	env := envMap{
		"MEDB_INIT_ADMIN_NAME":  "root",
		"MEDB_INIT_ADMIN_TOKEN": token,
	}
	auth := newAuthStore(db)
	if err := initializeAuth(db, auth, env.lookup, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	return newAPIServer(db, testServeConfig(dir)), db, token
}

func callRaw(t *testing.T, api http.Handler, token, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" && method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	return response
}

func callJSON(t *testing.T, api http.Handler, token, method, path string, value any) *httptest.ResponseRecorder {
	t.Helper()
	body := ""
	if value != nil {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		body = string(encoded)
	}
	return callRaw(t, api, token, method, path, body)
}

func responseCode(t *testing.T, response *httptest.ResponseRecorder) string {
	t.Helper()
	var envelope errorEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error response %q: %v", response.Body.String(), err)
	}
	return envelope.Error.Code
}

func TestDocumentAPIWithNamespacedCollectionAndArbitraryID(t *testing.T) {
	api, _, token := newTestAPI(t)
	collection := "prod/eu/users"
	id := "a/b?#\x00\n雪"
	document := map[string]any{"name": "Ada", "active": true}

	response := callJSON(t, api, token, http.MethodPost, "/v1/set", map[string]any{
		"collection": collection, "id": id, "document": document,
	})
	if response.Code != http.StatusNoContent {
		t.Fatalf("set: status %d, body %s", response.Code, response.Body.String())
	}

	response = callJSON(t, api, token, http.MethodPost, "/v1/get", map[string]any{
		"collection": collection, "id": id,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("get: status %d, body %s", response.Code, response.Body.String())
	}
	var got struct {
		Document map[string]any `json:"document"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Document["name"] != "Ada" || got.Document["active"] != true {
		t.Fatalf("unexpected document: %#v", got.Document)
	}

	response = callJSON(t, api, token, http.MethodPost, "/v1/has", map[string]any{
		"collection": collection, "id": id,
	})
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"exists":true`) {
		t.Fatalf("has: status %d, body %s", response.Code, response.Body.String())
	}

	response = callJSON(t, api, token, http.MethodPost, "/v1/count", map[string]any{"collection": collection})
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"count":1`) {
		t.Fatalf("count: status %d, body %s", response.Code, response.Body.String())
	}

	response = callJSON(t, api, token, http.MethodPost, "/v1/scan", map[string]any{"collection": collection})
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"id":"a/b?#\u0000\n雪"`) {
		t.Fatalf("scan: status %d, body %s", response.Code, response.Body.String())
	}
	if mediaType := response.Header().Get("Content-Type"); !strings.HasPrefix(mediaType, "application/x-ndjson") {
		t.Fatalf("scan Content-Type = %q", mediaType)
	}

	response = callJSON(t, api, token, http.MethodPost, "/v1/delete", map[string]any{
		"collection": collection, "id": id,
	})
	if response.Code != http.StatusNoContent {
		t.Fatalf("delete: status %d, body %s", response.Code, response.Body.String())
	}
	response = callJSON(t, api, token, http.MethodPost, "/v1/get", map[string]any{
		"collection": collection, "id": id,
	})
	if response.Code != http.StatusNotFound || responseCode(t, response) != "not_found" {
		t.Fatalf("missing get: status %d, body %s", response.Code, response.Body.String())
	}
}

func TestCollectionsHideMetadataAndNullDocument(t *testing.T) {
	api, _, token := newTestAPI(t)
	response := callRaw(t, api, token, http.MethodPost, "/v1/set", `{"collection":"docs","id":"","document":null}`)
	if response.Code != http.StatusNoContent {
		t.Fatalf("set null: status %d, body %s", response.Code, response.Body.String())
	}
	response = callJSON(t, api, token, http.MethodPost, "/v1/get", map[string]any{"collection": "docs", "id": ""})
	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != `{"document":null}` {
		t.Fatalf("get null: status %d, body %q", response.Code, response.Body.String())
	}
	response = callJSON(t, api, token, http.MethodGet, "/v1/collections", nil)
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "_meta") {
		t.Fatalf("collections: status %d, body %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"docs"`) {
		t.Fatalf("collections omitted docs: %s", response.Body.String())
	}
	response = callJSON(t, api, token, http.MethodPost, "/v1/get", map[string]any{"collection": "_meta/users", "id": "x"})
	if response.Code != http.StatusForbidden || responseCode(t, response) != "reserved_collection" {
		t.Fatalf("reserved collection: status %d, body %s", response.Code, response.Body.String())
	}
}

func TestNoAuthModeOpensDataRoutesAndHidesAuthManagement(t *testing.T) {
	dir := t.TempDir()
	db, err := medb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil && !errors.Is(err, medb.ErrClosed) {
			t.Errorf("close database: %v", err)
		}
	})
	cfg := testServeConfig(dir)
	cfg.noAuth = true
	var stderr bytes.Buffer
	if err := prepareAuthentication(db, cfg, envMap{
		"MEDB_INIT_ADMIN_TOKEN_FILE": "/missing/secret",
	}.lookup, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "authentication is disabled") {
		t.Fatalf("missing no-auth warning: %q", stderr.String())
	}
	if _, err := newAuthStore(db).state.Get(stateID); !errors.Is(err, medb.ErrNotFound) {
		t.Fatalf("no-auth mode initialized authentication state: %v", err)
	}

	api := newAPIServer(db, cfg)
	response := callRaw(t, api, "not-a-token", http.MethodPost, "/v1/set",
		`{"collection":"docs","id":"open/id","document":true}`)
	if response.Code != http.StatusNoContent {
		t.Fatalf("unauthenticated set: status %d, body %s", response.Code, response.Body.String())
	}
	response = callJSON(t, api, "", http.MethodPost, "/v1/drop", map[string]any{"collection": "docs"})
	if response.Code != http.StatusNoContent {
		t.Fatalf("unauthenticated drop: status %d, body %s", response.Code, response.Body.String())
	}
	response = callJSON(t, api, "", http.MethodGet, "/v1/auth/users", nil)
	if response.Code != http.StatusNotFound || responseCode(t, response) != "route_not_found" {
		t.Fatalf("auth route: status %d, body %s", response.Code, response.Body.String())
	}
	response = callJSON(t, api, "", http.MethodPost, "/v1/get", map[string]any{
		"collection": "_meta/users", "id": "x",
	})
	if response.Code != http.StatusForbidden || responseCode(t, response) != "reserved_collection" {
		t.Fatalf("reserved collection: status %d, body %s", response.Code, response.Body.String())
	}
}

func TestAuthenticationRolesAndRevocation(t *testing.T) {
	api, _, adminToken := newTestAPI(t)
	response := callJSON(t, api, adminToken, http.MethodPost, "/v1/auth/users/create", map[string]any{
		"name": "reader", "role": "reader",
	})
	if response.Code != http.StatusCreated {
		t.Fatalf("create user: status %d, body %s", response.Code, response.Body.String())
	}
	var createdUser struct {
		User userView `json:"user"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &createdUser); err != nil {
		t.Fatal(err)
	}

	response = callJSON(t, api, adminToken, http.MethodPost, "/v1/auth/tokens/create", map[string]any{
		"user_id": createdUser.User.ID, "label": "test client",
	})
	if response.Code != http.StatusCreated {
		t.Fatalf("create token: status %d, body %s", response.Code, response.Body.String())
	}
	var createdToken struct {
		TokenID string `json:"token_id"`
		Token   string `json:"token"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &createdToken); err != nil {
		t.Fatal(err)
	}
	if !validToken(createdToken.Token) {
		t.Fatalf("invalid returned token %q", createdToken.Token)
	}

	response = callJSON(t, api, createdToken.Token, http.MethodGet, "/v1/collections", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("reader list: status %d, body %s", response.Code, response.Body.String())
	}
	response = callJSON(t, api, createdToken.Token, http.MethodPost, "/v1/set", map[string]any{
		"collection": "docs", "id": "one", "document": 1,
	})
	if response.Code != http.StatusForbidden || responseCode(t, response) != "forbidden" {
		t.Fatalf("reader write: status %d, body %s", response.Code, response.Body.String())
	}

	response = callJSON(t, api, adminToken, http.MethodPost, "/v1/auth/users/update", map[string]any{
		"user_id": createdUser.User.ID, "role": "writer",
	})
	if response.Code != http.StatusOK {
		t.Fatalf("promote user: status %d, body %s", response.Code, response.Body.String())
	}
	response = callJSON(t, api, createdToken.Token, http.MethodPost, "/v1/set", map[string]any{
		"collection": "docs", "id": "one", "document": 1,
	})
	if response.Code != http.StatusNoContent {
		t.Fatalf("writer write: status %d, body %s", response.Code, response.Body.String())
	}

	response = callJSON(t, api, adminToken, http.MethodPost, "/v1/auth/tokens/revoke", map[string]any{
		"token_id": createdToken.TokenID,
	})
	if response.Code != http.StatusNoContent {
		t.Fatalf("revoke: status %d, body %s", response.Code, response.Body.String())
	}
	response = callJSON(t, api, createdToken.Token, http.MethodGet, "/v1/collections", nil)
	if response.Code != http.StatusUnauthorized || responseCode(t, response) != "invalid_token" {
		t.Fatalf("revoked token: status %d, body %s", response.Code, response.Body.String())
	}
}

func TestStrictProtocolErrors(t *testing.T) {
	api, _, token := newTestAPI(t)
	tests := []struct {
		name   string
		token  string
		method string
		path   string
		body   string
		status int
		code   string
	}{
		{name: "missing auth", method: http.MethodGet, path: "/v1/collections", status: 401, code: "authentication_required"},
		{name: "unknown route authenticated", token: token, method: http.MethodGet, path: "/v1/nope", status: 404, code: "route_not_found"},
		{name: "wrong method", token: token, method: http.MethodGet, path: "/v1/get", status: 405, code: "method_not_allowed"},
		{name: "query rejected", token: token, method: http.MethodGet, path: "/v1/collections?x=1", status: 400, code: "invalid_request"},
		{name: "duplicate field", token: token, method: http.MethodPost, path: "/v1/get", body: `{"collection":"a","collection":"b","id":"x"}`, status: 400, code: "invalid_request"},
		{name: "unknown field", token: token, method: http.MethodPost, path: "/v1/get", body: `{"collection":"a","id":"x","extra":true}`, status: 400, code: "invalid_request"},
		{name: "lone surrogate", token: token, method: http.MethodPost, path: "/v1/get", body: `{"collection":"a","id":"\ud800"}`, status: 400, code: "invalid_json"},
		{name: "invalid collection", token: token, method: http.MethodPost, path: "/v1/get", body: `{"collection":"Bad","id":"x"}`, status: 400, code: "invalid_collection"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := callRaw(t, api, test.token, test.method, test.path, test.body)
			if response.Code != test.status || responseCode(t, response) != test.code {
				t.Fatalf("status %d, code %q, body %s", response.Code, responseCode(t, response), response.Body.String())
			}
		})
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/get", strings.NewReader(`{"collection":"a","id":"x"}`))
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusUnsupportedMediaType || responseCode(t, response) != "unsupported_media_type" {
		t.Fatalf("missing media type: status %d, body %s", response.Code, response.Body.String())
	}
}

func TestInitializationFromSecretFileAndIgnoredAfterward(t *testing.T) {
	dir := t.TempDir()
	token, err := newToken()
	if err != nil {
		t.Fatal(err)
	}
	secretPath := dir + "/admin-token"
	if err := os.WriteFile(secretPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := medb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	env := envMap{
		"MEDB_INIT_ADMIN_NAME":       "container-admin",
		"MEDB_INIT_ADMIN_TOKEN_FILE": secretPath,
	}
	if err := initializeAuth(db, newAuthStore(db), env.lookup, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(secretPath); err != nil {
		t.Fatal(err)
	}

	db, err = medb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var stderr bytes.Buffer
	if err := initializeAuth(db, newAuthStore(db), env.lookup, &stderr); err != nil {
		t.Fatalf("initialized database read removed secret: %v", err)
	}
	if !strings.Contains(stderr.String(), "ignored") {
		t.Fatalf("missing ignored initialization diagnostic: %q", stderr.String())
	}
	if _, ok := newAuthStore(db).authenticate(token, time.Now()); !ok {
		t.Fatal("initial token does not authenticate")
	}
}

func TestInitializationResumesAfterTokenWrite(t *testing.T) {
	dir := t.TempDir()
	db, err := medb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	auth := newAuthStore(db)
	token, err := newToken()
	if err != nil {
		t.Fatal(err)
	}
	uid := medb.NewID()
	if err := auth.tokens.Set(tokenID(token), tokenRecord{
		UserID: uid, Label: "initial", CreatedAt: nowTimestamp(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = medb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	env := envMap{"MEDB_INIT_ADMIN_NAME": "resumed", "MEDB_INIT_ADMIN_TOKEN": token}
	auth = newAuthStore(db)
	if err := initializeAuth(db, auth, env.lookup, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	state, err := auth.state.Get(stateID)
	if err != nil || validateStateRecord(state) != nil {
		t.Fatalf("state = %#v, %v", state, err)
	}
	if _, ok := auth.authenticate(token, time.Now()); !ok {
		t.Fatal("resumed token does not authenticate")
	}
}

func TestTokenGenerateAndOfflineRecovery(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"token", "generate"}, &stdout, &stderr, envMap{}.lookup); err != nil {
		t.Fatal(err)
	}
	if token := strings.TrimSpace(stdout.String()); !validToken(token) {
		t.Fatalf("generated invalid token %q", token)
	}

	dir := t.TempDir()
	stdout.Reset()
	stderr.Reset()
	if err := recoverAuth(recoverConfig{dir: dir, name: "recovered"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(stdout.String()), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("invalid recovery output line %q", line)
		}
		values[key] = value
	}
	if !validToken(values["token"]) || !validHexID(values["user_id"], 32) || !validHexID(values["token_id"], 64) {
		t.Fatalf("invalid recovery output: %v", values)
	}
	db, err := medb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, ok := newAuthStore(db).authenticate(values["token"], time.Now()); !ok {
		t.Fatal("recovery token does not authenticate")
	}
}

func TestServeConfigEnvironmentAndFlagPrecedence(t *testing.T) {
	env := envMap{
		"MEDB_DIR":              "/env/data",
		"MEDB_LISTEN":           "127.0.0.1:9000",
		"MEDB_NO_AUTH":          "true",
		"MEDB_MAX_DOC_SIZE":     "bad",
		"MEDB_FLUSH_INTERVAL":   "2s",
		"MEDB_MAX_REQUEST_SIZE": "99",
	}
	cfg, err := parseServeConfig([]string{"--dir", "/flag/data", "--max-doc-size", "123"}, &bytes.Buffer{}, env.lookup)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.dir != "/flag/data" || cfg.listen != "127.0.0.1:9000" || !cfg.noAuth || cfg.maxDocSize != 123 || cfg.flushInterval != 2*time.Second || cfg.maxRequestSize != 99 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	overridden, err := parseServeConfig([]string{
		"--dir", "/flag/data", "--max-doc-size", "123", "--no-auth=false",
	}, &bytes.Buffer{}, env.lookup)
	if err != nil {
		t.Fatal(err)
	}
	if overridden.noAuth {
		t.Fatal("--no-auth=false did not override MEDB_NO_AUTH=true")
	}
	enabled, err := parseServeConfig([]string{"--dir", "/flag/data", "--no-auth"}, &bytes.Buffer{}, envMap{}.lookup)
	if err != nil {
		t.Fatal(err)
	}
	if !enabled.noAuth {
		t.Fatal("bare --no-auth did not enable unauthenticated mode")
	}

	_, err = parseServeConfig(nil, &bytes.Buffer{}, env.lookup)
	if err == nil || !strings.Contains(err.Error(), "max document size") {
		t.Fatalf("invalid environment error = %v", err)
	}
	_, err = parseServeConfig(nil, &bytes.Buffer{}, envMap{
		"MEDB_DIR": "/env/data", "MEDB_NO_AUTH": "sometimes",
	}.lookup)
	if err == nil || !strings.Contains(err.Error(), "Go boolean") {
		t.Fatalf("invalid no-auth environment error = %v", err)
	}
}

func TestRequestSizeLimit(t *testing.T) {
	api, _, token := newTestAPI(t)
	api.cfg.maxRequestSize = 32
	response := callRaw(t, api, token, http.MethodPost, "/v1/set", fmt.Sprintf(
		`{"collection":"docs","id":"x","document":%q}`, strings.Repeat("x", 100),
	))
	if response.Code != http.StatusRequestEntityTooLarge || responseCode(t, response) != "request_too_large" {
		t.Fatalf("status %d, body %s", response.Code, response.Body.String())
	}
}
