package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/antonmedv/medb"
)

// A JSON null decodes into a Go string or bool without an error, so every
// required member has to reject it rather than silently accept the zero value.
func TestNullMembersAreRejected(t *testing.T) {
	api, _, token := newTestAPI(t)
	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "null id", path: "/v1/set", body: `{"collection":"docs","id":null,"document":42}`},
		{name: "null collection", path: "/v1/get", body: `{"collection":null,"id":"x"}`},
		{name: "null id on get", path: "/v1/get", body: `{"collection":"docs","id":null}`},
		{name: "null id on delete", path: "/v1/delete", body: `{"collection":"docs","id":null}`},
		{name: "null collection on count", path: "/v1/count", body: `{"collection":null}`},
		{name: "null name", path: "/v1/auth/users/create", body: `{"name":null,"role":"reader"}`},
		{name: "null role", path: "/v1/auth/users/create", body: `{"name":"x","role":null}`},
		{name: "null user_id", path: "/v1/auth/tokens/list", body: `{"user_id":null}`},
		{name: "null token_id", path: "/v1/auth/tokens/revoke", body: `{"token_id":null}`},
		{
			name: "null disabled",
			path: "/v1/auth/users/update",
			body: `{"user_id":"00000000000000000000000000000000","disabled":null}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := callRaw(t, api, token, http.MethodPost, test.path, test.body)
			if response.Code != http.StatusBadRequest || responseCode(t, response) != "invalid_request" {
				t.Fatalf("status %d, body %s", response.Code, response.Body.String())
			}
		})
	}

	// The rejected set must not have written anything under the empty ID.
	response := callJSON(t, api, token, http.MethodPost, "/v1/has", map[string]any{
		"collection": "docs", "id": "",
	})
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"exists":false`) {
		t.Fatalf("rejected null id still wrote a document: %s", response.Body.String())
	}
}

// A null "disabled" is rejected, but a present boolean still toggles the flag
// and an omitted member still leaves it alone.
func TestUserUpdateDisabledFlag(t *testing.T) {
	api, _, token := newTestAPI(t)
	response := callJSON(t, api, token, http.MethodPost, "/v1/auth/users/create", map[string]any{
		"name": "worker", "role": "writer",
	})
	if response.Code != http.StatusCreated {
		t.Fatalf("create user: status %d, body %s", response.Code, response.Body.String())
	}
	var created struct {
		User userView `json:"user"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	for _, disabled := range []bool{true, false} {
		response = callJSON(t, api, token, http.MethodPost, "/v1/auth/users/update", map[string]any{
			"user_id": created.User.ID, "disabled": disabled,
		})
		if response.Code != http.StatusOK {
			t.Fatalf("set disabled=%v: status %d, body %s", disabled, response.Code, response.Body.String())
		}
		var updated struct {
			User userView `json:"user"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &updated); err != nil {
			t.Fatal(err)
		}
		if updated.User.Disabled != disabled {
			t.Fatalf("disabled = %v, want %v", updated.User.Disabled, disabled)
		}
		if updated.User.Role != roleWriter {
			t.Fatalf("omitted role changed to %q", updated.User.Role)
		}
	}

	// A null "disabled" must not re-enable a disabled user.
	response = callJSON(t, api, token, http.MethodPost, "/v1/auth/users/update", map[string]any{
		"user_id": created.User.ID, "disabled": true,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("disable: status %d, body %s", response.Code, response.Body.String())
	}
	response = callRaw(t, api, token, http.MethodPost, "/v1/auth/users/update",
		`{"user_id":"`+created.User.ID+`","disabled":null}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("null disabled: status %d, body %s", response.Code, response.Body.String())
	}
	response = callJSON(t, api, token, http.MethodPost, "/v1/auth/tokens/create", map[string]any{
		"user_id": created.User.ID, "label": "for a disabled user",
	})
	if response.Code != http.StatusConflict || responseCode(t, response) != "user_disabled" {
		t.Fatalf("null disabled re-enabled the user: status %d, body %s", response.Code, response.Body.String())
	}
}

// expires_at is the one member where the specification allows an explicit null.
func TestTokenExpiry(t *testing.T) {
	api, _, token := newTestAPI(t)
	response := callJSON(t, api, token, http.MethodPost, "/v1/auth/users/create", map[string]any{
		"name": "expiring", "role": "reader",
	})
	if response.Code != http.StatusCreated {
		t.Fatalf("create user: status %d, body %s", response.Code, response.Body.String())
	}
	var created struct {
		User userView `json:"user"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	response = callRaw(t, api, token, http.MethodPost, "/v1/auth/tokens/create",
		`{"user_id":"`+created.User.ID+`","label":"no expiry","expires_at":null}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("null expires_at: status %d, body %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"expires_at":null`) {
		t.Fatalf("null expires_at not preserved: %s", response.Body.String())
	}

	for _, value := range []string{
		"2020-01-01T00:00:00Z",      // already past
		"2999-01-01T00:00:00+01:00", // not UTC
		"not a timestamp",           //
	} {
		response = callJSON(t, api, token, http.MethodPost, "/v1/auth/tokens/create", map[string]any{
			"user_id": created.User.ID, "label": "bad expiry", "expires_at": value,
		})
		if response.Code != http.StatusBadRequest || responseCode(t, response) != "invalid_request" {
			t.Fatalf("expires_at %q: status %d, body %s", value, response.Code, response.Body.String())
		}
	}

	response = callJSON(t, api, token, http.MethodPost, "/v1/auth/tokens/create", map[string]any{
		"user_id": created.User.ID, "label": "future", "expires_at": "2999-01-01T00:00:00Z",
	})
	if response.Code != http.StatusCreated {
		t.Fatalf("future expires_at: status %d, body %s", response.Code, response.Body.String())
	}
	var future struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &future); err != nil {
		t.Fatal(err)
	}
	response = callJSON(t, api, future.Token, http.MethodGet, "/v1/collections", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("unexpired token: status %d, body %s", response.Code, response.Body.String())
	}
}

func TestHealthEndpoint(t *testing.T) {
	api, _, _ := newTestAPI(t)

	// /healthz is the only route which needs no credential.
	response := callRaw(t, api, "", http.MethodGet, "/healthz", "")
	if response.Code != http.StatusOK ||
		strings.TrimSpace(response.Body.String()) != `{"status":"ok"}` {
		t.Fatalf("health: status %d, body %q", response.Code, response.Body.String())
	}
	if cache := response.Header().Get("Cache-Control"); cache != "no-store" {
		t.Fatalf("health Cache-Control = %q", cache)
	}

	response = callRaw(t, api, "", http.MethodPost, "/healthz", "")
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("health POST: status %d, allow %q", response.Code, response.Header().Get("Allow"))
	}
	response = callRaw(t, api, "", http.MethodGet, "/healthz?probe=1", "")
	if response.Code != http.StatusBadRequest || responseCode(t, response) != "invalid_request" {
		t.Fatalf("health query: status %d, body %s", response.Code, response.Body.String())
	}

	// A shutting-down or failed server reports unavailable so a load balancer
	// stops sending it traffic.
	api.shuttingDown.Store(true)
	response = callRaw(t, api, "", http.MethodGet, "/healthz", "")
	if response.Code != http.StatusServiceUnavailable || responseCode(t, response) != "unavailable" {
		t.Fatalf("health during shutdown: status %d, body %s", response.Code, response.Body.String())
	}
	api.shuttingDown.Store(false)
	api.unavailable.Store(true)
	response = callRaw(t, api, "", http.MethodGet, "/healthz", "")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("health while unavailable: status %d", response.Code)
	}
}

func TestUnavailableServerRejectsAPIRequests(t *testing.T) {
	api, _, token := newTestAPI(t)
	api.shuttingDown.Store(true)
	response := callJSON(t, api, token, http.MethodGet, "/v1/collections", nil)
	if response.Code != http.StatusServiceUnavailable || responseCode(t, response) != "unavailable" {
		t.Fatalf("status %d, body %s", response.Code, response.Body.String())
	}
	response = callRaw(t, api, "", http.MethodGet, "/not/under/v1", "")
	if response.Code != http.StatusNotFound || responseCode(t, response) != "route_not_found" {
		t.Fatalf("non-v1 path: status %d, body %s", response.Code, response.Body.String())
	}
}

// A library panic must become internal_error and leave a report in the log
// rather than terminating the process or vanishing silently.
func TestPanicBecomesInternalErrorAndIsLogged(t *testing.T) {
	api, db, token := newTestAPI(t)
	var logs bytes.Buffer
	api.log = newLogger(&logs)

	// A stored user record which cannot decode makes Collection.All panic.
	if err := medb.C[json.RawMessage](db, userCollection).Set(
		"ffffffffffffffffffffffffffffffff", json.RawMessage(`"not an object"`)); err != nil {
		t.Fatal(err)
	}
	response := callJSON(t, api, token, http.MethodGet, "/v1/auth/users", nil)
	if response.Code != http.StatusInternalServerError || responseCode(t, response) != "internal_error" {
		t.Fatalf("status %d, body %s", response.Code, response.Body.String())
	}
	if !strings.Contains(logs.String(), "handler panic") {
		t.Fatalf("panic was not logged: %q", logs.String())
	}
	if !strings.Contains(logs.String(), "stack=") {
		t.Fatalf("panic log carries no stack: %q", logs.String())
	}
	// The response must not leak the panic text or the stored document.
	if strings.Contains(response.Body.String(), "not an object") ||
		strings.Contains(response.Body.String(), "decode") {
		t.Fatalf("internal error leaked detail: %s", response.Body.String())
	}
}

// A malformed stored record which does not panic is still an internal error,
// and the cause reaches the log.
func TestMalformedStoredRecordIsLogged(t *testing.T) {
	api, _, token := newTestAPI(t)
	var logs bytes.Buffer
	api.log = newLogger(&logs)

	uid := "ffffffffffffffffffffffffffffffff"
	if err := api.auth.users.Set(uid, userRecord{
		Name: "broken", Role: "sorcerer", CreatedAt: nowTimestamp(), UpdatedAt: nowTimestamp(),
	}); err != nil {
		t.Fatal(err)
	}
	response := callJSON(t, api, token, http.MethodPost, "/v1/auth/tokens/list", map[string]any{
		"user_id": uid,
	})
	if response.Code != http.StatusInternalServerError || responseCode(t, response) != "internal_error" {
		t.Fatalf("status %d, body %s", response.Code, response.Body.String())
	}
	if !strings.Contains(logs.String(), "invalid user role") {
		t.Fatalf("cause was not logged: %q", logs.String())
	}

	// An update against the same record refuses to write on top of it.
	logs.Reset()
	response = callJSON(t, api, token, http.MethodPost, "/v1/auth/users/update", map[string]any{
		"user_id": uid, "role": "reader",
	})
	if response.Code != http.StatusInternalServerError || responseCode(t, response) != "internal_error" {
		t.Fatalf("update: status %d, body %s", response.Code, response.Body.String())
	}
	if !strings.Contains(logs.String(), "malformed authentication record") {
		t.Fatalf("update cause was not logged: %q", logs.String())
	}
	stored, err := api.auth.users.Get(uid)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Role != "sorcerer" {
		t.Fatalf("a refused update still modified the record: %+v", stored)
	}
}
