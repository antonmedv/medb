package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/antonmedv/medb"
)

func createUser(t *testing.T, api *apiServer, token, name string, userRole role) userView {
	t.Helper()
	response := callJSON(t, api, token, http.MethodPost, "/v1/auth/users/create", map[string]any{
		"name": name, "role": userRole,
	})
	if response.Code != http.StatusCreated {
		t.Fatalf("create user %s: status %d, body %s", name, response.Code, response.Body.String())
	}
	var created struct {
		User userView `json:"user"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	return created.User
}

func TestListUsers(t *testing.T) {
	api, _, token := newTestAPI(t)
	reader := createUser(t, api, token, "reader", roleReader)

	response := callJSON(t, api, token, http.MethodGet, "/v1/auth/users", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("list users: status %d, body %s", response.Code, response.Body.String())
	}
	var listed struct {
		Users []userView `json:"users"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Users) != 2 {
		t.Fatalf("listed %d users: %s", len(listed.Users), response.Body.String())
	}
	// Users are ordered by ID, which is how Collection.All iterates.
	for i := 1; i < len(listed.Users); i++ {
		if listed.Users[i-1].ID >= listed.Users[i].ID {
			t.Fatalf("users are not ordered by ID: %s", response.Body.String())
		}
	}
	var found *userView
	for i := range listed.Users {
		if listed.Users[i].ID == reader.ID {
			found = &listed.Users[i]
		}
	}
	if found == nil {
		t.Fatalf("created user missing from the listing: %s", response.Body.String())
	}
	if found.Name != "reader" || found.Role != roleReader || found.Disabled {
		t.Fatalf("unexpected user view: %+v", *found)
	}
	if found.CreatedAt == "" || found.UpdatedAt == "" {
		t.Fatalf("user view has no timestamps: %+v", *found)
	}
	// A listing must never carry a credential.
	if strings.Contains(response.Body.String(), "medb_") {
		t.Fatal("user listing leaked a token")
	}
}

func TestListTokens(t *testing.T) {
	api, _, token := newTestAPI(t)
	owner := createUser(t, api, token, "owner", roleWriter)
	other := createUser(t, api, token, "other", roleReader)

	response := callJSON(t, api, token, http.MethodPost, "/v1/auth/tokens/create", map[string]any{
		"user_id": owner.ID, "label": "deployment", "expires_at": "2999-01-01T00:00:00Z",
	})
	if response.Code != http.StatusCreated {
		t.Fatalf("create token: status %d, body %s", response.Code, response.Body.String())
	}
	var created struct {
		TokenID string `json:"token_id"`
		Token   string `json:"token"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if response = callJSON(t, api, token, http.MethodPost, "/v1/auth/tokens/create", map[string]any{
		"user_id": other.ID, "label": "unrelated",
	}); response.Code != http.StatusCreated {
		t.Fatalf("create unrelated token: status %d, body %s", response.Code, response.Body.String())
	}

	response = callJSON(t, api, token, http.MethodPost, "/v1/auth/tokens/list", map[string]any{
		"user_id": owner.ID,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("list tokens: status %d, body %s", response.Code, response.Body.String())
	}
	var listed struct {
		Tokens []tokenView `json:"tokens"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Tokens) != 1 {
		t.Fatalf("listed %d tokens for one user: %s", len(listed.Tokens), response.Body.String())
	}
	view := listed.Tokens[0]
	if view.TokenID != created.TokenID || view.UserID != owner.ID || view.Label != "deployment" {
		t.Fatalf("unexpected token view: %+v", view)
	}
	if view.ExpiresAt == nil || *view.ExpiresAt != "2999-01-01T00:00:00Z" {
		t.Fatalf("token expiry not reported: %+v", view)
	}
	// The plaintext token is disclosed once, at creation, and never again.
	if strings.Contains(response.Body.String(), created.Token) ||
		strings.Contains(response.Body.String(), "medb_") {
		t.Fatalf("token listing leaked a plaintext token: %s", response.Body.String())
	}
}

func TestAuthManagementNotFound(t *testing.T) {
	api, _, token := newTestAPI(t)
	unknownUser := "00000000000000000000000000000000"
	unknownToken := strings.Repeat("0", 64)

	tests := []struct {
		name string
		path string
		body map[string]any
	}{
		{name: "update", path: "/v1/auth/users/update", body: map[string]any{"user_id": unknownUser, "role": "reader"}},
		{name: "token create", path: "/v1/auth/tokens/create", body: map[string]any{"user_id": unknownUser, "label": "x"}},
		{name: "token list", path: "/v1/auth/tokens/list", body: map[string]any{"user_id": unknownUser}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := callJSON(t, api, token, http.MethodPost, test.path, test.body)
			if response.Code != http.StatusNotFound || responseCode(t, response) != "not_found" {
				t.Fatalf("status %d, body %s", response.Code, response.Body.String())
			}
		})
	}

	// Revocation is idempotent, so an unknown token ID is still a success.
	response := callJSON(t, api, token, http.MethodPost, "/v1/auth/tokens/revoke", map[string]any{
		"token_id": unknownToken,
	})
	if response.Code != http.StatusNoContent {
		t.Fatalf("revoke unknown: status %d, body %s", response.Code, response.Body.String())
	}

	// Malformed identifiers are schema errors, not lookups.
	for _, body := range []string{
		`{"user_id":"tooshort"}`,
		`{"user_id":"NOTLOWERCASEHEX0000000000000000A"}`,
	} {
		response = callRaw(t, api, token, http.MethodPost, "/v1/auth/tokens/list", body)
		if response.Code != http.StatusBadRequest || responseCode(t, response) != "invalid_request" {
			t.Fatalf("%s: status %d, body %s", body, response.Code, response.Body.String())
		}
	}
	response = callRaw(t, api, token, http.MethodPost, "/v1/auth/tokens/revoke", `{"token_id":"abc"}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("short token id: status %d", response.Code)
	}
	// users/update needs at least one of role and disabled.
	response = callJSON(t, api, token, http.MethodPost, "/v1/auth/users/update", map[string]any{
		"user_id": unknownUser,
	})
	if response.Code != http.StatusBadRequest || responseCode(t, response) != "invalid_request" {
		t.Fatalf("empty update: status %d, body %s", response.Code, response.Body.String())
	}
}

func TestCredentialsFailClosed(t *testing.T) {
	api, db, token := newTestAPI(t)
	auth := newAuthStore(db)

	// A disabled user's token stops working.
	user := createUser(t, api, token, "temporary", roleReader)
	response := callJSON(t, api, token, http.MethodPost, "/v1/auth/tokens/create", map[string]any{
		"user_id": user.ID, "label": "temporary",
	})
	if response.Code != http.StatusCreated {
		t.Fatalf("create token: status %d, body %s", response.Code, response.Body.String())
	}
	var created struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if _, ok := auth.authenticate(created.Token, time.Now()); !ok {
		t.Fatal("new token does not authenticate")
	}
	response = callJSON(t, api, token, http.MethodPost, "/v1/auth/users/update", map[string]any{
		"user_id": user.ID, "disabled": true,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("disable: status %d, body %s", response.Code, response.Body.String())
	}
	if _, ok := auth.authenticate(created.Token, time.Now()); ok {
		t.Fatal("a disabled user still authenticates")
	}
	response = callJSON(t, api, created.Token, http.MethodGet, "/v1/collections", nil)
	if response.Code != http.StatusUnauthorized || responseCode(t, response) != "invalid_token" {
		t.Fatalf("disabled user request: status %d, body %s", response.Code, response.Body.String())
	}

	// An expired token stops working at its expiry instant.
	expiry := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	expiringID := tokenID("medb_" + strings.Repeat("A", 43))
	if err := auth.tokens.Set(expiringID, tokenRecord{
		UserID: user.ID, Label: "expiring", CreatedAt: nowTimestamp(), ExpiresAt: &expiry,
	}); err != nil {
		t.Fatal(err)
	}
	parsed, err := parseUTCTimestamp(expiry)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := auth.authenticate("medb_"+strings.Repeat("A", 43), parsed); ok {
		t.Fatal("a token authenticates at its expiry instant")
	}

	// Malformed Authorization headers never resolve to a principal.
	for _, header := range []string{"", "Token abc", "Bearer", "Bearer a b", "bearer not-a-token"} {
		request := newRequestWithAuthorization(t, header)
		actor, fail := api.authenticateRequest(request)
		if fail == nil {
			t.Fatalf("header %q authenticated as %q", header, actor)
		}
	}
	// Two Authorization headers are rejected outright.
	request := newRequestWithAuthorization(t, "Bearer "+token)
	request.Header.Add("Authorization", "Bearer "+token)
	if _, fail := api.authenticateRequest(request); fail != failInvalidToken {
		t.Fatalf("duplicate header failure = %v", fail)
	}
}

func newRequestWithAuthorization(t *testing.T, header string) *http.Request {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, "/v1/collections", nil)
	if err != nil {
		t.Fatal(err)
	}
	if header != "" {
		request.Header.Set("Authorization", header)
	}
	return request
}

// The server refuses to start when no enabled administrator holds a live token,
// because that state is only recoverable offline.
func TestStartupRequiresAnActiveAdministrator(t *testing.T) {
	dir := t.TempDir()
	db, err := medb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	token, err := newToken()
	if err != nil {
		t.Fatal(err)
	}
	auth := newAuthStore(db)
	if err := initializeAuth(db, auth, envMap{"MEDB_INIT_ADMIN_TOKEN": token}.lookup,
		newLogger(io.Discard)); err != nil {
		t.Fatal(err)
	}
	ok, err := auth.hasActiveAdmin(time.Now())
	if err != nil || !ok {
		t.Fatalf("hasActiveAdmin = %v, %v", ok, err)
	}

	// Disable the only administrator.
	for id, user := range auth.users.All() {
		user.Disabled = true
		user.UpdatedAt = nowTimestamp()
		if err := auth.users.Set(id, user); err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := auth.hasActiveAdmin(time.Now()); err != nil || ok {
		t.Fatalf("hasActiveAdmin after disabling = %v, %v", ok, err)
	}
	err = initializeAuth(db, auth, envMap{}.lookup, newLogger(io.Discard))
	if err == nil || !strings.Contains(err.Error(), "medb auth recover") {
		t.Fatalf("err = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Offline recovery restores a usable administrator.
	var stdout bytes.Buffer
	if err := recoverAuth(recoverConfig{dir: dir, name: "operator"}, &stdout, newLogger(io.Discard)); err != nil {
		t.Fatal(err)
	}
	db, err = medb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := initializeAuth(db, newAuthStore(db), envMap{}.lookup, newLogger(io.Discard)); err != nil {
		t.Fatalf("recovered database refuses to start: %v", err)
	}
}

// An expired administrator token does not count as an active credential.
func TestStartupRejectsExpiredAdministratorToken(t *testing.T) {
	dir := t.TempDir()
	db, err := medb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	auth := newAuthStore(db)
	uid := medb.NewID()
	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	created := time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)
	if err := auth.users.Set(uid, userRecord{
		Name: "admin", Role: roleAdmin, CreatedAt: created, UpdatedAt: created,
	}); err != nil {
		t.Fatal(err)
	}
	if err := auth.tokens.Set(tokenID("medb_"+strings.Repeat("B", 43)), tokenRecord{
		UserID: uid, Label: "expired", CreatedAt: created, ExpiresAt: &past,
	}); err != nil {
		t.Fatal(err)
	}
	if err := auth.state.Set(stateID, stateRecord{Version: 1, AuthInitialized: true}); err != nil {
		t.Fatal(err)
	}
	if ok, err := auth.hasActiveAdmin(time.Now()); err != nil || ok {
		t.Fatalf("hasActiveAdmin = %v, %v", ok, err)
	}
}

func TestInitializationRejectsConflictingState(t *testing.T) {
	t.Run("foreign reserved collection", func(t *testing.T) {
		dir := t.TempDir()
		db, err := medb.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if err := medb.C[string](db, "_meta/application").Set("x", "y"); err != nil {
			t.Fatal(err)
		}
		token, err := newToken()
		if err != nil {
			t.Fatal(err)
		}
		err = initializeAuth(db, newAuthStore(db),
			envMap{"MEDB_INIT_ADMIN_TOKEN": token}.lookup, newLogger(io.Discard))
		if err == nil || !strings.Contains(err.Error(), "conflicts with server metadata") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("unsupported state version", func(t *testing.T) {
		dir := t.TempDir()
		db, err := medb.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		auth := newAuthStore(db)
		if err := auth.state.Set(stateID, stateRecord{Version: 2, AuthInitialized: true}); err != nil {
			t.Fatal(err)
		}
		err = initializeAuth(db, auth, envMap{}.lookup, newLogger(io.Discard))
		if err == nil || !strings.Contains(err.Error(), "invalid authentication state") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("unrelated records without a marker", func(t *testing.T) {
		dir := t.TempDir()
		db, err := medb.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		auth := newAuthStore(db)
		if err := auth.users.Set(medb.NewID(), userRecord{
			Name: "stray", Role: roleReader, CreatedAt: nowTimestamp(), UpdatedAt: nowTimestamp(),
		}); err != nil {
			t.Fatal(err)
		}
		token, err := newToken()
		if err != nil {
			t.Fatal(err)
		}
		err = initializeAuth(db, auth, envMap{"MEDB_INIT_ADMIN_TOKEN": token}.lookup,
			newLogger(io.Discard))
		if err == nil || !strings.Contains(err.Error(), "unrelated authentication records") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestDropAndReservedNamespaceEnforcement(t *testing.T) {
	api, _, token := newTestAPI(t)
	if response := callJSON(t, api, token, http.MethodPost, "/v1/set", map[string]any{
		"collection": "temp/data", "id": "a", "document": 1,
	}); response.Code != http.StatusNoContent {
		t.Fatalf("set: status %d", response.Code)
	}
	response := callJSON(t, api, token, http.MethodPost, "/v1/drop", map[string]any{
		"collection": "temp/data",
	})
	if response.Code != http.StatusNoContent {
		t.Fatalf("drop: status %d, body %s", response.Code, response.Body.String())
	}
	// Drop is idempotent.
	response = callJSON(t, api, token, http.MethodPost, "/v1/drop", map[string]any{
		"collection": "temp/data",
	})
	if response.Code != http.StatusNoContent {
		t.Fatalf("second drop: status %d", response.Code)
	}
	response = callJSON(t, api, token, http.MethodPost, "/v1/count", map[string]any{
		"collection": "temp/data",
	})
	if !strings.Contains(response.Body.String(), `"count":0`) {
		t.Fatalf("count after drop: %s", response.Body.String())
	}

	// Every data endpoint refuses the reserved namespace, including for admins.
	for _, path := range []string{"/v1/drop", "/v1/count", "/v1/scan"} {
		for _, name := range []string{"_meta", "_meta/state", "_meta/tokens"} {
			response = callJSON(t, api, token, http.MethodPost, path, map[string]any{"collection": name})
			if response.Code != http.StatusForbidden || responseCode(t, response) != "reserved_collection" {
				t.Fatalf("%s %s: status %d, body %s", path, name, response.Code, response.Body.String())
			}
		}
	}
	for _, path := range []string{"/v1/has", "/v1/delete"} {
		response = callJSON(t, api, token, http.MethodPost, path, map[string]any{
			"collection": "_meta/users", "id": "x",
		})
		if response.Code != http.StatusForbidden {
			t.Fatalf("%s: status %d, body %s", path, response.Code, response.Body.String())
		}
	}

	// A missing document reports absence rather than an error.
	response = callJSON(t, api, token, http.MethodPost, "/v1/has", map[string]any{
		"collection": "temp/data", "id": "gone",
	})
	if !strings.Contains(response.Body.String(), `"exists":false`) {
		t.Fatalf("has missing: %s", response.Body.String())
	}
	response = callJSON(t, api, token, http.MethodPost, "/v1/delete", map[string]any{
		"collection": "temp/data", "id": "gone",
	})
	if response.Code != http.StatusNoContent {
		t.Fatalf("delete missing: status %d", response.Code)
	}
	response = callJSON(t, api, token, http.MethodPost, "/v1/scan", map[string]any{
		"collection": "temp/data",
	})
	if response.Code != http.StatusOK || response.Body.Len() != 0 {
		t.Fatalf("scan empty: status %d, body %q", response.Code, response.Body.String())
	}
}

func TestValidationHelpers(t *testing.T) {
	t.Run("collection names", func(t *testing.T) {
		valid := []string{"users", "audit-log", "user_profiles", "2026/events", "prod/eu/users",
			strings.Repeat("a", 240)}
		invalid := []string{"", "/users", "users/", "users//active", "Users", "users.json",
			strings.Repeat("a", 241), "users/../etc", "us ers"}
		for _, name := range valid {
			if !validCollectionName(name) {
				t.Errorf("%q rejected", name)
			}
		}
		for _, name := range invalid {
			if validCollectionName(name) {
				t.Errorf("%q accepted", name)
			}
		}
	})

	t.Run("reserved names", func(t *testing.T) {
		for _, name := range []string{"_meta", "_meta/state", "_meta/a/b"} {
			if !reservedCollection(name) {
				t.Errorf("%q not reserved", name)
			}
		}
		for _, name := range []string{"meta", "_metadata", "users/_meta"} {
			if reservedCollection(name) {
				t.Errorf("%q reserved", name)
			}
		}
	})

	t.Run("hex identifiers", func(t *testing.T) {
		if !validHexID(strings.Repeat("a", 32), 32) || !validHexID("0123456789abcdef0123456789abcdef", 32) {
			t.Error("valid hex ID rejected")
		}
		for _, value := range []string{"", "abc", strings.Repeat("A", 32), strings.Repeat("g", 32)} {
			if validHexID(value, 32) {
				t.Errorf("%q accepted", value)
			}
		}
	})

	t.Run("roles", func(t *testing.T) {
		if !roleAllows(roleAdmin, roleReader) || !roleAllows(roleWriter, roleReader) ||
			!roleAllows(roleReader, roleReader) {
			t.Error("hierarchy denies an allowed role")
		}
		if roleAllows(roleReader, roleWriter) || roleAllows(roleWriter, roleAdmin) {
			t.Error("hierarchy allows an escalation")
		}
		// An unknown role ranks below every requirement.
		if roleAllows("sorcerer", roleReader) {
			t.Error("an unknown role was allowed")
		}
		if validRole("sorcerer") || validRole("") {
			t.Error("an unknown role validated")
		}
	})

	t.Run("labels", func(t *testing.T) {
		if validateLabel("") == nil || validateLabel(strings.Repeat("x", 257)) == nil {
			t.Error("out-of-range label accepted")
		}
		if validateLabel("\xff\xfe") == nil {
			t.Error("invalid UTF-8 label accepted")
		}
		if err := validateLabel(strings.Repeat("x", 256)); err != nil {
			t.Errorf("256-byte label rejected: %v", err)
		}
	})

	t.Run("timestamps", func(t *testing.T) {
		if _, err := parseUTCTimestamp("2026-08-21T12:00:00+02:00"); err == nil {
			t.Error("non-UTC timestamp accepted")
		}
		if _, err := parseUTCTimestamp("21 Aug 2026Z"); err == nil {
			t.Error("malformed timestamp accepted")
		}
		if _, err := parseUTCTimestamp("2026-08-21T12:00:00Z"); err != nil {
			t.Errorf("valid timestamp rejected: %v", err)
		}
	})

	t.Run("stored records", func(t *testing.T) {
		now := nowTimestamp()
		good := userRecord{Name: "a", Role: roleReader, CreatedAt: now, UpdatedAt: now}
		if err := validateUserRecord(strings.Repeat("a", 32), good); err != nil {
			t.Errorf("valid user rejected: %v", err)
		}
		if validateUserRecord("short", good) == nil {
			t.Error("bad user ID accepted")
		}
		earlier := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
		backwards := userRecord{Name: "a", Role: roleReader, CreatedAt: now, UpdatedAt: earlier}
		if validateUserRecord(strings.Repeat("a", 32), backwards) == nil {
			t.Error("user updated before creation accepted")
		}
		for _, broken := range []userRecord{
			{Name: "", Role: roleReader, CreatedAt: now, UpdatedAt: now},
			{Name: "a", Role: "nope", CreatedAt: now, UpdatedAt: now},
			{Name: "a", Role: roleReader, CreatedAt: "bad", UpdatedAt: now},
			{Name: "a", Role: roleReader, CreatedAt: now, UpdatedAt: "bad"},
		} {
			if validateUserRecord(strings.Repeat("a", 32), broken) == nil {
				t.Errorf("broken user accepted: %+v", broken)
			}
		}

		token := tokenRecord{UserID: strings.Repeat("a", 32), Label: "l", CreatedAt: now}
		if err := validateTokenRecord(strings.Repeat("a", 64), token); err != nil {
			t.Errorf("valid token rejected: %v", err)
		}
		if validateTokenRecord("short", token) == nil {
			t.Error("bad token ID accepted")
		}
		bad := "bad"
		for _, broken := range []tokenRecord{
			{UserID: "short", Label: "l", CreatedAt: now},
			{UserID: strings.Repeat("a", 32), Label: "", CreatedAt: now},
			{UserID: strings.Repeat("a", 32), Label: "l", CreatedAt: "bad"},
			{UserID: strings.Repeat("a", 32), Label: "l", CreatedAt: now, ExpiresAt: &bad},
			{UserID: strings.Repeat("a", 32), Label: "l", CreatedAt: now, ExpiresAt: &now},
		} {
			if validateTokenRecord(strings.Repeat("a", 64), broken) == nil {
				t.Errorf("broken token accepted: %+v", broken)
			}
		}

		if validateStateRecord(stateRecord{Version: 1, AuthInitialized: true}) != nil {
			t.Error("valid state rejected")
		}
		if validateStateRecord(stateRecord{Version: 1}) == nil ||
			validateStateRecord(stateRecord{Version: 9, AuthInitialized: true}) == nil {
			t.Error("invalid state accepted")
		}
	})

	t.Run("tokens", func(t *testing.T) {
		token, err := newToken()
		if err != nil {
			t.Fatal(err)
		}
		if !validToken(token) || !strings.HasPrefix(token, "medb_") || len(token) != 48 {
			t.Fatalf("generated token %q", token)
		}
		for _, value := range []string{"", "medb_", token[:47], "xxxx_" + token[5:],
			"medb_" + strings.Repeat("!", 43)} {
			if validToken(value) {
				t.Errorf("%q accepted", value)
			}
		}
		if tokenID(token) == tokenID(token+"x") || !validHexID(tokenID(token), 64) {
			t.Error("token digest is not a distinct 64-character hex ID")
		}
	})
}
