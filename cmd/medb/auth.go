package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/antonmedv/medb"
)

const (
	stateCollection = "_meta/state"
	userCollection  = "_meta/users"
	tokenCollection = "_meta/tokens"
	stateID         = "server"
)

type role string

const (
	roleReader role = "reader"
	roleWriter role = "writer"
	roleAdmin  role = "admin"
)

type stateRecord struct {
	Version         int  `json:"version"`
	AuthInitialized bool `json:"auth_initialized"`
}

type userRecord struct {
	Name      string `json:"name"`
	Role      role   `json:"role"`
	Disabled  bool   `json:"disabled"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type tokenRecord struct {
	UserID    string  `json:"user_id"`
	Label     string  `json:"label"`
	CreatedAt string  `json:"created_at"`
	ExpiresAt *string `json:"expires_at"`
}

type principal struct {
	userID string
	name   string
	role   role
}

type authStore struct {
	state  *medb.Collection[stateRecord]
	users  *medb.Collection[userRecord]
	tokens *medb.Collection[tokenRecord]
}

func newAuthStore(db *medb.DB) *authStore {
	return &authStore{
		state:  medb.C[stateRecord](db, stateCollection),
		users:  medb.C[userRecord](db, userCollection),
		tokens: medb.C[tokenRecord](db, tokenCollection),
	}
}

func newToken() (string, error) {
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return "", fmt.Errorf("medb: generate token: %w", err)
	}
	return "medb_" + base64.RawURLEncoding.EncodeToString(secret[:]), nil
}

func validToken(token string) bool {
	if len(token) != 48 || !strings.HasPrefix(token, "medb_") {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token[len("medb_"):])
	return err == nil && len(decoded) == 32
}

func tokenID(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func nowTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func validateLabel(value string) error {
	if !utf8.ValidString(value) {
		return errors.New("must be valid UTF-8")
	}
	if len(value) == 0 || len(value) > 256 {
		return fmt.Errorf("must contain between 1 and 256 UTF-8 encoded bytes, got %d", len(value))
	}
	return nil
}

func validRole(value role) bool {
	return value == roleReader || value == roleWriter || value == roleAdmin
}

func roleAllows(actual, required role) bool {
	rank := func(r role) int {
		switch r {
		case roleReader:
			return 1
		case roleWriter:
			return 2
		case roleAdmin:
			return 3
		default:
			return 0
		}
	}
	return rank(actual) >= rank(required)
}

func validHexID(value string, size int) bool {
	if len(value) != size {
		return false
	}
	for i := range len(value) {
		if !('0' <= value[i] && value[i] <= '9') && !('a' <= value[i] && value[i] <= 'f') {
			return false
		}
	}
	return true
}

func parseUTCTimestamp(value string) (time.Time, error) {
	if !strings.HasSuffix(value, "Z") {
		return time.Time{}, errors.New("timestamp must use UTC with a Z suffix")
	}
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}

func validateUserRecord(id string, user userRecord) error {
	if !validHexID(id, 32) {
		return errors.New("invalid user ID")
	}
	if err := validateLabel(user.Name); err != nil {
		return fmt.Errorf("invalid user name: %w", err)
	}
	if !validRole(user.Role) {
		return errors.New("invalid user role")
	}
	created, err := parseUTCTimestamp(user.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid user creation time: %w", err)
	}
	updated, err := parseUTCTimestamp(user.UpdatedAt)
	if err != nil {
		return fmt.Errorf("invalid user update time: %w", err)
	}
	if updated.Before(created) {
		return errors.New("user update time precedes creation time")
	}
	return nil
}

func validateTokenRecord(id string, token tokenRecord) error {
	if !validHexID(id, 64) {
		return errors.New("invalid token ID")
	}
	if !validHexID(token.UserID, 32) {
		return errors.New("invalid token user ID")
	}
	if err := validateLabel(token.Label); err != nil {
		return fmt.Errorf("invalid token label: %w", err)
	}
	created, err := parseUTCTimestamp(token.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid token creation time: %w", err)
	}
	if token.ExpiresAt != nil {
		expires, err := parseUTCTimestamp(*token.ExpiresAt)
		if err != nil {
			return fmt.Errorf("invalid token expiry: %w", err)
		}
		if !expires.After(created) {
			return errors.New("token expiry must follow creation time")
		}
	}
	return nil
}

func validateStateRecord(state stateRecord) error {
	if state.Version != 1 || !state.AuthInitialized {
		return errors.New("unsupported or incomplete authentication state")
	}
	return nil
}

func (a *authStore) authenticate(token string, now time.Time) (principal, bool) {
	if !validToken(token) {
		return principal{}, false
	}
	tid := tokenID(token)
	record, err := a.tokens.Get(tid)
	if err != nil || validateTokenRecord(tid, record) != nil {
		return principal{}, false
	}
	if record.ExpiresAt != nil {
		expires, err := parseUTCTimestamp(*record.ExpiresAt)
		if err != nil || !now.Before(expires) {
			return principal{}, false
		}
	}
	user, err := a.users.Get(record.UserID)
	if err != nil || validateUserRecord(record.UserID, user) != nil || user.Disabled {
		return principal{}, false
	}
	return principal{userID: record.UserID, name: user.Name, role: user.Role}, true
}

func (a *authStore) hasActiveAdmin(now time.Time) (ok bool, err error) {
	defer func() {
		if value := recover(); value != nil {
			ok = false
			err = fmt.Errorf("invalid authentication records: %v", value)
		}
	}()
	for tid, record := range a.tokens.All() {
		if validateTokenRecord(tid, record) != nil {
			continue
		}
		if record.ExpiresAt != nil {
			expires, e := parseUTCTimestamp(*record.ExpiresAt)
			if e != nil || !now.Before(expires) {
				continue
			}
		}
		user, e := a.users.Get(record.UserID)
		if e != nil || validateUserRecord(record.UserID, user) != nil {
			continue
		}
		if !user.Disabled && user.Role == roleAdmin {
			return true, nil
		}
	}
	return false, nil
}

func initializeAuth(db *medb.DB, auth *authStore, getenv envLookup, stderr io.Writer) error {
	state, err := auth.state.Get(stateID)
	switch {
	case err == nil:
		if err := validateStateRecord(state); err != nil {
			return fmt.Errorf("medb: invalid authentication state: %w", err)
		}
		if initEnvironmentPresent(getenv) {
			_, _ = fmt.Fprintln(stderr, "medb: authentication already initialized; MEDB_INIT_* values ignored")
		}
		ok, err := auth.hasActiveAdmin(time.Now())
		if err != nil {
			return fmt.Errorf("medb: validate administrators: %w", err)
		}
		if !ok {
			return errors.New("medb: no enabled administrator has an active token; stop the server and run medb auth recover")
		}
		return nil
	case !errors.Is(err, medb.ErrNotFound):
		return fmt.Errorf("medb: read authentication state: %w", err)
	}

	if auth.state.Count() != 0 {
		return errors.New("medb: authentication state marker is missing but _meta/state is not empty")
	}
	for _, name := range db.Collections() {
		if (name == "_meta" || strings.HasPrefix(name, "_meta/")) &&
			!slices.Contains([]string{stateCollection, userCollection, tokenCollection}, name) {
			return fmt.Errorf("medb: reserved collection %q conflicts with server metadata", name)
		}
	}

	name := envString(getenv, "MEDB_INIT_ADMIN_NAME", "admin")
	if err := validateLabel(name); err != nil {
		return fmt.Errorf("medb: invalid MEDB_INIT_ADMIN_NAME: %w", err)
	}
	token, err := initialToken(getenv)
	if err != nil {
		return err
	}
	tid := tokenID(token)
	record, err := auth.tokens.Get(tid)
	var uid string
	switch {
	case errors.Is(err, medb.ErrNotFound):
		if auth.tokens.Count() != 0 || auth.users.Count() != 0 {
			return errors.New("medb: authentication marker is missing but unrelated authentication records exist")
		}
		uid = uniqueUserID(auth.users)
		now := nowTimestamp()
		record = tokenRecord{UserID: uid, Label: "initial", CreatedAt: now, ExpiresAt: nil}
		if err := auth.tokens.Set(tid, record); err != nil {
			return fmt.Errorf("medb: write initial token: %w", err)
		}
	case err != nil:
		return fmt.Errorf("medb: read partial initial token: %w", err)
	default:
		if auth.tokens.Count() != 1 || validateTokenRecord(tid, record) != nil || record.Label != "initial" || record.ExpiresAt != nil {
			return errors.New("medb: conflicting partial authentication token")
		}
		uid = record.UserID
	}

	user, err := auth.users.Get(uid)
	switch {
	case errors.Is(err, medb.ErrNotFound):
		if auth.users.Count() != 0 {
			return errors.New("medb: conflicting partial administrator records")
		}
		now := nowTimestamp()
		user = userRecord{Name: name, Role: roleAdmin, CreatedAt: now, UpdatedAt: now}
		if err := auth.users.Set(uid, user); err != nil {
			return fmt.Errorf("medb: write initial administrator: %w", err)
		}
	case err != nil:
		return fmt.Errorf("medb: read partial administrator: %w", err)
	default:
		if auth.users.Count() != 1 || validateUserRecord(uid, user) != nil || user.Name != name || user.Role != roleAdmin || user.Disabled {
			return errors.New("medb: conflicting partial administrator record")
		}
	}

	if validateTokenRecord(tid, record) != nil || validateUserRecord(uid, user) != nil || user.Disabled || user.Role != roleAdmin {
		return errors.New("medb: initial administrator credential did not validate")
	}
	if err := auth.state.Set(stateID, stateRecord{Version: 1, AuthInitialized: true}); err != nil {
		return fmt.Errorf("medb: write authentication state: %w", err)
	}
	return nil
}

func initEnvironmentPresent(getenv envLookup) bool {
	for _, name := range []string{"MEDB_INIT_ADMIN_NAME", "MEDB_INIT_ADMIN_TOKEN", "MEDB_INIT_ADMIN_TOKEN_FILE"} {
		if value, ok := getenv(name); ok && value != "" {
			return true
		}
	}
	return false
}

func initialToken(getenv envLookup) (string, error) {
	direct, directOK := getenv("MEDB_INIT_ADMIN_TOKEN")
	if direct == "" {
		directOK = false
	}
	path, fileOK := getenv("MEDB_INIT_ADMIN_TOKEN_FILE")
	if path == "" {
		fileOK = false
	}
	if directOK == fileOK {
		return "", errors.New("medb: uninitialized authentication requires exactly one of MEDB_INIT_ADMIN_TOKEN and MEDB_INIT_ADMIN_TOKEN_FILE")
	}
	value := direct
	if fileOK {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("medb: read MEDB_INIT_ADMIN_TOKEN_FILE: %w", err)
		}
		data = trimOneLineEnding(data)
		value = string(data)
	}
	if !validToken(value) {
		return "", errors.New("medb: initial administrator token has invalid format; use medb token generate")
	}
	return value, nil
}

func trimOneLineEnding(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
		if len(data) > 0 && data[len(data)-1] == '\r' {
			data = data[:len(data)-1]
		}
	}
	return data
}

func uniqueUserID(users *medb.Collection[userRecord]) string {
	for {
		id := medb.NewID()
		if !users.Has(id) {
			return id
		}
	}
}

func uniqueToken(tokens *medb.Collection[tokenRecord]) (token, id string, err error) {
	for {
		token, err = newToken()
		if err != nil {
			return "", "", err
		}
		id = tokenID(token)
		if !tokens.Has(id) {
			return token, id, nil
		}
	}
}

func recoverAuth(cfg recoverConfig, stdout, stderr io.Writer) (err error) {
	_, _ = fmt.Fprintln(stderr, "medb: warning: creating a new offline administrator credential")
	db, err := medb.Open(cfg.dir)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			if closeErr := db.Close(); err == nil {
				err = closeErr
			}
		}
	}()

	auth := newAuthStore(db)
	uid := uniqueUserID(auth.users)
	token, tid, err := uniqueToken(auth.tokens)
	if err != nil {
		return err
	}
	now := nowTimestamp()
	if err := auth.tokens.Set(tid, tokenRecord{UserID: uid, Label: "recovery", CreatedAt: now}); err != nil {
		return fmt.Errorf("medb: write recovery token: %w", err)
	}
	if err := auth.users.Set(uid, userRecord{Name: cfg.name, Role: roleAdmin, CreatedAt: now, UpdatedAt: now}); err != nil {
		return fmt.Errorf("medb: write recovery administrator: %w", err)
	}
	if err := auth.state.Set(stateID, stateRecord{Version: 1, AuthInitialized: true}); err != nil {
		return fmt.Errorf("medb: write authentication state: %w", err)
	}
	//lint:ignore SA4023 DB.Close returns nil after a clean close; staticcheck loses that fact across the nested module boundary.
	if err := db.Close(); err != nil {
		return err
	}
	closed = true
	_, err = fmt.Fprintf(stdout, "user_id=%s\ntoken_id=%s\ntoken=%s\n", uid, tid, token)
	return err
}
