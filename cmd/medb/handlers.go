package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/antonmedv/medb"
)

var errMalformedAuthRecord = errors.New("malformed authentication record")

type documentResponse struct {
	Document json.RawMessage `json:"document"`
}

type scanRecord struct {
	ID       string          `json:"id"`
	Document json.RawMessage `json:"document"`
}

type userView struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Role      role   `json:"role"`
	Disabled  bool   `json:"disabled"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type tokenView struct {
	TokenID   string  `json:"token_id"`
	UserID    string  `json:"user_id"`
	Label     string  `json:"label"`
	CreatedAt string  `json:"created_at"`
	ExpiresAt *string `json:"expires_at"`
}

func viewUser(id string, user userRecord) userView {
	return userView{
		ID:        id,
		Name:      user.Name,
		Role:      user.Role,
		Disabled:  user.Disabled,
		CreatedAt: user.CreatedAt,
		UpdatedAt: user.UpdatedAt,
	}
}

func viewToken(id string, token tokenRecord) tokenView {
	return tokenView{
		TokenID:   id,
		UserID:    token.UserID,
		Label:     token.Label,
		CreatedAt: token.CreatedAt,
		ExpiresAt: token.ExpiresAt,
	}
}

func (s *apiServer) handleCollections(w http.ResponseWriter, _ *http.Request, _ principal) {
	collections := make([]string, 0)
	for _, name := range s.db.Collections() {
		if !reservedCollection(name) {
			collections = append(collections, name)
		}
	}
	writeJSON(w, http.StatusOK, struct {
		Collections []string `json:"collections"`
	}{Collections: collections})
}

func (s *apiServer) handleGet(w http.ResponseWriter, r *http.Request, _ principal) {
	fields, fail := readObject(w, r, s.cfg.maxRequestSize,
		[]string{"collection", "id"}, []string{"collection", "id"})
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	collection, id, fail := documentTarget(fields, s.cfg.maxIDSize)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	document, err := medb.C[json.RawMessage](s.db, collection).Get(id)
	switch {
	case errors.Is(err, medb.ErrNotFound):
		writeFailure(w, failNotFound)
	case errors.Is(err, medb.ErrClosed):
		writeFailure(w, failUnavailable)
	case err != nil:
		writeFailure(w, failInternal)
	default:
		writeJSON(w, http.StatusOK, documentResponse{Document: document})
	}
}

func (s *apiServer) handleSet(w http.ResponseWriter, r *http.Request, _ principal) {
	fields, fail := readObject(w, r, s.cfg.maxRequestSize,
		[]string{"collection", "id", "document"}, []string{"collection", "id", "document"})
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	collection, id, fail := documentTarget(fields, s.cfg.maxIDSize)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	if err := medb.C[json.RawMessage](s.db, collection).Set(id, fields["document"]); err != nil {
		s.mutationError(w, err)
		return
	}
	writeNoContent(w)
}

func (s *apiServer) handleDelete(w http.ResponseWriter, r *http.Request, _ principal) {
	fields, fail := readObject(w, r, s.cfg.maxRequestSize,
		[]string{"collection", "id"}, []string{"collection", "id"})
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	collection, id, fail := documentTarget(fields, s.cfg.maxIDSize)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	if err := medb.C[json.RawMessage](s.db, collection).Delete(id); err != nil {
		s.mutationError(w, err)
		return
	}
	writeNoContent(w)
}

func (s *apiServer) handleHas(w http.ResponseWriter, r *http.Request, _ principal) {
	fields, fail := readObject(w, r, s.cfg.maxRequestSize,
		[]string{"collection", "id"}, []string{"collection", "id"})
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	collection, id, fail := documentTarget(fields, s.cfg.maxIDSize)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Exists bool `json:"exists"`
	}{Exists: medb.C[json.RawMessage](s.db, collection).Has(id)})
}

func (s *apiServer) handleCount(w http.ResponseWriter, r *http.Request, _ principal) {
	fields, fail := readObject(w, r, s.cfg.maxRequestSize,
		[]string{"collection"}, []string{"collection"})
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	collection, fail := collectionTarget(fields)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Count int `json:"count"`
	}{Count: medb.C[json.RawMessage](s.db, collection).Count()})
}

func (s *apiServer) handleScan(w http.ResponseWriter, r *http.Request, _ principal) {
	fields, fail := readObject(w, r, s.cfg.maxRequestSize,
		[]string{"collection"}, []string{"collection"})
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	collection, fail := collectionTarget(fields)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	flusher, _ := w.(http.Flusher)
	for id, document := range medb.C[json.RawMessage](s.db, collection).All() {
		select {
		case <-r.Context().Done():
			return
		default:
		}
		if err := encoder.Encode(scanRecord{ID: id, Document: document}); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
}

func (s *apiServer) handleDrop(w http.ResponseWriter, r *http.Request, _ principal) {
	fields, fail := readObject(w, r, s.cfg.maxRequestSize,
		[]string{"collection"}, []string{"collection"})
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	collection, fail := collectionTarget(fields)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	if err := s.db.Drop(collection); err != nil {
		s.mutationError(w, err)
		return
	}
	writeNoContent(w)
}

func (s *apiServer) handleUsers(w http.ResponseWriter, _ *http.Request, _ principal) {
	users := make([]userView, 0)
	for id, user := range s.auth.users.All() {
		if validateUserRecord(id, user) != nil {
			writeFailure(w, failInternal)
			return
		}
		users = append(users, viewUser(id, user))
	}
	writeJSON(w, http.StatusOK, struct {
		Users []userView `json:"users"`
	}{Users: users})
}

func (s *apiServer) handleUserCreate(w http.ResponseWriter, r *http.Request, _ principal) {
	fields, fail := readObject(w, r, s.cfg.maxRequestSize,
		[]string{"name", "role"}, []string{"name", "role"})
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	name, fail := stringField(fields, "name")
	if fail != nil || validateLabel(name) != nil {
		writeFailure(w, failInvalidRequest)
		return
	}
	roleValue, fail := stringField(fields, "role")
	userRole := role(roleValue)
	if fail != nil || !validRole(userRole) {
		writeFailure(w, failInvalidRequest)
		return
	}
	id := uniqueUserID(s.auth.users)
	now := nowTimestamp()
	user := userRecord{Name: name, Role: userRole, CreatedAt: now, UpdatedAt: now}
	if err := s.auth.users.Set(id, user); err != nil {
		s.mutationError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, struct {
		User userView `json:"user"`
	}{User: viewUser(id, user)})
}

func (s *apiServer) handleUserUpdate(w http.ResponseWriter, r *http.Request, _ principal) {
	fields, fail := readObject(w, r, s.cfg.maxRequestSize,
		[]string{"user_id", "role", "disabled"}, []string{"user_id"})
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	uid, fail := stringField(fields, "user_id")
	if fail != nil || !validHexID(uid, 32) {
		writeFailure(w, failInvalidRequest)
		return
	}
	var newRole *role
	if _, ok := fields["role"]; ok {
		value, fieldFailure := stringField(fields, "role")
		parsed := role(value)
		if fieldFailure != nil || !validRole(parsed) {
			writeFailure(w, failInvalidRequest)
			return
		}
		newRole = &parsed
	}
	disabled, fail := optionalBoolField(fields, "disabled")
	if fail != nil || newRole == nil && disabled == nil {
		writeFailure(w, failInvalidRequest)
		return
	}
	err := s.auth.users.Update(uid, func(user userRecord) (userRecord, error) {
		if validateUserRecord(uid, user) != nil {
			return user, errMalformedAuthRecord
		}
		if newRole != nil {
			user.Role = *newRole
		}
		if disabled != nil {
			user.Disabled = *disabled
		}
		user.UpdatedAt = nowTimestamp()
		return user, nil
	})
	switch {
	case errors.Is(err, medb.ErrNotFound):
		writeFailure(w, failNotFound)
		return
	case errors.Is(err, errMalformedAuthRecord):
		writeFailure(w, failInternal)
		return
	case err != nil:
		s.mutationError(w, err)
		return
	}
	user, err := s.auth.users.Get(uid)
	if err != nil {
		writeFailure(w, failInternal)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		User userView `json:"user"`
	}{User: viewUser(uid, user)})
}

func (s *apiServer) handleTokenCreate(w http.ResponseWriter, r *http.Request, _ principal) {
	fields, fail := readObject(w, r, s.cfg.maxRequestSize,
		[]string{"user_id", "label", "expires_at"}, []string{"user_id", "label"})
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	uid, fail := stringField(fields, "user_id")
	if fail != nil || !validHexID(uid, 32) {
		writeFailure(w, failInvalidRequest)
		return
	}
	label, fail := stringField(fields, "label")
	if fail != nil || validateLabel(label) != nil {
		writeFailure(w, failInvalidRequest)
		return
	}
	user, err := s.auth.users.Get(uid)
	switch {
	case errors.Is(err, medb.ErrNotFound):
		writeFailure(w, failNotFound)
		return
	case err != nil || validateUserRecord(uid, user) != nil:
		writeFailure(w, failInternal)
		return
	case user.Disabled:
		writeFailure(w, failUserDisabled)
		return
	}

	created := time.Now().UTC()
	createdAt := created.Format(time.RFC3339Nano)
	expiresAt, fail := optionalStringField(fields, "expires_at")
	if fail != nil {
		writeFailure(w, failInvalidRequest)
		return
	}
	if expiresAt != nil {
		expires, err := parseUTCTimestamp(*expiresAt)
		if err != nil || !expires.After(created) {
			writeFailure(w, failInvalidRequest)
			return
		}
		canonical := expires.UTC().Format(time.RFC3339Nano)
		expiresAt = &canonical
	}
	token, tid, err := uniqueToken(s.auth.tokens)
	if err != nil {
		writeFailure(w, failInternal)
		return
	}
	record := tokenRecord{UserID: uid, Label: label, CreatedAt: createdAt, ExpiresAt: expiresAt}
	if err := s.auth.tokens.Set(tid, record); err != nil {
		s.mutationError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, struct {
		TokenID   string  `json:"token_id"`
		Token     string  `json:"token"`
		UserID    string  `json:"user_id"`
		Label     string  `json:"label"`
		CreatedAt string  `json:"created_at"`
		ExpiresAt *string `json:"expires_at"`
	}{
		TokenID: tid, Token: token, UserID: uid, Label: label,
		CreatedAt: createdAt, ExpiresAt: expiresAt,
	})
}

func (s *apiServer) handleTokens(w http.ResponseWriter, r *http.Request, _ principal) {
	fields, fail := readObject(w, r, s.cfg.maxRequestSize,
		[]string{"user_id"}, []string{"user_id"})
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	uid, fail := stringField(fields, "user_id")
	if fail != nil || !validHexID(uid, 32) {
		writeFailure(w, failInvalidRequest)
		return
	}
	user, err := s.auth.users.Get(uid)
	if errors.Is(err, medb.ErrNotFound) {
		writeFailure(w, failNotFound)
		return
	}
	if err != nil || validateUserRecord(uid, user) != nil {
		writeFailure(w, failInternal)
		return
	}
	tokens := make([]tokenView, 0)
	for tid, token := range s.auth.tokens.All() {
		if validateTokenRecord(tid, token) != nil {
			writeFailure(w, failInternal)
			return
		}
		if token.UserID == uid {
			tokens = append(tokens, viewToken(tid, token))
		}
	}
	writeJSON(w, http.StatusOK, struct {
		Tokens []tokenView `json:"tokens"`
	}{Tokens: tokens})
}

func (s *apiServer) handleTokenRevoke(w http.ResponseWriter, r *http.Request, _ principal) {
	fields, fail := readObject(w, r, s.cfg.maxRequestSize,
		[]string{"token_id"}, []string{"token_id"})
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	tid, fail := stringField(fields, "token_id")
	if fail != nil || !validHexID(tid, 64) {
		writeFailure(w, failInvalidRequest)
		return
	}
	if err := s.auth.tokens.Delete(tid); err != nil {
		s.mutationError(w, err)
		return
	}
	writeNoContent(w)
}
