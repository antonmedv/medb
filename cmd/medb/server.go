package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antonmedv/medb"
)

type route struct {
	method   string
	role     role
	mutating bool
	handler  func(http.ResponseWriter, *http.Request, principal)
}

type apiServer struct {
	db   *medb.DB
	auth *authStore
	cfg  serveConfig

	gate         sync.RWMutex
	shuttingDown atomic.Bool
	unavailable  atomic.Bool
	failureOnce  sync.Once
	failureCh    chan error
}

func newAPIServer(db *medb.DB, cfg serveConfig) *apiServer {
	return &apiServer{
		db:        db,
		auth:      newAuthStore(db),
		cfg:       cfg,
		failureCh: make(chan error, 1),
	}
}

func (s *apiServer) routes() map[string]route {
	return map[string]route{
		"/v1/collections":        {method: http.MethodGet, role: roleReader, handler: s.handleCollections},
		"/v1/get":                {method: http.MethodPost, role: roleReader, handler: s.handleGet},
		"/v1/set":                {method: http.MethodPost, role: roleWriter, mutating: true, handler: s.handleSet},
		"/v1/delete":             {method: http.MethodPost, role: roleWriter, mutating: true, handler: s.handleDelete},
		"/v1/has":                {method: http.MethodPost, role: roleReader, handler: s.handleHas},
		"/v1/count":              {method: http.MethodPost, role: roleReader, handler: s.handleCount},
		"/v1/scan":               {method: http.MethodPost, role: roleReader, handler: s.handleScan},
		"/v1/drop":               {method: http.MethodPost, role: roleAdmin, mutating: true, handler: s.handleDrop},
		"/v1/auth/users":         {method: http.MethodGet, role: roleAdmin, handler: s.handleUsers},
		"/v1/auth/users/create":  {method: http.MethodPost, role: roleAdmin, mutating: true, handler: s.handleUserCreate},
		"/v1/auth/users/update":  {method: http.MethodPost, role: roleAdmin, mutating: true, handler: s.handleUserUpdate},
		"/v1/auth/tokens/create": {method: http.MethodPost, role: roleAdmin, mutating: true, handler: s.handleTokenCreate},
		"/v1/auth/tokens/list":   {method: http.MethodPost, role: roleAdmin, handler: s.handleTokens},
		"/v1/auth/tokens/revoke": {method: http.MethodPost, role: roleAdmin, mutating: true, handler: s.handleTokenRevoke},
	}
}

func (s *apiServer) ServeHTTP(base http.ResponseWriter, r *http.Request) {
	w := &trackedResponseWriter{ResponseWriter: base}
	w.Header().Set("Cache-Control", "no-store")
	defer func() {
		if recover() != nil && !w.wroteHeader {
			writeFailure(w, failInternal)
		}
	}()

	if r.URL.Path == "/healthz" {
		s.handleHealth(w, r)
		return
	}
	underV1 := r.URL.Path == "/v1" || strings.HasPrefix(r.URL.Path, "/v1/")
	if !underV1 {
		writeFailure(w, failRouteNotFound)
		return
	}

	routes := s.routes()
	selected, exists := routes[r.URL.Path]
	mutating := exists && selected.method == r.Method && selected.mutating
	if mutating {
		s.gate.Lock()
		defer s.gate.Unlock()
	} else {
		s.gate.RLock()
		defer s.gate.RUnlock()
	}

	if s.shuttingDown.Load() || s.unavailable.Load() {
		writeFailure(w, failUnavailable)
		return
	}
	actor, authFailure := s.authenticateRequest(r)
	if authFailure != nil {
		writeFailure(w, authFailure)
		return
	}
	if !exists {
		writeFailure(w, failRouteNotFound)
		return
	}
	if r.Method != selected.method {
		w.Header().Set("Allow", selected.method)
		writeFailure(w, failMethod)
		return
	}
	if r.URL.RawQuery != "" {
		writeFailure(w, failInvalidRequest)
		return
	}
	if r.Method == http.MethodGet && (r.ContentLength != 0 || len(r.TransferEncoding) != 0) {
		writeFailure(w, failInvalidRequest)
		return
	}
	if !roleAllows(actor.role, selected.role) {
		writeFailure(w, failForbidden)
		return
	}
	selected.handler(w, r, actor)
}

func (s *apiServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeFailure(w, failMethod)
		return
	}
	if r.URL.RawQuery != "" || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		writeFailure(w, failInvalidRequest)
		return
	}
	if s.shuttingDown.Load() || s.unavailable.Load() {
		writeFailure(w, failUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Status string `json:"status"`
	}{Status: "ok"})
}

func (s *apiServer) authenticateRequest(r *http.Request) (principal, *apiFailure) {
	values := r.Header.Values("Authorization")
	if len(values) == 0 {
		return principal{}, failAuthRequired
	}
	if len(values) != 1 {
		return principal{}, failInvalidToken
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return principal{}, failInvalidToken
	}
	actor, ok := s.auth.authenticate(parts[1], time.Now())
	if !ok {
		return principal{}, failInvalidToken
	}
	return actor, nil
}

func (s *apiServer) markStorageFailure(err error) {
	s.failureOnce.Do(func() {
		s.unavailable.Store(true)
		select {
		case s.failureCh <- err:
		default:
		}
	})
}

func (s *apiServer) mutationError(w http.ResponseWriter, err error) {
	switch {
	case err == nil:
		return
	case errors.Is(err, medb.ErrTooLarge):
		writeFailure(w, failDocumentTooLarge)
	case errors.Is(err, medb.ErrClosed):
		s.unavailable.Store(true)
		writeFailure(w, failUnavailable)
	default:
		s.markStorageFailure(err)
		writeFailure(w, failStorage)
	}
}

type trackedResponseWriter struct {
	http.ResponseWriter
	wroteHeader bool
}

func (w *trackedResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *trackedResponseWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (w *trackedResponseWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func serve(ctx context.Context, cfg serveConfig, stderr io.Writer, getenv envLookup) (err error) {
	db, err := medb.Open(cfg.dir,
		medb.WithMaxDocSize(cfg.maxDocSize),
		medb.WithFlushBytes(cfg.flushBytes),
		medb.WithFlushInterval(cfg.flushInterval),
	)
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
	if err := initializeAuth(db, auth, getenv, stderr); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("medb: listen on %s: %w", cfg.listen, err)
	}

	api := newAPIServer(db, cfg)
	httpServer := &http.Server{
		Handler:           api,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpServer.Serve(listener)
	}()
	_, _ = fmt.Fprintf(stderr, "medb: listening on %s\n", listener.Addr())

	var terminalErr error
	select {
	case <-ctx.Done():
	case terminalErr = <-api.failureCh:
	case terminalErr = <-serveErr:
		if errors.Is(terminalErr, http.ErrServerClosed) {
			terminalErr = nil
		}
	}

	api.shuttingDown.Store(true)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	shutdownErr := httpServer.Shutdown(shutdownCtx)
	cancel()
	if shutdownErr != nil {
		_ = httpServer.Close()
	}
	//lint:ignore SA4023 DB.Close returns nil after a clean close; staticcheck loses that fact across the nested module boundary.
	if closeErr := db.Close(); closeErr != nil && terminalErr == nil {
		terminalErr = closeErr
	}
	closed = true
	if terminalErr != nil {
		return fmt.Errorf("medb: server stopped: %w", terminalErr)
	}
	if shutdownErr != nil {
		return fmt.Errorf("medb: graceful shutdown: %w", shutdownErr)
	}
	return nil
}
