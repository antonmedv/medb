package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antonmedv/medb"
)

// HTTP transport limits.
//
// readTimeout bounds a complete request read so a client which stalls part way
// through a body cannot hold a connection open indefinitely, and writeTimeout
// does the same for a response. A scan streams for as long as its client keeps
// making progress: it extends its own write deadline before each record, so
// writeTimeout bounds the gap between records rather than the whole stream.
const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 60 * time.Second
	writeTimeout      = 60 * time.Second
	idleTimeout       = 60 * time.Second
	maxHeaderBytes    = 64 << 10
)

type route struct {
	method  string
	role    role
	handler func(http.ResponseWriter, *http.Request)
}

type apiServer struct {
	db     *medb.DB
	auth   *authStore
	cfg    serveConfig
	log    *slog.Logger
	routes map[string]route

	shuttingDown atomic.Bool
	unavailable  atomic.Bool
	failureOnce  sync.Once
	failureCh    chan error
}

func newAPIServer(db *medb.DB, cfg serveConfig, logger *slog.Logger) *apiServer {
	s := &apiServer{
		db:        db,
		auth:      newAuthStore(db),
		cfg:       cfg,
		log:       logger,
		failureCh: make(chan error, 1),
	}
	s.routes = s.buildRoutes()
	return s
}

// buildRoutes returns the route table. The table depends only on configuration,
// so it is built once and then read without synchronization.
func (s *apiServer) buildRoutes() map[string]route {
	routes := map[string]route{
		"/v1/collections": {method: http.MethodGet, role: roleReader, handler: s.handleCollections},
		"/v1/get":         {method: http.MethodPost, role: roleReader, handler: s.handleGet},
		"/v1/set":         {method: http.MethodPost, role: roleWriter, handler: s.handleSet},
		"/v1/delete":      {method: http.MethodPost, role: roleWriter, handler: s.handleDelete},
		"/v1/has":         {method: http.MethodPost, role: roleReader, handler: s.handleHas},
		"/v1/count":       {method: http.MethodPost, role: roleReader, handler: s.handleCount},
		"/v1/scan":        {method: http.MethodPost, role: roleReader, handler: s.handleScan},
		"/v1/drop":        {method: http.MethodPost, role: roleAdmin, handler: s.handleDrop},
	}
	if s.cfg.noAuth {
		return routes
	}
	routes["/v1/auth/users"] = route{method: http.MethodGet, role: roleAdmin, handler: s.handleUsers}
	routes["/v1/auth/users/create"] = route{method: http.MethodPost, role: roleAdmin, handler: s.handleUserCreate}
	routes["/v1/auth/users/update"] = route{method: http.MethodPost, role: roleAdmin, handler: s.handleUserUpdate}
	routes["/v1/auth/tokens/create"] = route{method: http.MethodPost, role: roleAdmin, handler: s.handleTokenCreate}
	routes["/v1/auth/tokens/list"] = route{method: http.MethodPost, role: roleAdmin, handler: s.handleTokens}
	routes["/v1/auth/tokens/revoke"] = route{method: http.MethodPost, role: roleAdmin, handler: s.handleTokenRevoke}
	return routes
}

// ServeHTTP dispatches one request. Handlers run concurrently; the server
// relies on MeDB's own locking for thread safety and operation ordering, and
// holds no lock of its own across a body read or a response write.
//
// Cache-Control is set here for every response the server produces.
func (s *apiServer) ServeHTTP(base http.ResponseWriter, r *http.Request) {
	w := &trackedResponseWriter{ResponseWriter: base}
	w.Header().Set("Cache-Control", "no-store")
	defer func() {
		value := recover()
		if value == nil {
			return
		}
		// A handler which aborts deliberately is not a server fault; let
		// net/http drop the connection without a report.
		if value == http.ErrAbortHandler {
			panic(value)
		}
		s.log.Error("handler panic",
			"method", r.Method, "path", r.URL.Path, "panic", fmt.Sprint(value),
			"stack", string(debug.Stack()))
		if !w.wroteHeader {
			writeFailure(w, failInternal)
		}
	}()

	if r.URL.Path == "/healthz" {
		s.handleHealth(w, r)
		return
	}
	if r.URL.Path != "/v1" && !strings.HasPrefix(r.URL.Path, "/v1/") {
		writeFailure(w, failRouteNotFound)
		return
	}
	if s.shuttingDown.Load() || s.unavailable.Load() {
		writeFailure(w, failUnavailable)
		return
	}

	// Authentication precedes route lookup so an unknown path under /v1 cannot
	// be probed without a credential.
	actor := roleAdmin
	if !s.cfg.noAuth {
		var authFailure *apiFailure
		actor, authFailure = s.authenticateRequest(r)
		if authFailure != nil {
			writeFailure(w, authFailure)
			return
		}
	}
	selected, exists := s.routes[r.URL.Path]
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
	if !roleAllows(actor, selected.role) {
		writeFailure(w, failForbidden)
		return
	}
	selected.handler(w, r)
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

func (s *apiServer) authenticateRequest(r *http.Request) (role, *apiFailure) {
	values := r.Header.Values("Authorization")
	if len(values) == 0 {
		return "", failAuthRequired
	}
	if len(values) != 1 {
		return "", failInvalidToken
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", failInvalidToken
	}
	actor, ok := s.auth.authenticate(parts[1], time.Now())
	if !ok {
		return "", failInvalidToken
	}
	return actor, nil
}

func (s *apiServer) markStorageFailure(err error) {
	s.failureOnce.Do(func() {
		s.unavailable.Store(true)
		s.log.Error("storage failure; server is shutting down", "error", err)
		select {
		case s.failureCh <- err:
		default:
		}
	})
}

// internalError reports a server-side fault. The response carries no detail;
// the cause is logged instead. Never pass a value derived from a credential or
// a stored document.
func (s *apiServer) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.log.Error("internal error", "method", r.Method, "path", r.URL.Path, "error", err)
	writeFailure(w, failInternal)
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

func newLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func prepareAuthentication(db *medb.DB, cfg serveConfig, getenv envLookup, logger *slog.Logger) error {
	if cfg.noAuth {
		logger.Warn("authentication is disabled; all data endpoints are open")
		return nil
	}
	return initializeAuth(db, newAuthStore(db), getenv, logger)
}

type trackedResponseWriter struct {
	http.ResponseWriter
	wroteHeader bool
}

// Unwrap lets http.ResponseController reach the deadline and flush methods of
// the writer underneath this one.
func (w *trackedResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
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

func serve(ctx context.Context, cfg serveConfig, logger *slog.Logger, getenv envLookup) (err error) {
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

	if err := prepareAuthentication(db, cfg, getenv, logger); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("medb: listen on %s: %w", cfg.listen, err)
	}

	api := newAPIServer(db, cfg, logger)
	httpServer := &http.Server{
		Handler:           api,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpServer.Serve(listener)
	}()
	logger.Info("listening", "address", listener.Addr().String())

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
