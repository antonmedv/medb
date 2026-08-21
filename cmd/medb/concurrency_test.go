package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/antonmedv/medb"
)

// startTestServer runs an unauthenticated API over a real TCP listener. A real
// connection is required to observe blocking: httptest.ResponseRecorder accepts
// a whole response without ever making a handler wait on a socket.
func startTestServer(t *testing.T) (string, *medb.DB) {
	t.Helper()
	dir := t.TempDir()
	db, err := medb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil && !errorIsClosed(err) {
			t.Errorf("close database: %v", err)
		}
	})
	cfg := testServeConfig(dir)
	cfg.noAuth = true
	server := httptest.NewServer(newAPIServer(db, cfg, newLogger(&bytes.Buffer{})))
	t.Cleanup(server.Close)
	return server.URL, db
}

func errorIsClosed(err error) bool {
	return err != nil && strings.Contains(err.Error(), "closed")
}

// seedLargeCollection stores enough data that a scan response cannot fit in the
// kernel socket buffers, so the scan handler has to block on a write.
func seedLargeCollection(t *testing.T, db *medb.DB, collection string) {
	t.Helper()
	document, err := json.Marshal(strings.Repeat("x", 64<<10))
	if err != nil {
		t.Fatal(err)
	}
	docs := medb.C[json.RawMessage](db, collection)
	for i := range 256 {
		if err := docs.Set(fmt.Sprintf("%04d", i), document); err != nil {
			t.Fatal(err)
		}
	}
}

// A scan streams under no server-wide lock, so a client which stops consuming
// the stream must not stall unrelated writes.
func TestScanInFlightDoesNotBlockWrites(t *testing.T) {
	base, db := startTestServer(t)
	seedLargeCollection(t, db, "docs")

	response, err := http.Post(base+"/v1/scan", "application/json",
		strings.NewReader(`{"collection":"docs"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("scan: status %d", response.StatusCode)
	}
	// Read one byte so the handler is certainly streaming, then stop consuming.
	if _, err := response.Body.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	written, err := client.Post(base+"/v1/set", "application/json",
		strings.NewReader(`{"collection":"docs","id":"during-scan","document":1}`))
	if err != nil {
		t.Fatalf("write blocked by an in-flight scan: %v", err)
	}
	defer written.Body.Close()
	if written.StatusCode != http.StatusNoContent {
		t.Fatalf("set during scan: status %d", written.StatusCode)
	}
}

// A request body is read inside the handler under no server-wide lock, so a
// client which stalls part way through one must not stall other requests.
func TestPartialRequestBodyDoesNotBlockOtherRequests(t *testing.T) {
	base, _ := startTestServer(t)
	address := strings.TrimPrefix(base, "http://")

	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	body := `{"collection":"docs","id":"stalled","document":1}`
	if _, err := fmt.Fprintf(conn, "POST /v1/set HTTP/1.1\r\nHost: %s\r\n"+
		"Content-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
		address, len(body), body[:8]); err != nil {
		t.Fatal(err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	for _, target := range []string{"/healthz", "/v1/collections"} {
		response, err := client.Get(base + target)
		if err != nil {
			t.Fatalf("%s blocked by a stalled request body: %v", target, err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d", target, response.StatusCode)
		}
	}

	written, err := client.Post(base+"/v1/set", "application/json",
		strings.NewReader(`{"collection":"docs","id":"unrelated","document":2}`))
	if err != nil {
		t.Fatalf("write blocked by a stalled request body: %v", err)
	}
	defer written.Body.Close()
	if written.StatusCode != http.StatusNoContent {
		t.Fatalf("unrelated set: status %d", written.StatusCode)
	}
}

// Mutations still run concurrently with each other.
func TestConcurrentWritesAllSucceed(t *testing.T) {
	base, _ := startTestServer(t)
	const writers = 16
	errs := make(chan error, writers)
	for i := range writers {
		go func() {
			client := &http.Client{Timeout: 20 * time.Second}
			response, err := client.Post(base+"/v1/set", "application/json",
				strings.NewReader(fmt.Sprintf(`{"collection":"docs","id":"%d","document":%d}`, i, i)))
			if err != nil {
				errs <- err
				return
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusNoContent {
				errs <- fmt.Errorf("set %d: status %d", i, response.StatusCode)
				return
			}
			errs <- nil
		}()
	}
	for range writers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	response, err := http.Post(base+"/v1/count", "application/json",
		strings.NewReader(`{"collection":"docs"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), fmt.Sprintf(`"count":%d`, writers)) {
		t.Fatalf("count after concurrent writes: %s", payload)
	}
}
