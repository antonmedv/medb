package medb

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type flakyLog struct {
	file
	writeErr    error                // returned by every Write when set
	truncateErr error                // returned by every Truncate when set
	syncErr     func(call int) error // consulted per Sync call (1-based)
	syncDelay   time.Duration
	syncs       atomic.Int32
}

func (f *flakyLog) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.file.Write(p)
}

func (f *flakyLog) Sync() error {
	n := int(f.syncs.Add(1))
	if f.syncDelay > 0 {
		time.Sleep(f.syncDelay)
	}
	if f.syncErr != nil {
		if err := f.syncErr(n); err != nil {
			return err
		}
	}
	return f.file.Sync()
}

func (f *flakyLog) Truncate(size int64) error {
	if f.truncateErr != nil {
		return f.truncateErr
	}
	return f.file.Truncate(size)
}

// openFlaky opens a DB with the timer trigger parked and the WAL handle
// wrapped, before any write has happened. The unsynchronized swap is safe:
// the run goroutine touches db.log only after a notify/stop channel
// operation, which establishes the happens-before edge.
func openFlaky(t *testing.T, dir string, fl *flakyLog) *DB {
	t.Helper()
	db, err := Open(dir, WithFlushInterval(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	fl.file = db.log
	db.log = fl
	return db
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

func TestWALWriteFailure(t *testing.T) {
	boom := errors.New("injected write failure")
	dir := t.TempDir()
	fl := &flakyLog{writeErr: boom}
	db := openFlaky(t, dir, fl)

	users := C[string](db, "users")
	if err := users.Set("u1", "ada"); !errors.Is(err, boom) {
		t.Fatalf("Set = %v, want the injected error", err)
	}

	// Fail-stop: every later commit reports the original failure.
	if err := users.Set("u2", "grace"); !errors.Is(err, boom) {
		t.Fatalf("Set after failure = %v, want the injected error", err)
	}
	if err := users.Delete("u1"); !errors.Is(err, boom) {
		t.Fatalf("Delete after failure = %v, want the injected error", err)
	}

	// Close surfaces the failure instead of pretending all is well.
	if err := db.Close(); !errors.Is(err, boom) {
		t.Fatalf("Close = %v, want the injected error", err)
	}

	// Nothing reached the disk, so nothing may reappear.
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if C[string](reopened, "users").Has("u1") {
		t.Fatal("unacknowledged write survived the failure")
	}
}

// Every waiter sharing a failed group commit gets the error, not just one.
func TestGroupCommitFailureReportsAllWaiters(t *testing.T) {
	boom := errors.New("injected sync failure")
	fl := &flakyLog{
		syncDelay: 100 * time.Millisecond,
		syncErr: func(call int) error {
			if call == 1 {
				return nil // first commit succeeds, slowly
			}
			return boom
		},
	}
	db := openFlaky(t, t.TempDir(), fl)
	defer func() { _ = db.Close() }()

	users := C[int](db, "users")

	first := make(chan error, 1)
	go func() { first <- users.Set("u0", 0) }()

	// Wait until the first commit is inside its slow Sync, so the batch it
	// belongs to is already sealed; everything below lands in later batches.
	waitUntil(t, 5*time.Second, func() bool { return fl.syncs.Load() >= 1 })

	const waiters = 5
	errs := make(chan error, waiters)
	var wg sync.WaitGroup
	for i := range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- users.Set(fmt.Sprintf("u%d", i+1), i)
		}()
	}
	wg.Wait()
	close(errs)

	if err := <-first; err != nil {
		t.Fatalf("first commit = %v, want nil", err)
	}
	for err := range errs {
		if !errors.Is(err, boom) {
			t.Fatalf("batched commit = %v, want the injected error", err)
		}
	}
}

// A snapshot that fails must leave the WAL intact: the acknowledged data
// stays recoverable and the error is reported by Close.
func TestSnapshotTruncateFailurePreservesData(t *testing.T) {
	boom := errors.New("injected truncate failure")
	dir := t.TempDir()
	fl := &flakyLog{truncateErr: boom}
	db := openFlaky(t, dir, fl)

	users := C[string](db, "users")
	for id, v := range map[string]string{"u1": "ada", "u2": "grace"} {
		if err := users.Set(id, v); err != nil {
			t.Fatalf("Set(%s): %v", id, err)
		}
	}

	if err := db.Close(); !errors.Is(err, boom) {
		t.Fatalf("Close = %v, want the injected error", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	users = C[string](reopened, "users")
	for id, want := range map[string]string{"u1": "ada", "u2": "grace"} {
		if got, err := users.Get(id); err != nil || got != want {
			t.Fatalf("%s = %q, %v, want %q", id, got, err, want)
		}
	}
}

// Group commit must batch: N concurrent writers may not pay N fsyncs.
func TestGroupCommitBatchesSyncs(t *testing.T) {
	fl := &flakyLog{syncDelay: time.Millisecond}
	db := openFlaky(t, t.TempDir(), fl)

	users := C[int](db, "users")
	const writers, docs = 8, 25
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range docs {
				if err := users.Set(fmt.Sprintf("w%d-%d", w, i), i); err != nil {
					t.Errorf("Set: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	commits := int32(writers * docs)
	if syncs := fl.syncs.Load(); syncs >= commits*3/4 {
		t.Fatalf("%d commits took %d syncs: group commit is not batching", commits, syncs)
	}
}
