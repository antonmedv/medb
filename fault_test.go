package medb

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
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

// powerLog simulates a disk with a volatile write cache: Write lands in the
// cache, Sync moves the cache to the durable image. A power cut keeps the
// durable image plus an arbitrary prefix of the cache — exactly the fsync
// contract. Unlike the SIGKILL harness in crash_test.go (a killed process
// keeps the OS page cache, so even unsynced writes survive), this catches a
// missing or misplaced Sync in the commit path.
type powerLog struct {
	inner file // real WAL handle, kept only so Close releases it
	mu    sync.Mutex
	disk  []byte // synced: survives the cut
	cache []byte // written, not synced: may partially survive
}

func (p *powerLog) Write(b []byte) (int, error) {
	p.mu.Lock()
	p.cache = append(p.cache, b...)
	p.mu.Unlock()
	return len(b), nil
}

func (p *powerLog) Sync() error {
	p.mu.Lock()
	p.disk = append(p.disk, p.cache...)
	p.cache = p.cache[:0]
	p.mu.Unlock()
	return nil
}

func (p *powerLog) Truncate(int64) error {
	p.mu.Lock()
	p.disk, p.cache = nil, nil
	p.mu.Unlock()
	return nil
}

func (p *powerLog) Close() error { return p.inner.Close() }

// cut returns the two survival layers at the moment of power loss.
func (p *powerLog) cut() (disk, cache []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.disk), slices.Clone(p.cache)
}

// Every Set acknowledged before a power cut must be recoverable from the
// bytes that were synced at that moment, regardless of how much of the
// unsynced tail survives or where it tears.
func TestPowerLossDurability(t *testing.T) {
	for round, cutAfter := range []int64{10, 40, 80} {
		t.Run(fmt.Sprintf("round%d", round), func(t *testing.T) {
			pl := &powerLog{}
			db, err := Open(t.TempDir(), WithFlushInterval(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			pl.inner = db.log
			db.log = pl

			docs := C[int](db, "docs")

			var (
				mu    sync.Mutex
				acked = map[string]int{}
				total atomic.Int64
				stop  atomic.Bool
			)
			var wg sync.WaitGroup
			for w := range 4 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; !stop.Load(); i++ {
						id := fmt.Sprintf("w%d-%d", w, i)
						if err := docs.Set(id, i); err != nil {
							t.Errorf("Set(%s): %v", id, err)
							return
						}
						mu.Lock()
						acked[id] = i
						mu.Unlock()
						total.Add(1)
					}
				}()
			}
			waitUntil(t, 10*time.Second, func() bool { return total.Load() >= cutAfter })

			// Power cut: freeze the acknowledged set first, then read the
			// disk. The disk only grows, so everything acknowledged before
			// the freeze is covered by the later read.
			mu.Lock()
			ackedAtCut := maps.Clone(acked)
			mu.Unlock()
			disk, cache := pl.cut()

			stop.Store(true)
			wg.Wait()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			for name, tail := range map[string][]byte{
				"no unsynced tail":   nil,
				"torn unsynced tail": cache[:len(cache)/2],
				"full unsynced tail": cache,
			} {
				t.Run(name, func(t *testing.T) {
					dir := t.TempDir()
					wal := append(slices.Clone(disk), tail...)
					if err := os.WriteFile(filepath.Join(dir, "wal.log"), wal, 0o600); err != nil {
						t.Fatal(err)
					}
					reopened, err := Open(dir)
					if err != nil {
						t.Fatalf("Open after power cut: %v", err)
					}
					defer reopened.Close()
					docs := C[int](reopened, "docs")
					for id, want := range ackedAtCut {
						got, err := docs.Get(id)
						if err != nil {
							t.Fatalf("acknowledged %s lost after power cut: %v", id, err)
						}
						if got != want {
							t.Fatalf("%s = %d, want %d", id, got, want)
						}
					}
				})
			}
		})
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
