package wal

import (
	"errors"
	"sync"
	"testing"
)

type fakeFile struct {
	mu        sync.Mutex
	written   []byte
	syncs     int
	writeErr  error
	syncErr   error
	truncErr  error
	closeErr  error
	closeDone bool
}

func (f *fakeFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	f.written = append(f.written, p...)
	return len(p), nil
}

func (f *fakeFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncs++
	return f.syncErr
}

func (f *fakeFile) Truncate(int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.truncErr != nil {
		return f.truncErr
	}
	f.written = nil
	return nil
}

func (f *fakeFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeDone = true
	return f.closeErr
}

func (f *fakeFile) fail(write, sync error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeErr, f.syncErr = write, sync
}

func (f *fakeFile) contents() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return string(f.written)
}

func (f *fakeFile) syncCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.syncs
}

// A failed commit must poison the log: every later Enqueue reports the
// original failure instead of silently accepting writes it cannot durably
// store.
func TestSyncFailureIsFailStop(t *testing.T) {
	want := errors.New("sync failed")
	f := &fakeFile{syncErr: want}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("a")).Wait(); !errors.Is(err, want) {
		t.Fatalf("first Enqueue: got %v, want %v", err, want)
	}
	for i := range 3 {
		if err := l.Enqueue([]byte("b")).Wait(); !errors.Is(err, want) {
			t.Fatalf("Enqueue %d after failure: got %v, want %v", i, err, want)
		}
	}
	if n := f.syncCount(); n != 1 {
		t.Fatalf("%d syncs attempted, want 1: the log kept writing after a failure", n)
	}
	if l.Size() != 0 {
		t.Fatalf("Size()=%d after a failed commit, want 0", l.Size())
	}
}

func TestWriteFailureIsFailStop(t *testing.T) {
	want := errors.New("write failed")
	f := &fakeFile{writeErr: want}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("a")).Wait(); !errors.Is(err, want) {
		t.Fatalf("first Enqueue: got %v, want %v", err, want)
	}
	if err := l.Enqueue([]byte("b")).Wait(); !errors.Is(err, want) {
		t.Fatalf("second Enqueue: got %v, want %v", err, want)
	}
	if got := f.contents(); got != "" {
		t.Fatalf("log holds %q after a failed write", got)
	}
	if n := f.syncCount(); n != 0 {
		t.Fatalf("%d syncs after a failed write, want 0", n)
	}
}

func TestFailureReachesEveryWaiter(t *testing.T) {
	want := errors.New("sync failed")
	f := &fakeFile{syncErr: want}
	l := newLog(f, 0)
	defer l.Close()

	tickets := make([]Ticket, 8)
	for i := range tickets {
		tickets[i] = l.Enqueue([]byte("rec"))
	}
	for i, ticket := range tickets {
		if err := ticket.Wait(); !errors.Is(err, want) {
			t.Fatalf("waiter %d: got %v, want %v", i, err, want)
		}
	}
}

func TestFailureAfterSuccessfulCommits(t *testing.T) {
	f := &fakeFile{}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("kept")).Wait(); err != nil {
		t.Fatal(err)
	}
	size := l.Size()
	if size != int64(len("kept\n")) {
		t.Fatalf("Size()=%d, want %d", size, len("kept\n"))
	}

	want := errors.New("sync failed")
	f.fail(nil, want)
	if err := l.Enqueue([]byte("lost")).Wait(); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	if l.Size() != size {
		t.Fatalf("Size()=%d after a failed commit, want %d unchanged", l.Size(), size)
	}
	if got := f.contents(); got != "kept\nlost\n" {
		t.Fatalf("log holds %q; the failed record reached the file but was not counted", got)
	}
}

func TestTruncateFailure(t *testing.T) {
	want := errors.New("truncate failed")
	f := &fakeFile{truncErr: want}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	if err := l.Exec(l.Truncate); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	if l.Size() == 0 {
		t.Fatal("Size() reset despite a failed truncate")
	}
}

// A poisoned log must not run a flush: medb truncates the log inside Exec, so
// running fn here would make a write that failed durable and destroy the log.
func TestExecSkippedAfterFailure(t *testing.T) {
	want := errors.New("sync failed")
	f := &fakeFile{syncErr: want}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("a")).Wait(); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	ran := false
	err := l.Exec(func() error {
		ran = true
		return nil
	})
	if !errors.Is(err, want) {
		t.Fatalf("Exec returned %v, want %v", err, want)
	}
	if ran {
		t.Fatal("fn ran on a poisoned log")
	}
}

func TestExecRunsWhileHealthy(t *testing.T) {
	f := &fakeFile{}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("a")).Wait(); err != nil {
		t.Fatal(err)
	}
	ran := false
	if err := l.Exec(func() error { ran = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("fn did not run on a healthy log")
	}
}

func TestTruncateSyncs(t *testing.T) {
	f := &fakeFile{}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	before := f.syncCount()
	if err := l.Exec(l.Truncate); err != nil {
		t.Fatal(err)
	}
	if f.syncCount() == before {
		t.Fatal("Truncate did not sync the new length")
	}
}

func TestTruncateSyncFailure(t *testing.T) {
	want := errors.New("sync failed")
	f := &fakeFile{}
	l := newLog(f, 0)
	defer l.Close()

	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	f.fail(nil, want)
	if err := l.Exec(l.Truncate); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	if l.Size() == 0 {
		t.Fatal("Size() reset despite a failed sync")
	}
}
