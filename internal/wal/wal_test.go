package wal_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/antonmedv/medb/internal/wal"
)

func open(tb testing.TB, path string) *wal.Log {
	tb.Helper()
	l, err := wal.Open(path)
	if err != nil {
		tb.Fatal(err)
	}
	return l
}

func appendRaw(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func records(t *testing.T, path string) [][]byte {
	t.Helper()
	recs, err := wal.Records(path)
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

func TestEnqueueAndRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)
	want := []string{"one", "two", "three"}
	for _, s := range want {
		if err := l.Enqueue([]byte(s)).Wait(); err != nil {
			t.Fatal(err)
		}
	}
	if l.Size() == 0 {
		t.Fatal("size not tracked")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	recs := records(t, path)
	if len(recs) != len(want) {
		t.Fatalf("got %d records, want %d", len(recs), len(want))
	}
	for i, rec := range recs {
		if string(rec) != want[i] {
			t.Fatalf("record %d is %q, want %q", i, rec, want[i])
		}
	}
}

// Records batched together must survive verbatim: a buffer reused across
// commits can overwrite a batch still being written or acked.
func TestConcurrentEnqueue(t *testing.T) {
	const writers, each = 16, 50
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range each {
				if err := l.Enqueue([]byte(fmt.Sprintf("w%d-r%d", w, i))).Wait(); err != nil {
					t.Error(err)
				}
			}
		}(w)
	}
	wg.Wait()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	recs := records(t, path)
	if len(recs) != writers*each {
		t.Fatalf("got %d records, want %d", len(recs), writers*each)
	}
	seen := make(map[string]bool, len(recs))
	for _, r := range recs {
		if seen[string(r)] {
			t.Fatalf("duplicate record %q", r)
		}
		seen[string(r)] = true
	}
	for w := range writers {
		for i := range each {
			if want := fmt.Sprintf("w%d-r%d", w, i); !seen[want] {
				t.Errorf("missing record %q", want)
			}
		}
	}
}

func TestTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)
	for range 3 {
		if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	appendRaw(t, path, "torn garbage")
	if recs := records(t, path); len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
}

func TestTerminatedGarbageIsARecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)
	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	appendRaw(t, path, "{partial\n")
	recs := records(t, path)
	if len(recs) != 2 || string(recs[1]) != "{partial" {
		t.Fatalf("got %q, want the terminated line returned verbatim", recs)
	}
}

func TestExecTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l := open(t, path)
	if err := l.Enqueue([]byte("rec")).Wait(); err != nil {
		t.Fatal(err)
	}
	if err := l.Exec(l.Truncate); err != nil {
		t.Fatal(err)
	}
	if l.Size() != 0 {
		t.Fatalf("size %d after truncate", l.Size())
	}
	if err := l.Enqueue([]byte("after")).Wait(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "after\n" {
		t.Fatalf("log holds %q, want the post-truncate record at offset 0", data)
	}

	l = open(t, path)
	defer l.Close()
	if l.Size() != int64(len("after\n")) {
		t.Fatalf("size %d after reopen, want %d", l.Size(), len("after\n"))
	}
}
