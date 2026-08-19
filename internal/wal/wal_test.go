package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/antonmedv/medb/internal/wal"
)

func open(t *testing.T, path string) *wal.Log {
	t.Helper()
	l, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
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
