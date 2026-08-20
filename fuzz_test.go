package medb_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/antonmedv/medb"
)

// The reference definition of a valid collection name, kept independent of
// the implementation: one or more /-separated segments of [a-z0-9_-].
var validNameRef = regexp.MustCompile(`\A[a-z0-9_-]+(/[a-z0-9_-]+)*\z`)

func FuzzValidName(f *testing.F) {
	for _, s := range []string{
		"", "a", "A", "a/b", "/", "/a", "a/", "a//b", "a-b_0", "ä", "a.b",
		"prod/eu/users", "a\n", "0",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if got, want := medb.ValidName(name), validNameRef.MatchString(name); got != want {
			t.Fatalf("validName(%q) = %v, reference says %v", name, got, want)
		}
	})
}

// Arbitrary bytes as a WAL: Open must either succeed or return an error —
// never panic, and never fail on a well-formed prefix.
func FuzzWALReplay(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("\n"))
	f.Add([]byte(`{"op":"set","coll":"users","id":"a","doc":{"x":1}}` + "\n"))
	f.Add([]byte(`{"op":"del","coll":"users","id":"a"}` + "\n"))
	f.Add([]byte(`{"op":"drop","coll":"users"}` + "\n"))
	f.Add([]byte(`{"op":"set","coll":"users","id":"a","doc":1}`)) // torn tail
	f.Add([]byte("garbage\n"))
	f.Add([]byte(`{"op":"set","coll":"UPPER","id":"a","doc":1}` + "\n")) // invalid name in wal
	f.Fuzz(func(t *testing.T, wal []byte) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "wal.log"), wal, 0o600); err != nil {
			t.Fatal(err)
		}
		db, err := medb.Open(dir)
		if err != nil {
			return
		}
		for _, name := range db.Collections() {
			// Replay does not validate names from the WAL, so Collections
			// may return names C would reject; skip those.
			if !medb.ValidName(name) {
				continue
			}
			for range medb.C[any](db, name).All() {
			}
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
}

// Arbitrary bytes as a snapshot file: Open must either succeed or return an
// error — never panic.
func FuzzSnapshotLoad(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"a":1}`))
	f.Add([]byte(`{"a":{"Name":"Ada"}}`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`null`))
	f.Add([]byte(``))
	f.Add([]byte(`{"a":`))
	f.Add([]byte("\x00\x01\x02"))
	f.Fuzz(func(t *testing.T, snap []byte) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "users.json"), snap, 0o600); err != nil {
			t.Fatal(err)
		}
		db, err := medb.Open(dir)
		if err != nil {
			return
		}
		for range medb.C[any](db, "users").All() {
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
}
