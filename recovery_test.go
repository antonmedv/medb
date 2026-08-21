package medb_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/antonmedv/medb"
)

// copyDir copies the database directory into a fresh temp dir.
func copyDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o600)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

// neverFlush keeps the timer and size triggers out of the way so that all
// committed data lives only in the WAL until Close.
var neverFlush = []medb.Option{medb.WithFlushInterval(time.Hour)}

func TestRecoveryFromWALOnly(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, neverFlush...)
	users := medb.C[User](db, "users")
	set(t, users, "u1", User{Name: "Ada", Age: 36})
	set(t, users, "u2", User{Name: "Grace"})
	if err := users.Delete("u2"); err != nil {
		t.Fatal(err)
	}

	crashed := copyDir(t, dir)
	closeDB(t, db)

	// Precondition: the state exists only in the WAL, not in a snapshot.
	if _, err := os.Stat(filepath.Join(crashed, "users.json")); !os.IsNotExist(err) {
		t.Fatalf("users.json unexpectedly present: %v", err)
	}
	if wal, err := os.ReadFile(filepath.Join(crashed, "wal.log")); err != nil || len(wal) == 0 {
		t.Fatalf("expected a non-empty wal.log: %v", err)
	}

	db = openDB(t, crashed)
	defer closeDB(t, db)
	users = medb.C[User](db, "users")
	if got := get(t, users, "u1"); got.Name != "Ada" || got.Age != 36 {
		t.Fatalf("u1 = %+v", got)
	}
	if users.Has("u2") {
		t.Fatal("deleted u2 came back after WAL replay")
	}
}

func TestRecoverySnapshotPlusWAL(t *testing.T) {
	dir := t.TempDir()

	// First generation: snapshotted state.
	db := openDB(t, dir)
	set(t, medb.C[User](db, "users"), "old", User{Name: "Old"})
	closeDB(t, db)

	// Second generation: more writes, kept only in the WAL.
	db = openDB(t, dir, neverFlush...)
	users := medb.C[User](db, "users")
	set(t, users, "new", User{Name: "New"})
	if err := users.Delete("old"); err != nil {
		t.Fatal(err)
	}
	crashed := copyDir(t, dir)
	closeDB(t, db)

	db = openDB(t, crashed)
	defer closeDB(t, db)
	users = medb.C[User](db, "users")
	if users.Has("old") {
		t.Fatal("WAL delete not replayed over snapshot")
	}
	if got := get(t, users, "new"); got.Name != "New" {
		t.Fatalf("new = %+v", got)
	}
}

func TestSetDropSetSameWALGeneration(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, neverFlush...)
	users := medb.C[User](db, "users")
	set(t, users, "u1", User{Name: "Gone"})
	if err := db.Drop("users"); err != nil {
		t.Fatal(err)
	}
	set(t, users, "u2", User{Name: "Kept"})

	crashed := copyDir(t, dir)
	closeDB(t, db)

	for name, d := range map[string]string{"crash copy": crashed, "clean close": dir} {
		t.Run(name, func(t *testing.T) {
			db := openDB(t, d)
			defer closeDB(t, db)
			users := medb.C[User](db, "users")
			if users.Has("u1") {
				t.Fatal("document from before Drop survived")
			}
			if got := get(t, users, "u2"); got.Name != "Kept" {
				t.Fatalf("u2 = %+v", got)
			}
		})
	}
}

// TestTornWALTail cuts the WAL at various byte offsets, simulating a crash
// mid-append. Every complete, newline-terminated record must be recovered;
// the partial tail must be ignored; Open must never fail.
func TestTornWALTail(t *testing.T) {
	// Build a WAL with three set records via a crash copy.
	src := t.TempDir()
	db := openDB(t, src, neverFlush...)
	docs := medb.C[int](db, "docs")
	ids := []string{"a", "b", "c"}
	for i, id := range ids {
		set(t, docs, id, i+1)
	}
	wal, err := os.ReadFile(filepath.Join(copyDir(t, src), "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	closeDB(t, db)

	// Cut points: every record boundary, its neighbors, and a coarse sweep.
	cuts := map[int]bool{0: true, len(wal): true}
	for i, b := range wal {
		if b == '\n' {
			for _, c := range []int{i, i + 1, i + 2} {
				if c <= len(wal) {
					cuts[c] = true
				}
			}
		}
	}
	for c := 0; c < len(wal); c += 5 {
		cuts[c] = true
	}

	for cut := range cuts {
		prefix := wal[:cut]
		complete := bytes.Count(prefix, []byte{'\n'})

		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "wal.log"), prefix, 0o600); err != nil {
			t.Fatal(err)
		}
		db, err := medb.Open(dir)
		if err != nil {
			t.Fatalf("cut %d: Open failed: %v", cut, err)
		}
		docs := medb.C[int](db, "docs")
		if n := docs.Count(); n != complete {
			t.Fatalf("cut %d: Count = %d, want %d complete records", cut, n, complete)
		}
		for i := range complete {
			if v, err := docs.Get(ids[i]); err != nil || v != i+1 {
				t.Fatalf("cut %d: Get(%s) = %d, %v", cut, ids[i], v, err)
			}
		}
		closeDB(t, db)
	}
}

// A torn first record leaves no complete record for replay to mark dirty.
// Recovery must still remove it before appending: otherwise the next committed
// record is glued to the fragment and the following Open sees corrupt JSON.
func TestTornWALTailBeforeFirstRecordAllowsFutureWrites(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")
	if err := os.WriteFile(walPath, []byte(`{"op":"set","coll":"docs"`), 0o600); err != nil {
		t.Fatal(err)
	}

	db := openDB(t, dir, neverFlush...)
	set(t, medb.C[int](db, "docs"), "kept", 42)
	crashed := copyDir(t, dir)
	closeDB(t, db)

	db = openDB(t, crashed)
	defer closeDB(t, db)
	if got := get(t, medb.C[int](db, "docs"), "kept"); got != 42 {
		t.Fatalf("kept = %d, want 42", got)
	}
}

func TestCorruptWALRecord(t *testing.T) {
	// A complete (newline-terminated) but malformed record is corruption,
	// not a torn tail, and must fail loudly. Valid JSON with a bogus
	// collection name or a missing document is just as corrupt as garbage:
	// accepting it would plant undecodable state (a nil doc makes All panic)
	// or write snapshot files outside the collection namespace.
	records := map[string]string{
		"not json":     "{oops\n",
		"invalid name": `{"op":"set","coll":"UPPER","id":"a","doc":1}` + "\n",
		"missing doc":  `{"op":"set","coll":"users","id":"a"}` + "\n",
	}
	for name, rec := range records {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "wal.log"), []byte(rec), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := medb.Open(dir)
			if err == nil {
				t.Fatal("Open succeeded on corrupt WAL")
			}
			if !strings.Contains(err.Error(), "corrupt wal record") {
				t.Fatalf("error does not identify the corruption: %v", err)
			}
		})
	}
}

func TestInvalidNameInWALIsCorruption(t *testing.T) {
	dir := t.TempDir()
	// A tampered-but-valid record with a traversal name must fail Open:
	// applied, it would make the next snapshot write outside the DB
	// directory and Collections return names C panics on.
	rec := `{"op":"set","coll":"../evil","id":"x","doc":{}}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, "wal.log"), []byte(rec), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := medb.Open(dir)
	if err == nil {
		t.Fatal("Open succeeded on WAL with a traversal collection name")
	}
	if !strings.Contains(err.Error(), "invalid collection name") {
		t.Fatalf("error does not identify the invalid name: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "..", "evil.json")); !os.IsNotExist(err) {
		t.Fatalf("evil.json escaped the DB directory: %v", err)
	}
}

func TestCorruptSnapshot(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "users.json"), []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := medb.Open(dir)
	if err == nil {
		t.Fatal("Open succeeded on corrupt snapshot")
	}
	if !strings.Contains(err.Error(), "corrupt snapshot") {
		t.Fatalf("error does not identify the corruption: %v", err)
	}
	if !strings.Contains(err.Error(), "users.json") {
		t.Fatalf("error does not name the corrupt file: %v", err)
	}
}

func TestForeignFilesIgnored(t *testing.T) {
	dir := t.TempDir()
	foreign := []string{"notes.txt", "Bad.json", "README"}
	for _, name := range foreign {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("keep me"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(dir, "Nope"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "Nope", "users.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "good.json"), []byte(`{"x":{"Name":"Ada"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	db := openDB(t, dir)
	if got, want := db.Collections(), []string{"good"}; len(got) != 1 || got[0] != want[0] {
		t.Fatalf("Collections = %v, want %v", got, want)
	}
	if got := get(t, medb.C[User](db, "good"), "x"); got.Name != "Ada" {
		t.Fatalf("good/x = %+v", got)
	}
	closeDB(t, db)

	// Foreign files must survive snapshots untouched.
	for _, name := range foreign {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(data) != "keep me" {
			t.Fatalf("foreign file %s damaged: %q, %v", name, data, err)
		}
	}
}

// Open must pin the directory it was given. A relative dir plus a later
// os.Chdir used to send the snapshot into the new working directory, truncate
// the original WAL to zero.
func TestRelativeDirSurvivesChdir(t *testing.T) {
	root, elsewhere := t.TempDir(), t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(cwd); err != nil {
			t.Fatal(err)
		}
	})
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}

	db := openDB(t, "data", neverFlush...)
	set(t, medb.C[string](db, "users"), "id", "acked")
	if err := os.Chdir(elsewhere); err != nil {
		t.Fatal(err)
	}
	closeDB(t, db) // must not report "remove data/lock: no such file or directory"

	if ents, err := os.ReadDir(elsewhere); err != nil || len(ents) != 0 {
		t.Fatalf("Close wrote into the new working directory: %v, %v", ents, err)
	}
	dir := filepath.Join(root, "data")
	if err := os.Chdir(cwd); err != nil {
		t.Fatal(err)
	}
	db = openDB(t, dir)
	defer closeDB(t, db)
	if got := get(t, medb.C[string](db, "users"), "id"); got != "acked" {
		t.Fatalf("got %q, want %q", got, "acked")
	}
}

// A collection untouched since its last flush must not be rewritten by
// later flushes — snapshot I/O is proportional to what changed, not to the
// whole database.
func TestSnapshotRewritesOnlyDirtyCollections(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, medb.WithFlushInterval(30*time.Millisecond))
	defer closeDB(t, db)

	cold := medb.C[User](db, "cold")
	hot := medb.C[User](db, "hot")
	set(t, cold, "c1", User{Name: "cold"})
	set(t, hot, "h0", User{Name: "hot"})

	// Wait for the flush that persists both collections.
	coldPath := filepath.Join(dir, "cold.json")
	hotPath := filepath.Join(dir, "hot.json")
	waitFor(t, 5*time.Second, func() bool {
		_, err1 := os.Stat(coldPath)
		_, err2 := os.Stat(hotPath)
		return err1 == nil && err2 == nil
	})
	coldStat, err := os.Stat(coldPath)
	if err != nil {
		t.Fatal(err)
	}

	// Drive at least three more flushes by keeping hot dirty and waiting
	// for each write to reach its snapshot file.
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("h%d", i)
		set(t, hot, id, User{Age: i})
		waitFor(t, 5*time.Second, func() bool {
			data, err := os.ReadFile(hotPath)
			if err != nil {
				return false
			}
			var coll map[string]User
			return json.Unmarshal(data, &coll) == nil && coll[id].Age == i
		})
	}

	after, err := os.Stat(coldPath)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(coldStat.ModTime()) {
		t.Fatal("clean collection was rewritten by a flush it took no part in")
	}
}

func TestWALTruncatedAfterClose(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	set(t, medb.C[User](db, "users"), "u1", User{Name: "Ada"})
	closeDB(t, db)

	info, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("wal.log is %d bytes after Close, want 0", info.Size())
	}
}

func TestDropRemovesSnapshotFile(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	set(t, medb.C[User](db, "prod/users"), "u1", User{Name: "Ada"})
	closeDB(t, db)

	path := filepath.Join(dir, "prod", "users.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("snapshot file missing before drop: %v", err)
	}

	db = openDB(t, dir)
	if err := db.Drop("prod/users"); err != nil {
		t.Fatal(err)
	}
	closeDB(t, db)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("snapshot file still present after Drop: %v", err)
	}
}

func TestEmptyCollectionSurvivesReopen(t *testing.T) {
	// Deleting the last document leaves an empty collection, which is a
	// different state than a dropped one and must persist as such.
	dir := t.TempDir()
	db := openDB(t, dir)
	users := medb.C[User](db, "users")
	set(t, users, "u1", User{Name: "Ada"})
	if err := users.Delete("u1"); err != nil {
		t.Fatal(err)
	}
	if got := db.Collections(); len(got) != 1 {
		t.Fatalf("Collections = %v, want [users]", got)
	}
	closeDB(t, db)

	db = openDB(t, dir)
	defer closeDB(t, db)
	if got := db.Collections(); len(got) != 1 || got[0] != "users" {
		t.Fatalf("Collections = %v after reopen, want [users]", got)
	}
	if n := medb.C[User](db, "users").Count(); n != 0 {
		t.Fatalf("Count = %d, want 0", n)
	}
}
