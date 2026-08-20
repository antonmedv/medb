package medb_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/antonmedv/medb"
)

func TestErrNotFound(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	set(t, users, "u1", User{Name: "Ada"})

	if _, err := users.Get("missing"); !errors.Is(err, medb.ErrNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrNotFound", err)
	}
	ghosts := medb.C[User](db, "ghosts")
	if _, err := ghosts.Get("u1"); !errors.Is(err, medb.ErrNotFound) {
		t.Fatalf("Get on missing collection = %v, want ErrNotFound", err)
	}
}

func TestReadsAfterClose(t *testing.T) {
	db := openDB(t, t.TempDir())
	users := medb.C[User](db, "users")
	set(t, users, "u1", User{Name: "Ada"})
	closeDB(t, db)

	if _, err := users.Get("u1"); !errors.Is(err, medb.ErrClosed) {
		t.Fatalf("Get after close = %v, want ErrClosed", err)
	}
	if users.Has("u1") {
		t.Fatal("Has after close = true, want false")
	}
	if n := users.Count(); n != 0 {
		t.Fatalf("Count after close = %d, want 0", n)
	}
	for id := range users.All() {
		t.Fatalf("All after close yielded %q", id)
	}
	if got := db.Collections(); got != nil {
		t.Fatalf("Collections after close = %v, want nil", got)
	}
	if err := db.Close(); !errors.Is(err, medb.ErrClosed) {
		t.Fatalf("second Close = %v, want ErrClosed", err)
	}
}

func TestErrTooLarge(t *testing.T) {
	db := openDB(t, t.TempDir(), medb.WithMaxDocSize(16))
	defer closeDB(t, db)

	docs := medb.C[string](db, "docs")
	set(t, docs, "small", "ok")

	err := docs.Set("big", strings.Repeat("x", 32))
	if !errors.Is(err, medb.ErrTooLarge) {
		t.Fatalf("Set(big) = %v, want ErrTooLarge", err)
	}
	if docs.Has("big") {
		t.Fatal("oversized document was stored")
	}

	err = docs.Update("small", func(string) (string, error) {
		return strings.Repeat("y", 32), nil
	})
	if !errors.Is(err, medb.ErrTooLarge) {
		t.Fatalf("Update to oversized = %v, want ErrTooLarge", err)
	}
	if got := get(t, docs, "small"); got != "ok" {
		t.Fatalf("doc changed by failed Update: %q", got)
	}
}

func TestErrLocked(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)

	if _, err := medb.Open(dir); !errors.Is(err, medb.ErrLocked) {
		t.Fatalf("second Open = %v, want ErrLocked", err)
	}
	closeDB(t, db)

	// The lock is released on Close.
	db = openDB(t, dir)
	closeDB(t, db)
}

func TestOptionValidation(t *testing.T) {
	tests := []struct {
		name string
		opt  medb.Option
	}{
		{"zero max doc size", medb.WithMaxDocSize(0)},
		{"negative max doc size", medb.WithMaxDocSize(-1)},
		{"zero flush bytes", medb.WithFlushBytes(0)},
		{"negative flush bytes", medb.WithFlushBytes(-1)},
		{"zero flush interval", medb.WithFlushInterval(0)},
		{"negative flush interval", medb.WithFlushInterval(-1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := medb.Open(t.TempDir(), tt.opt); err == nil {
				t.Fatal("Open succeeded with invalid option")
			}
		})
	}
}

func TestOpenOnFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := medb.Open(path); err == nil {
		t.Fatal("Open on a regular file succeeded")
	}
}

func TestOpenAfterFailedOpen(t *testing.T) {
	// A failed Open must release the lock so a corrected retry can succeed.
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")
	if err := os.WriteFile(walPath, []byte("garbage\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := medb.Open(dir); err == nil {
		t.Fatal("Open succeeded on corrupt wal")
	}
	if err := os.Remove(walPath); err != nil {
		t.Fatal(err)
	}
	db := openDB(t, dir)
	closeDB(t, db)
}

func TestInvalidNamePanics(t *testing.T) {
	invalid := []string{
		"", "/", "A", "Users", "a b", "a.b", "..", "a..b",
		"/a", "a/", "a//b", "ä", "日本", "a\n", "a\x00b",
	}
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)
	for _, name := range invalid {
		t.Run(fmt.Sprintf("C(%q)", name), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("C(%q) did not panic", name)
				}
			}()
			medb.C[User](db, name)
		})
	}

	t.Run("Drop", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("Drop with invalid name did not panic")
			}
		}()
		_ = db.Drop("Bad Name")
	})
}

func TestUnusualValidNames(t *testing.T) {
	valid := []string{"a", "0", "-", "_", "a-b_c9", "a/b/c", "0/1"}
	dir := t.TempDir()
	db := openDB(t, dir)
	for _, name := range valid {
		set(t, medb.C[string](db, name), "id", name)
	}
	closeDB(t, db)

	db = openDB(t, dir)
	defer closeDB(t, db)
	for _, name := range valid {
		if got := get(t, medb.C[string](db, name), "id"); got != name {
			t.Fatalf("collection %q: got %q", name, got)
		}
	}
}

func TestUpdateMissing(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	called := false
	err := users.Update("missing", func(u User) (User, error) {
		called = true
		return u, nil
	})
	if !errors.Is(err, medb.ErrNotFound) {
		t.Fatalf("Update(missing) = %v, want ErrNotFound", err)
	}
	if called {
		t.Fatal("callback was invoked for a missing document")
	}
}

func TestUpdateCallbackError(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	set(t, users, "u1", User{Name: "Ada"})

	boom := errors.New("boom")
	err := users.Update("u1", func(u User) (User, error) {
		u.Name = "mutated"
		return u, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Update = %v, want the callback error", err)
	}
	if got := get(t, users, "u1"); got.Name != "Ada" {
		t.Fatalf("failed Update mutated the document: %+v", got)
	}
}

func TestSetMarshalError(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	chans := medb.C[chan int](db, "chans")
	if err := chans.Set("c1", make(chan int)); err == nil {
		t.Fatal("Set with unmarshalable type succeeded")
	}
	if chans.Has("c1") {
		t.Fatal("unmarshalable document was stored")
	}

	// The DB stays usable after a marshal error.
	users := medb.C[User](db, "users")
	set(t, users, "u1", User{Name: "Ada"})
}

func TestGetDecodeError(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	set(t, medb.C[User](db, "users"), "u1", User{Name: "Ada"})

	ints := medb.C[int](db, "users")
	_, err := ints.Get("u1")
	if err == nil {
		t.Fatal("Get with mismatched type succeeded")
	}
	if errors.Is(err, medb.ErrNotFound) || errors.Is(err, medb.ErrClosed) {
		t.Fatalf("decode failure reported as %v", err)
	}
}

func TestAllDecodePanics(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	set(t, medb.C[User](db, "users"), "u1", User{Name: "Ada"})

	defer func() {
		if recover() == nil {
			t.Fatal("All with mismatched type did not panic")
		}
	}()
	for range medb.C[int](db, "users").All() {
	}
}

func TestRawMessagePassthrough(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	raw := medb.C[json.RawMessage](db, "raw")
	doc := json.RawMessage(`{"k":[1,2,{"x":null}]}`)
	set(t, raw, "r1", doc)
	if got := get(t, raw, "r1"); string(got) != string(doc) {
		t.Fatalf("got %s, want %s", got, doc)
	}
}

func TestZeroValueDocs(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)

	set(t, medb.C[struct{}](db, "empty"), "e1", struct{}{})
	set(t, medb.C[User](db, "users"), "u1", User{})
	set(t, medb.C[*User](db, "ptrs"), "p1", nil)
	closeDB(t, db)

	db = openDB(t, dir)
	defer closeDB(t, db)
	if !medb.C[struct{}](db, "empty").Has("e1") {
		t.Fatal("empty struct doc lost")
	}
	if got := get(t, medb.C[User](db, "users"), "u1"); !reflect.DeepEqual(got, User{}) {
		t.Fatalf("zero User = %+v", got)
	}
	if got := get(t, medb.C[*User](db, "ptrs"), "p1"); got != nil {
		t.Fatalf("nil pointer doc = %+v", got)
	}
}

func TestExoticIDs(t *testing.T) {
	ids := []string{
		"",
		"a\nb",
		"héllo wörld",
		`q"uo\te`,
		"tab\tand\x00nul",
		strings.Repeat("x", 10_000),
	}
	dir := t.TempDir()
	db := openDB(t, dir)
	docs := medb.C[string](db, "docs")
	for i, id := range ids {
		set(t, docs, id, fmt.Sprintf("v%d", i))
	}
	if n := docs.Count(); n != len(ids) {
		t.Fatalf("Count = %d, want %d", n, len(ids))
	}
	closeDB(t, db)

	// IDs must survive both the WAL encoding and the snapshot encoding.
	db = openDB(t, dir)
	defer closeDB(t, db)
	docs = medb.C[string](db, "docs")
	for i, id := range ids {
		if got := get(t, docs, id); got != fmt.Sprintf("v%d", i) {
			t.Fatalf("id %q = %q, want v%d", id, got, i)
		}
	}
}

func TestDeleteMissing(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)

	users := medb.C[User](db, "users")
	if err := users.Delete("missing"); err != nil {
		t.Fatalf("Delete(missing) = %v, want nil", err)
	}
	// Deleting from a collection that never existed must not create it.
	if got := db.Collections(); len(got) != 0 {
		t.Fatalf("Collections = %v, want none", got)
	}
}

func TestDropMissing(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer closeDB(t, db)
	if err := db.Drop("never-existed"); err != nil {
		t.Fatalf("Drop(missing) = %v, want nil", err)
	}
}
