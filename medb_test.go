package medb_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/antonmedv/medb"
)

func TestNamespaces(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	if err := medb.C[user](db, "prod/users").Set("a", user{Name: "A"}); err != nil {
		t.Fatal(err)
	}
	db = reopen(t, db, dir)
	defer db.Close()

	if _, err := os.Stat(filepath.Join(dir, "prod", "users.json")); err != nil {
		t.Fatal(err)
	}
	if got := db.Collections(); !slices.Equal(got, []string{"prod/users"}) {
		t.Fatalf("collections %v", got)
	}
	if _, err := medb.C[user](db, "prod/users").Get("a"); err != nil {
		t.Fatal(err)
	}
}

func TestValidNames(t *testing.T) {
	db := open(t, t.TempDir())
	defer db.Close()
	for _, name := range []string{"a", "0", "-", "_", "users", "user_1", "my-coll", "prod/users", "a/b/c", "a1/b2/c3"} {
		if err := medb.C[user](db, name).Set("x", user{Name: "X"}); err != nil {
			t.Errorf("%q: %v", name, err)
		}
	}
}

func TestInvalidNames(t *testing.T) {
	db := open(t, t.TempDir())
	defer db.Close()
	names := []string{"", "Users", "a b", "a//b", "/a", "a/", "a.b", "../etc", "wal.log"}
	names = append(names, "/", "//", "a/b/", "a/B/c", "café", "a\x00b", "a\tb")
	for _, name := range names {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("no panic for %q", name)
				}
			}()
			medb.C[user](db, name)
		}()
	}
}

func TestDrop(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	medb.C[user](db, "users").Set("a", user{Name: "A"})
	medb.C[user](db, "orders").Set("o", user{Name: "O"})
	if err := db.Drop("users"); err != nil {
		t.Fatal(err)
	}
	if err := db.Drop("missing"); err != nil {
		t.Fatal(err)
	}
	if got := db.Collections(); !slices.Equal(got, []string{"orders"}) {
		t.Fatalf("collections %v", got)
	}
	db.Close()

	if _, err := os.Stat(filepath.Join(dir, "users.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("users.json survived drop")
	}
	db = open(t, dir)
	defer db.Close()
	if got := db.Collections(); !slices.Equal(got, []string{"orders"}) {
		t.Fatalf("collections after reopen %v", got)
	}
}

func TestPermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	db := open(t, dir)
	if err := medb.C[user](db, "users").Set("ada", user{"Ada", 36}); err != nil {
		t.Fatal(err)
	}
	if err := medb.C[user](db, "prod/orders").Set("o1", user{"O", 1}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".", "wal.log", "LOCK", "users.json", "prod", "prod/orders.json"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if perm := info.Mode().Perm(); perm&0o077 != 0 {
			t.Errorf("%s has mode %v, want no group or world access", name, perm)
		}
	}
}

func TestLocked(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	defer db.Close()
	if _, err := medb.Open(dir); !errors.Is(err, medb.ErrLocked) {
		t.Fatalf("got %v, want ErrLocked", err)
	}
}

func TestInvalidOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  medb.Option
	}{
		{"zero doc size", medb.WithMaxDocSize(0)},
		{"negative doc size", medb.WithMaxDocSize(-1)},
		{"zero flush bytes", medb.WithFlushBytes(0)},
		{"negative flush bytes", medb.WithFlushBytes(-1)},
		{"zero flush interval", medb.WithFlushInterval(0)},
		{"negative flush interval", medb.WithFlushInterval(-time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "data")
			db, err := medb.Open(dir, tc.opt)
			if err == nil {
				db.Close()
				t.Fatal("Open accepted an invalid option")
			}
			t.Log(err)
			if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("Open created %s after rejecting the options", dir)
			}
		})
	}
}

func TestClosed(t *testing.T) {
	db := open(t, t.TempDir())
	users := medb.C[user](db, "users")
	users.Set("a", user{Name: "A"})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := users.Set("b", user{}); !errors.Is(err, medb.ErrClosed) {
		t.Fatalf("Set: %v", err)
	}
	if _, err := users.Get("a"); !errors.Is(err, medb.ErrClosed) {
		t.Fatalf("Get: %v", err)
	}
	if err := db.Drop("users"); !errors.Is(err, medb.ErrClosed) {
		t.Fatalf("Drop: %v", err)
	}
	if err := db.Close(); !errors.Is(err, medb.ErrClosed) {
		t.Fatalf("Close: %v", err)
	}
}

func TestClosedReadsAreEmpty(t *testing.T) {
	db := open(t, t.TempDir())
	users := medb.C[user](db, "users")
	if err := users.Set("a", user{Name: "A"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := users.Get("a"); !errors.Is(err, medb.ErrClosed) {
		t.Errorf("Get: got %v, want ErrClosed", err)
	}
	if users.Has("a") {
		t.Error("Has reported a document from a closed database")
	}
	if n := users.Count(); n != 0 {
		t.Errorf("Count = %d, want 0", n)
	}
	if got := db.Collections(); len(got) != 0 {
		t.Errorf("Collections = %v, want none", got)
	}
	n := 0
	for range users.All() {
		n++
	}
	if n != 0 {
		t.Errorf("All yielded %d documents from a closed database", n)
	}
}

func TestNewID(t *testing.T) {
	a, b := medb.NewID(), medb.NewID()
	if len(a) != 32 || a == b {
		t.Fatalf("ids %q %q", a, b)
	}
}
