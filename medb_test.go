package medb_test

import (
	"testing"

	"github.com/antonmedv/medb"
)

func TestWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	db, err := medb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	type User struct{ Name string }

	users := medb.C[User](db, "prod/users")
	if err := users.Set("foo", User{Name: "name"}); err != nil {
		t.Fatal(err)
	}

	if u, err := users.Get("foo"); err != nil {
		t.Fatal(err)
	} else {
		if u.Name != "name" {
			t.Fatal("unexpected name")
		}
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
