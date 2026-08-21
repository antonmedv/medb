//go:build go1.27

package medb_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/antonmedv/medb"
)

func TestDBCollectionMethod(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	users := db.C[User]("users")
	set(t, users, "ada", User{Name: "Ada", Age: 36})
	closeDB(t, db)

	db = openDB(t, dir)
	defer closeDB(t, db)
	users = db.C[User]("users")
	if got := get(t, users, "ada"); got.Name != "Ada" || got.Age != 36 {
		t.Fatalf("ada = %+v, want {Name:Ada Age:36}", got)
	}
}

func ExampleDB_C() {
	dir, err := os.MkdirTemp("", "medb-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	db, err := medb.Open(dir)
	if err != nil {
		panic(err)
	}

	type User struct {
		Name string
	}
	users := db.C[User]("users")
	if err := users.Set("ada", User{Name: "Ada"}); err != nil {
		panic(err)
	}

	user, err := users.Get("ada")
	if err != nil {
		panic(err)
	}
	fmt.Println(user.Name)
	if err := db.Close(); err != nil {
		panic(err)
	}

	// Output:
	// Ada
}
