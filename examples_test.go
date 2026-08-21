package medb_test

import (
	"fmt"
	"os"

	"github.com/antonmedv/medb"
)

func ExampleOpen() {
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
		Age  int
	}
	users := medb.C[User](db, "users")

	if err := users.Set("ada", User{Name: "Ada", Age: 36}); err != nil {
		panic(err)
	}
	user, err := users.Get("ada")
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s is %d\n", user.Name, user.Age)
	if err := db.Close(); err != nil {
		panic(err)
	}

	// Output:
	// Ada is 36
}

func ExampleCollection_Update() {
	dir, err := os.MkdirTemp("", "medb-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	db, err := medb.Open(dir)
	if err != nil {
		panic(err)
	}

	counters := medb.C[int](db, "counters")
	if err := counters.Set("visits", 1); err != nil {
		panic(err)
	}
	if err := counters.Update("visits", func(n int) (int, error) {
		return n + 1, nil
	}); err != nil {
		panic(err)
	}

	visits, err := counters.Get("visits")
	if err != nil {
		panic(err)
	}
	fmt.Println(visits)
	if err := db.Close(); err != nil {
		panic(err)
	}

	// Output:
	// 2
}

func ExampleCollection_All() {
	dir, err := os.MkdirTemp("", "medb-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	db, err := medb.Open(dir)
	if err != nil {
		panic(err)
	}

	users := medb.C[string](db, "users")
	if err := users.Set("grace", "Grace Hopper"); err != nil {
		panic(err)
	}
	if err := users.Set("ada", "Ada Lovelace"); err != nil {
		panic(err)
	}

	for id, name := range users.All() {
		fmt.Printf("%s: %s\n", id, name)
	}
	if err := db.Close(); err != nil {
		panic(err)
	}

	// Output:
	// ada: Ada Lovelace
	// grace: Grace Hopper
}
