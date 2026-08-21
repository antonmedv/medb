# MeDB

I was building a simple Go server and kept users in a map. I wanted to save them to disk, but a full database felt like
too much, so I just wrote the map to a JSON file. It worked, but I wanted the same simplicity with durability. A map in memory, stored as JSON on disk, without losing
records if the process crashes.

That is how this database was born. MeDB is a small embedded in memory database that persists data to JSON files on disk.

1. Writes are durable: acknowledged writes are fsynced.
2. Data is stored as JSON files.
3. Only one process can open the database at a time.
4. Concurrent reads/writes are safe.

## Usage

```go
package main

import (
	"fmt"

	"github.com/antonmedv/medb"
)

type User struct {
	Name string
	Age  int
}

func main() {
	db, err := medb.Open("data")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			panic(err)
		}
	}()

	users := medb.C[User](db, "users")

	if err := users.Set("ada", User{Name: "Ada", Age: 36}); err != nil {
		panic(err)
	}

	user, err := users.Get("ada")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s is %d years old\n", user.Name, user.Age)
}
```

## License

[MIT](LICENSE)
