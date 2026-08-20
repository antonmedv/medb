package medb_test

import (
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/antonmedv/medb"
)

func TestModel(t *testing.T) {
	for seed := range uint64(3) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			runModel(t, seed, 250)
		})
	}
}

func runModel(t *testing.T, seed uint64, steps int) {
	r := rand.New(rand.NewPCG(seed, 42))
	colls := []string{"a", "b", "nested/c"}
	ids := []string{"0", "1", "2", "3", "4", "5"}

	dir := t.TempDir()
	db := openDB(t, dir, medb.WithFlushInterval(50*time.Millisecond))
	defer func() { _ = db.Close() }()

	// The model mirrors apply(): delete keeps an empty collection alive,
	// drop removes it entirely.
	model := map[string]map[string]int{}

	for step := range steps {
		coll := colls[r.IntN(len(colls))]
		id := ids[r.IntN(len(ids))]
		c := medb.C[int](db, coll)

		switch p := r.IntN(100); {
		case p < 40: // set
			v := r.IntN(1000)
			if err := c.Set(id, v); err != nil {
				t.Fatalf("step %d: Set(%s/%s): %v", step, coll, id, err)
			}
			if model[coll] == nil {
				model[coll] = map[string]int{}
			}
			model[coll][id] = v

		case p < 60: // delete
			if err := c.Delete(id); err != nil {
				t.Fatalf("step %d: Delete(%s/%s): %v", step, coll, id, err)
			}
			delete(model[coll], id)

		case p < 75: // update
			err := c.Update(id, func(n int) (int, error) { return n + 1, nil })
			if _, ok := model[coll][id]; ok {
				if err != nil {
					t.Fatalf("step %d: Update(%s/%s): %v", step, coll, id, err)
				}
				model[coll][id]++
			} else if !errors.Is(err, medb.ErrNotFound) {
				t.Fatalf("step %d: Update(missing %s/%s) = %v, want ErrNotFound", step, coll, id, err)
			}

		case p < 80: // drop
			if err := db.Drop(coll); err != nil {
				t.Fatalf("step %d: Drop(%s): %v", step, coll, err)
			}
			delete(model, coll)

		case p < 85: // reopen
			closeDB(t, db)
			db = openDB(t, dir, medb.WithFlushInterval(50*time.Millisecond))

		default: // point read
			v, err := c.Get(id)
			want, ok := model[coll][id]
			switch {
			case ok && (err != nil || v != want):
				t.Fatalf("step %d: Get(%s/%s) = %d, %v, want %d", step, coll, id, v, err, want)
			case !ok && !errors.Is(err, medb.ErrNotFound):
				t.Fatalf("step %d: Get(missing %s/%s) = %v, want ErrNotFound", step, coll, id, err)
			}
			if c.Has(id) != ok {
				t.Fatalf("step %d: Has(%s/%s) = %v, want %v", step, coll, id, !ok, ok)
			}
		}

		if step%50 == 49 {
			compareModel(t, step, db, model)
		}
	}

	closeDB(t, db)
	db = openDB(t, dir)
	compareModel(t, steps, db, model)
}

func compareModel(t *testing.T, step int, db *medb.DB, model map[string]map[string]int) {
	t.Helper()
	if got, want := db.Collections(), slices.Sorted(maps.Keys(model)); !slices.Equal(got, want) {
		t.Fatalf("step %d: Collections = %v, want %v", step, got, want)
	}
	for coll, docs := range model {
		c := medb.C[int](db, coll)
		if n := c.Count(); n != len(docs) {
			t.Fatalf("step %d: Count(%s) = %d, want %d", step, coll, n, len(docs))
		}
		got := map[string]int{}
		for id, v := range c.All() {
			got[id] = v
		}
		if !maps.Equal(got, docs) {
			t.Fatalf("step %d: All(%s) = %v, want %v", step, coll, got, docs)
		}
	}
}
