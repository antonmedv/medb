//go:build go1.27

package medb

// C returns a typed handle for name in db. It provides method syntax for Go
// 1.27 and newer. The package-level [C] function provides the same behavior and
// works with older Go versions.
func (db *DB) C[T any](name string) *Collection[T] {
	return C[T](db, name)
}
