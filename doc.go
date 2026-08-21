// Package medb provides a small embedded document database.
//
// A database keeps documents in memory and stores them as JSON on disk. It
// writes every successful change to a durable log before returning. All DB and
// Collection methods are safe to call from concurrent goroutines. Only one
// process can open a database directory at a time. Callers can use errors.Is to
// test the sentinel errors exposed by this package.
package medb
