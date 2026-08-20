package medb

// Test-only seams for the black-box medb_test package.

type LogFile = file

// SwapLog replaces the WAL file handle and returns the previous one. Call it
// only right after Open and before any write: the swap itself is
// unsynchronized and relies on the run goroutine being idle (it only touches
// db.log after a notify/stop channel operation, which establishes the
// happens-before edge with the swap).
func SwapLog(db *DB, f LogFile) LogFile {
	old := db.log
	db.log = f
	return old
}

var ValidName = validName
