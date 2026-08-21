package medb

import (
	"crypto/rand"
	"encoding/hex"
)

// NewID returns a random 128-bit identifier as 32 lowercase hexadecimal
// characters. It panics if the system cannot provide secure random bytes.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
