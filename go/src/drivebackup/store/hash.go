package store

import (
	"crypto/sha256"
	"io"
	"fmt"
)

func hashFilename(r io.Reader) string {
	hash := sha256.New()
	io.Copy(hash, r)
	return fmt.Sprintf("%x", hash.Sum(nil))
}
