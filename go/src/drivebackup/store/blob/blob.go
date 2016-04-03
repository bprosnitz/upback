package blob

import "io"

type BlobService interface {
	Put(name string, data io.Reader) error
	Get(name string) (data io.Reader, err error)
}
