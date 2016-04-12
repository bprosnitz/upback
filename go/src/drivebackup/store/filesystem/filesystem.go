package filesystem

import "fmt"

type Version string

type BlobRef struct {
	Store string // the storage name/type where the blob is stored (can be a combination of "googlecloud" and bucket name)
	Name string // the blob name within the store
}

func (r *BlobRef) String() string {
	return fmt.Sprintf("%s:%s", r.Store, r.Name)
}

type StoredBlobRef struct {
	BlobRef
	Version Version
}

func (r *StoredBlobRef) String() string {
	return fmt.Sprintf("%v@%s", r.BlobRef, r.Version)
}

type FilesystemService interface {
	Bucket(bucket string) Bucket
}

type Bucket interface {
	NewPutTransaction() PutTransaction
	Select() Selector
}

type PutTransaction interface {
	PutTransactionPath
	Commit() error
}

type PutTransactionPath interface {
	Dir(path string) PutTransactionPath
	File(name string, blobRef BlobRef)
}

type Selector interface {
	Version(version Version) Selector
	Latest() Selector
	Dir(path string) Selector
	File(name string) Selector

	SelectorOp
}

type SelectorOp interface {
	DirSelectorOp
	FileSelectorOp

	Versions() ([]Version, error) // list all version of the file/dir
}

// DirSelectorOp only succeeds on dirs
type DirSelectorOp interface{
	List() ([]string, error)
}

// FileSelectorOp only succeeds on files
type FileSelectorOp interface {
	BlobRef() (StoredBlobRef, error) // fails if multiple files
}