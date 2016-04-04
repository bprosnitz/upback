package mock

import (
	"drivebackup/store/filesystem"
	"fmt"
	"strings"
	"os"
	"time"
	"path/filepath"
)

type MockFilesystemService struct {
	m map[string]*mockBucket
}
var _ filesystem.FilesystemService = (*MockFilesystemService)(nil)
func (m *MockFilesystemService) Bucket(bucket string) filesystem.Bucket {
	if b, ok := m.m[bucket]; ok {
		return b
	}
	b := &mockBucket{fileVersions: map[string]*mockFile{}}
	m.m[bucket] = b
	return b
}

// represents all versions of a particular file
type mockFile struct {
	entries []*filesystem.StoredBlobRef
}

type mockDir struct {
	versions []*filesystem.Version
}

type mockBucket struct {
	fileVersions map[string]*mockFile
	dirVersions map[string]*mockDir
	latestVersion string
}

// put transaction
type mockPutTransaction struct {
	blobs map[string]filesystem.BlobRef
	dirs map[string]bool
	bucket *mockBucket
	currPath string
}

type mockConstraintType int

const (
	mockVersionConstraint mockConstraintType = iota
	mockLatestConstraint
	mockDirConstraint
	mockFileConstraint
)

type mockConstraint struct {
	t mockConstraintType
	version filesystem.Version // if version constraint
	path string // if dir/file constraint
}

type mockSelector struct {
	constraints []mockConstraint
	bucket *mockBucket
}

func (m *mockBucket) NewPutTransaction() filesystem.PutTransaction {
	return &mockPutTransaction{bucket: m}
}
func (m *mockBucket) Select() filesystem.Selector {
	return &mockSelector{bucket: m}
}

func (tx *mockPutTransaction) Commit() error {
	version := fmt.Sprintf("%d", time.Now().Unix())
	for version == tx.bucket.latestVersion {
		<- time.After(100 * time.Millisecond)
		version = fmt.Sprintf("%d", time.Now().Unix())
	}
	for path := range tx.dirs {
		dir, ok := tx.bucket.dirVersions[path]
		if !ok {
			dir = &mockDir{}
			tx.bucket.dirVersions[path] = dir
		}
		dir.versions = append(dir.versions, version)
	}
	for name, ref := range tx.blobs {
		cell, ok := tx.bucket.fileVersions[name]
		if !ok {
			cell = &mockFile{}
			tx.bucket.fileVersions[name] = cell
		}
		cell.entries = append(cell.entries, &filesystem.StoredBlobRef{ref, version})
	}
	tx.bucket.latestVersion = version
	return nil
}

func (tx *mockPutTransaction) Dir(path string) filesystem.PutTransactionPath {
	fullPath := filepath.Join(tx.currPath, path)
	tx.dirs[fullPath] = true
	return &mockPutTransaction{blobs: tx.blobs, dirs: tx.dirs, bucket: tx.bucket, path: fullPath}
}

func (tx *mockPutTransaction) File(name string, blobRef filesystem.BlobRef) {
	tx.blobs[filepath.Join(tx.currPath, name)] = blobRef
}

func (s *mockSelector) Version(version filesystem.Version) filesystem.Selector {
	return &mockSelector{
		constraints: append(s.constraints, &mockConstraint{t: mockVersionConstraint, version: version}),
		bucket: s.bucket,
	}
}
func (s *mockSelector) Latest() filesystem.Selector {
	return &mockSelector{
		constraints: append(s.constraints, &mockConstraint{t: mockLatestConstraint}),
		bucket: s.bucket,
	}
}
func (s *mockSelector) Dir(path string) filesystem.Selector {
	return &mockSelector{
		constraints: append(s.constraints, &mockConstraint{t: mockDirConstraint, path: path}),
		bucket: s.bucket,
	}
}
func (s *mockSelector) File(name string) filesystem.Selector {
	return &mockSelector{
		constraints: append(s.constraints, &mockConstraint{t: mockFileConstraint, path: name}),
		bucket: s.bucket,
	}
}
func (s *mockSelector) validate(fileOp bool) (err error) {
	var sawVersionConstraint bool
	firstFileIndex := -1
	for i, constraint := range s.constraints {
		switch constraint.t {
		case mockVersionConstraint, mockLatestConstraint:
			if sawVersionConstraint {
				return false, fmt.Errorf("Version()/Latest() may only be called once")
			}
			sawVersionConstraint = true
		case mockFileConstraint:
			if firstFileIndex != -1 {
				return false, fmt.Errorf("File() must not be called twice")
			}
			firstFileIndex = i
		}
	}
	if firstFileIndex < len(s.constraints) - 2 {
		return fmt.Errorf("File() must be the last filesystem selector")
	} else if s.constraints[len(s.constraints)-1].t == mockDirConstraint {
		return fmt.Errorf("File() must be the last filesystem selector")
	}
	if fileOp {
		if firstFileIndex == -1 {
			return fmt.Errorf("No File() selector specified for file operation")
		}
	} else {
		if firstFileIndex != -1 {
			return fmt.Errorf("File() selector incorrectly specified for directory operation")
		}
	}
	return nil
}
func (s *mockSelector) List() ([]string, error) {
	if err := s.validate(false); err != nil {
		return nil, err
	}
	// DOSTUFF
}
func (s *mockSelector) Ref() (filesystem.StoredBlobRef, error) {
	if err := s.validate(true); err != nil {
		return nil, err
	}
	// DOSTUFF

}
func (s *mockSelector) Versions() ([]filesystem.StoredBlobRef, error) {
	if err := s.validate(true); err != nil {
		return nil, err
	}
	// DOSTUFF

}