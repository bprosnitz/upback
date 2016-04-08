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
	if m.m == nil {
		m.m = map[string]*mockBucket{}
	}
	m.m[bucket] = b
	return b
}

// represents all versions of a particular file
type mockFile struct {
	entries []*filesystem.StoredBlobRef
}

type mockDir struct {
	versions []filesystem.Version
}

type mockBucket struct {
	fileVersions map[string]*mockFile
	dirVersions map[string]*mockDir
	latestVersion filesystem.Version
}

// put transaction
type mockPutTransaction struct {
	blobs  map[string]filesystem.BlobRef
	dirs   map[string]bool
	bucket *mockBucket
	path   string
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
	version := filesystem.Version(fmt.Sprintf("%d", time.Now().Unix()))
	for version == tx.bucket.latestVersion {
		<- time.After(100 * time.Millisecond)
		version = filesystem.Version(fmt.Sprintf("%d", time.Now().Unix()))
	}
	if tx.bucket.dirVersions == nil {
		tx.bucket.dirVersions = map[string]*mockDir{}
	}
	if tx.bucket.fileVersions == nil {
		tx.bucket.fileVersions = map[string]*mockFile{}
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
	fullPath := filepath.Join(tx.path, path)
	if tx.dirs == nil {
		tx.dirs = map[string]bool{}
	}
	tx.dirs[fullPath] = true
	return &mockPutTransaction{blobs: tx.blobs, dirs: tx.dirs, bucket: tx.bucket, path: fullPath}
}

func (tx *mockPutTransaction) File(name string, blobRef filesystem.BlobRef) {
	if tx.blobs == nil {
		tx.blobs = map[string]filesystem.BlobRef{}
	}
	tx.blobs[filepath.Join(tx.path, name)] = blobRef
}

func (s *mockSelector) Version(version filesystem.Version) filesystem.Selector {
	return &mockSelector{
		constraints: append(s.constraints, mockConstraint{t: mockVersionConstraint, version: version}),
		bucket: s.bucket,
	}
}
func (s *mockSelector) Latest() filesystem.Selector {
	return &mockSelector{
		constraints: append(s.constraints, mockConstraint{t: mockLatestConstraint}),
		bucket: s.bucket,
	}
}
func (s *mockSelector) Dir(path string) filesystem.Selector {
	return &mockSelector{
		constraints: append(s.constraints, mockConstraint{t: mockDirConstraint, path: path}),
		bucket: s.bucket,
	}
}
func (s *mockSelector) File(name string) filesystem.Selector {
	return &mockSelector{
		constraints: append(s.constraints, mockConstraint{t: mockFileConstraint, path: name}),
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
				return fmt.Errorf("Version()/Latest() may only be called once")
			}
			sawVersionConstraint = true
		case mockFileConstraint:
			if firstFileIndex != -1 {
				return fmt.Errorf("File() must not be called twice")
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
func (s *mockSelector) version() (found bool, version filesystem.Version) {
	var dirPath string
	var filePath string
	for _, constraint := range s.constraints {
		switch constraint.t {
		case mockVersionConstraint:
			return true, constraint.version
		case mockLatestConstraint:
			if filePath != "" {
				file := s.bucket.fileVersions[filePath]
				return true, file.entries[len(file.entries)-1].Version
			} else if dirPath != "" {
				dir := s.bucket.dirVersions[dirPath]
				return true, dir.versions[len(dir.versions)-1]
			} else {
				return true, s.bucket.latestVersion
			}
		case mockDirConstraint:
			dirPath = filepath.Join(dirPath, constraint.path)
		case mockFileConstraint:
			dirPath = ""
			filePath = filepath.Join(dirPath, constraint.path)
		}
	}
	return false, ""
}
func (s *mockSelector) dirPath() string {
	var dirPath string
	for _, constraint := range s.constraints {
		switch constraint.t {
		case mockDirConstraint:
			dirPath = filepath.Join(dirPath, constraint.path)
		}
	}
	return dirPath
}
func (s *mockSelector) filePath() string {
	var path string
	for _, constraint := range s.constraints {
		switch constraint.t {
		case mockDirConstraint:
			path = filepath.Join(path, constraint.path)
		case mockFileConstraint:
			path = filepath.Join(path, constraint.path)
		}
	}
	return path
}
func (s *mockSelector) List() ([]string, error) {
	if err := s.validate(false); err != nil {
		return nil, err
	}
	versionFound, version := s.version()
	dirPath := s.dirPath()
	dir := s.bucket.dirVersions[dirPath]
	var validVersions []filesystem.Version
	if versionFound {
		for _, dirVersion := range dir.versions {
			if dirVersion == version {
				validVersions = append(validVersions, version)
				break
			}
		}
	} else {
		validVersions = dir.versions
	}

	var results map[string]bool
	for path, dir := range s.bucket.dirVersions {
		for _, validVersion := range validVersions {
			for _, dirVersion := range dir.versions {
				 if dirVersion == validVersion && strings.HasPrefix(path, dirPath + string(os.PathSeparator)) {
					seg := strings.Split(strings.TrimPrefix(path, dirPath), string(os.PathSeparator))[0]
					results[dirPath + string(os.PathSeparator) + seg] = true
				}
			}
		}
	}
	for path, file := range s.bucket.fileVersions {
		for _, validVersion := range validVersions {
			for _, fileEntry := range file.entries {
				if fileEntry.Version == validVersion && strings.HasPrefix(path, dirPath + string(os.PathSeparator)) {
					seg := strings.Split(strings.TrimPrefix(path, dirPath), string(os.PathSeparator))[0]
					results[dirPath + string(os.PathSeparator) + seg] = true
				}
			}
		}
	}

	var finalResults []string
	for key := range results {
		finalResults = append(finalResults, key)
	}
	return finalResults, nil
}
func (s *mockSelector) Ref() (filesystem.StoredBlobRef, error) {
	refs, err := s.Versions()
	if err != nil {
		return filesystem.StoredBlobRef{}, err
	}
	switch len(refs) {
	case 0:
		return filesystem.StoredBlobRef{}, fmt.Errorf("no results found")
	case 1:
		return refs[0], nil
	default:
		return filesystem.StoredBlobRef{}, fmt.Errorf("expected 1 version, got %v", len(refs))
	}
}
func (s *mockSelector) Versions() ([]filesystem.StoredBlobRef, error) {
	if err := s.validate(true); err != nil {
		return nil, err
	}
	versionFound, version := s.version()
	filePath := s.filePath()
	file := s.bucket.fileVersions[filePath]
	if versionFound {
		for _, entry := range file.entries {
			if entry.Version == version {
				return []filesystem.StoredBlobRef{
					filesystem.StoredBlobRef{
						BlobRef: entry.BlobRef,
						Version: entry.Version,
					},
				}, nil
			}
		}
		return nil, fmt.Errorf("no results found")
	} else {
		var results []filesystem.StoredBlobRef
		for _, entry := range file.entries {
			results = append(results, filesystem.StoredBlobRef{
				BlobRef: entry.BlobRef,
				Version: entry.Version,
			})
		}
		return results, nil
	}
}