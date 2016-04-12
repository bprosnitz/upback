package mock

import (
	"drivebackup/store/filesystem"
	"fmt"
	"strings"
	"os"
	"time"
	"path/filepath"
	"sort"
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
func (m *MockFilesystemService) String() string {
	str := "buckets:\n"
	for name, bucket := range m.m {
		str += fmt.Sprintf("%q:\n%v\n", name, bucket)
	}
	return str
}

// represents all versions of a particular file
type mockFile struct {
	entries []*filesystem.StoredBlobRef
}

func (f *mockFile) String() string {
	var str string
	for i, entry := range f.entries {
		if i > 0 {
			str += ","
		}
		str += entry.String()
	}
	return str
}

type mockDir struct {
	versions []filesystem.Version
}

func (f *mockDir) String() string {
	var str string
	for i, version := range f.versions {
		if i > 0 {
			str += ","
		}
		str += "@" + string(version)
	}
	return str
}

type mockBucket struct {
	fileVersions map[string]*mockFile
	dirVersions map[string]*mockDir
	latestVersion filesystem.Version
}

func (m *mockBucket) String() string {
	keysSeen := map[string]bool{}
	var keys []string
	for path := range m.fileVersions {
		keysSeen[path] = true
		keys = append(keys, path)
	}
	for path := range m.dirVersions {
		if !keysSeen[path] {
			keys = append(keys, path)
		}
	}
	sort.Strings(keys)

	str := fmt.Sprintf("latest version: %s\n", m.latestVersion)
	for _, key := range keys {
		str += fmt.Sprintf("%s file versions: %v dir versions: %v\n", key, m.fileVersions[key], m.dirVersions[key])
	}
	return str
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
	parts := strings.Split(fullPath, string(os.PathSeparator))
	for i := 0; i <= len(parts); i++ {
		tx.dirs[strings.Join(parts[:i], string(os.PathSeparator))] = true
	}
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
func (s *mockSelector) validate(dirOp, fileOp bool) (err error) {
	if len(s.constraints) == 0 {
		return nil
	}

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
	if fileOp {
		if firstFileIndex == -1 {
			if !dirOp {
				return fmt.Errorf("No File() selector specified for file operation")
			}
		} else {
			if firstFileIndex < len(s.constraints) - 2 {
				return fmt.Errorf("File() must be the last filesystem selector")
			} else if s.constraints[len(s.constraints) - 1].t == mockDirConstraint {
				return fmt.Errorf("File() must be the last filesystem selector")
			}
		}
	}
	if dirOp {
		if firstFileIndex != -1 && !fileOp {
			return fmt.Errorf("File() selector incorrectly specified for directory operation")
		}
	}
	return nil
}
func (s *mockSelector) versionConstraint() (found bool, version filesystem.Version) {
	var dirPath string
	var filePath string
	for _, constraint := range s.constraints {
		switch constraint.t {
		case mockVersionConstraint:
			return true, constraint.version
		case mockLatestConstraint:
			if filePath != "" {
				file, ok := s.bucket.fileVersions[filePath]
				if !ok {
					return false, ""
				}
				return true, file.entries[len(file.entries)-1].Version
			} else if dirPath != "" {
				dir, ok := s.bucket.dirVersions[dirPath]
				if !ok {
					return false, ""
				}
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
	if err := s.validate(true, false); err != nil {
		return nil, err
	}
	versionFound, version := s.versionConstraint()
	dirPath := s.dirPath()
	dir, ok := s.bucket.dirVersions[dirPath]
	if !ok {
		return nil, fmt.Errorf("dir not found: %v", dirPath)
	}
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

	results := map[string]bool{}
	for path, dir := range s.bucket.dirVersions {
		for _, validVersion := range validVersions {
			for _, dirVersion := range dir.versions {
				if dirVersion == validVersion && inDir(path, dirPath) {
					results[oneLevelPath(path, dirPath)] = true
				}
			}
		}
	}
	for path, file := range s.bucket.fileVersions {
		for _, validVersion := range validVersions {
			for _, fileEntry := range file.entries {
				if fileEntry.Version == validVersion && inDir(path, dirPath) {
					results[oneLevelPath(path, dirPath)] = true
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
func (s *mockSelector) BlobRef() (filesystem.StoredBlobRef, error) {
	if err := s.validate(false, true); err != nil {
		return filesystem.StoredBlobRef{}, err
	}
	versionFound, version := s.versionConstraint()
	if !versionFound {
		return filesystem.StoredBlobRef{}, fmt.Errorf("version must be specified for BlobRef()")
	}
	filePath := s.filePath()
	file, ok := s.bucket.fileVersions[filePath]
	if !ok {
		return filesystem.StoredBlobRef{}, fmt.Errorf("File not found")
	}
	for _, entry := range file.entries {
		if entry.Version == version {
			return *entry, nil
		}
	}
	return filesystem.StoredBlobRef{}, fmt.Errorf("file %q has no version %q", filePath, version)
}
func (s *mockSelector) Versions() ([]filesystem.Version, error) {
	if err := s.validate(true, true); err != nil {
		return nil, err
	}
	versionFound, version := s.versionConstraint()
	var versions []string
	filePath := s.filePath()
	if file, ok := s.bucket.fileVersions[filePath]; ok {
		fileVersions, err := s.fileVersions(file, versionFound, version)
		if err != nil {
			return nil, err
		}
		for _, version := range fileVersions {
			versions = append(versions, string(version))
		}
	}
	dirPath := s.dirPath()
	if dir, ok := s.bucket.dirVersions[dirPath]; ok {
		dirVersions, err := s.dirVersions(dir, versionFound, version)
		if err != nil {
			return nil, err
		}
		for _, version := range dirVersions {
			versions = append(versions, string(version))
		}
	}
	sort.Strings(versions)
	if len(versions) == 0 {
		return nil, fmt.Errorf("file not found")
	}
	outVersions := make([]filesystem.Version, len(versions))
	for i, v := range versions {
		outVersions[i] = filesystem.Version(v)
	}
	return outVersions, nil
}

func (s *mockSelector) fileVersions(file *mockFile, versionFound bool, version filesystem.Version) ([]filesystem.Version, error) {
	if versionFound {
		for _, entry := range file.entries {
			if entry.Version == version {
				return []filesystem.Version{entry.Version}, nil
			}
		}
		return nil, fmt.Errorf("no results found")
	} else {
		var results []filesystem.Version
		for _, entry := range file.entries {
			results = append(results, entry.Version)
		}
		return results, nil
	}
}

func (s *mockSelector) dirVersions(dir *mockDir, versionFound bool, version filesystem.Version) ([]filesystem.Version, error) {
	if versionFound {
		for _, dirVersion := range dir.versions {
			if dirVersion == version {
				return []filesystem.Version{dirVersion}, nil
			}
		}
		return nil, fmt.Errorf("no results found")
	} else {
		var results []filesystem.Version
		for _, dirVersion := range dir.versions {
			results = append(results, dirVersion)
		}
		return results, nil
	}
}

func inDir(path, dirPath string) bool {
	if dirPath == "" {
		return path != ""
	}
	separatoredSubpath := dirPath
	if len(dirPath) > 0 && dirPath[len(dirPath)-1] != os.PathSeparator {
		separatoredSubpath += string(os.PathSeparator)
	}
	return strings.HasPrefix(path, separatoredSubpath)
}

func oneLevelName(path, base string) string {
	if base == "" {
		return strings.Split(path, string(os.PathSeparator))[0]
	}
	prefixLessPath := strings.TrimPrefix(path, base)
	if prefixLessPath[0] == os.PathSeparator {
		prefixLessPath = prefixLessPath[1:]
	}
	return strings.Split(prefixLessPath, string(os.PathSeparator))[0]
}

func oneLevelPath(path, base string) string {
	if base == "" {
		return oneLevelName(path, base)
	}
	suffixPath := base
	if suffixPath[len(suffixPath)-1] != os.PathSeparator {
		suffixPath += string(os.PathSeparator)
	}
	return suffixPath + oneLevelName(path, base)
}