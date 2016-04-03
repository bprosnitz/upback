package mock

import (
	"drivebackup/store/filesystem"
	"fmt"
	"strings"
	"os"
	"time"
)

type MockFilesystemService struct {
	m map[string]*mockBucket
}
var _ filesystem.FilesystemService = (*MockFilesystemService)(nil)
func (m *MockFilesystemService) Bucket(bucket string) filesystem.Bucket {
	if b, ok := m.m[bucket]; ok {
		return b
	}
	b := &mockBucket{fileVersions: map[string]*mockFileVersions{}}
	m.m[bucket] = b
	return b
}

// a particular version of a file
type mockFileEntry struct {
	blobRef filesystem.BlobRef
	version string
}

// represents all versions of a particular file
type mockFileVersions struct {
	entries []*mockFileEntry
}

type mockBucket struct {
	fileVersions map[string]*mockFileVersions
	latestVersion string
}

func (m *mockBucket) NewPutTransaction() filesystem.PutTransaction {
	return &mockPutTransaction{bucket: m}
}
func (m *mockBucket) Commit(tx filesystem.PutTransaction) error {
	mtx, ok := tx.(*mockPutTransaction)
	if !ok {
		return fmt.Errorf("only can commit *MockPutTransaction")
	}
	version := fmt.Sprintf("%d", time.Now().Unix())
	for version == m.latestVersion {
		<- time.After(100 * time.Millisecond)
		version = fmt.Sprintf("%d", time.Now().Unix())
	}
	for name, ref := range mtx.blobs {
		cell, ok := m.fileVersions[name]
		if !ok {
			cell = &mockFileVersions{}
			m.fileVersions[name] = cell
		}
		cell.entries = append(cell.entries, &mockFileEntry{ref, version})
	}
	m.latestVersion = version
	return nil
}
func (m *mockBucket) VersionsForPath(path string) ([]filesystem.FilesystemVersion, error) {
	var fsv []filesystem.FilesystemVersion
	if versions, ok := m.fileVersions[path]; ok {
		for _, entry := range versions.entries {
			fsv = append(&mockFilesystemVersion{entry.version, m})
		}
	}
	return fsv, nil
}
func (m *mockBucket) LatestFile(path string) (filesystem.BlobRef, error) {
	if versions, ok := m.fileVersions[path]; ok {
		var maxKey string
		for _, entry := range versions.entries {
			if entry.version > maxKey {
				maxKey = entry.version
			}
		}
		for _, entry := range versions.entries {
			if entry.version == maxKey {
				return entry.blobRef, nil
			}
		}
		}
	return nil, nil
}
func (m *mockBucket) Latest() (filesystem.FilesystemVersion, error) {
	if m.latestVersion == "" {
		return nil, nil
	}
	return &mockFilesystemVersion{m.latestVersion, m}, nil
}

// put transaction
type mockPutTransaction struct {
	blobs map[string]filesystem.BlobRef
	bucket *mockBucket
}

// view into the filesystem for a specific version
type mockFilesystemVersion struct {
	version string
	bucket *mockBucket
}

func (m *mockFilesystemVersion) entry(path string) *mockFileEntry {
	if file, ok := m.bucket.fileVersions[path]; ok {
		for _, entry := range file.entries {
			if entry.version == m.version {
				return entry
			}
		}
	}
	return nil
}

func (m *mockFilesystemVersion) ListDir(path string) ([]string, error) {
	if path[len(path)-1] != os.PathSeparator {
		path = append(path, os.PathSeparator)
	}

	files := map[string]bool{}
	for filePath, _ := range m.bucket.fileVersions {
		if strings.HasPrefix(filePath, path) {
			files[path + strings.Split(strings.TrimPrefix(filePath, path),""+os.PathSeparator)[0]] = true
		}
	}
	return files, nil
}

func (m *mockFilesystemVersion) GetFile(path string) (filesystem.BlobRef, error) {
	entry := m.entry(path)
	if entry != nil {
		return entry.blobRef, nil
	}
	return nil, nil
}