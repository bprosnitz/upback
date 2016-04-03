package filesystem_test

import (
	"testing"
	"drivebackup/store/filesystem"
)

func bucket(t *testing.T, service filesystem.FilesystemService, name string) filesystem.Bucket {
	bucket := service.Bucket(name)
	version, err := bucket.Latest()
	if err != nil {
		t.Errorf("error fetching Latest(): %v", err)
	}
	if version != nil {
		t.Errorf("latest version expected to be nil, but wasn't")
	}
	return bucket
}

func filesystemTest(t *testing.T, service filesystem.FilesystemService) {
	bucket1 := bucket(t, service, "testbucket1")

	tx1 := bucket1.NewPutTransaction()
	tx1.Dir("x/y").Dir("z")
	tx1.Dir("a").Dir("b/c").File("d", filesystem.BlobRef{"store_a", "store_a_abcd"})
	if err := tx1.Commit(); err != nil {
		t.Errorf("error committing tx1: %v", err)
	}
	if err := tx1.Commit(); err == nil {
		t.Errorf("expected error committing transaction twice")
	}

	files, err := bucket1.Select().Dir("x").Dir("y").Dir("z").List()
	if err != nil {
		t.Errorf("error listing directory: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected directory to be empty")
	}
}