package filesystem_test

import (
	"testing"
	"drivebackup/store/filesystem"
	"drivebackup/store/filesystem/mock"
	"sort"
	"reflect"
)

func TestMockFilesystemService(t *testing.T) {
	filesystemTest(t, func() filesystem.FilesystemService {
		return &mock.MockFilesystemService{}
	})
}

type T interface {
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

type tWrapper struct {
	name string
	fatal bool
	t *testing.T
}

func (t *tWrapper) Errorf(format string, args ...interface{}) {
	nargs := []interface{}{t.name}
	nargs = append(nargs, args...)
	t.t.Errorf("%s" + format, nargs...)
}

func (t *tWrapper) Fatalf(format string, args ...interface{}) {
	nargs := []interface{}{t.name}
	nargs = append(nargs, args...)
	t.t.Errorf("%s" + format, nargs...)
	t.fatal = true
	panic("Fatalf")
}

func (t *tWrapper) Run(f func(t T, service filesystem.FilesystemService), serviceFactory func() filesystem.FilesystemService) {
	defer func() {
		if t.fatal {
			recover()
		}
	}()
	f(t, serviceFactory())
}

func putAndGetFileTest(t T, service filesystem.FilesystemService) {
	bucket1 := service.Bucket("testbucket1")

	in := filesystem.BlobRef{"store_a", "store_a_abcd"}
	tx1 := bucket1.NewPutTransaction()
	tx1.File("a", in)
	if err := tx1.Commit(); err != nil {
		t.Fatalf("error committing tx1: %v", err)
	}

	storedRef, err := bucket1.Select().File("a").Latest().BlobRef()
	if err != nil {
		t.Fatalf("error fetching ref: %v", err)
	}
	if storedRef.BlobRef != in {
		t.Errorf("got %v, want %v", storedRef.BlobRef, in)
	}
}

func multipleResultRefTest(t T, service filesystem.FilesystemService) {
	bucket1 := service.Bucket("testbucket1")

	in1 := filesystem.BlobRef{"store_a1", "store_a_abcd1"}
	tx1 := bucket1.NewPutTransaction()
	tx1.File("a", in1)
	if err := tx1.Commit(); err != nil {
		t.Fatalf("error committing tx1: %v", err)
	}

	in2 := filesystem.BlobRef{"store_a2", "store_a_abcd2"}
	tx1 = bucket1.NewPutTransaction()
	tx1.File("a", in2)
	if err := tx1.Commit(); err != nil {
		t.Fatalf("error committing tx1: %v", err)
	}

	_, err := bucket1.Select().File("a").BlobRef()
	if err == nil {
		t.Fatalf("expected error calling ref with multiple results")
	}
}

func multipleResultVersionsTest(t T, service filesystem.FilesystemService) {
	bucket1 := service.Bucket("testbucket1")

	in1 := filesystem.BlobRef{"store_a1", "store_a_abcd1"}
	tx1 := bucket1.NewPutTransaction()
	tx1.File("a", in1)
	if err := tx1.Commit(); err != nil {
		t.Fatalf("error committing tx1: %v", err)
	}

	in2 := filesystem.BlobRef{"store_a2", "store_a_abcd2"}
	tx1 = bucket1.NewPutTransaction()
	tx1.File("a", in2)
	if err := tx1.Commit(); err != nil {
		t.Fatalf("error committing tx1: %v", err)
	}

	versions, err := bucket1.Select().File("a").Versions()
	if err != nil {
		t.Fatalf("error fetching ref: %v", err)
	}

	if len(versions) != 2 || versions[0] == versions[1] {
		t.Fatalf("invalid versions: %v", versions)
	}
}

func latestFileTest(t T, service filesystem.FilesystemService) {
	bucket1 := service.Bucket("testbucket1")

	in1 := filesystem.BlobRef{"store_a1", "store_a_abcd1"}
	tx1 := bucket1.NewPutTransaction()
	tx1.File("a", in1)
	if err := tx1.Commit(); err != nil {
		t.Fatalf("error committing tx1: %v", err)
	}

	in2 := filesystem.BlobRef{"store_a2", "store_a_abcd2"}
	tx1 = bucket1.NewPutTransaction()
	tx1.File("a", in2)
	if err := tx1.Commit(); err != nil {
		t.Fatalf("error committing tx1: %v", err)
	}

	storedRef, err := bucket1.Select().File("a").Latest().BlobRef()
	if err != nil {
		t.Fatalf("error fetching ref: %v", err)
	}
	if storedRef.BlobRef != in2 {
		t.Errorf("got %v, want %v", storedRef.BlobRef, in2)
	}

	storedRef, err = bucket1.Select().Latest().File("a").BlobRef()
	if err != nil {
		t.Fatalf("error fetching ref: %v", err)
	}
	if storedRef.BlobRef != in2 {
		t.Errorf("got %v, want %v", storedRef.BlobRef, in2)
	}
}

func putDirTest(t T, service filesystem.FilesystemService) {
	bucket1 := service.Bucket("testbucket1")

	tx1 := bucket1.NewPutTransaction()
	tx1.Dir("a")
	if err := tx1.Commit(); err != nil {
		t.Fatalf("error committing tx1: %v", err)
	}

	dirs, err := bucket1.Select().List()
	if err != nil {
		t.Fatalf("error fetching list: %v", err)
	}
	if len(dirs) != 1 || dirs[0] != "a" {
		t.Errorf("got dirs %v, expected %v", dirs, []string{"a"})
	}

	dirs, err = bucket1.Select().Dir("a").List()
	if err != nil {
		t.Fatalf("error fetching list: %v", err)
	}
	if len(dirs) != 0 {
		t.Errorf("got dirs %v, expected none", dirs)
	}
}

func multipleVersionDirTest(t T, service filesystem.FilesystemService) {
	bucket1 := service.Bucket("testbucket1")

	tx1 := bucket1.NewPutTransaction()
	tx1.Dir("a")
	tx1.Dir("b")
	if err := tx1.Commit(); err != nil {
		t.Fatalf("error committing tx1: %v", err)
	}

	tx2 := bucket1.NewPutTransaction()
	tx2.Dir("a")
	tx2.Dir("c")
	if err := tx2.Commit(); err != nil {
		t.Fatalf("error committing tx1: %v", err)
	}

	allNames, err := bucket1.Select().List()
	if err != nil {
		t.Fatalf("error listing dir: %v", err)
	}
	sort.Strings(allNames)
	if !reflect.DeepEqual(allNames, []string{"a","b","c"}) {
		t.Errorf("got unexpected dir listing: %v", allNames)
	}

	latestNames, err := bucket1.Select().Latest().List()
	if err != nil {
		t.Fatalf("error listing dir: %v", err)
	}
	sort.Strings(latestNames)
	if !reflect.DeepEqual(latestNames, []string{"a","c"}) {
		t.Errorf("got unexpected dir listing: %v", latestNames)
	}

	versions, err := bucket1.Select().Dir("a").Versions()
	if err != nil {
		t.Fatalf("err getting versions: %v", err)
	}
	if len(versions) != 2 || versions[0] == versions[1] {
		t.Errorf("invalid versions: %v", versions)
	}
}

func sameNameDirAndFileDifferentVersion(t T, service filesystem.FilesystemService) {
	bucket1 := service.Bucket("testbucket1")

	tx1 := bucket1.NewPutTransaction()
	tx1.Dir("a/b")
	if err := tx1.Commit(); err != nil {
		t.Fatalf("error committing tx1: %v", err)
	}

	tx2 := bucket1.NewPutTransaction()
	in := filesystem.BlobRef{"store_a", "store_a_abcd"}
	tx2.Dir("a").File("b", in)
	if err := tx2.Commit(); err != nil {
		t.Fatalf("error committing tx1: %v", err)
	}

	versions, err := bucket1.Select().Versions()
	if err != nil {
		t.Fatalf("err getting versions: %v", err)
	}
	if len(versions) != 2 || versions[0] == versions[1] || versions[1] < versions[0] {
		t.Errorf("invalid versions: %v", versions)
	}
}

func filesystemTest(t *testing.T, serviceFactory func() filesystem.FilesystemService) {
	tests := []struct{
		Name string
		Func func(t T, service filesystem.FilesystemService)
	}{
		{ "Put and Get File", putAndGetFileTest},
		{ "Multiple Result Ref", multipleResultRefTest},
		{ "Multiple Result Versions", multipleResultVersionsTest},
		{ "Latest File", latestFileTest},
		{ "Put Dir Test", putDirTest},
		{ "Multiple Version Dir", multipleVersionDirTest},
		{ "File and Dir Same Name", sameNameDirAndFileDifferentVersion},
	}
	for _, test := range tests {
		wrap := &tWrapper{name: test.Name, t: t}
		wrap.Run(test.Func, serviceFactory)
	}
}