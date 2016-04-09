package filesystem_test

import (
	"testing"
	"drivebackup/store/filesystem"
	"drivebackup/store/filesystem/mock"
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

	storedRef, err := bucket1.Select().File("a").Ref()
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

	_, err := bucket1.Select().File("a").Ref()
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

	storedRefs, err := bucket1.Select().File("a").Versions()
	if err != nil {
		t.Fatalf("error fetching ref: %v", err)
	}

	// TODO(bprosnitz) Must we enforce sorted order?
	if storedRefs[0].BlobRef != in1 {
		t.Errorf("got %v, want %v", storedRefs[0].BlobRef, in1)
	}
	if storedRefs[1].BlobRef != in2 {
		t.Errorf("got %v, want %v", storedRefs[1].BlobRef, in2)
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

	storedRef, err := bucket1.Select().File("a").Latest().Ref()
	if err != nil {
		t.Fatalf("error fetching ref: %v", err)
	}
	if storedRef.BlobRef != in2 {
		t.Errorf("got %v, want %v", storedRef.BlobRef, in2)
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
	}
	for _, test := range tests {
		wrap := &tWrapper{name: test.Name, t: t}
		wrap.Run(test.Func, serviceFactory)
	}

	/*	bucket1 := service.Bucket("testbucket1")

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
	} else if len(files) != 0 {
		t.Errorf("expected directory to be empty")
	}
	ref, err := bucket1.Select().Dir("a/b/c").File("d").Ref()
	if err != nil {
		t.Errorf("error fetching ref for a/b/c/d: %v", err)
	} else if got, want := ref.BlobRef, (filesystem.BlobRef{"store_a", "store_a_abcd"}); got != want {
		t.Errorf("a/b/c/d: got %v, want %v", got, want)
	}


	tx2 := bucket1.NewPutTransaction()
	tx2.Dir("a").Dir("b/c").File("d", filesystem.BlobRef{"store_a", "store_a_abcd"})
	tx2.Dir("a").Dir("b/c/d").File("e", filesystem.BlobRef{"store_a", "store_a_abcde"})
	if err := tx1.Commit(); err == nil {
		t.Errorf("expected failure commiting file a/b/c/d/e within other file path a/b/c/d, but got none")
	}

	tx3 := bucket1.NewPutTransaction()
	tx3.Dir("x/y").File("z", filesystem.BlobRef{"store_b", "store_b_xyz"})
	tx3.Dir("a").Dir("b/c").File("d", filesystem.BlobRef{"store_b", "store_b_abcd"})
	if err := tx3.Commit(); err != nil {
		t.Errorf("error committing tx1: %v", err)
	}
	ref, err = bucket1.Select().Dir("x").Dir("y").File("z").Ref()
	if err != nil {
		t.Errorf("error fetching ref x/y/z: %v", err)
	}  else if got, want := ref.BlobRef, (filesystem.BlobRef{"store_b", "store_b_xyz"}); got != want {
		t.Errorf("a/b/c/d: got %v, want %v", got, want)
	}
	ref, err = bucket1.Select().Dir("a/b/c").File("d").Ref()
	if err == nil {
		t.Errorf("expected error fetching ref for file with multiple versions")
	}
	ref, err = bucket1.Select().Dir("a/b/c").File("d").Latest().Ref()
	if err != nil {
		t.Errorf("error fetching ref for a/b/c/d: %v", err)
	} else if got, want := ref.BlobRef, (filesystem.BlobRef{"store_b", "store_b_abcd"}); got != want {
		t.Errorf("a/b/c/d: got %v, want %v", got, want)
	}
	ref, err = bucket1.Select().Latest().Dir("a/b/c").File("d").Ref()
	if err != nil {
		t.Errorf("error fetching ref for a/b/c/d: %v", err)
	} else if got, want := ref.BlobRef, (filesystem.BlobRef{"store_b", "store_b_abcd"}); got != want {
		t.Errorf("a/b/c/d: got %v, want %v", got, want)
	}
	refs, err := bucket1.Select().Dir("a/b/c").File("d").Versions()
	if err != nil {
		t.Errorf("error fetching refs for a/b/c/d: %v", err)
	} else if len(refs) != 2 {
		t.Errorf("expected 2 refs when retrieving all versions of file")
	} else if refs[0] == refs[1] {
		t.Errorf("refs are expected to differ, but were identical")
	}

	// TODO(bprosnitz) Test .Latest() before and after file (difference should be that before, you restrict files to just that version and after you only get versions for that file)
*/
}