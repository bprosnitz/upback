package blob_test

import (
	"testing"
	"drivebackup/store/blob"
	"io/ioutil"
	"bytes"
	"drivebackup/store/blob/mock"
)

func TestMockBlobService(t *testing.T) {
	blobTest(t, &mock.MockBlobService{})
}

func blobExpectMissing(t *testing.T, service blob.BlobService, name string) {
	reader, err := service.Get(name)
	if err != nil {
		t.Errorf("error in Get(%q): %v", name, err)
		return
	}
	if reader != nil {
		t.Errorf("Get(%q) unexpectedly returned a blob")
	}
}

func blobExpect(t *testing.T, service blob.BlobService, name, data string) {
	reader, err := service.Get(name)
	if err != nil {
		t.Errorf("error in Get(%q): %v", name, err)
		return
	}
	if reader == nil {
		t.Errorf("Get(%q) unexpectedly returned nil")
		return
	}
	out, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Errorf("error reading output from Get(%q): %v", name, err)
		return
	}
	if !bytes.Equal([]byte(data), out) {
		t.Errorf("invalid blob bytes returned from Get(%q): got %x, want %x", name, out, []byte(data))
	}
}

func blobPut(t *testing.T, service blob.BlobService, name, data string) {
	if err := service.Put(name, bytes.NewReader([]byte(data))); err != nil {
		t.Errorf("error in Put(%q): %v", name, err)
	}
}

func blobTest(t *testing.T, service blob.BlobService) {
	blobExpectMissing(t, service, "")
	blobExpectMissing(t, service, "abcd")
	blobPut(t, service, "abcd", "result_abcd")
	blobExpect(t, service, "abcd", "result_abcd")
	blobExpectMissing(t, service, "efgh")
	blobPut(t, service, "efgh", "result_efgh")
	blobPut(t, service, "ijkl", "result_ijkl")
	blobExpect(t, service, "abcd", "result_abcd")
	blobExpect(t, service, "efgh", "result_efgh")
	blobExpect(t, service, "ijkl", "result_ijkl")
}