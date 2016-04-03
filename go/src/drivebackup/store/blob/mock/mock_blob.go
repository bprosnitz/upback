package mock

import (
	"drivebackup/store/blob"
	"io"
	"fmt"
	"io/ioutil"
	"bytes"
)

type MockBlobService struct {
	m map[string][]byte
}

var _ blob.BlobService = (*MockBlobService)(nil)

func (mock *MockBlobService) Put(name string, data io.Reader) error {
	if _, ok := mock.m[name]; ok {
		return fmt.Errorf("%s already exists in blob service", name)
	}
	b, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	}
	if mock.m == nil {
		mock.m = map[string][]byte{}
	}
	mock.m[name] = b
	return nil
}

func (mock *MockBlobService) Get(name string) (io.Reader, error) {
	if data, ok := mock.m[name]; ok {
		return bytes.NewReader(data), nil
	} else {
		return nil, nil
	}
}