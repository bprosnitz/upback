package store

import (
	"io"
	"google.golang.org/cloud/datastore"
	storage "google.golang.org/api/storage/v1"
	"golang.org/x/net/context"
	"fmt"
	"time"
	"github.com/golang/protobuf/ptypes/timestamp"
)

const BACKUP_BUCKET string = "BACKUP_OBJECTS"

type Transaction struct{
	category Category
	objects  map[string]io.ReadSeeker
}

func (tr *Transaction) Put(path string, file io.ReadSeeker) (error) {
	if _, ok := tr.objects[path];ok  {
		return fmt.Errorf("object %q already exists", path)
	}
	tr.objects[path] = file
	return nil
}

// Not atomic
func (tr *Transaction) Commit(ctx *context.Context, client *datastore.Client, service *storage.Service) error {
	filenameMap := map[string]string{}
	timestamp := time.Now()
	for path, obj := range tr.objects {
		name := hashFilename(obj)
		if _, err := obj.Seek(0, 0); err != nil {
			return err
		}
		filenameMap[path] = name
		storageObj := &storage.Object{
			Name: name,
			Metadata: map[string]string{
				"CATEGORY": string(tr.category),
				"PATH": path,
				"TIMESTAMP": fmt.Sprintf("%d", timestamp.Unix()),
			},
		}
		_, err := service.Objects.Insert(BACKUP_BUCKET, storageObj).Media(obj).Do()
		if  err != nil {
			return err
		}
	}

	return putBlobRefs(ctx, client, tr.category, timestamp, filenameMap)
}

func ReadFile(ctx *context.Context, client *datastore.Client, service *storage.Service, category Category, path string) (io.ReadCloser, error) {
	ref, err := latestBlobRef(ctx, client, category, path)
	if err != nil {
		return err
	}
	obj := service.Objects.Get(BACKUP_BUCKET, ref.BlobLocation)
	response, err := obj.Download()
	return response.Body, err
}

func ListDir(ctx *context.Context, client *datastore.Client, service *storage.Service, category Category, path string) ([]DirEntry, error) {
	return listDir(ctx, client,category,path)
}