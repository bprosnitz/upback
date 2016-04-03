package store

import (
	"golang.org/x/net/context"
	"google.golang.org/cloud/datastore"
	"google.golang.org/cloud/bigtable"
	"time"
	"strings"
)

type blobRef struct {
	FilePath string
	Timestamp time.Time
	BlobLocation string
}

// [category] -> { file, timestamp unix, blob hex name}
// "DRIVE"    -> { "a/b/c/file.go", 23423421532151, "afd3495286f"}

func putBlobRefs(ctx *context.Context, client *datastore.Client, timestamp time.Time, category Category, filenameMap map[string]string) error {
	categoryKey := datastore.NewKey(ctx,
		"Category",
		"",
		string(category),
		nil)

	for origFile, storedFile := range filenameMap {
		refKey := datastore.NewKey(ctx,
			"BlobRef",
			"",
			0,
			categoryKey)
		ref := &blobRef{
			FilePath: origFile,
			Timestamp: timestamp,
			BlobLocation: storedFile,
		}
		if _, err := client.Put(ctx, refKey, ref); err != nil {
			return err
		}
	}
	return nil
}

func latestBlobRef(ctx *context.Context, client *datastore.Client, category Category, filepath string) (*blobRef, error) {
	categoryKey := datastore.NewKey(ctx,
		"Category",
		"",
		string(category),
		nil)
	query := datastore.NewQuery("BlobRef").
		Ancestor(categoryKey).
		Filter("FilePath =", filepath).
		Order("-Timestamp").
		Limit(1)
	it := client.Run(ctx, query)
	var ref blobRef
	_, err := it.Next(&ref)
	return &ref, err
}


func listDir(ctx *context.Context, client *datastore.Client, category Category, filepath string) ([]DirEntry, error) {
	categoryKey := datastore.NewKey(ctx,
		"Category",
		"",
		string(category),
		nil)
	if filepath[len(filepath)-1] != '/' {
		filepath += "/"
	}
	query := datastore.NewQuery("BlobRef").
	Ancestor(categoryKey).
	Filter("FilePath >=", filepath).
	Filter("FilePath <", filepath[:len(filepath)-1]+"0")
	it := client.Run(ctx, query)
	seen := map[string]bool{}
	var entries []DirEntry
	for {
		var ref blobRef
		_, err := it.Next(&ref)
		if err != nil {
			return entries, err
		}
		tail := strings.TrimPrefix(ref.FilePath, filepath)
		name := strings.Split(tail, "/")[0]
		if seen[name] {
			continue
		}
		seen[name] = true
		t := FILE
		if tail != name {
			t = DIR
		}
		entries = append(entries, DirEntry{
			FilePath: filepath + name,
			Type: t,
			Category: category,
		})
	}
}