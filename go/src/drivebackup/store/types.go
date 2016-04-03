package store

import (
	"io"
)

type Category string

const (
	DRIVE Category = iota
	FLICKR
)

type QualifiedPath struct {
	Category Category
	FilePath string
}

type Object struct {
	Path string
	io.Reader
}

type FileType int

const (
	FILE FileType = iota
	DIR
)

type DirEntry struct {
	Category Category
	FilePath string
	Type FileType
}