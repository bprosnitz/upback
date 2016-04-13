package selector

import "drivebackup/store/filesystem"

type ConstraintType int

const (
	VersionConstraint ConstraintType = iota
	LatestConstraint
	DirConstraint
	FileConstraint
)

func (c ConstraintType) String() string {
	switch c {
	case VersionConstraint:
		return "version"
	case LatestConstraint:
		return "latest"
	case DirConstraint:
		return "dir"
	case FileConstraint:
		return "file"
	}
	panic("unknown type")
}

func (c ConstraintType) kind() constraintKind {
	switch c {
	case VersionConstraint, LatestConstraint:
		return kindVersion
	case DirConstraint, FileConstraint:
		return kindLocation
	}
	panic("unknown type")
}

type constraintKind int

const (
	kindVersion constraintKind = iota
	kindLocation
)

type Constraint struct {
	Type ConstraintType

	Version filesystem.Version
	Location string
}