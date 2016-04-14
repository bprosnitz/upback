package selector

import (
	"drivebackup/store/filesystem"
	"path/filepath"
)

func extract(selector []Constraint) (path, latestVersionPath string, isFile bool, version filesystem.Version) {
	// Handle VersionConstraint
	for _, constraint := range selector {
		if constraint.Type == VersionConstraint {
			version = constraint.Version
		}
	}

	// Determine if file / dir.
	for _, constraint := range selector {
		if constraint.Type == FileConstraint {
			isFile = true
		}
	}

	// Process up to the version constraint, if any.
	var pathBeforeVersion string
	var hasLatestConstraint bool
	loopPre: for _, constraint := range selector {
		switch constraint.Type {
		case FileConstraint, DirConstraint:
			pathBeforeVersion = filepath.Join(pathBeforeVersion, constraint.Location)
		case VersionConstraint:
			hasLatestConstraint = true
			break loopPre
		case LatestConstraint:
			break loopPre
		}
	}

	if hasLatestConstraint {
		latestVersionPath = pathBeforeVersion
	}

	// Process paths after the version constraint.
	var seenVersion bool
	path = pathBeforeVersion
	for _, constraint := range selector {
		if !seenVersion {
			switch constraint.Type {
			case VersionConstraint, LatestConstraint:
				seenVersion = true
			}
			continue
		}
		switch constraint.Type {
		case FileConstraint, DirConstraint:
			path = filepath.Join(path, constraint.Location)
		}
	}

	return
}
