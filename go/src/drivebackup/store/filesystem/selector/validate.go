package selector

import "fmt"

type ValidationFlag int

const (
	NoFlags ValidationFlag = 0
	RequireVersion ValidationFlag = 1
	RequireFile ValidationFlag = 2
)

func (f ValidationFlag) IsSet(m ValidationFlag) bool {
	return f & m != 0
}

func validate(selector []Constraint, flags ValidationFlag) error {
	// First check that there are no more than 1 version constraint specified.
	numVersionConstraints := 0
	for _, c := range selector {
		if c.Type.kind() == kindVersion {
			numVersionConstraints++
		}
	}
	if numVersionConstraints > 1 {
		return fmt.Errorf("only one version constraint may be specified")
	}
	if flags.IsSet(RequireVersion) && numVersionConstraints < 0 {
		return fmt.Errorf("a version constrain must be specified")
	}

	// Next check that file constraints come after all dir constraints.
	var fileSeen bool
	for _, c := range selector {
		switch c.Type {
		case FileConstraint:
			fileSeen = true
		case DirConstraint:
			if fileSeen {
				return fmt.Errorf("file constraints may only come after all dir constraints")
			}
		}
	}
	if flags.IsSet(RequireFile) && !fileSeen {
		return fmt.Errorf("a file constrain must be specified")
	}

	// Finally, check that all location parameters for location kind are non-empty.
	for _, c := range selector {
		if c.Type.kind() == kindLocation && c.Location == "" {
			return fmt.Errorf("path/name parameter must be non-empty")
		}
	}

	return nil
}
