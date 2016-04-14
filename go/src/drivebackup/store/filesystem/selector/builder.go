package selector

import "drivebackup/store/filesystem"

func NewSelectorBuilder(buildFunc func(path, latestVersionPath string, isFile bool, version filesystem.Version) filesystem.SelectorOp) *SelectorBuilder {
	return &SelectorBuilder{Build: buildFunc}
}

type SelectorBuilder struct {
	Selector []Constraint
	Build func(path, latestVersionPath string, isFile bool, version filesystem.Version) filesystem.SelectorOp
}

var _ filesystem.Selector = (*SelectorBuilder)(nil)

func (b *SelectorBuilder) Version(version filesystem.Version) filesystem.Selector {
	b.Selector = append(b.Selector, Constraint{
		Type: VersionConstraint,
		Version: version,
	})
	return b
}
func (b *SelectorBuilder) Latest() filesystem.Selector {
	b.Selector = append(b.Selector, Constraint{
		Type: LatestConstraint,
	})
	return b
}
func (b *SelectorBuilder) Dir(path string) filesystem.Selector {
	b.Selector = append(b.Selector, Constraint{
		Type: DirConstraint,
		Location: path,
	})
	return b
}
func (b *SelectorBuilder) File(name string) filesystem.Selector {
	b.Selector = append(b.Selector, Constraint{
		Type: FileConstraint,
		Location: name,
	})
	return b
}

func (b *SelectorBuilder) Versions() ([]filesystem.Version, error) {
	if err := validate(b.Selector, NoFlags); err != nil {
		return nil, err
	}
	path, latestVersionPath, isFile, version := extract(b.Selector)
	return b.Build(path, latestVersionPath, isFile, version).Versions()
}
func (b *SelectorBuilder) List() ([]string, error) {
	if err := validate(b.Selector, NoFlags); err != nil {
		return nil, err
	}
	path, latestVersionPath, isFile, version := extract(b.Selector)
	return b.Build(path, latestVersionPath, isFile, version).List()
}
func (b *SelectorBuilder) BlobRef() (filesystem.StoredBlobRef, error) {
	if err := validate(b.Selector, RequireFile | RequireVersion); err != nil {
		return filesystem.StoredBlobRef{}, err
	}
	path, latestVersionPath, isFile, version := extract(b.Selector)
	return b.Build(path, latestVersionPath, isFile, version).BlobRef()
}