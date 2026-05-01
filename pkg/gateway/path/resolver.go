package path

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/invergent-ai/surogate-hub/pkg/graveler"

	"github.com/invergent-ai/surogate-hub/pkg/block"
)

const (
	Separator = "/"

	rePath      = "(?P<path>.*)"
	reReference = `(?P<ref>[^/]+)`
)

var (
	EncodedPathRe          = regexp.MustCompile(fmt.Sprintf("^/?%s/%s", reReference, rePath))
	EncodedPathReferenceRe = regexp.MustCompile(fmt.Sprintf("^/?%s$", reReference))

	ErrPathMalformed = errors.New("encoded path is malformed")
)

type Ref struct {
	Branch   string
	CommitID graveler.CommitID
}

type ResolvedPath struct {
	Ref      string
	Path     string
	WithPath bool
}

type ResolvedAbsolutePath struct {
	Repo      string
	Reference string
	Path      string
}

func ResolveAbsolutePath(encodedPath string) (ResolvedAbsolutePath, error) {
	const encodedPartsCount = 4
	encodedPath = strings.TrimLeft(encodedPath, "/")
	bucketParts := strings.SplitN(encodedPath, "/", 3) //nolint:mnd
	if len(bucketParts) == 3 {
		if repo, ok := BucketToRepositoryID(bucketParts[0]); ok {
			return ResolvedAbsolutePath{
				Repo:      repo,
				Reference: bucketParts[1],
				Path:      bucketParts[2],
			}, nil
		}
	}
	parts := strings.SplitN(encodedPath, "/", encodedPartsCount)
	if len(parts) != encodedPartsCount {
		return ResolvedAbsolutePath{}, ErrPathMalformed
	}
	return ResolvedAbsolutePath{
		Repo:      parts[0] + Separator + parts[1],
		Reference: parts[2],
		Path:      parts[3],
	}, nil
}

func ResolvePath(encodedPath string) (ResolvedPath, error) {
	r := ResolvedPath{}
	if len(encodedPath) == 0 {
		return r, nil // empty path.
	}
	// try reference with path or just reference regexp
	for _, re := range []*regexp.Regexp{EncodedPathRe, EncodedPathReferenceRe} {
		match := re.FindStringSubmatch(encodedPath)
		if len(match) == 0 {
			continue
		}
		for i, name := range re.SubexpNames() {
			switch name {
			case "ref":
				r.Ref = match[i]
			case "path":
				r.Path = match[i]
				r.WithPath = true
			}
		}
		return r, nil
	}
	return r, ErrPathMalformed
}

func WithRef(path, ref string) string {
	return block.JoinPathParts([]string{ref, path})
}
