package stats

import (
	"fmt"
	"strings"
)

// SplitNamespacedRepo splits an `{owner}/{name}` namespaced repository id and returns the two
// halves. It returns an error if the id does not contain a slash, has an empty owner, or has an
// empty name. This lives in pkg/stats rather than alongside the catalog wiring so API and gateway
// callers can use it without pulling in the catalog dependency.
func SplitNamespacedRepo(id string) (owner, name string, err error) {
	i := strings.IndexByte(id, '/')
	if i <= 0 || i == len(id)-1 {
		return "", "", fmt.Errorf("not an owner/name id: %q", id)
	}
	return id[:i], id[i+1:], nil
}
