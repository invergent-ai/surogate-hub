package gc

import (
	"context"
	"net/url"
	"strings"

	"github.com/treeverse/lakefs/pkg/block"
)

func ListXorbsFromWalker(ctx context.Context, walker block.Walker, storageURI *url.URL) ([]XorbRef, error) {
	var refs []XorbRef
	err := walker.Walk(ctx, storageURI, block.WalkOptions{}, func(entry block.ObjectStoreEntry) error {
		ref, ok := xorbRefFromKey(entry.RelativeKey)
		if !ok {
			ref, ok = xorbRefFromKey(entry.FullKey)
		}
		if ok {
			refs = append(refs, ref)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return refs, nil
}

func xorbRefFromKey(key string) (XorbRef, bool) {
	rest, ok := strings.CutPrefix(key, "xet/xorbs/")
	if !ok {
		return XorbRef{}, false
	}
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return XorbRef{}, false
	}
	return XorbRef{Prefix: parts[0], Hash: parts[1]}, true
}
