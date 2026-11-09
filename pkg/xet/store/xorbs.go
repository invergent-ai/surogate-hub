package store

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/invergent-ai/surogate-hub/pkg/block"
	"github.com/invergent-ai/surogate-hub/pkg/config"
)

func XorbObjectPointer(storageNamespace, prefix, hash string) block.ObjectPointer {
	return block.ObjectPointer{
		StorageID:        config.SingleBlockstoreID,
		StorageNamespace: storageNamespace,
		Identifier:       "xet/xorbs/" + prefix + "/" + hash,
		IdentifierType:   block.IdentifierTypeRelative,
	}
}

// ParsePhysicalAddress returns the file hash of an "xet://<hash>" address.
func ParsePhysicalAddress(address string) (string, bool) {
	return strings.CutPrefix(address, "xet://")
}

// StorageNamespace returns where xorbs are stored, or "" when xet has nowhere
// to put them. Object stores need blockstore.default_namespace_prefix, because
// "<type>://_hub_xet" would name a bucket.
func StorageNamespace(cfg config.Config) string {
	storage := cfg.StorageConfig().GetStorageByID(config.SingleBlockstoreID)
	if storage == nil {
		return ""
	}
	if prefix := storage.GetDefaultNamespacePrefix(); prefix != nil && *prefix != "" {
		return strings.TrimRight(*prefix, "/") + "/_hub_xet"
	}
	switch storage.BlockstoreType() {
	case block.BlockstoreTypeLocal, block.BlockstoreTypeMem:
		return storage.BlockstoreType() + "://_hub_xet"
	}
	return ""
}

// XorbWalkURI is the storage URI to list xorbs with. The local blockstore
// resolves "local://_hub_xet" against its root for reads and writes, but its
// walker only accepts an absolute path under that root.
func XorbWalkURI(cfg config.Config) (*url.URL, error) {
	namespace := StorageNamespace(cfg)
	if namespace == "" {
		return nil, fmt.Errorf("xet storage is not configured")
	}
	uri, err := url.Parse(strings.TrimRight(namespace, "/"))
	if err != nil {
		return nil, err
	}
	if uri.Scheme != block.BlockstoreTypeLocal {
		return uri, nil
	}
	local, err := cfg.StorageConfig().GetStorageByID(config.SingleBlockstoreID).BlockstoreLocalParams()
	if err != nil {
		return nil, err
	}
	return &url.URL{Scheme: block.BlockstoreTypeLocal, Path: filepath.Join(local.Path, uri.Host, uri.Path)}, nil
}
