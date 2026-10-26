package store

import (
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
