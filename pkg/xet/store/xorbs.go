package store

import (
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
)

func XorbObjectPointer(storageNamespace, prefix, hash string) block.ObjectPointer {
	return block.ObjectPointer{
		StorageID:        config.SingleBlockstoreID,
		StorageNamespace: storageNamespace,
		Identifier:       "xet/xorbs/" + prefix + "/" + hash,
		IdentifierType:   block.IdentifierTypeRelative,
	}
}
