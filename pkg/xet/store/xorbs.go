package store

import (
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
