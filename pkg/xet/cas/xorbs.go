package cas

import (
	"context"
	"io"

	"github.com/treeverse/lakefs/pkg/block"
	xetstore "github.com/treeverse/lakefs/pkg/xet/store"
)

type XorbStore struct {
	adapter          block.Adapter
	storageNamespace string
}

type PutXorbResult struct {
	WasInserted bool
}

func NewXorbStore(adapter block.Adapter, storageNamespace string) *XorbStore {
	return &XorbStore{
		adapter:          adapter,
		storageNamespace: storageNamespace,
	}
}

func (s *XorbStore) Put(ctx context.Context, prefix, hash string, sizeBytes int64, reader io.Reader) (PutXorbResult, error) {
	obj := s.objectPointer(prefix, hash)
	exists, err := s.adapter.Exists(ctx, obj)
	if err != nil {
		return PutXorbResult{}, err
	}
	if exists {
		return PutXorbResult{WasInserted: false}, nil
	}
	if _, err := s.adapter.Put(ctx, obj, sizeBytes, reader, block.PutOpts{}); err != nil {
		return PutXorbResult{}, err
	}
	return PutXorbResult{WasInserted: true}, nil
}

func (s *XorbStore) Get(ctx context.Context, prefix, hash string) (io.ReadCloser, error) {
	return s.adapter.Get(ctx, s.objectPointer(prefix, hash))
}

func (s *XorbStore) Exists(ctx context.Context, prefix, hash string) (bool, error) {
	return s.adapter.Exists(ctx, s.objectPointer(prefix, hash))
}

func (s *XorbStore) objectPointer(prefix, hash string) block.ObjectPointer {
	return xetstore.XorbObjectPointer(s.storageNamespace, prefix, hash)
}
