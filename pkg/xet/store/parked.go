package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/kv"
)

// ParkedKind names what a parked key holds.
type ParkedKind string

const (
	ParkedFile ParkedKind = "file"
	ParkedXorb ParkedKind = "xorb"
)

// ParkedState is where a parked key is in its lifecycle. Only the sweep moves a
// key to deleting and only a restore moves it to restoring; both go through a
// compare-and-set from parked, so exactly one of them wins.
type ParkedState string

const (
	StateParked    ParkedState = "parked"
	StateRestoring ParkedState = "restoring"
	StateDeleting  ParkedState = "deleting"
)

// Parked is a live file record or xorb footer moved aside by xet GC. Data is
// empty for a xorb object that never had a footer.
type Parked struct {
	State    ParkedState `json:"state"`
	ParkedAt int64       `json:"parked_at"`
	Data     []byte      `json:"data,omitempty"`
}

type RestoreResult int

const (
	NotParked RestoreResult = iota
	Restored
	Deleting
)

// Park moves a live value aside. It reports false when there is no live value
// or the hash is already parked.
func (r *Registry) Park(ctx context.Context, kind ParkedKind, hash string, now time.Time) (bool, error) {
	live, err := r.store.Get(ctx, []byte(Partition), liveKey(kind, hash))
	if errors.Is(err, kv.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	created, err := r.createParked(ctx, kind, hash, Parked{State: StateParked, ParkedAt: now.Unix(), Data: live.Value})
	if err != nil || !created {
		return false, err
	}
	return true, r.store.Delete(ctx, []byte(Partition), liveKey(kind, hash))
}

// ParkOrphanXorb parks a xorb object that has no footer, so it is deleted a
// grace period later unless an upload claims it first.
func (r *Registry) ParkOrphanXorb(ctx context.Context, hash string, now time.Time) (bool, error) {
	return r.createParked(ctx, ParkedXorb, hash, Parked{State: StateParked, ParkedAt: now.Unix()})
}

func (r *Registry) ListParked(ctx context.Context, kind ParkedKind, batchSize int) (map[string]Parked, error) {
	parked := make(map[string]Parked)
	err := r.scanPrefix(ctx, parkedPrefix(kind), batchSize, func(hash string, value []byte) error {
		var p Parked
		if err := json.Unmarshal(value, &p); err != nil {
			return fmt.Errorf("decode parked xet %s %s: %w", kind, hash, err)
		}
		parked[hash] = p
		return nil
	})
	return parked, err
}

// Restore brings a parked value back to its live key. rebuild supplies the value
// when the parked key has none; with claimOrphan, a hash that is not parked at
// all is claimed and rebuilt, so it cannot interleave with a sweep parking or
// deleting it. A newer live value always wins over the parked one.
func (r *Registry) Restore(ctx context.Context, kind ParkedKind, hash string, rebuild func() ([]byte, error), claimOrphan bool) (RestoreResult, error) {
	for attempt := 0; attempt < 2; attempt++ {
		res, err := r.store.Get(ctx, []byte(Partition), parkedKey(kind, hash))
		if errors.Is(err, kv.ErrNotFound) {
			if !claimOrphan {
				return NotParked, nil
			}
			claim := Parked{State: StateRestoring, ParkedAt: time.Now().Unix()}
			created, err := r.createParked(ctx, kind, hash, claim)
			if err != nil {
				return NotParked, err
			}
			if !created {
				continue
			}
			return r.finishRestore(ctx, kind, hash, claim, rebuild)
		}
		if err != nil {
			return NotParked, err
		}
		p, err := decodeParked(kind, hash, res.Value)
		if err != nil {
			return NotParked, err
		}
		switch p.State {
		case StateDeleting:
			return Deleting, nil
		case StateRestoring:
			return r.finishRestore(ctx, kind, hash, p, rebuild)
		case StateParked:
			p.State = StateRestoring
			swapped, err := r.swapParked(ctx, kind, hash, p, res.Predicate)
			if err != nil {
				return NotParked, err
			}
			if !swapped {
				continue
			}
			return r.finishRestore(ctx, kind, hash, p, rebuild)
		default:
			return NotParked, fmt.Errorf("parked xet %s %s: unknown state %q", kind, hash, p.State)
		}
	}
	return NotParked, fmt.Errorf("restore xet %s %s: parked key kept changing", kind, hash)
}

// DeleteParked deletes a parked value for good: removeObject (if any) runs
// first, then the parked key goes. It reports false when a restore holds the key.
func (r *Registry) DeleteParked(ctx context.Context, kind ParkedKind, hash string, removeObject func() error) (bool, error) {
	res, err := r.store.Get(ctx, []byte(Partition), parkedKey(kind, hash))
	if errors.Is(err, kv.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	p, err := decodeParked(kind, hash, res.Value)
	if err != nil {
		return false, err
	}
	switch p.State {
	case StateParked:
		p.State = StateDeleting
		swapped, err := r.swapParked(ctx, kind, hash, p, res.Predicate)
		if err != nil || !swapped {
			return false, err
		}
	case StateDeleting:
	default:
		return false, nil
	}
	if removeObject != nil {
		if err := removeObject(); err != nil {
			return false, err
		}
	}
	return true, r.store.Delete(ctx, []byte(Partition), parkedKey(kind, hash))
}

// GetFileRestoring reads a file record, bringing it back first if xet GC parked it.
func (r *Registry) GetFileRestoring(ctx context.Context, fileHash string) (ShardFileInfo, error) {
	file, err := r.GetFile(ctx, fileHash)
	if !errors.Is(err, kv.ErrNotFound) {
		return file, err
	}
	if restored, rerr := r.Restore(ctx, ParkedFile, fileHash, nil, false); rerr != nil {
		return ShardFileInfo{}, rerr
	} else if restored != Restored {
		return ShardFileInfo{}, err
	}
	return r.GetFile(ctx, fileHash)
}

// GetXorbInfoRestoring reads a xorb footer, bringing it back first if xet GC
// parked it. rebuild reads the footer from the object for a parked orphan.
func (r *Registry) GetXorbInfoRestoring(ctx context.Context, xorbHash string, rebuild func() ([]byte, error)) ([]byte, error) {
	footer, err := r.GetXorbInfo(ctx, xorbHash)
	if !errors.Is(err, kv.ErrNotFound) {
		return footer, err
	}
	if restored, rerr := r.Restore(ctx, ParkedXorb, xorbHash, rebuild, false); rerr != nil {
		return nil, rerr
	} else if restored != Restored {
		return nil, err
	}
	return r.GetXorbInfo(ctx, xorbHash)
}

func (r *Registry) finishRestore(ctx context.Context, kind ParkedKind, hash string, p Parked, rebuild func() ([]byte, error)) (RestoreResult, error) {
	data := p.Data
	if len(data) == 0 {
		if rebuild == nil {
			return NotParked, fmt.Errorf("parked xet %s %s has no data to restore", kind, hash)
		}
		var err error
		if data, err = rebuild(); err != nil {
			return NotParked, err
		}
	}
	err := r.store.SetIf(ctx, []byte(Partition), liveKey(kind, hash), data, nil)
	if err != nil && !errors.Is(err, kv.ErrPredicateFailed) {
		return NotParked, err
	}
	return Restored, r.store.Delete(ctx, []byte(Partition), parkedKey(kind, hash))
}

func (r *Registry) createParked(ctx context.Context, kind ParkedKind, hash string, p Parked) (bool, error) {
	return r.swapParked(ctx, kind, hash, p, nil)
}

func (r *Registry) swapParked(ctx context.Context, kind ParkedKind, hash string, p Parked, predicate kv.Predicate) (bool, error) {
	data, err := json.Marshal(p)
	if err != nil {
		return false, err
	}
	err = r.store.SetIf(ctx, []byte(Partition), parkedKey(kind, hash), data, predicate)
	if errors.Is(err, kv.ErrPredicateFailed) {
		return false, nil
	}
	return err == nil, err
}

func decodeParked(kind ParkedKind, hash string, value []byte) (Parked, error) {
	var p Parked
	if err := json.Unmarshal(value, &p); err != nil {
		return Parked{}, fmt.Errorf("decode parked xet %s %s: %w", kind, hash, err)
	}
	return p, nil
}

func parkedPrefix(kind ParkedKind) string {
	return "xet/gc/" + string(kind) + "/"
}

func parkedKey(kind ParkedKind, hash string) []byte {
	return []byte(parkedPrefix(kind) + hash)
}

func liveKey(kind ParkedKind, hash string) []byte {
	if kind == ParkedFile {
		return fileKey(hash)
	}
	return xorbKey(hash)
}
