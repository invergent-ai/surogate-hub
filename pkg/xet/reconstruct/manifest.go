package reconstruct

import "fmt"

type IndexRange struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
}

type HTTPRange struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

type ManifestTerm struct {
	Hash           string     `json:"hash"`
	Range          IndexRange `json:"range"`
	UnpackedLength uint32     `json:"unpacked_length"`
}

type XorbRangeDescriptor struct {
	Chunks IndexRange `json:"chunks"`
	Bytes  HTTPRange  `json:"bytes"`
}

type XorbMultiRangeFetch struct {
	URL    string                `json:"url"`
	Ranges []XorbRangeDescriptor `json:"ranges"`
}

type Manifest struct {
	OffsetIntoFirstRange uint64                           `json:"offset_into_first_range"`
	Terms                []ManifestTerm                   `json:"terms"`
	Xorbs                map[string][]XorbMultiRangeFetch `json:"xorbs"`
}

type ResolvedRange struct {
	URL   string
	Bytes HTTPRange
}

type RangeResolver func(xorbHash string, chunks IndexRange) (ResolvedRange, error)

func BuildManifest(terms []Term, resolve RangeResolver) (Manifest, error) {
	manifest := Manifest{
		Xorbs: make(map[string][]XorbMultiRangeFetch),
	}
	if len(terms) == 0 {
		return manifest, nil
	}
	manifest.OffsetIntoFirstRange = terms[0].ChunkOffset

	for i := 0; i < len(terms); {
		current := ManifestTerm{
			Hash: terms[i].XorbHash,
			Range: IndexRange{
				Start: terms[i].ChunkIndex,
				End:   terms[i].ChunkIndex + 1,
			},
		}
		if terms[i].ChunkSizeBytes > uint64(^uint32(0)) {
			return Manifest{}, fmt.Errorf("chunk too large")
		}
		current.UnpackedLength = uint32(terms[i].ChunkSizeBytes)
		i++

		for i < len(terms) && terms[i].XorbHash == current.Hash && terms[i].ChunkIndex == current.Range.End {
			if terms[i].ChunkSizeBytes > uint64(^uint32(0))-uint64(current.UnpackedLength) {
				return Manifest{}, fmt.Errorf("term too large")
			}
			current.Range.End++
			current.UnpackedLength += uint32(terms[i].ChunkSizeBytes)
			i++
		}

		resolved, err := resolve(current.Hash, current.Range)
		if err != nil {
			return Manifest{}, err
		}
		manifest.Terms = append(manifest.Terms, current)
		manifest.Xorbs[current.Hash] = append(manifest.Xorbs[current.Hash], XorbMultiRangeFetch{
			URL: resolved.URL,
			Ranges: []XorbRangeDescriptor{{
				Chunks: current.Range,
				Bytes:  resolved.Bytes,
			}},
		})
	}

	return manifest, nil
}
