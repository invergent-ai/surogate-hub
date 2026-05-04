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

type ManifestV1 struct {
	OffsetIntoFirstRange uint64                       `json:"offset_into_first_range"`
	Terms                []ManifestTerm               `json:"terms"`
	FetchInfo            map[string][]XorbFetchInfoV1 `json:"fetch_info"`
}

type XorbFetchInfoV1 struct {
	Range    IndexRange `json:"range"`
	URL      string     `json:"url"`
	URLRange HTTPRange  `json:"url_range"`
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

	groupedTerms, err := GroupTerms(terms)
	if err != nil {
		return Manifest{}, err
	}
	for _, current := range groupedTerms {
		resolved, err := resolve(current.Hash, current.Range)
		if err != nil {
			return Manifest{}, err
		}
		manifest.Terms = append(manifest.Terms, current)
		manifest.appendXorbRange(current.Hash, resolved.URL, XorbRangeDescriptor{
			Chunks: current.Range,
			Bytes:  resolved.Bytes,
		})
	}

	return manifest, nil
}

func GroupTerms(terms []Term) ([]ManifestTerm, error) {
	var grouped []ManifestTerm
	for i := 0; i < len(terms); {
		current := ManifestTerm{
			Hash: terms[i].XorbHash,
			Range: IndexRange{
				Start: terms[i].ChunkIndex,
				End:   terms[i].ChunkIndex + 1,
			},
		}
		if terms[i].ChunkSizeBytes > uint64(^uint32(0)) {
			return nil, fmt.Errorf("chunk too large")
		}
		current.UnpackedLength = uint32(terms[i].ChunkSizeBytes)
		i++

		for i < len(terms) && terms[i].XorbHash == current.Hash && terms[i].ChunkIndex == current.Range.End {
			if terms[i].ChunkSizeBytes > uint64(^uint32(0))-uint64(current.UnpackedLength) {
				return nil, fmt.Errorf("term too large")
			}
			current.Range.End++
			current.UnpackedLength += uint32(terms[i].ChunkSizeBytes)
			i++
		}

		grouped = append(grouped, current)
	}
	return grouped, nil
}

func (m *Manifest) appendXorbRange(xorbHash, url string, descriptor XorbRangeDescriptor) {
	fetches := m.Xorbs[xorbHash]
	for i := range fetches {
		if fetches[i].URL == url {
			if len(fetches[i].Ranges) == 0 {
				fetches[i].Ranges = append(fetches[i].Ranges, descriptor)
			} else {
				fetches[i].Ranges[0] = mergeXorbRangeDescriptor(fetches[i].Ranges[0], descriptor)
			}
			m.Xorbs[xorbHash] = fetches
			return
		}
	}
	m.Xorbs[xorbHash] = append(fetches, XorbMultiRangeFetch{
		URL:    url,
		Ranges: []XorbRangeDescriptor{descriptor},
	})
}

func mergeXorbRangeDescriptor(a, b XorbRangeDescriptor) XorbRangeDescriptor {
	return XorbRangeDescriptor{
		Chunks: IndexRange{
			Start: min(a.Chunks.Start, b.Chunks.Start),
			End:   max(a.Chunks.End, b.Chunks.End),
		},
		Bytes: HTTPRange{
			Start: min(a.Bytes.Start, b.Bytes.Start),
			End:   max(a.Bytes.End, b.Bytes.End),
		},
	}
}

func (m Manifest) V1() ManifestV1 {
	v1 := ManifestV1{
		OffsetIntoFirstRange: m.OffsetIntoFirstRange,
		Terms:                m.Terms,
		FetchInfo:            make(map[string][]XorbFetchInfoV1, len(m.Xorbs)),
	}
	for xorbHash, fetches := range m.Xorbs {
		for _, fetch := range fetches {
			for _, descriptor := range fetch.Ranges {
				v1.FetchInfo[xorbHash] = append(v1.FetchInfo[xorbHash], XorbFetchInfoV1{
					Range:    descriptor.Chunks,
					URL:      fetch.URL,
					URLRange: descriptor.Bytes,
				})
			}
		}
	}
	return v1
}
