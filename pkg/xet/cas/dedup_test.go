package cas

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/invergent-ai/surogate-hub/pkg/block/mem"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
	xetstore "github.com/invergent-ai/surogate-hub/pkg/xet/store"
	"github.com/invergent-ai/surogate-hub/pkg/xet/xettest"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"
)

type dedupFixture struct {
	ctx      context.Context
	registry *xetstore.Registry
	handler  http.Handler
}

func newDedupFixture(t *testing.T, opts ...HandlerOption) dedupFixture {
	t.Helper()
	ctx := context.Background()
	registry := xetstore.NewRegistry(kvtest.GetStore(ctx, t))
	opts = append([]HandlerOption{
		WithXorbStore(NewXorbStore(mem.New(ctx), "mem://xet-cas")),
		WithTokenSigningKey([]byte("test-token-key")),
		WithTokenAuthRequired(),
	}, opts...)
	return dedupFixture{ctx: ctx, registry: registry, handler: NewHandler(registry, opts...)}
}

func (f dedupFixture) do(t *testing.T, method, path string, body []byte, mode, repo string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testXETToken(t, "user", TokenGrant{Mode: mode, Repo: repo, Ref: "main", Path: "model.bin"}, time.Now().Add(time.Hour)))
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

// upload stores a xorb of the chunks and registers a file made of all of them
// in repo, as hf_xet does.
func (f dedupFixture) upload(t *testing.T, repo string, chunks ...[]byte) (fileHash, xorbHash string) {
	t.Helper()
	xorbHash, xorb := xettest.SerializedXorb(t, chunks...)
	rec := f.do(t, http.MethodPost, "/v1/xorbs/default/"+xorbHash, xorb, "write", repo)
	require.Equalf(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	fileHash = xettest.FileHash(t, chunks...)
	var size uint32
	for _, c := range chunks {
		size += uint32(len(c))
	}
	rec = f.do(t, http.MethodPost, "/v1/shards", xettest.Shard(t, fileHash, xorbHash, 0, uint32(len(chunks)), size, chunks...), "write", repo)
	require.Equalf(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	return fileHash, xorbHash
}

func (f dedupFixture) lookup(t *testing.T, chunk []byte, repo string) *httptest.ResponseRecorder {
	t.Helper()
	return f.do(t, http.MethodGet, "/v1/chunks/default-merkledb/"+xetstore.ComputeDataHash(chunk), nil, "write", repo)
}

// eligibleChunk returns chunk content whose hash hf_xet would query.
func eligibleChunk(t *testing.T, tag string) []byte {
	t.Helper()
	for i := 0; i < 1_000_000; i++ {
		data := []byte(fmt.Sprintf("%s-%d", tag, i))
		if xetstore.GlobalDedupEligible(xetstore.ComputeDataHash(data)) {
			return data
		}
	}
	t.Fatal("no eligible chunk found")
	return nil
}

func ineligibleChunk(t *testing.T, tag string) []byte {
	t.Helper()
	for i := 0; ; i++ {
		data := []byte(fmt.Sprintf("%s-%d", tag, i))
		if !xetstore.GlobalDedupEligible(xetstore.ComputeDataHash(data)) {
			return data
		}
	}
}

// requireKeyedShard checks a chunk lookup answer: a shard with no file records
// that names the xorbs and carries the given chunks only in keyed form.
func requireKeyedShard(t *testing.T, body []byte, xorbHashes []string, chunks ...[]byte) {
	t.Helper()
	files, err := xetstore.ParseShardFiles(body)
	require.NoError(t, err)
	require.Empty(t, files)
	key := body[len(body)-200+72 : len(body)-200+104]
	require.NotEqual(t, make([]byte, 32), key, "chunk hashes must be keyed")
	for _, x := range xorbHashes {
		require.True(t, bytes.Contains(body, rawHash(t, x)), "xorb %s missing", x)
	}
	for _, c := range chunks {
		raw := rawHash(t, xetstore.ComputeDataHash(c))
		require.False(t, bytes.Contains(body, raw), "plain chunk hash leaked")
		h := blake3.New(32, key)
		_, _ = h.Write(raw)
		require.True(t, bytes.Contains(body, h.Sum(nil)), "keyed chunk hash missing")
	}
}

func rawHash(t *testing.T, hash string) []byte {
	t.Helper()
	var b bytes.Buffer
	xettest.WriteHash(t, &b, hash)
	return b.Bytes()
}

func TestPostShardIndexesTheFirstAndEligibleChunks(t *testing.T) {
	f := newDedupFixture(t)
	first, eligible, other := ineligibleChunk(t, "first"), eligibleChunk(t, "eligible"), ineligibleChunk(t, "other")
	fileHash, xorbHash := f.upload(t, "owner-a/repo", first, eligible, other)

	for _, c := range [][]byte{first, eligible} {
		got, err := f.registry.DedupEntryForChunk(f.ctx, "owner-a", xetstore.ComputeDataHash(c))
		require.NoError(t, err)
		require.Equal(t, xetstore.DedupEntry{FileHash: fileHash, XorbHash: xorbHash}, got)
	}
	_, err := f.registry.DedupEntryForChunk(f.ctx, "owner-a", xetstore.ComputeDataHash(other))
	require.ErrorIs(t, err, kv.ErrNotFound)
}

func TestChunkLookupReturnsAKeyedShardForTheSameOwner(t *testing.T) {
	f := newDedupFixture(t)
	first, eligible := ineligibleChunk(t, "first"), eligibleChunk(t, "eligible")
	_, xorbHash := f.upload(t, "owner-a/repo", first, eligible)

	for _, c := range [][]byte{first, eligible} {
		rec := f.lookup(t, c, "owner-a/other-repo")
		require.Equalf(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		requireKeyedShard(t, rec.Body.Bytes(), []string{xorbHash}, first, eligible)
	}
}

func TestChunkLookupIsScopedToTheOwnerUnlessGlobal(t *testing.T) {
	first, second := ineligibleChunk(t, "first"), ineligibleChunk(t, "second")

	owner := newDedupFixture(t)
	owner.upload(t, "owner-a/repo", first, second)
	require.Equal(t, http.StatusNotFound, owner.lookup(t, first, "owner-b/repo").Code)

	global := newDedupFixture(t, WithGlobalDedup())
	_, xorbHash := global.upload(t, "owner-a/repo", first, second)
	rec := global.lookup(t, first, "owner-b/repo")
	require.Equalf(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	requireKeyedShard(t, rec.Body.Bytes(), []string{xorbHash}, first, second)
}

func TestChunkLookupNeedsAWriteToken(t *testing.T) {
	f := newDedupFixture(t)
	first := ineligibleChunk(t, "first")
	f.upload(t, "owner-a/repo", first)

	rec := f.do(t, http.MethodGet, "/v1/chunks/default-merkledb/"+xetstore.ComputeDataHash(first), nil, "read", "owner-a/repo")
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestChunkLookupMissesUnknownParkedOrMalformed(t *testing.T) {
	f := newDedupFixture(t)
	first := ineligibleChunk(t, "first")
	fileHash, xorbHash := f.upload(t, "owner-a/repo", first)

	require.Equal(t, http.StatusNotFound, f.lookup(t, ineligibleChunk(t, "unknown"), "owner-a/repo").Code)
	require.Equal(t, http.StatusBadRequest, f.do(t, http.MethodGet, "/v1/chunks/default-merkledb/xyz", nil, "write", "owner-a/repo").Code)

	// A parked footer is not offered: GC considers the xorb unused.
	_, err := f.registry.Park(f.ctx, xetstore.ParkedXorb, xorbHash, time.Now())
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, f.lookup(t, first, "owner-a/repo").Code)
	_, err = f.registry.Restore(f.ctx, xetstore.ParkedXorb, xorbHash, nil, false)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, f.lookup(t, first, "owner-a/repo").Code)

	// Neither is a parked file record.
	_, err = f.registry.Park(f.ctx, xetstore.ParkedFile, fileHash, time.Now())
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, f.lookup(t, first, "owner-a/repo").Code)
}

// registerFileOfXorbs stores each xorb and a file made of all their chunks.
func (f dedupFixture) registerFileOfXorbs(t *testing.T, xorbs ...[][]byte) (string, []string) {
	t.Helper()
	var all [][]byte
	var hashes []string
	var segments []xetstore.ShardFileSegment
	var size uint64
	for _, chunks := range xorbs {
		hash, data := xettest.SerializedXorb(t, chunks...)
		rec := f.do(t, http.MethodPost, "/v1/xorbs/default/"+hash, data, "write", "owner-a/repo")
		require.Equalf(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var n uint64
		for _, c := range chunks {
			n += uint64(len(c))
		}
		segments = append(segments, xetstore.ShardFileSegment{XorbHash: hash, SizeBytes: n, ChunkIndexEnd: uint32(len(chunks))})
		hashes = append(hashes, hash)
		all = append(all, chunks...)
		size += n
	}
	fileHash := xettest.FileHash(t, all...)
	_, err := f.registry.RegisterFile(f.ctx, xetstore.ShardFileInfo{FileHash: fileHash, SizeBytes: size, Segments: segments})
	require.NoError(t, err)
	return fileHash, hashes
}

func TestChunkLookupStartsAtTheXorbHoldingTheChunk(t *testing.T) {
	f := newDedupFixture(t)
	a1, a2 := ineligibleChunk(t, "a1"), ineligibleChunk(t, "a2")
	b1, b2 := eligibleChunk(t, "b1"), ineligibleChunk(t, "b2")
	fileHash, xorbs := f.registerFileOfXorbs(t, [][]byte{a1, a2}, [][]byte{b1, b2})
	require.NoError(t, f.registry.IndexDedupChunks(f.ctx, "owner-a", map[string]xetstore.DedupEntry{xetstore.ComputeDataHash(b1): {FileHash: fileHash, XorbHash: xorbs[1]}}))

	rec := f.lookup(t, b1, "owner-a/repo")
	require.Equalf(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	requireKeyedShard(t, rec.Body.Bytes(), []string{xorbs[1]}, b1, b2)
	require.False(t, bytes.Contains(rec.Body.Bytes(), rawHash(t, xorbs[0])), "xorbs before the chunk are not offered")
}

func TestChunkLookupNeverOffersALaterOneChunkXorb(t *testing.T) {
	f := newDedupFixture(t)
	a1, a2 := ineligibleChunk(t, "a1"), ineligibleChunk(t, "a2")
	lone := ineligibleChunk(t, "lone")
	c1, c2 := ineligibleChunk(t, "c1"), ineligibleChunk(t, "c2")
	fileHash, xorbs := f.registerFileOfXorbs(t, [][]byte{a1, a2}, [][]byte{lone}, [][]byte{c1, c2})
	require.NoError(t, f.registry.IndexDedupChunks(f.ctx, "owner-a", map[string]xetstore.DedupEntry{xetstore.ComputeDataHash(a1): {FileHash: fileHash, XorbHash: xorbs[0]}}))

	rec := f.lookup(t, a1, "owner-a/repo")
	require.Equalf(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	requireKeyedShard(t, rec.Body.Bytes(), []string{xorbs[0], xorbs[2]}, a1, a2, c1, c2)
	require.False(t, bytes.Contains(rec.Body.Bytes(), rawHash(t, xorbs[1])), "a one-chunk xorb's hash is a real chunk hash")
}

func TestPostShardClaimsTheFileForTheTokenRepository(t *testing.T) {
	f := newDedupFixture(t)
	fileHash, _ := f.upload(t, "owner-a/repo", ineligibleChunk(t, "a"), ineligibleChunk(t, "b"))

	claimed, err := f.registry.HasUploadClaim(f.ctx, "owner-a/repo", fileHash)
	require.NoError(t, err)
	require.True(t, claimed)
	claimed, err = f.registry.HasUploadClaim(f.ctx, "owner-a/other", fileHash)
	require.NoError(t, err)
	require.False(t, claimed)
}

func shardKey(body []byte) []byte {
	return body[len(body)-200+72 : len(body)-200+104]
}

func TestChunkLookupKeyIsStablePerScope(t *testing.T) {
	f := newDedupFixture(t)
	a := ineligibleChunk(t, "a")
	b := ineligibleChunk(t, "b")
	f.upload(t, "owner-a/repo", a, ineligibleChunk(t, "a2"))
	f.upload(t, "owner-b/repo", b, ineligibleChunk(t, "b2"))

	first := f.lookup(t, a, "owner-a/repo")
	again := f.lookup(t, a, "owner-a/other")
	other := f.lookup(t, b, "owner-b/repo")
	for _, rec := range []*httptest.ResponseRecorder{first, again, other} {
		require.Equalf(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	}
	// One key per scope and day keeps the client to one shard collection per key.
	require.Equal(t, shardKey(first.Body.Bytes()), shardKey(again.Body.Bytes()))
	require.NotEqual(t, shardKey(first.Body.Bytes()), shardKey(other.Body.Bytes()))
}

func TestChunkLookupIsOffWhenAShardWouldOutliveTheXorbGrace(t *testing.T) {
	f := newDedupFixture(t, WithDedupShardValidFor(0))
	a := ineligibleChunk(t, "a")
	f.upload(t, "owner-a/repo", a, ineligibleChunk(t, "a2"))

	require.Equal(t, http.StatusNotFound, f.lookup(t, a, "owner-a/repo").Code)
}

func TestDedupShardValidForStaysWellUnderTheXorbGrace(t *testing.T) {
	require.Equal(t, 24*time.Hour, DedupShardValidFor(168*time.Hour))
	require.Equal(t, 12*time.Hour, DedupShardValidFor(48*time.Hour))
	require.Zero(t, DedupShardValidFor(2*time.Hour))
	require.Zero(t, DedupShardValidFor(0))
}
