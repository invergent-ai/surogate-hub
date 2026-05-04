package s3

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/invergent-ai/surogate-hub/pkg/block/params"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
	"github.com/stretchr/testify/require"
)

type deleteObjectRequest struct {
	Objects []deletedObject `xml:"Object"`
}

type deletedObject struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId"`
}

func newDestroyTestAdapter(endpoint string) *Adapter {
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("access-key", "secret-key", ""),
	}
	clients := NewClientCache(cfg, params.S3{
		Endpoint:       endpoint,
		ForcePathStyle: true,
	})
	clients.DiscoverBucketRegion(false)
	return &Adapter{clients: clients}
}

func TestDestroyPrefixDeletesObjectVersionsAndDeleteMarkers(t *testing.T) {
	var deleted []deletedObject
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && hasQueryParam(r, "versions"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name>
  <Prefix>repo/</Prefix>
  <KeyMarker></KeyMarker>
  <VersionIdMarker></VersionIdMarker>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Version>
    <Key>repo/live</Key>
    <VersionId>v2</VersionId>
    <IsLatest>true</IsLatest>
    <LastModified>2026-05-04T00:00:00.000Z</LastModified>
    <ETag>"etag"</ETag>
    <Size>1</Size>
    <StorageClass>STANDARD</StorageClass>
  </Version>
  <Version>
    <Key>repo/live</Key>
    <VersionId>v1</VersionId>
    <IsLatest>false</IsLatest>
    <LastModified>2026-05-04T00:00:00.000Z</LastModified>
    <ETag>"etag"</ETag>
    <Size>1</Size>
    <StorageClass>STANDARD</StorageClass>
  </Version>
  <DeleteMarker>
    <Key>repo/deleted</Key>
    <VersionId>d1</VersionId>
    <IsLatest>false</IsLatest>
    <LastModified>2026-05-04T00:00:00.000Z</LastModified>
  </DeleteMarker>
</ListVersionsResult>`))
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name>
  <Prefix>repo/</Prefix>
  <KeyCount>0</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>`))
		case r.Method == http.MethodPost && hasQueryParam(r, "delete"):
			var req deleteObjectRequest
			require.NoError(t, xml.NewDecoder(r.Body).Decode(&req))
			deleted = append(deleted, req.Objects...)
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	adapter := newDestroyTestAdapter(srv.URL)
	adapter.destroyPrefix("bucket", "repo/", "s3://bucket/repo")

	require.ElementsMatch(t, []deletedObject{
		{Key: "repo/live", VersionID: "v2"},
		{Key: "repo/live", VersionID: "v1"},
		{Key: "repo/deleted", VersionID: "d1"},
	}, deleted)
}

func TestDestroyPrefixWarnsInsteadOfPurgedWhenDeleteErrors(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "s3-destroy.log")
	logging.SetOutputFormat("json")
	logging.SetLevel("info")
	require.NoError(t, logging.SetOutputs([]string{logPath}, 1, 1))
	t.Cleanup(func() {
		_ = logging.SetOutputs([]string{"="}, 1, 1)
		logging.SetOutputFormat("text")
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && hasQueryParam(r, "versions"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name>
  <Prefix>repo/</Prefix>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
</ListVersionsResult>`))
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name>
  <Prefix>repo/</Prefix>
  <KeyCount>1</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>repo/live</Key>
    <LastModified>2026-05-04T00:00:00.000Z</LastModified>
    <ETag>"etag"</ETag>
    <Size>1</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>`))
		case r.Method == http.MethodPost && hasQueryParam(r, "delete"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Error>
    <Key>repo/live</Key>
    <Code>AccessDenied</Code>
    <Message>denied</Message>
  </Error>
</DeleteResult>`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	adapter := newDestroyTestAdapter(srv.URL)
	adapter.destroyPrefix("bucket", "repo/", "s3://bucket/repo")

	logs, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.Contains(t, string(logs), "Destroy: storage namespace cleanup completed with delete errors")
	require.NotContains(t, string(logs), "Destroy: storage namespace purged")
}

func hasQueryParam(r *http.Request, key string) bool {
	_, ok := r.URL.Query()[key]
	return ok
}
