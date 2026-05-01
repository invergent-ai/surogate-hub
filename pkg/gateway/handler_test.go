package gateway_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/invergent-ai/surogate-hub/pkg/gateway/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const repoName = "test-user/example"

func setupTest(t *testing.T, method, target string, body io.Reader) *http.Response {
	h, _ := testutil.GetBasicHandler(t, &testutil.FakeAuthService{
		BareDomain:      "example.com",
		AccessKeyID:     "AKIAIO5FODNN7EXAMPLE",
		SecretAccessKey: "MockAccessSecretKey",
		UserID:          "65867",
		Region:          "MockRegion",
	}, repoName)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(method, target, body)
	req.Host = "host.domain.com"
	req.Header.Set("Content-Type", "text/tab - separated - values")
	payloadHashBytes := sha256.Sum256(nil)
	payloadHash := hex.EncodeToString(payloadHashBytes[:])
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	err := v4.NewSigner().SignHTTP(context.Background(), aws.Credentials{
		AccessKeyID:     "AKIAIO5FODNN7EXAMPLE",
		SecretAccessKey: "MockAccessSecretKey",
	}, req, payloadHash, "s3", "us-east-1", time.Date(2020, 5, 17, 9, 39, 7, 0, time.UTC))
	require.NoError(t, err)
	h.ServeHTTP(rr, req)
	return rr.Result()
}

func TestPathWithTrailingSlash(t *testing.T) {
	result := setupTest(t, http.MethodHead, "/test-user/example/", nil)
	testPathWithTrailingSlash(t, result)
}

func testPathWithTrailingSlash(t *testing.T, result *http.Response) {
	assert.Equal(t, 200, result.StatusCode)
	bytes, err := io.ReadAll(result.Body)
	assert.NoError(t, err)
	assert.Len(t, bytes, 0)
	assert.Contains(t, result.Header, "X-Amz-Request-Id")
}
