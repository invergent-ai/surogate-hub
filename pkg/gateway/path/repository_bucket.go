package path

import (
	"math/big"
	"strings"

	"github.com/invergent-ai/surogate-hub/pkg/graveler"
)

const repositoryBucketPrefix = "s"

func RepositoryIDToBucket(repository string) string {
	if repository == "" {
		return ""
	}
	n := new(big.Int).SetBytes([]byte(repository))
	return repositoryBucketPrefix + n.Text(36)
}

func BucketToRepositoryID(bucket string) (string, bool) {
	if !strings.HasPrefix(bucket, repositoryBucketPrefix) {
		return "", false
	}
	n, ok := new(big.Int).SetString(strings.TrimPrefix(bucket, repositoryBucketPrefix), 36)
	if !ok {
		return "", false
	}
	repository := string(n.Bytes())
	if graveler.ValidateRepositoryID(graveler.RepositoryID(repository)) != nil {
		return "", false
	}
	return repository, true
}
