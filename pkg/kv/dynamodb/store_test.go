package dynamodb_test

import (
	"context"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/kv/dynamodb"
	"github.com/invergent-ai/surogate-hub/pkg/testutil"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/kv/kvparams"
	"github.com/invergent-ai/surogate-hub/pkg/kv/kvtest"
)

func TestDynamoKV(t *testing.T) {
	kvtest.DriverTest(t, func(t testing.TB, ctx context.Context) kv.Store {
		t.Helper()
		testParams = &kvparams.DynamoDB{
			TableName:          testutil.UniqueKVTableName(),
			ScanLimit:          kvtest.MaxPageSize,
			Endpoint:           databaseURI,
			AwsRegion:          "us-east-1",
			AwsAccessKeyID:     "fakeMyKeyId",
			AwsSecretAccessKey: "fakeSecretAccessKey",
		}

		store, err := kv.Open(ctx, kvparams.Config{DynamoDB: testParams, Type: dynamodb.DriverName})
		if err != nil {
			t.Fatalf("failed to open kv '%s' store: %s", dynamodb.DriverName, err)
		}
		t.Cleanup(store.Close)
		return store
	})
}
