package dynamodb_test

import (
	"log"
	"os"
	"testing"

	"github.com/invergent-ai/surogate-hub/pkg/kv/kvparams"
	"github.com/invergent-ai/surogate-hub/pkg/testutil"
)

var testParams *kvparams.DynamoDB
var databaseURI string

func TestMain(m *testing.M) {
	var err error
	var cleanupFunc func()
	databaseURI, cleanupFunc, err = testutil.GetDynamoDBInstance()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	code := m.Run()
	cleanupFunc()
	os.Exit(code)
}
