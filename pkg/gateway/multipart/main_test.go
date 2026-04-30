package multipart_test

import (
	"flag"
	"os"
	"testing"

	_ "github.com/invergent-ai/surogate-hub/pkg/kv/mem"
	_ "github.com/invergent-ai/surogate-hub/pkg/kv/postgres"
	"github.com/invergent-ai/surogate-hub/pkg/logging"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		// keep the log level calm
		logging.SetLevel("panic")
	}
	code := m.Run()
	defer os.Exit(code)
}
