#!/bin/bash

# System tests env vars
export TEST_WEBHOOK_HOST="localhost"
export ESTI_SETUP_SGHUB="true"
export ESTI_BLOCKSTORE_TYPE="local"
export ESTI_STORAGE_NAMESPACE="local://system-testing"
export ESTI_S3_ENDPOINT="localhost:8000"
export ESTI_FORCE_PATH_STYLE="true"

# Hub env vars for test
export SGHUBACTION_VAR="this_is_actions_var"
