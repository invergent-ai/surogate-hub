#!/bin/bash

# System tests env vars
export TEST_WEBHOOK_HOST="localhost"
export ESTI_SETUP_SGHUB="true"
export ESTI_STORAGE_NAMESPACE="local://system-testing"

# Hub env vars for test
export SGHUBACTION_VAR="this_is_actions_var"
