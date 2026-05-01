#!/bin/bash

cd "$(dirname "$0")" || exit
. set_env_vars.sh

SGHUB_LOG=$(mktemp --suffix=.log --tmpdir hub_XXX)
TEST_LOG=$(mktemp --suffix=.log --tmpdir hub_tests_XXX)
TEST_DATA_DIR=/tmp/sghub
RUN_RESULT=2

trap cleanup EXIT

cleanup() {
  pkill sghub
  if [ $RUN_RESULT == 0 ]; then
    echo "Tests successful, cleaning up logs files"
    rm $SGHUB_LOG
    rm $TEST_LOG
  elif [ $RUN_RESULT == 1 ]; then
    echo "Tests failed! See logs for more information: $SGHUB_LOG $TEST_LOG"
  fi
}

invalid_option() {
  echo "Error: Invalid option"
  Help
}

help() {
  echo "Local system tests execution"
  echo
  echo "Syntax: runner [-h|r]"
  echo "options:"
  echo "h     Print this Help."
  echo "r     Runs the given process [sghub | tests | all]."
  echo
}

wait_for_sghub_ready() {
  echo "Waiting for Surogate Hub ready"
  until curl --output /dev/null --silent --head --fail localhost:8000/_health; do
      printf '.'
      sleep 1
  done
  echo "Surogate Hub is ready"
}

run_tests() {
  echo "Run Tests (logs at $TEST_LOG)"
  go test -v ../../esti --args --system-tests --use-local-credentials --test.skip=".*GC" "$@" | tee "$TEST_LOG"
  return "${PIPESTATUS[0]}"
}

run_sghub() {
  echo "Run Surogate Hub (logs at $SGHUB_LOG)"
  ../../sghub run -c sghub.yaml | tee "$SGHUB_LOG"
}

prepare_test_state() {
  echo "Cleaning local system test state at $TEST_DATA_DIR"
  rm -rf "$TEST_DATA_DIR"
}

run_all() {
  prepare_test_state
  run_sghub &

  wait_for_sghub_ready

  run_tests "$@"
  RUN_RESULT=$?
  return $RUN_RESULT		# restore failure (the previous line succeeds in sh)
}

# Get the options
while getopts ":hr:" option; do
  case $option in
  h) # Display Help
    help
    exit
    ;;
  r) # Run
    run=$OPTARG
    shift 2
    if [ "$run" == "test" ]; then
      run_tests "$@"
    elif [ "$run" == "sghub" ]; then
      run_sghub
    elif [ "$run" == "all" ]; then
      run_all "$@"
    else
      invalid_option
    fi
    exit
    ;;
  \?) # Invalid option
    invalid_option
    exit
    ;;
  esac
done

help # No arguments passed
