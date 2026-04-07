#!/bin/bash
# ESB Spine integration test runner
# Usage: ./scripts/run-integration-tests.sh [test-file]
set -euo pipefail

cd "$(dirname "$0")/.."

if [ $# -eq 1 ]; then
  node --test "test/integration/$1"
else
  node --test 'test/integration/*.test.js'
fi
