#!/usr/bin/env bash
# Fast development loop: unit + component tests. No Docker required.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
cd "$PROJECT_ROOT"
exec mvn "$@" test
