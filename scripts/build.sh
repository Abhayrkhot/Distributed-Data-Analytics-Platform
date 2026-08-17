#!/usr/bin/env bash
# Build all modules and place shaded jars in target-jars/ (mounted at /opt/jars).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
cd "$PROJECT_ROOT"
exec mvn -q "$@" clean package
