#!/bin/bash
set -euo pipefail

# Only run in remote (cloud) environments
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# Install cppcheck (used by `make lint` / `make cppcheck`).
# The Makefile gracefully skips cppcheck when absent, so don't fail the
# session if the install doesn't succeed.
if ! command -v cppcheck >/dev/null 2>&1; then
  sudo apt-get update -qq && sudo apt-get install -y -qq cppcheck || true
fi
