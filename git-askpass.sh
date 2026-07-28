#!/bin/sh
set -eu

case "${1:-}" in
  *Username*)
    printf '%s\n' "x-access-token"
    ;;
  *Password*)
    if [ -z "${GITHUB_TOKEN:-}" ]; then
      exit 1
    fi
    printf '%s\n' "$GITHUB_TOKEN"
    ;;
  *)
    exit 1
    ;;
esac
