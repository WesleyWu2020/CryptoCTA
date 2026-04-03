#!/usr/bin/env bash
set -euo pipefail

BASE_SHA="${1:-${BASE_SHA:-}}"
HEAD_SHA="${2:-${HEAD_SHA:-HEAD}}"

if [[ -z "${BASE_SHA}" ]]; then
  echo "docs-drift-check skipped: BASE_SHA is not set"
  exit 0
fi

if ! git cat-file -e "${BASE_SHA}^{commit}" 2>/dev/null; then
  echo "docs-drift-check failed: BASE_SHA not found: ${BASE_SHA}" >&2
  exit 2
fi

if ! git cat-file -e "${HEAD_SHA}^{commit}" 2>/dev/null; then
  echo "docs-drift-check failed: HEAD_SHA not found: ${HEAD_SHA}" >&2
  exit 2
fi

changed_files="$(git diff --name-only "${BASE_SHA}" "${HEAD_SHA}")"

echo "docs-drift-check range: ${BASE_SHA}..${HEAD_SHA}"

require_doc_update() {
  local module_prefix="$1"
  local doc_path="$2"

  if echo "${changed_files}" | rg -q "^${module_prefix}"; then
    if ! echo "${changed_files}" | rg -q "^${doc_path}$"; then
      echo "docs-drift-check failed:" >&2
      echo "- Changed module: ${module_prefix}" >&2
      echo "- Required doc update missing: ${doc_path}" >&2
      return 1
    fi
  fi
  return 0
}

status=0
require_doc_update "src/cta_core/strategy_runtime/" "src/cta_core/strategy_runtime/ARCHITECTURE.md" || status=1
require_doc_update "src/cta_core/data/" "src/cta_core/data/CONSTRAINTS.md" || status=1
require_doc_update "src/cta_core/execution/" "src/cta_core/execution/CONSTRAINTS.md" || status=1
require_doc_update "src/cta_core/risk/" "src/cta_core/risk/CONSTRAINTS.md" || status=1

if [[ "${status}" -ne 0 ]]; then
  exit "${status}"
fi

echo "docs-drift-check passed"
