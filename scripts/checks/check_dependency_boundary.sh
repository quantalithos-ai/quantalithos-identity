#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
python3 "${script_dir}/dependency_boundary_audit.py" --repo-root "${repo_root}" "$@"
