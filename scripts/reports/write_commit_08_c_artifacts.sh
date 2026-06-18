#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "${script_dir}/write_commit_08_c_artifacts.py" "$@"
