#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
reports_dir="${repo_root}/scripts/reports"
checks_dir="${repo_root}/scripts/checks"

run_id=""
suite=""
artifact_root=""
report_root=""
config_profile=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --suite)
      suite="$2"
      shift 2
      ;;
    --run-id)
      run_id="$2"
      shift 2
      ;;
    --artifact-root)
      artifact_root="$2"
      shift 2
      ;;
    --report-root)
      report_root="$2"
      shift 2
      ;;
    --config-profile)
      config_profile="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "${run_id}" ]]; then
  echo "run_release_gate.sh requires --run-id" >&2
  exit 2
fi

if [[ -z "${suite}" ]]; then
  echo "commit-08-b keeps release-main-smoke in commit-08-c; pass --suite report-generation-audit for the current audit subset" >&2
  exit 2
fi
if [[ "${suite}" != "report-generation-audit" ]]; then
  echo "unsupported release suite at commit-08-b: ${suite}" >&2
  exit 2
fi

if [[ -z "${artifact_root}" ]]; then
  artifact_root="artifacts/test/${run_id}"
fi
if [[ -z "${report_root}" ]]; then
  report_root="reports/runs/${run_id}"
fi
if [[ -z "${config_profile}" ]]; then
  config_profile="release-candidate"
fi

if [[ ! -f "${repo_root}/${artifact_root}/meta/context.json" ]]; then
  echo "missing raw artifacts under ${artifact_root}; materialize them before running the release audit subset" >&2
  exit 2
fi

bash "${reports_dir}/generate_reports.sh" \
  --run-id "${run_id}" \
  --artifact-root "${artifact_root}" \
  --report-root "${report_root}"

bash "${reports_dir}/generate_evidence_index.sh" \
  --run-id "${run_id}" \
  --artifact-root "${artifact_root}" \
  --report-root "${report_root}"

bash "${checks_dir}/check_dependency_boundary.sh" \
  --run-id "${run_id}" \
  --report-root "${report_root}"

bash "${checks_dir}/check_redaction.sh" \
  --run-id "${run_id}" \
  --artifact-root "${artifact_root}" \
  --report-root "${report_root}"

bash "${reports_dir}/build_gate_summary.sh" \
  --run-id "${run_id}" \
  --artifact-root "${artifact_root}" \
  --report-root "${report_root}" \
  --require-complete-p0

bash "${checks_dir}/check_artifact_report_pairing.sh" \
  --run-id "${run_id}" \
  --artifact-root "${artifact_root}" \
  --report-root "${report_root}"

bash "${checks_dir}/check_no_static_evidence.sh" \
  --run-id "${run_id}" \
  --artifact-root "${artifact_root}" \
  --report-root "${report_root}"

echo "run_release_gate completed for ${suite} with config profile ${config_profile}"
