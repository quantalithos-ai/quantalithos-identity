#!/usr/bin/env python3
"""Build a run-scoped gate summary from raw artifacts and generated reports."""

from __future__ import annotations

import argparse
from pathlib import Path

from identity_artifact_tools import iter_suite_report_artifacts, read_json


FORMAL_P0_SUITES = [
    "contract-domain-fast",
    "service-flow-fast",
    "config-redline",
    "dependency-boundary",
    "infra-runtime-fake",
    "entry-worker-job",
    "operations-replay-core",
    "redaction-boundary",
    "report-generation-audit",
    "release-main-smoke",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a gate summary from suite artifacts and generated reports.",
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--report-root", required=True)
    parser.add_argument(
        "--require-complete-p0",
        action="store_true",
        help="Fail if the formal P0 blocking suite set is incomplete.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    artifact_root = Path(args.artifact_root)
    report_root = Path(args.report_root)

    suite_reports = {
        payload["suite"]: payload
        for payload in (read_json(path) for path in iter_suite_report_artifacts(artifact_root))
    }
    present_suites = sorted(suite_reports.keys())
    missing_suites = sorted(set(FORMAL_P0_SUITES) - set(present_suites))

    overall_status = "passed"
    if any(payload["status"] != "passed" for payload in suite_reports.values()):
        overall_status = "failed"
    if args.require_complete_p0 and missing_suites:
        overall_status = "failed"

    lines = [
        "# gate-summary",
        "",
        f"- run_id: `{args.run_id}`",
        f"- overall_status: `{overall_status}`",
        f"- report root: `{report_root}`",
        f"- suite report count: `{len(present_suites)}`",
        "",
        "## Blocking Suites",
        "",
    ]
    for suite in FORMAL_P0_SUITES:
        if suite in suite_reports:
            payload = suite_reports[suite]
            lines.append(
                "- "
                f"`{suite}`: status=`{payload['status']}`; "
                f"raw=`{artifact_root / 'suites' / suite / 'report.json'}`; "
                f"report=`{report_root / 'suites' / f'{suite}.md'}`"
            )
        else:
            lines.append(f"- `{suite}`: status=`missing`")

    lines.extend(
        [
            "",
            "## Generated Reports",
            "",
            f"- summary: `{report_root / 'summary.md'}`",
            f"- evidence index: `{report_root / 'evidence-index.md'}`",
            f"- report audit: `{report_root / 'report-audit.md'}`",
            f"- redaction check: `{report_root / 'redaction-check.md'}`",
            f"- dependency boundary: `{report_root / 'dependency-boundary.md'}`",
            "",
        ]
    )
    if missing_suites:
        lines.extend(
            [
                "## Missing Suites",
                "",
                *(f"- `{suite}`" for suite in missing_suites),
                "",
            ]
        )

    output_path = report_root / "gate-summary.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")

    if overall_status != "passed":
        raise SystemExit("Gate summary detected failed or incomplete blocking suites.")


if __name__ == "__main__":
    main()
