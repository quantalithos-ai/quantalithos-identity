#!/usr/bin/env python3
"""Generate run-scoped suite reports from raw suite artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from identity_artifact_tools import (
    iter_suite_report_artifacts,
    read_json,
    suite_cases_root,
    suite_report_markdown_path,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate suite reports and a run summary from raw artifacts.",
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--report-root", required=True)
    return parser.parse_args()


def render_suite_report(
    *,
    artifact_root: Path,
    report_root: Path,
    suite_report: dict[str, Any],
) -> None:
    suite = suite_report["suite"]
    suite_path = suite_report_markdown_path(report_root, suite)
    suite_path.parent.mkdir(parents=True, exist_ok=True)

    case_lines: list[str] = []
    assertion_lines: list[str] = []
    for case_id in suite_report["case_refs"]:
        case_path = suite_cases_root(artifact_root, suite) / f"{case_id}.json"
        case_payload = read_json(case_path)
        case_lines.append(
            "- "
            f"`{case_id}`: status=`{case_payload['status']}`; "
            f"tc_refs={','.join(case_payload['tc_refs'])}; "
            f"evidence_refs={','.join(case_payload['evidence_refs'])}"
        )
        for assertion in case_payload["assertions"]:
            assertion_lines.append(
                "- "
                f"`{assertion['assertion_ref']}`: "
                f"{assertion['assertion_status']} - {assertion['safe_message']}"
            )

    lines = [
        f"# {suite}",
        "",
        f"- run_id: `{suite_report['run_id']}`",
        f"- suite status: `{suite_report['status']}`",
        f"- config profile: `{suite_report['config_profile']}`",
        f"- started_at: `{suite_report['started_at']}`",
        f"- finished_at: `{suite_report['finished_at']}`",
        f"- duration_ms: `{suite_report['duration_ms']}`",
        "",
        "## Cases",
        "",
        *case_lines,
        "",
        "## Assertions",
        "",
        *assertion_lines,
        "",
        "## Raw Artifacts",
        "",
        f"- suite raw artifact: `{artifact_root / 'suites' / suite / 'report.json'}`",
        f"- case artifacts: `{artifact_root / 'suites' / suite / 'cases'}/*.json`",
        f"- stdout digest: `{suite_report.get('stdout_digest', 'not_applicable')}`",
        f"- stderr digest: `{suite_report.get('stderr_digest', 'not_applicable')}`",
        "",
    ]
    suite_path.write_text("\n".join(lines), encoding="utf-8")


def render_summary(
    *,
    run_id: str,
    artifact_root: Path,
    report_root: Path,
    suite_reports: list[dict[str, Any]],
) -> None:
    summary_path = report_root / "summary.md"
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# run summary",
        "",
        f"- run_id: `{run_id}`",
        f"- artifact root: `{artifact_root}`",
        f"- report root: `{report_root}`",
        f"- suite count: `{len(suite_reports)}`",
        f"- source commits: `{artifact_root / 'meta' / 'source-commits.json'}`",
        f"- config digest: `{artifact_root / 'meta' / 'config-digest.json'}`",
        f"- evidence index artifact: `{artifact_root / 'evidence-index.json'}`",
        "",
        "## Suites",
        "",
    ]
    for suite_report in suite_reports:
        suite_markdown = report_root / "suites" / f"{suite_report['suite']}.md"
        lines.append(
            "- "
            f"`{suite_report['suite']}`: status=`{suite_report['status']}`; "
            f"duration_ms=`{suite_report['duration_ms']}`; "
            f"report=`{suite_markdown}`"
        )
    lines.append("")
    summary_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()

    artifact_root = Path(args.artifact_root)
    report_root = Path(args.report_root)

    suite_report_paths = iter_suite_report_artifacts(artifact_root)
    if not suite_report_paths:
        raise SystemExit(f"No suite report artifacts found under {artifact_root / 'suites'}")

    suite_reports = [read_json(path) for path in suite_report_paths]
    for suite_report in suite_reports:
        render_suite_report(
            artifact_root=artifact_root,
            report_root=report_root,
            suite_report=suite_report,
        )
    render_summary(
        run_id=args.run_id,
        artifact_root=artifact_root,
        report_root=report_root,
        suite_reports=suite_reports,
    )


if __name__ == "__main__":
    main()
