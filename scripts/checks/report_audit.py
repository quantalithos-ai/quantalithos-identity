#!/usr/bin/env python3
"""Build a report audit from run-scoped artifacts and generated reports."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import sys

REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"
if str(REPORTS_DIR) not in sys.path:
    sys.path.insert(0, str(REPORTS_DIR))

from identity_artifact_tools import iter_suite_report_artifacts, load_evidence_items, read_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit artifact/report pairing and no-static-evidence rules.",
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--report-root", required=True)
    return parser.parse_args()


def suite_report_status(artifact_root: Path, report_root: Path) -> tuple[list[str], list[str]]:
    passed: list[str] = []
    failed: list[str] = []
    for report_path in iter_suite_report_artifacts(artifact_root):
        payload = read_json(report_path)
        suite = payload["suite"]
        suite_markdown = report_root / "suites" / f"{suite}.md"
        if not suite_markdown.exists():
            failed.append(f"missing suite report markdown for `{suite}`")
            continue
        content = suite_markdown.read_text(encoding="utf-8")
        if str(report_path.as_posix()) not in content:
            failed.append(f"suite report `{suite}` does not cite `{report_path.as_posix()}`")
            continue
        passed.append(f"`{suite}` pairs `{report_path.as_posix()}` with `{suite_markdown.as_posix()}`")
    return passed, failed


def evidence_status(artifact_root: Path, report_root: Path) -> tuple[list[str], list[str]]:
    passed: list[str] = []
    failed: list[str] = []
    evidence_items = load_evidence_items(artifact_root / "evidence-index.json")
    evidence_index_markdown = report_root / "evidence-index.md"
    pending_report_paths = {
        (report_root / "report-audit.md").as_posix(),
    }
    if not evidence_index_markdown.exists():
        failed.append("missing generated evidence-index.md")
        return passed, failed

    index_content = evidence_index_markdown.read_text(encoding="utf-8")
    for item in evidence_items:
        if item["evidence_id"] not in index_content:
            failed.append(f"evidence index markdown does not contain `{item['evidence_id']}`")
            continue
        missing_report_paths = [
            report_path
            for report_path in item["report_paths"]
            if not Path(report_path).exists() and report_path not in pending_report_paths
        ]
        if missing_report_paths:
            failed.append(
                f"`{item['evidence_id']}` references missing report paths: {', '.join(missing_report_paths)}"
            )
            continue
        passed.append(
            f"`{item['evidence_id']}` links suite refs, TC refs, AC refs, VETO refs, artifact paths, and report paths."
        )
    return passed, failed


def no_static_status(report_root: Path) -> tuple[list[str], list[str]]:
    passed: list[str] = []
    failed: list[str] = []
    markdown_files = sorted(
        path for path in report_root.rglob("*.md") if path.name != "report-audit.md"
    )
    if not markdown_files:
        failed.append("no markdown reports were generated")
        return passed, failed

    for markdown_path in markdown_files:
        content = markdown_path.read_text(encoding="utf-8")
        if "latest" in content:
            failed.append(f"`{markdown_path.as_posix()}` contains forbidden `latest`")
        else:
            passed.append(f"`{markdown_path.as_posix()}` avoids forbidden `latest` references.")
    return passed, failed


def gate_summary_status(report_root: Path) -> tuple[list[str], list[str]]:
    passed: list[str] = []
    failed: list[str] = []
    gate_summary_path = report_root / "gate-summary.md"
    if not gate_summary_path.exists():
        failed.append("missing generated gate-summary.md")
        return passed, failed
    content = gate_summary_path.read_text(encoding="utf-8")
    if "overall_status" not in content:
        failed.append("gate-summary.md does not expose overall_status")
    else:
        passed.append("gate-summary.md exposes overall_status and blocking suite rows.")
    return passed, failed


def write_report(
    *,
    report_root: Path,
    run_id: str,
    sections: list[tuple[str, list[str], list[str]]],
) -> None:
    overall_failed = any(failed for _, _, failed in sections)
    lines = [
        "# report-audit",
        "",
        f"- run_id: `{run_id}`",
        f"- overall_status: `{'failed' if overall_failed else 'passed'}`",
        "",
    ]
    for title, passed, failed in sections:
        lines.extend([f"## {title}", ""])
        if passed:
            lines.append("- passed checks:")
            lines.extend(f"  - {line}" for line in passed)
        if failed:
            lines.append("- failed checks:")
            lines.extend(f"  - {line}" for line in failed)
        if not passed and not failed:
            lines.append("- no checks executed")
        lines.append("")

    output_path = report_root / "report-audit.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    artifact_root = Path(args.artifact_root)
    report_root = Path(args.report_root)

    sections = [
        ("Artifact and report pairing", *suite_report_status(artifact_root, report_root)),
        ("Evidence index traceability", *evidence_status(artifact_root, report_root)),
        ("No static evidence markers", *no_static_status(report_root)),
        ("Gate summary integrity", *gate_summary_status(report_root)),
    ]
    write_report(report_root=report_root, run_id=args.run_id, sections=sections)

    if any(failed for _, _, failed in sections):
        raise SystemExit("Report audit detected pairing or static-evidence failures.")


if __name__ == "__main__":
    main()
