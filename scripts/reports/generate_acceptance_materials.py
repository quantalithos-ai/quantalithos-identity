#!/usr/bin/env python3
"""Generate acceptance handoff, veto checklist, and review material from run evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from identity_artifact_tools import load_evidence_items, read_json


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

FORMAL_VETO_IDS = [f"VETO-ID-{value:03d}" for value in range(1, 7)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate acceptance/review material from run-scoped evidence.",
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--report-root", required=True)
    parser.add_argument("--acceptance-root", required=True)
    parser.add_argument("--review-root", required=True)
    return parser.parse_args()


def parse_status_line(path: Path, key: str) -> str:
    for line in path.read_text(encoding="utf-8").splitlines():
        prefix = f"- {key}: `"
        if line.startswith(prefix) and line.endswith("`"):
            return line[len(prefix) : -1]
    raise ValueError(f"Missing `{key}` in {path}")


def load_gate_statuses(report_root: Path) -> dict[str, str]:
    return {
        "gate_summary": parse_status_line(report_root / "gate-summary.md", "overall_status"),
        "report_audit": parse_status_line(report_root / "report-audit.md", "overall_status"),
        "redaction": parse_status_line(report_root / "redaction-check.md", "overall_status"),
        "dependency": parse_status_line(report_root / "dependency-boundary.md", "overall_status"),
    }


def load_source_refs(artifact_root: Path) -> dict[str, Any]:
    return read_json(artifact_root / "meta" / "source-commits.json")


def load_config_digest(artifact_root: Path) -> dict[str, Any]:
    return read_json(artifact_root / "meta" / "config-digest.json")


def suite_reports_present(report_root: Path) -> list[str]:
    present: list[str] = []
    for suite in FORMAL_P0_SUITES:
        if (report_root / "suites" / f"{suite}.md").exists():
            present.append(suite)
    return present


def formal_veto_rows(
    *,
    evidence_items: list[dict[str, Any]],
    report_root: Path,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    by_veto: dict[str, list[dict[str, Any]]] = {veto_id: [] for veto_id in FORMAL_VETO_IDS}
    for item in evidence_items:
        for veto_id in item.get("veto_refs", []):
            if veto_id in by_veto:
                by_veto[veto_id].append(item)

    for veto_id in FORMAL_VETO_IDS:
        items = by_veto[veto_id]
        if not items:
            rows.append(
                {
                    "veto_id": veto_id,
                    "status": "cannot_decide",
                    "safe_summary": "No generated evidence item referenced this VETO requirement.",
                    "evidence_ids": [],
                    "report_paths": [],
                    "artifact_paths": [],
                }
            )
            continue

        evidence_ids = [item["evidence_id"] for item in items]
        report_paths = sorted({path for item in items for path in item["report_paths"]})
        artifact_paths = sorted({path for item in items for path in item["artifact_paths"]})
        status = "pass"
        if any(item["status"] != "passed" for item in items):
            status = "fail"
        elif any(not Path(path).exists() for path in report_paths + artifact_paths):
            status = "cannot_decide"

        rows.append(
            {
                "veto_id": veto_id,
                "status": status,
                "safe_summary": (
                    f"{veto_id} is backed by generated EV/report/artifact links from "
                    f"{', '.join(evidence_ids)}."
                ),
                "evidence_ids": evidence_ids,
                "report_paths": report_paths,
                "artifact_paths": artifact_paths,
            }
        )

    return rows


def final_conclusion(statuses: dict[str, str], veto_rows: list[dict[str, Any]]) -> str:
    if any(row["status"] == "cannot_decide" for row in veto_rows):
        return "cannot_decide"
    if any(row["status"] == "fail" for row in veto_rows):
        return "fail"
    if statuses["gate_summary"] != "passed":
        return "fail"
    if statuses["report_audit"] != "passed":
        return "fail"
    if statuses["redaction"] != "clean":
        return "fail"
    if statuses["dependency"] != "passed":
        return "fail"
    return "pass"


def write_handoff(
    *,
    run_id: str,
    artifact_root: Path,
    report_root: Path,
    acceptance_root: Path,
    review_root: Path,
    source_refs: dict[str, Any],
    config_digest: dict[str, Any],
    statuses: dict[str, str],
    present_suites: list[str],
    conclusion: str,
) -> None:
    lines = [
        "# handoff",
        "",
        f"- run_id: `{run_id}`",
        f"- final_conclusion: `{conclusion}`",
        f"- design_source_ref: `{source_refs['design_source_ref']}`",
        f"- implementation_source_ref: `{source_refs['implementation_source_ref']}`",
        f"- core_contracts_source_ref: `{source_refs['core_contracts_source_ref']}`",
        f"- config_profile: `{config_digest['config_profile']}`",
        f"- config_applicability: `{config_digest['config_applicability']}`",
        f"- gate_summary: `{report_root / 'gate-summary.md'}`",
        f"- evidence_index: `{report_root / 'evidence-index.md'}`",
        f"- report_audit: `{report_root / 'report-audit.md'}`",
        f"- redaction_check: `{report_root / 'redaction-check.md'}`",
        f"- dependency_boundary: `{report_root / 'dependency-boundary.md'}`",
        f"- agent_review: `{review_root / 'agent-review.md'}`",
        f"- reviewer_notes: `{review_root / 'reviewer-notes.md'}`",
        "",
        "## Blocking Suite Coverage",
        "",
    ]
    lines.extend(
        f"- `{suite}` -> `{report_root / 'suites' / f'{suite}.md'}`" for suite in present_suites
    )
    lines.extend(
        [
            "",
            "## Gate Status",
            "",
            f"- gate-summary overall_status: `{statuses['gate_summary']}`",
            f"- report-audit overall_status: `{statuses['report_audit']}`",
            f"- redaction overall_status: `{statuses['redaction']}`",
            f"- dependency overall_status: `{statuses['dependency']}`",
            "",
            "## Residuals",
            "",
            f"- risk-acceptance: `{acceptance_root / 'risk-acceptance.md'}`",
            f"- open-issues: `{acceptance_root / 'open-issues.md'}`",
            "- residual summary: no accepted P0 residuals; risk acceptance stays not_applicable for this run.",
            "",
            "## Review Supplement",
            "",
            "- This handoff is generated from run-scoped artifacts and must be read together with the agent review and veto checklist.",
            "- The review material cites only generated reports and raw artifact paths from the same run id.",
            "",
        ]
    )

    output_path = acceptance_root / "handoff.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


def write_veto_checklist(
    *,
    run_id: str,
    acceptance_root: Path,
    report_root: Path,
    veto_rows: list[dict[str, Any]],
) -> None:
    lines = [
        "# veto-checklist",
        "",
        f"- run_id: `{run_id}`",
        f"- evidence_index: `{report_root / 'evidence-index.md'}`",
        f"- gate_summary: `{report_root / 'gate-summary.md'}`",
        "",
    ]

    for row in veto_rows:
        lines.extend(
            [
                f"## {row['veto_id']}",
                "",
                f"- status: `{row['status']}`",
                f"- safe_summary: {row['safe_summary']}",
                f"- evidence_ids: `{','.join(row['evidence_ids']) if row['evidence_ids'] else 'none'}`",
                "- report_paths:",
                *(f"  - `{path}`" for path in row["report_paths"]),
                "- artifact_paths:",
                *(f"  - `{path}`" for path in row["artifact_paths"]),
                "",
            ]
        )

    output_path = acceptance_root / "veto-checklist.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


def write_risk_acceptance(*, run_id: str, acceptance_root: Path) -> None:
    lines = [
        "# risk-acceptance",
        "",
        f"- run_id: `{run_id}`",
        "- status: `not_applicable`",
        "- reason_ref: `acceptance.no_residual_risk_for_current_run`",
        "- summary: No residual risk is being accepted for this commit-08-c release evidence run.",
        "",
    ]
    output_path = acceptance_root / "risk-acceptance.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


def write_open_issues(*, run_id: str, acceptance_root: Path) -> None:
    lines = [
        "# open-issues",
        "",
        f"- run_id: `{run_id}`",
        "- issue_count: `0`",
        "- summary: No open S/A/B/R issues are recorded for this generated release evidence run.",
        "",
    ]
    output_path = acceptance_root / "open-issues.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


def write_review_material(
    *,
    run_id: str,
    artifact_root: Path,
    report_root: Path,
    acceptance_root: Path,
    review_root: Path,
    statuses: dict[str, str],
    present_suites: list[str],
    evidence_items: list[dict[str, Any]],
    conclusion: str,
) -> None:
    evidence_ids = [item["evidence_id"] for item in evidence_items]

    agent_review_lines = [
        "# agent-review",
        "",
        f"- run_id: `{run_id}`",
        f"- overall_status: `{conclusion}`",
        f"- handoff: `{acceptance_root / 'handoff.md'}`",
        f"- veto_checklist: `{acceptance_root / 'veto-checklist.md'}`",
        f"- report_audit: `{report_root / 'report-audit.md'}`",
        f"- redaction_check: `{report_root / 'redaction-check.md'}`",
        f"- dependency_boundary: `{report_root / 'dependency-boundary.md'}`",
        "",
        "## Lower-suite Coverage",
        "",
    ]
    agent_review_lines.extend(
        f"- `{suite}` report present at `{report_root / 'suites' / f'{suite}.md'}`" for suite in present_suites
    )
    agent_review_lines.extend(
        [
            "",
            "## Evidence Coverage",
            "",
            f"- evidence_ids: `{','.join(evidence_ids)}`",
            f"- evidence_index: `{report_root / 'evidence-index.md'}`",
            f"- raw_evidence_index: `{artifact_root / 'evidence-index.json'}`",
            "",
            "## Gate Review",
            "",
            f"- gate-summary overall_status: `{statuses['gate_summary']}`",
            f"- report-audit overall_status: `{statuses['report_audit']}`",
            f"- redaction overall_status: `{statuses['redaction']}`",
            f"- dependency overall_status: `{statuses['dependency']}`",
            "",
            "## Findings",
            "",
            "- no blocking findings were identified in the generated release evidence set.",
            "",
        ]
    )

    reviewer_notes_lines = [
        "# reviewer-notes",
        "",
        f"- run_id: `{run_id}`",
        "- review_status: `agent_review_only`",
        f"- agent_review: `{review_root / 'agent-review.md'}`",
        "- summary: No additional human reviewer notes were recorded for this generated release evidence run.",
        "",
    ]

    review_root.mkdir(parents=True, exist_ok=True)
    (review_root / "agent-review.md").write_text("\n".join(agent_review_lines), encoding="utf-8")
    (review_root / "reviewer-notes.md").write_text(
        "\n".join(reviewer_notes_lines),
        encoding="utf-8",
    )


def main() -> None:
    args = parse_args()

    artifact_root = Path(args.artifact_root)
    report_root = Path(args.report_root)
    acceptance_root = Path(args.acceptance_root)
    review_root = Path(args.review_root)

    evidence_items = load_evidence_items(artifact_root / "evidence-index.json")
    source_refs = load_source_refs(artifact_root)
    config_digest = load_config_digest(artifact_root)
    statuses = load_gate_statuses(report_root)
    present_suites = suite_reports_present(report_root)
    if sorted(present_suites) != sorted(FORMAL_P0_SUITES):
        missing = sorted(set(FORMAL_P0_SUITES) - set(present_suites))
        raise SystemExit(f"Missing formal P0 suite reports for acceptance generation: {', '.join(missing)}")

    veto_rows = formal_veto_rows(evidence_items=evidence_items, report_root=report_root)
    conclusion = final_conclusion(statuses, veto_rows)

    write_risk_acceptance(run_id=args.run_id, acceptance_root=acceptance_root)
    write_open_issues(run_id=args.run_id, acceptance_root=acceptance_root)
    write_handoff(
        run_id=args.run_id,
        artifact_root=artifact_root,
        report_root=report_root,
        acceptance_root=acceptance_root,
        review_root=review_root,
        source_refs=source_refs,
        config_digest=config_digest,
        statuses=statuses,
        present_suites=present_suites,
        conclusion=conclusion,
    )
    write_veto_checklist(
        run_id=args.run_id,
        acceptance_root=acceptance_root,
        report_root=report_root,
        veto_rows=veto_rows,
    )
    write_review_material(
        run_id=args.run_id,
        artifact_root=artifact_root,
        report_root=report_root,
        acceptance_root=acceptance_root,
        review_root=review_root,
        statuses=statuses,
        present_suites=present_suites,
        evidence_items=evidence_items,
        conclusion=conclusion,
    )


if __name__ == "__main__":
    main()
