#!/usr/bin/env python3
"""Render a human-readable evidence index from raw evidence items."""

from __future__ import annotations

import argparse
from pathlib import Path

from identity_artifact_tools import load_evidence_items


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate reports/runs/<run_id>/evidence-index.md from raw evidence-index.json.",
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--report-root", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    artifact_root = Path(args.artifact_root)
    report_root = Path(args.report_root)
    evidence_items = load_evidence_items(artifact_root / "evidence-index.json")

    lines = [
        "# evidence-index",
        "",
        f"- run_id: `{args.run_id}`",
        f"- raw artifact: `{artifact_root / 'evidence-index.json'}`",
        f"- evidence item count: `{len(evidence_items)}`",
        "",
    ]

    for item in evidence_items:
        lines.extend(
            [
                f"## {item['evidence_id']}",
                "",
                f"- status: `{item['status']}`",
                f"- redaction_status: `{item['redaction_status']}`",
                f"- review_status: `{item['review_status']}`",
                f"- suite_refs: `{','.join(item['suite_refs'])}`",
                f"- tc_refs: `{','.join(item['tc_refs'])}`",
                f"- ac_refs: `{','.join(item['ac_refs'])}`",
                f"- veto_refs: `{','.join(item['veto_refs']) if item['veto_refs'] else 'none'}`",
                f"- safe_summary: {item['safe_summary']}",
                "- artifact paths:",
                *(f"  - `{path}`" for path in item["artifact_paths"]),
                "- artifact digests:",
                *(f"  - `{digest}`" for digest in item["artifact_digests"]),
                "- report paths:",
                *(f"  - `{path}`" for path in item["report_paths"]),
                "",
            ]
        )

    output_path = report_root / "evidence-index.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
