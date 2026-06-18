#!/usr/bin/env python3
"""Scan run-scoped artifacts and reports for forbidden redaction patterns."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


FORBIDDEN_PATTERNS = {
    "private_key": re.compile(r"BEGIN [A-Z ]*PRIVATE KEY"),
    "authorization_header": re.compile(r"authorization:\s+\S+", re.IGNORECASE),
    "api_key_assignment": re.compile(r"(x-api-key|api_key)\s*[:=]\s*\S+", re.IGNORECASE),
    "secret_assignment": re.compile(r"secret\s*[:=]\s*\S+", re.IGNORECASE),
    "token_assignment": re.compile(r"token\s*[:=]\s*\S+", re.IGNORECASE),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan artifacts and reports for forbidden redaction leaks.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--report-root", required=True)
    return parser.parse_args()


def scan_files(paths: list[Path]) -> tuple[list[str], list[str]]:
    passed: list[str] = []
    failed: list[str] = []
    for path in paths:
        content = path.read_text(encoding="utf-8")
        hit_names = [name for name, pattern in FORBIDDEN_PATTERNS.items() if pattern.search(content)]
        if hit_names:
            failed.append(f"`{path.as_posix()}` matched forbidden patterns: {', '.join(hit_names)}")
        else:
            passed.append(f"`{path.as_posix()}` stayed clean across the formal deny-list scan.")
    return passed, failed


def main() -> None:
    args = parse_args()
    artifact_root = Path(args.artifact_root)
    report_root = Path(args.report_root)

    scan_paths = sorted(
        [
            *artifact_root.rglob("*.json"),
            *artifact_root.rglob("*.log"),
            *report_root.rglob("*.md"),
        ]
    )
    passed, failed = scan_files(scan_paths)

    lines = [
        "# redaction-check",
        "",
        f"- run_id: `{args.run_id}`",
        f"- overall_status: `{'failed' if failed else 'clean'}`",
        f"- scanned_file_count: `{len(scan_paths)}`",
        "",
        "## Clean Files",
        "",
        *(f"- {line}" for line in passed),
        "",
        "## Findings",
        "",
    ]
    if failed:
        lines.extend(f"- {line}" for line in failed)
    else:
        lines.append("- no forbidden patterns were found in the run-scoped artifacts or generated reports")
    lines.append("")

    output_path = report_root / "redaction-check.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")

    if failed:
        raise SystemExit("Redaction audit detected forbidden material patterns.")


if __name__ == "__main__":
    main()
