#!/usr/bin/env python3
"""Check compile-time sibling path dependencies against the formal boundary."""

from __future__ import annotations

import argparse
from pathlib import Path
import tomllib


ALLOWED_SIBLING_PATHS = {
    "../quantalithos-core/crates/contracts",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan Cargo manifests for disallowed sibling path dependencies.",
    )
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--report-root", required=True)
    return parser.parse_args()


def dependency_tables(payload: dict) -> list[dict]:
    tables: list[dict] = []
    for key in ("dependencies", "dev-dependencies", "build-dependencies"):
        value = payload.get(key)
        if isinstance(value, dict):
            tables.append(value)
    targets = payload.get("target")
    if isinstance(targets, dict):
        for target_payload in targets.values():
            if isinstance(target_payload, dict):
                for key in ("dependencies", "dev-dependencies", "build-dependencies"):
                    value = target_payload.get(key)
                    if isinstance(value, dict):
                        tables.append(value)
    return tables


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root)
    report_root = Path(args.report_root)

    cargo_tomls = sorted(repo_root.glob("Cargo.toml")) + sorted(repo_root.glob("crates/*/Cargo.toml"))
    passed: list[str] = []
    failed: list[str] = []

    for cargo_toml in cargo_tomls:
        payload = tomllib.loads(cargo_toml.read_text(encoding="utf-8"))
        for table in dependency_tables(payload):
            for dependency_name, dependency_value in table.items():
                if not isinstance(dependency_value, dict):
                    continue
                path_value = dependency_value.get("path")
                if not isinstance(path_value, str):
                    continue
                if path_value.startswith("../quantalithos-") and path_value not in ALLOWED_SIBLING_PATHS:
                    failed.append(
                        f"`{cargo_toml.as_posix()}` uses disallowed sibling path dependency "
                        f"`{dependency_name}` -> `{path_value}`"
                    )
                else:
                    passed.append(
                        f"`{cargo_toml.as_posix()}` keeps `{dependency_name}` within the allowed compile-time boundary."
                    )

    lines = [
        "# dependency-boundary",
        "",
        f"- run_id: `{args.run_id}`",
        f"- overall_status: `{'failed' if failed else 'passed'}`",
        f"- scanned_manifest_count: `{len(cargo_tomls)}`",
        "",
        "## Allowed Dependency Findings",
        "",
        *(f"- {line}" for line in passed),
        "",
        "## Violations",
        "",
    ]
    if failed:
        lines.extend(f"- {line}" for line in failed)
    else:
        lines.append("- no disallowed sibling compile-time path dependencies were found")
    lines.append("")

    output_path = report_root / "dependency-boundary.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")

    if failed:
        raise SystemExit("Dependency boundary audit detected disallowed sibling path dependencies.")


if __name__ == "__main__":
    main()
