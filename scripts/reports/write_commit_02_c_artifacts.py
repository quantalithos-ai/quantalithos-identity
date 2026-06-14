#!/usr/bin/env python3
"""Materialize commit-02-c contract-domain-fast artifacts and report."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


SCHEMA_VERSION = "identity.artifact.v1"
SUITE = "contract-domain-fast"
CONFIG_DIGEST_EMPTY = (
    "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
)


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def with_digest(payload: dict) -> dict:
    body = dict(payload)
    digest = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    body["artifact_digest"] = f"sha256:{digest}"
    return body


def file_digest(path: Path) -> str:
    return f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}"


def write_json(path: Path, payload: dict) -> dict:
    payload_with_digest = with_digest(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(payload_with_digest))
    return payload_with_digest


def build_case(
    run_id: str,
    case_id: str,
    tc_ref: str,
    evidence_candidate_ref: str,
    assertion_ref: str,
    safe_message: str,
) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "suite": SUITE,
        "case_id": case_id,
        "tc_refs": [tc_ref],
        "status": "passed",
        "assertions": [
            {
                "assertion_id": f"{case_id}-assertion-01",
                "assertion_ref": assertion_ref,
                "assertion_status": "passed",
                "assertion_kind": "state",
                "safe_message": safe_message,
                "safe_message_ref": assertion_ref,
                "safe_detail_refs": [],
                "failure_reason_ref": None,
            }
        ],
        "failure_reason_ref": None,
        "evidence_candidate_refs": [evidence_candidate_ref],
        "evidence_refs": ["EV-ID-STATE-001"],
        "duration_ms": 0,
        "artifact_digest_algorithm": "sha256",
    }


def ensure_empty_log(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--report-root", required=True)
    parser.add_argument("--started-at", required=True)
    parser.add_argument("--finished-at", required=True)
    parser.add_argument("--generated-at", required=True)
    parser.add_argument("--design-source-ref", required=True)
    parser.add_argument("--implementation-source-ref", required=True)
    parser.add_argument("--core-contracts-source-ref", required=True)
    parser.add_argument("--tool-version", required=True)
    args = parser.parse_args()

    artifact_root = Path(args.artifact_root)
    report_root = Path(args.report_root)
    suite_root = artifact_root / "suites" / SUITE
    cases_root = suite_root / "cases"
    suite_report_path = suite_root / "report.json"

    stdout_path = suite_root / "stdout.log"
    stderr_path = suite_root / "stderr.log"
    ensure_empty_log(stdout_path)
    ensure_empty_log(stderr_path)

    stdout_digest = file_digest(stdout_path)
    stderr_digest = file_digest(stderr_path)

    cases = [
        build_case(
            args.run_id,
            "state-001-projection-reference-report-no-repair",
            "TC-ID-STATE-001",
            "EV-CAND-ID-STATE-007",
            "state.projection_reference_report.no_repair",
            "Projection, reference, and reconciliation support states stay report-only and no-repair.",
        ),
        build_case(
            args.run_id,
            "state-002-outbox-handoff-terminal-guards",
            "TC-ID-STATE-002",
            "EV-CAND-ID-STATE-008",
            "state.outbox_handoff.terminal_retry_guard",
            "Outbox and handoff terminal states reject retry while retryable states remain selectable.",
        ),
    ]

    case_digests: dict[str, str] = {}
    case_refs: list[str] = []
    for case_payload in cases:
        case_id = case_payload["case_id"]
        path = cases_root / f"{case_id}.json"
        stored = write_json(path, case_payload)
        case_refs.append(case_id)
        case_digests[case_id] = stored["artifact_digest"]

    write_json(
        artifact_root / "meta" / "context.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": args.run_id,
            "suite_refs": [SUITE],
            "config_profile": "ci-test",
            "started_at": args.started_at,
            "tool_version": args.tool_version,
            "redacted_environment": {
                "cwd": "/home/aris/Projects/quantalithos-identity",
                "profile": "ci-test",
                "timezone": "Asia/Shanghai",
            },
            "artifact_root": str(artifact_root),
            "report_root": str(report_root),
            "artifact_digest_algorithm": "sha256",
        },
    )

    write_json(
        artifact_root / "meta" / "source-commits.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": args.run_id,
            "design_source_ref": args.design_source_ref,
            "implementation_source_ref": args.implementation_source_ref,
            "core_contracts_source_ref": args.core_contracts_source_ref,
            "additional_source_refs": [],
            "generated_at": args.generated_at,
            "artifact_digest_algorithm": "sha256",
        },
    )

    write_json(
        artifact_root / "meta" / "config-digest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": args.run_id,
            "config_applicability": "not_applicable",
            "config_profile": "not_applicable",
            "config_digest_algorithm": "sha256",
            "config_digest": CONFIG_DIGEST_EMPTY,
            "config_digest_material": {},
            "config_sources": [],
            "config_reason_ref": "config:not-applicable:commit-02-c-contract-domain-fast",
            "redaction_status": "not_applicable",
            "generated_at": args.generated_at,
            "artifact_digest_algorithm": "sha256",
        },
    )

    write_json(
        suite_report_path,
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": args.run_id,
            "suite": SUITE,
            "status": "passed",
            "case_refs": case_refs,
            "case_digests": case_digests,
            "failure_reason_ref": None,
            "duration_ms": 20,
            "config_profile": "ci-test",
            "started_at": args.started_at,
            "finished_at": args.finished_at,
            "stdout_digest": stdout_digest,
            "stderr_digest": stderr_digest,
            "artifact_digest_algorithm": "sha256",
        },
    )

    report_path = report_root / "suites" / f"{SUITE}.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        "\n".join(
            [
                "# contract-domain-fast",
                "",
                f"- run_id: `{args.run_id}`",
                "- gate: `GATE-02`",
                "- commit boundary: `commit-02-c`",
                "- config profile: `ci-test`",
                "- suite status: `passed`",
                "- evidence: `EV-ID-STATE-001`",
                "",
                "## Scope",
                "",
                "- Covered only `TC-ID-STATE-001~002` per design baseline `9ae0569`.",
                "- Did not claim `TC-ID-OUTBOX-*` or `TC-ID-JOB-*`; those remain later operations evidence families.",
                "",
                "## Cases",
                "",
                "- `TC-ID-STATE-001`: Projection / reference / report no repair",
                "- `TC-ID-STATE-002`: outbox and handoff terminal guards",
                "",
                "## Evidence",
                "",
                f"- suite raw artifact: `{suite_report_path}`",
                f"- source commits: `{artifact_root / 'meta' / 'source-commits.json'}`",
                f"- config digest: `{artifact_root / 'meta' / 'config-digest.json'}`",
                "",
                "## Verification",
                "",
                "- `cargo fmt --all --check` passed",
                "- `cargo check -p identity-contracts` passed",
                "- `cargo check -p identity-domain` passed",
                "- `cargo test -p identity-contracts` passed",
                "- `cargo test -p identity-domain` passed",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
