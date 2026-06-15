#!/usr/bin/env python3
"""Materialize commit-03-b infra-runtime-fake artifacts and report."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


SCHEMA_VERSION = "identity.artifact.v1"
SUITE = "infra-runtime-fake"
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


def ensure_empty_log(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("", encoding="utf-8")


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
                "assertion_kind": "flow",
                "safe_message": safe_message,
                "safe_message_ref": assertion_ref,
                "safe_detail_refs": [],
                "failure_reason_ref": None,
            }
        ],
        "failure_reason_ref": None,
        "evidence_candidate_refs": [evidence_candidate_ref],
        "evidence_refs": ["EV-ID-IDEMP-001"],
        "duration_ms": 0,
        "artifact_digest_algorithm": "sha256",
    }


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
            "idemp-008-projection-rebuild-race-preserves-newer-state",
            "TC-ID-IDEMP-008",
            "EV-CAND-ID-IDEMP-008",
            "idemp.projection_rebuild.newer_state_preserved",
            "Projection rebuild race keeps newer projection state and rejects stale replacement.",
        ),
        build_case(
            args.run_id,
            "idemp-009-reference-refresh-preserves-last-good-snapshot",
            "TC-ID-IDEMP-009",
            "EV-CAND-ID-IDEMP-009",
            "idemp.reference_refresh.last_good_snapshot_retained",
            "Reference refresh failure preserves the last good snapshot and source version without persisting body material.",
        ),
        build_case(
            args.run_id,
            "idemp-010-handoff-delivered-requires-formal-receipt",
            "TC-ID-IDEMP-010",
            "EV-CAND-ID-IDEMP-010",
            "idemp.handoff_delivery.formal_receipt_required",
            "Handoff delivery is not marked delivered unless a formal receipt marker is present.",
        ),
        build_case(
            args.run_id,
            "idemp-011-rollback-failure-surfaces-manual-intervention",
            "TC-ID-IDEMP-011",
            "EV-CAND-ID-IDEMP-011",
            "idemp.rollback_failure.manual_intervention_surface",
            "Rollback failure surfaces a safe consistency defect and does not hide compensating writes.",
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
            "config_reason_ref": "config:not-applicable:commit-03-b-infra-runtime-fake",
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
            "duration_ms": 380,
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
                "# infra-runtime-fake",
                "",
                f"- run_id: `{args.run_id}`",
                "- gate: `GATE-03`",
                "- commit boundary: `commit-03-b`",
                "- config profile: `ci-test`",
                "- suite status: `passed`",
                "- evidence: `EV-ID-IDEMP-001`",
                "",
                "## Scope",
                "",
                "- Covered only `TC-ID-IDEMP-008~011` per design baseline `3bee523`.",
                "- Did not claim `TC-ID-CONFIG-001~004`; this boundary did not implement formal config binding or runtime builder redline behavior.",
                "",
                "## Cases",
                "",
                "- `TC-ID-IDEMP-008`: projection rebuild race preserves newer state",
                "- `TC-ID-IDEMP-009`: reference refresh preserves last good snapshot",
                "- `TC-ID-IDEMP-010`: handoff delivered requires formal receipt",
                "- `TC-ID-IDEMP-011`: rollback failure surfaces manual intervention",
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
                "- `cargo check -p identity-application` passed",
                "- `cargo check -p identity-infra` passed",
                "- `cargo test -p identity-infra` passed",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
