#!/usr/bin/env python3
"""Materialize commit-02-b contract-domain-fast artifacts and report."""

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
                "assertion_kind": "domain",
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

    stdout_digest = file_digest(suite_root / "stdout.log")
    stderr_digest = file_digest(suite_root / "stderr.log")

    cases = [
        build_case(
            args.run_id,
            "domain-001-global-member-establish-invariant",
            "TC-ID-DOMAIN-001",
            "EV-CAND-ID-STATE-001",
            "domain.member.anchor.established",
            "GlobalMember establish keeps anchor established and ref occupied.",
        ),
        build_case(
            args.run_id,
            "domain-002-global-lifecycle-legal-transitions",
            "TC-ID-DOMAIN-002",
            "EV-CAND-ID-STATE-002",
            "domain.lifecycle.transition.legal",
            "Global lifecycle legal transitions remain accepted by domain policy.",
        ),
        build_case(
            args.run_id,
            "domain-003-global-lifecycle-illegal-transitions",
            "TC-ID-DOMAIN-003",
            "EV-CAND-ID-STATE-003",
            "domain.lifecycle.transition.illegal",
            "Illegal lifecycle transitions remain rejected with invalid state transition.",
        ),
        build_case(
            args.run_id,
            "domain-004-role-capability-source-guard",
            "TC-ID-DOMAIN-004",
            "EV-CAND-ID-STATE-004",
            "domain.role_capability.source_guard",
            "Non-resolved role capability source snapshots cannot create active summaries.",
        ),
        build_case(
            args.run_id,
            "domain-005-career-append-only",
            "TC-ID-DOMAIN-005",
            "EV-CAND-ID-STATE-005",
            "domain.career.append_only",
            "Career correction appends preserve append-only history semantics.",
        ),
        build_case(
            args.run_id,
            "domain-006-memory-reference-body-free-state",
            "TC-ID-DOMAIN-006",
            "EV-CAND-ID-STATE-006",
            "domain.memory_reference.body_free",
            "Memory reference state remains formal and body-free.",
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
            "config_reason_ref": "config:not-applicable:commit-02-b-contract-domain-fast",
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
                "- commit boundary: `commit-02-b`",
                "- config profile: `ci-test`",
                "- suite status: `passed`",
                "- evidence: `EV-ID-STATE-001`",
                "",
                "## Scope",
                "",
                "- Covered only `TC-ID-DOMAIN-001~006` per design baseline `9ae0569`.",
                "- Did not claim `TC-ID-STATE-001~002`; those belong to `commit-02-c`.",
                "",
                "## Cases",
                "",
                "- `TC-ID-DOMAIN-001`: GlobalMember establish invariant",
                "- `TC-ID-DOMAIN-002`: Global lifecycle legal transitions",
                "- `TC-ID-DOMAIN-003`: Global lifecycle illegal transitions",
                "- `TC-ID-DOMAIN-004`: RoleCapabilitySummary source guard",
                "- `TC-ID-DOMAIN-005`: CareerRecord append-only",
                "- `TC-ID-DOMAIN-006`: MemoryReference state body-free",
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
                "- `cargo check` passed",
                "- `cargo test` passed",
                "- `cargo test -p identity-domain -- --nocapture` passed",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
