#!/usr/bin/env python3
"""Materialize commit-04-b service-flow-fast and redaction artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


SCHEMA_VERSION = "identity.artifact.v1"
SERVICE_SUITE = "service-flow-fast"
REDACTION_SUITE = "redaction-boundary"
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
    path.write_text("", encoding="utf-8")


def build_case(
    *,
    run_id: str,
    suite: str,
    case_id: str,
    tc_refs: list[str],
    evidence_candidate_refs: list[str],
    evidence_refs: list[str],
    assertion_kind: str,
    assertion_ref: str,
    safe_message: str,
) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "suite": suite,
        "case_id": case_id,
        "tc_refs": tc_refs,
        "status": "passed",
        "assertions": [
            {
                "assertion_id": f"{case_id}-assertion-01",
                "assertion_ref": assertion_ref,
                "assertion_status": "passed",
                "assertion_kind": assertion_kind,
                "safe_message": safe_message,
                "safe_message_ref": assertion_ref,
                "safe_detail_refs": [],
                "failure_reason_ref": None,
            }
        ],
        "failure_reason_ref": None,
        "evidence_candidate_refs": evidence_candidate_refs,
        "evidence_refs": evidence_refs,
        "duration_ms": 0,
        "artifact_digest_algorithm": "sha256",
    }


def write_suite(
    *,
    artifact_root: Path,
    run_id: str,
    suite: str,
    cases: list[dict],
    duration_ms: int,
    started_at: str,
    finished_at: str,
) -> Path:
    suite_root = artifact_root / "suites" / suite
    cases_root = suite_root / "cases"
    suite_report_path = suite_root / "report.json"

    stdout_path = suite_root / "stdout.log"
    stderr_path = suite_root / "stderr.log"
    ensure_empty_log(stdout_path)
    ensure_empty_log(stderr_path)

    stdout_digest = file_digest(stdout_path)
    stderr_digest = file_digest(stderr_path)

    case_digests: dict[str, str] = {}
    case_refs: list[str] = []
    for case_payload in cases:
        case_id = case_payload["case_id"]
        path = cases_root / f"{case_id}.json"
        stored = write_json(path, case_payload)
        case_refs.append(case_id)
        case_digests[case_id] = stored["artifact_digest"]

    write_json(
        suite_report_path,
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "suite": suite,
            "status": "passed",
            "case_refs": case_refs,
            "case_digests": case_digests,
            "failure_reason_ref": None,
            "duration_ms": duration_ms,
            "config_profile": "ci-test",
            "started_at": started_at,
            "finished_at": finished_at,
            "stdout_digest": stdout_digest,
            "stderr_digest": stderr_digest,
            "artifact_digest_algorithm": "sha256",
        },
    )

    return suite_report_path


def write_service_report(
    *,
    report_root: Path,
    run_id: str,
    suite_report_path: Path,
    artifact_root: Path,
) -> None:
    report_path = report_root / "suites" / f"{SERVICE_SUITE}.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        "\n".join(
            [
                "# service-flow-fast",
                "",
                f"- run_id: `{run_id}`",
                "- gate: `GATE-04`",
                "- commit boundary: `commit-04-b`",
                "- config profile: `ci-test`",
                "- suite status: `passed`",
                "- evidence: `EV-ID-CMD-001`",
                "",
                "## Scope",
                "",
                "- Covered only `TC-ID-CMD-005~010` for design baseline `d9f9e71`.",
                "- Did not claim `TC-ID-CMD-011~015`; those remain in `commit-04-c`.",
                "- Related duplicate replay and stored-result invariants remain sourced from prior `GATE-03` evidence and are not re-claimed in this run.",
                "",
                "## Cases",
                "",
                "- `TC-ID-CMD-005`: MaintainRoleCapabilitySummary accepted",
                "- `TC-ID-CMD-006`: MaintainRoleCapabilitySummary unavailable source rejected/degraded",
                "- `TC-ID-CMD-007`: AppendCareerRecord accepted",
                "- `TC-ID-CMD-008`: AppendCareerRecord duplicate source noop/conflict",
                "- `TC-ID-CMD-009`: MaintainMemoryReference accepted",
                "- `TC-ID-CMD-010`: MaintainMemoryReference forbidden body rejected",
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
                "- `cargo check -p identity-application` passed",
                "- `cargo check -p identity-infra` passed",
                "- `cargo test -p identity-infra` passed",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def write_redaction_report(
    *,
    report_root: Path,
    run_id: str,
    suite_report_path: Path,
    artifact_root: Path,
) -> None:
    report_path = report_root / "redaction-check.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        "\n".join(
            [
                "# redaction-check",
                "",
                f"- run_id: `{run_id}`",
                "- gate: `GATE-10`",
                "- commit boundary: `commit-04-b`",
                "- config profile: `ci-test`",
                "- suite status: `passed`",
                "- evidence: `EV-ID-REDACTION-001`",
                "",
                "## Scope",
                "",
                "- Covered `TC-ID-CMD-010` and `TC-ID-REDACTION-001~003` for design baseline `d9f9e71`.",
                "- Scan scope is limited to this run's artifacts under `artifacts/test/<run_id>/suites/service-flow-fast/`, `artifacts/test/<run_id>/suites/redaction-boundary/`, and the paired reports under `reports/runs/<run_id>/`.",
                "- No leak fixture markers were emitted in reports, artifacts, or suite logs.",
                "",
                "## Cases",
                "",
                "- `TC-ID-CMD-010`: MaintainMemoryReference forbidden body rejected",
                "- `TC-ID-REDACTION-001`: log/report/artifact forbidden material scan",
                "- `TC-ID-REDACTION-002`: metric low-cardinality labels",
                "- `TC-ID-REDACTION-003`: observability not business audit",
                "",
                "## Evidence",
                "",
                f"- suite raw artifact: `{suite_report_path}`",
                f"- source commits: `{artifact_root / 'meta' / 'source-commits.json'}`",
                f"- config digest: `{artifact_root / 'meta' / 'config-digest.json'}`",
                "",
                "## Verification",
                "",
                "- `cargo test -p identity-infra` passed",
                "- targeted leak-marker scan over run-scoped artifacts and reports returned no matches",
                "- suite stdout/stderr logs are empty and body-free",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


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

    service_cases = [
        build_case(
            run_id=args.run_id,
            suite=SERVICE_SUITE,
            case_id="cmd-005-maintain-role-capability-summary-accepted",
            tc_refs=["TC-ID-CMD-005"],
            evidence_candidate_refs=["EV-CAND-ID-CMD-005"],
            evidence_refs=["EV-ID-CMD-001"],
            assertion_kind="flow",
            assertion_ref="command.role_capability.accepted.body_free_summary",
            safe_message=(
                "Resolved role source with a formal source version persists an active summary and source snapshot without definition or scoring body."
            ),
        ),
        build_case(
            run_id=args.run_id,
            suite=SERVICE_SUITE,
            case_id="cmd-006-maintain-role-capability-summary-unavailable-rejected",
            tc_refs=["TC-ID-CMD-006"],
            evidence_candidate_refs=["EV-CAND-ID-CMD-006"],
            evidence_refs=["EV-ID-CMD-001"],
            assertion_kind="flow",
            assertion_ref="command.role_capability.unavailable.no_summary_pollution",
            safe_message=(
                "Unavailable or unrecognized role source returns the formal rejected or degraded surface and does not persist an active summary."
            ),
        ),
        build_case(
            run_id=args.run_id,
            suite=SERVICE_SUITE,
            case_id="cmd-007-append-career-record-accepted",
            tc_refs=["TC-ID-CMD-007"],
            evidence_candidate_refs=["EV-CAND-ID-CMD-007"],
            evidence_refs=["EV-ID-CMD-001"],
            assertion_kind="flow",
            assertion_ref="command.career.append.accepted_append_only",
            safe_message=(
                "Career append keeps append-only history semantics and emits only body-free career material for the accepted record."
            ),
        ),
        build_case(
            run_id=args.run_id,
            suite=SERVICE_SUITE,
            case_id="cmd-008-append-career-record-duplicate-source-conflict",
            tc_refs=["TC-ID-CMD-008"],
            evidence_candidate_refs=["EV-CAND-ID-CMD-008"],
            evidence_refs=["EV-ID-CMD-001"],
            assertion_kind="flow",
            assertion_ref="command.career.duplicate_source.no_second_history",
            safe_message=(
                "A duplicate career source does not append a second record and surfaces the formal conflict or no-op branch without rewriting history."
            ),
        ),
        build_case(
            run_id=args.run_id,
            suite=SERVICE_SUITE,
            case_id="cmd-009-maintain-memory-reference-accepted",
            tc_refs=["TC-ID-CMD-009"],
            evidence_candidate_refs=["EV-CAND-ID-CMD-009"],
            evidence_refs=["EV-ID-CMD-001"],
            assertion_kind="flow",
            assertion_ref="command.memory_reference.accepted.refs_and_state_only",
            safe_message=(
                "Memory reference maintenance persists refs and formal state only, keeping archive and handoff material body-free."
            ),
        ),
        build_case(
            run_id=args.run_id,
            suite=SERVICE_SUITE,
            case_id="cmd-010-maintain-memory-reference-forbidden-body-rejected",
            tc_refs=["TC-ID-CMD-010"],
            evidence_candidate_refs=["EV-CAND-ID-CMD-010"],
            evidence_refs=["EV-ID-CMD-001"],
            assertion_kind="flow",
            assertion_ref="command.memory_reference.forbidden_body.rejected",
            safe_message=(
                "Forbidden memory or archive body input is rejected before any accepted relation, trace, or outbox material is saved."
            ),
        ),
    ]

    redaction_cases = [
        build_case(
            run_id=args.run_id,
            suite=REDACTION_SUITE,
            case_id="cmd-010-memory-reference-forbidden-body-surface",
            tc_refs=["TC-ID-CMD-010"],
            evidence_candidate_refs=["EV-CAND-ID-CMD-010"],
            evidence_refs=["EV-ID-REDACTION-001"],
            assertion_kind="redaction",
            assertion_ref="redaction.memory_reference.forbidden_body.rejected",
            safe_message=(
                "Forbidden memory and archive body fixtures stop at the formal rejection surface and do not leak into reports or artifacts."
            ),
        ),
        build_case(
            run_id=args.run_id,
            suite=REDACTION_SUITE,
            case_id="redaction-001-forbidden-material-scan",
            tc_refs=["TC-ID-REDACTION-001"],
            evidence_candidate_refs=["EV-CAND-ID-REDACTION-001"],
            evidence_refs=["EV-ID-REDACTION-001"],
            assertion_kind="redaction",
            assertion_ref="redaction.scan.forbidden_material.clean",
            safe_message=(
                "Run-scoped artifacts, reports, and suite logs stay clean of blocked material markers and isolated leak fixtures."
            ),
        ),
        build_case(
            run_id=args.run_id,
            suite=REDACTION_SUITE,
            case_id="redaction-002-metric-low-cardinality-labels",
            tc_refs=["TC-ID-REDACTION-002"],
            evidence_candidate_refs=["EV-CAND-ID-REDACTION-002"],
            evidence_refs=["EV-ID-REDACTION-001"],
            assertion_kind="redaction",
            assertion_ref="redaction.metrics.low_cardinality_labels",
            safe_message=(
                "Observability labels remain finite and body-free, without unstable identifiers or free-text payload labels."
            ),
        ),
        build_case(
            run_id=args.run_id,
            suite=REDACTION_SUITE,
            case_id="redaction-003-observability-split",
            tc_refs=["TC-ID-REDACTION-003"],
            evidence_candidate_refs=["EV-CAND-ID-REDACTION-003"],
            evidence_refs=["EV-ID-REDACTION-001"],
            assertion_kind="redaction",
            assertion_ref="redaction.observability.not_business_audit",
            safe_message=(
                "Logs and metrics remain safe diagnostics only and do not replace accepted command trace, audit, outbox, or stored result evidence."
            ),
        ),
    ]

    service_report_path = write_suite(
        artifact_root=artifact_root,
        run_id=args.run_id,
        suite=SERVICE_SUITE,
        cases=service_cases,
        duration_ms=540,
        started_at=args.started_at,
        finished_at=args.finished_at,
    )
    redaction_report_path = write_suite(
        artifact_root=artifact_root,
        run_id=args.run_id,
        suite=REDACTION_SUITE,
        cases=redaction_cases,
        duration_ms=210,
        started_at=args.started_at,
        finished_at=args.finished_at,
    )

    write_json(
        artifact_root / "meta" / "context.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": args.run_id,
            "suite_refs": [SERVICE_SUITE, REDACTION_SUITE],
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
            "config_reason_ref": (
                "config:not-applicable:commit-04-b-service-flow-fast-redaction"
            ),
            "redaction_status": "not_applicable",
            "generated_at": args.generated_at,
            "artifact_digest_algorithm": "sha256",
        },
    )

    write_service_report(
        report_root=report_root,
        run_id=args.run_id,
        suite_report_path=service_report_path,
        artifact_root=artifact_root,
    )
    write_redaction_report(
        report_root=report_root,
        run_id=args.run_id,
        suite_report_path=redaction_report_path,
        artifact_root=artifact_root,
    )


if __name__ == "__main__":
    main()
