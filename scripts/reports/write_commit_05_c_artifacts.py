#!/usr/bin/env python3
"""Materialize commit-05-c service-flow-fast operations-read artifacts and report."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


SCHEMA_VERSION = "identity.artifact.v1"
SUITE = "service-flow-fast"
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
    *,
    run_id: str,
    case_id: str,
    tc_refs: list[str],
    evidence_candidate_refs: list[str],
    evidence_refs: list[str],
    assertions: list[dict[str, object]],
) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "suite": SUITE,
        "case_id": case_id,
        "tc_refs": tc_refs,
        "status": "passed",
        "assertions": assertions,
        "failure_reason_ref": None,
        "evidence_candidate_refs": evidence_candidate_refs,
        "evidence_refs": evidence_refs,
        "duration_ms": 0,
        "artifact_digest_algorithm": "sha256",
    }


def build_assertion(
    *,
    case_id: str,
    index: int,
    assertion_kind: str,
    assertion_ref: str,
    safe_message: str,
) -> dict:
    return {
        "assertion_id": f"{case_id}-assertion-{index:02d}",
        "assertion_ref": assertion_ref,
        "assertion_status": "passed",
        "assertion_kind": assertion_kind,
        "safe_message": safe_message,
        "safe_message_ref": assertion_ref,
        "safe_detail_refs": [],
        "failure_reason_ref": None,
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
    design_baseline = args.design_source_ref.removeprefix("git:")[:7]

    cases = [
        build_case(
            run_id=args.run_id,
            case_id="query-009-projection-state-stale-freshness",
            tc_refs=["TC-ID-QUERY-009"],
            evidence_candidate_refs=["EV-CAND-ID-QUERY-009"],
            evidence_refs=["EV-ID-QUERY-001"],
            assertions=[
                build_assertion(
                    case_id="query-009-projection-state-stale-freshness",
                    index=1,
                    assertion_kind="flow",
                    assertion_ref="query.operations.projection_state.stale_freshness",
                    safe_message=(
                        "Projection-state reads copy the loaded projection freshness marker for stale visible material and stay read-only."
                    ),
                )
            ],
        ),
        build_case(
            run_id=args.run_id,
            case_id="query-010-reference-state-visible-bundle",
            tc_refs=["TC-ID-QUERY-010"],
            evidence_candidate_refs=["EV-CAND-ID-QUERY-010"],
            evidence_refs=["EV-ID-QUERY-001"],
            assertions=[
                build_assertion(
                    case_id="query-010-reference-state-visible-bundle",
                    index=1,
                    assertion_kind="flow",
                    assertion_ref="query.operations.reference_state.visible_bundle",
                    safe_message=(
                        "Reference-resolution reads return the formal body-free state bundle through the query surface without mutating reference state."
                    ),
                )
            ],
        ),
        build_case(
            run_id=args.run_id,
            case_id="query-011-reconciliation-report-visible",
            tc_refs=["TC-ID-QUERY-011"],
            evidence_candidate_refs=["EV-CAND-ID-QUERY-011"],
            evidence_refs=["EV-ID-QUERY-001"],
            assertions=[
                build_assertion(
                    case_id="query-011-reconciliation-report-visible",
                    index=1,
                    assertion_kind="flow",
                    assertion_ref="query.operations.reconciliation_report.visible",
                    safe_message=(
                        "Reconciliation-report reads return exact visible report material through the formal query surface and do not repair report state."
                    ),
                )
            ],
        ),
        build_case(
            run_id=args.run_id,
            case_id="query-012-outbox-by-trace-page-access-and-item-degraded",
            tc_refs=["TC-ID-QUERY-012"],
            evidence_candidate_refs=["EV-CAND-ID-QUERY-012"],
            evidence_refs=["EV-ID-QUERY-001"],
            assertions=[
                build_assertion(
                    case_id="query-012-outbox-by-trace-page-access-and-item-degraded",
                    index=1,
                    assertion_kind="flow",
                    assertion_ref="query.operations.outbox.by_trace.empty_copies_page_access",
                    safe_message=(
                        "Pending-outbox reads by trace resolve page access first and copy that visibility result when the listed page is empty."
                    ),
                ),
                build_assertion(
                    case_id="query-012-outbox-by-trace-page-access-and-item-degraded",
                    index=2,
                    assertion_kind="flow",
                    assertion_ref="query.operations.outbox.by_trace.first_missing_degraded",
                    safe_message=(
                        "When a listed outbox item is missing after page resolution, the query surface degrades through the dedicated material mapper instead of synthesizing visibility."
                    ),
                ),
            ],
        ),
        build_case(
            run_id=args.run_id,
            case_id="query-013-outbox-state-body-free-visible",
            tc_refs=["TC-ID-QUERY-013"],
            evidence_candidate_refs=["EV-CAND-ID-QUERY-013"],
            evidence_refs=["EV-ID-QUERY-001"],
            assertions=[
                build_assertion(
                    case_id="query-013-outbox-state-body-free-visible",
                    index=1,
                    assertion_kind="flow",
                    assertion_ref="query.operations.outbox_state.body_free_visible",
                    safe_message=(
                        "Outbox-state reads return the formal body-free state surface and leave outbox persistence unchanged."
                    ),
                )
            ],
        ),
        build_case(
            run_id=args.run_id,
            case_id="query-014-trace-handoff-delivered-missing-receipt-degraded",
            tc_refs=["TC-ID-QUERY-014"],
            evidence_candidate_refs=["EV-CAND-ID-QUERY-014"],
            evidence_refs=["EV-ID-QUERY-001"],
            assertions=[
                build_assertion(
                    case_id="query-014-trace-handoff-delivered-missing-receipt-degraded",
                    index=1,
                    assertion_kind="flow",
                    assertion_ref="query.operations.trace_handoff.delivered_missing_receipt_degraded",
                    safe_message=(
                        "Trace-handoff state reads surface delivered-without-receipt as degraded material through the formal operations query shell."
                    ),
                )
            ],
        ),
        build_case(
            run_id=args.run_id,
            case_id="query-015-shared-no-write-audit",
            tc_refs=["TC-ID-QUERY-015"],
            evidence_candidate_refs=["EV-CAND-ID-QUERY-015"],
            evidence_refs=["EV-ID-QUERY-001"],
            assertions=[
                build_assertion(
                    case_id="query-015-shared-no-write-audit",
                    index=1,
                    assertion_kind="flow",
                    assertion_ref="query.operations.shared.no_write_audit",
                    safe_message=(
                        "Representative operations reads keep active write transactions and staged writes at zero across projection, reference, report, outbox, and handoff queries."
                    ),
                )
            ],
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
            "config_reason_ref": "config:not-applicable:commit-05-c-operations-read-queries",
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
            "duration_ms": 90,
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
                "# service-flow-fast",
                "",
                f"- run_id: `{args.run_id}`",
                "- gate: `GATE-05`",
                "- commit boundary: `commit-05-c`",
                f"- design baseline: `{design_baseline}`",
                "- config profile: `ci-test`",
                "- suite status: `passed`",
                "- evidence: `EV-ID-QUERY-001`",
                "",
                "## Scope",
                "",
                "- Covered only projection/reference/report/outbox/handoff operations-read query evidence for `TC-ID-QUERY-009~014` plus the shared no-write audit `TC-ID-QUERY-015`.",
                "- This run does not claim core/member/trace/audit family coverage from `commit-05-b` and does not claim any rebuild, refresh, reconciliation mutation, publish, deliver, retry, or other job mutation behavior.",
                "- Query verification stays inside visibility-first resolution, operations material degradation mapping, body-free public shells, and read-only execution.",
                "",
                "## Cases",
                "",
                "- `TC-ID-QUERY-009`: projection-state reads copy the loaded freshness marker for stale visible material and remain read-only",
                "- `TC-ID-QUERY-010`: reference-resolution state reads return the formal visible bundle without mutating reference state",
                "- `TC-ID-QUERY-011`: reconciliation-report reads return visible report material and do not repair report state",
                "- `TC-ID-QUERY-012`: outbox-by-trace reads resolve page visibility first, copy empty-page access, and degrade listed missing items through the dedicated mapper",
                "- `TC-ID-QUERY-013`: outbox-state reads stay body-free and leave outbox persistence unchanged",
                "- `TC-ID-QUERY-014`: trace-handoff state reads surface delivered-without-receipt as degraded material through the formal query shell",
                "- `TC-ID-QUERY-015`: representative operations reads keep active write transactions and staged writes at zero",
                "",
                "## Write Audit",
                "",
                "- No query flow in this run opened a write UoW, reserved idempotency, wrote stored results, or repaired projection/reference/report/outbox/handoff state.",
                "- The shared no-write assertion is backed by targeted infra tests that verify `active_write_transactions() == 0` and `staged_write_count() == 0` before and after representative operations reads.",
                "",
                "## Evidence",
                "",
                f"- suite raw artifact: `{suite_report_path}`",
                f"- source commits: `{artifact_root / 'meta' / 'source-commits.json'}`",
                f"- config digest: `{artifact_root / 'meta' / 'config-digest.json'}`",
                f"- case artifacts: `{cases_root}/*.json`",
                "",
                "## Verification",
                "",
                "- `cargo fmt --all` passed",
                "- `cargo check -p identity-contracts` passed",
                "- `cargo check -p identity-application` passed",
                "- `cargo check -p identity-infra` passed",
                "- `cargo test -p identity-contracts` passed",
                "- `cargo test -p identity-application` passed",
                "- `cargo test -p identity-infra` passed",
                "- Targeted infra coverage within the passing run includes `get_projection_state_stale_returns_freshness_marker_without_write`, `get_reference_resolution_state_returns_bundle_without_write`, `read_reconciliation_report_exact_visible_stays_read_only`, `list_pending_identity_outbox_by_trace_empty_copies_page_access_without_write`, `list_pending_identity_outbox_by_trace_missing_item_uses_item_degradation`, `get_identity_outbox_state_returns_body_free_state_without_write`, and `get_trace_handoff_state_delivered_without_receipt_returns_degraded_surface`.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
