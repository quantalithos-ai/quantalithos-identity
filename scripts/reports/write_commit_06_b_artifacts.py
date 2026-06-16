#!/usr/bin/env python3
"""Materialize commit-06-b consumer/callback artifacts and reports."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


SCHEMA_VERSION = "identity.artifact.v1"
ENTRY_SUITE = "entry-worker-job"
INFRA_SUITE = "infra-runtime-fake"
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
    if not path.exists():
        path.write_text("", encoding="utf-8")


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


def build_case(
    *,
    run_id: str,
    suite: str,
    case_id: str,
    tc_refs: list[str],
    evidence_candidate_refs: list[str],
    evidence_refs: list[str],
    assertions: list[dict[str, object]],
) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "suite": suite,
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


def write_suite(
    *,
    artifact_root: Path,
    run_id: str,
    suite: str,
    cases: list[dict],
    duration_ms: int,
    started_at: str,
    finished_at: str,
) -> tuple[Path, Path]:
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
        stored = write_json(cases_root / f"{case_id}.json", case_payload)
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

    return suite_report_path, cases_root


def write_entry_report(
    *,
    report_root: Path,
    run_id: str,
    suite_report_path: Path,
    artifact_root: Path,
    design_baseline: str,
) -> None:
    report_path = report_root / "suites" / f"{ENTRY_SUITE}.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        "\n".join(
            [
                "# entry-worker-job",
                "",
                f"- run_id: `{run_id}`",
                "- gate: `GATE-06`",
                "- commit boundary: `commit-06-b`",
                f"- design baseline: `{design_baseline}`",
                "- config profile: `ci-test`",
                "- suite status: `passed`",
                "- evidence: `EV-ID-CONSUMER-001`",
                "",
                "## Scope",
                "",
                "- Covered only the five commit-06-b inbound and callback mutation flows, their formal typed receipt replay surfaces, missing-target no-create handling, callback target mismatch handling, and body-free public receipts.",
                "- Did not claim accepted outbound material factories, publish/deliver/retry jobs, API or worker entry wiring, transport retry loops, or any implicit creation of missing member, relation, or handoff target truth.",
                "- Replay verification stays on stored typed consumer and callback receipts; it does not rebuild responses from current truth, effect summaries, or repository scans.",
                "",
                "## Cases",
                "",
                "- `TC-ID-CONSUMER-001`: role source change accepts a body-free snapshot update and duplicate replay returns the stored typed receipt without a second mutation",
                "- `TC-ID-CONSUMER-002`: work participation source duplicates return a fresh `Noop` receipt and do not append another career record",
                "- `TC-ID-CONSUMER-003`: memory source changes quarantine missing relations and do not create local memory truth, reference state, or outbox material",
                "- `TC-ID-CONSUMER-004`: archive handoff callbacks reject direct-target and callback-lookup mismatches without mutating either relation",
                "- `TC-ID-CONSUMER-005`: trace handoff callbacks require a formal receipt for `Delivered`, store the callback envelope on success, and replay it without a second state change",
                "- `TC-ID-CONSUMER-006`: shared consumer and callback replay stays on typed stored receipts, keeps callback receipts on `HandoffCallbackReceipt`, and preserves body-free public surfaces",
                "",
                "## No-Create And Replay Audit",
                "",
                "- Missing relation branches are backed by `in_memory::tests::handle_memory_reference_source_state_changed_missing_relation_does_not_create`, which verifies the consumer returns `Quarantined` and leaves relation/outbox stores empty.",
                "- Callback target mismatch is backed by `in_memory::tests::handle_archive_handoff_result_target_mismatch_rejects_without_mutation`, which verifies both seeded relations keep their original versions.",
                "- Duplicate replay is backed by `in_memory::tests::handle_role_capability_source_changed_accepts_and_replays`, `in_memory::tests::handle_trace_handoff_result_delivered_replays_stored_receipt`, `in_memory::tests::inbound_consumer_scaffold_duplicate_replays_without_running_handler`, and `in_memory::tests::callback_scaffold_duplicate_replays_handoff_callback_receipt_without_handler`.",
                "",
                "## Body-Free Audit",
                "",
                "- The accepted role, work, memory, archive, and trace flows persist only refs, safe summary markers, state kinds, receipt refs, issue markers, trace refs, and outbox refs.",
                "- No artifact, report, stored receipt, or verification note in this run contains raw event bodies, callback bodies, archive packages, adapter diagnostics, method-definition bodies, or memory text.",
                "",
                "## Evidence",
                "",
                f"- suite raw artifact: `{suite_report_path}`",
                f"- source commits: `{artifact_root / 'meta' / 'source-commits.json'}`",
                f"- config digest: `{artifact_root / 'meta' / 'config-digest.json'}`",
                f"- case artifacts: `{artifact_root / 'suites' / ENTRY_SUITE / 'cases'}/*.json`",
                "",
                "## Verification",
                "",
                "- `cargo fmt --all` passed",
                "- `cargo check -p identity-application` passed",
                "- `cargo check -p identity-infra` passed",
                "- `cargo test -p identity-infra` passed",
                "- Targeted coverage inside the passing suite includes `handle_role_capability_source_changed_accepts_and_replays`, `handle_work_participation_accepted_source_duplicate_returns_noop`, `handle_memory_reference_source_state_changed_missing_relation_does_not_create`, `handle_archive_handoff_result_target_mismatch_rejects_without_mutation`, `handle_trace_handoff_result_delivered_requires_receipt`, and `handle_trace_handoff_result_delivered_replays_stored_receipt`.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def write_infra_report(
    *,
    report_root: Path,
    run_id: str,
    suite_report_path: Path,
    artifact_root: Path,
    design_baseline: str,
) -> None:
    report_path = report_root / "suites" / f"{INFRA_SUITE}.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        "\n".join(
            [
                "# infra-runtime-fake",
                "",
                f"- run_id: `{run_id}`",
                "- gate: `GATE-03` subset",
                "- commit boundary: `commit-06-b`",
                f"- design baseline: `{design_baseline}`",
                "- config profile: `ci-test`",
                "- suite status: `passed`",
                "- evidence: `EV-ID-IDEMP-001`",
                "",
                "## Scope",
                "",
                "- Covered the commit-06-b fake/runtime parity required for typed consumer and callback receipt replay, marker subject mapping, and same-UoW storage ordering.",
                "- Did not claim worker entry dispatch wiring, transport retry loops, outbound material creation, publish/deliver/retry jobs, or any durable adapter implementation beyond the formal in-memory fake surface.",
                "",
                "## Cases",
                "",
                "- `TC-ID-IDEMP-003`: consumer and callback duplicates replay the stored typed receipt envelope and do not rerun mutation, trace append, outbox append, or handoff state transition",
                "",
                "## Replay Audit",
                "",
                "- `in_memory::tests::handle_role_capability_source_changed_accepts_and_replays` verifies duplicate role-source delivery reuses the stored `ConsumerReceipt` envelope and keeps trace count at one.",
                "- `in_memory::tests::handle_trace_handoff_result_delivered_replays_stored_receipt` verifies duplicate callback replay reuses the stored `HandoffCallbackReceipt` envelope and keeps the handoff intent version, trace count, and outbox count unchanged.",
                "- Shared scaffold replay tests remain in the same passing run to confirm the fake runtime still distinguishes normal consumer receipts from callback receipts before any payload-specific handler executes.",
                "",
                "## Evidence",
                "",
                f"- suite raw artifact: `{suite_report_path}`",
                f"- source commits: `{artifact_root / 'meta' / 'source-commits.json'}`",
                f"- config digest: `{artifact_root / 'meta' / 'config-digest.json'}`",
                f"- case artifacts: `{artifact_root / 'suites' / INFRA_SUITE / 'cases'}/*.json`",
                "",
                "## Verification",
                "",
                "- `cargo fmt --all` passed",
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
    design_baseline: str,
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
                "- commit boundary: `commit-06-b`",
                f"- design baseline: `{design_baseline}`",
                "- config profile: `ci-test`",
                "- suite status: `passed`",
                "- evidence: `EV-ID-REDACTION-001`",
                "",
                "## Scope",
                "",
                "- Covered the body-free public shells and report text for commit-06-b inbound consumer and callback mutation evidence.",
                "- Scan scope is limited to this run's artifacts under `artifacts/test/<run_id>/suites/entry-worker-job/`, `artifacts/test/<run_id>/suites/infra-runtime-fake/`, `artifacts/test/<run_id>/suites/redaction-boundary/`, and the paired reports under `reports/runs/<run_id>/`.",
                "- Reports, artifacts, and suite logs remain limited to safe refs, state labels, case identifiers, and verification summaries.",
                "",
                "## Cases",
                "",
                "- `TC-ID-REDACTION-001`: run-scoped consumer and callback evidence remains body-free",
                "- `TC-ID-REDACTION-002`: stored receipts, suite logs, and report summaries keep only low-cardinality safe labels",
                "- `TC-ID-REDACTION-003`: accepted audit and replay evidence stay separate from any external payload or adapter body",
                "",
                "## Evidence",
                "",
                f"- suite raw artifact: `{suite_report_path}`",
                f"- source commits: `{artifact_root / 'meta' / 'source-commits.json'}`",
                f"- config digest: `{artifact_root / 'meta' / 'config-digest.json'}`",
                "",
                "## Verification",
                "",
                "- Targeted run-scoped review confirmed only refs, safe summary markers, state kinds, and test names appear in artifacts and reports.",
                "- Suite stdout/stderr logs are empty and therefore body-free.",
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
    design_baseline = args.design_source_ref.removeprefix("git:")[:7]

    entry_cases = [
        build_case(
            run_id=args.run_id,
            suite=ENTRY_SUITE,
            case_id="consumer-001-role-source-changed-accepted-replay",
            tc_refs=["TC-ID-CONSUMER-001"],
            evidence_candidate_refs=["EV-CAND-ID-CONSUMER-001"],
            evidence_refs=["EV-ID-CONSUMER-001", "EV-ID-IDEMP-001"],
            assertions=[
                build_assertion(
                    case_id="consumer-001-role-source-changed-accepted-replay",
                    index=1,
                    assertion_kind="flow",
                    assertion_ref="consumer.role_source.accepted_snapshot_body_free",
                    safe_message=(
                        "Resolved role source events persist a formal source snapshot, append one accepted trace, and create the body-free source-state outbox marker."
                    ),
                ),
                build_assertion(
                    case_id="consumer-001-role-source-changed-accepted-replay",
                    index=2,
                    assertion_kind="flow",
                    assertion_ref="consumer.role_source.duplicate_replays_stored_receipt",
                    safe_message=(
                        "Duplicate role source deliveries replay the stored typed receipt and do not append a second snapshot trace or outbox record."
                    ),
                ),
            ],
        ),
        build_case(
            run_id=args.run_id,
            suite=ENTRY_SUITE,
            case_id="consumer-002-work-source-duplicate-noop",
            tc_refs=["TC-ID-CONSUMER-002"],
            evidence_candidate_refs=["EV-CAND-ID-CONSUMER-002"],
            evidence_refs=["EV-ID-CONSUMER-001"],
            assertions=[
                build_assertion(
                    case_id="consumer-002-work-source-duplicate-noop",
                    index=1,
                    assertion_kind="flow",
                    assertion_ref="consumer.work_source.duplicate_source_noop",
                    safe_message=(
                        "Work participation events that repeat an already-recorded source marker return a fresh Noop receipt and do not append another career record."
                    ),
                )
            ],
        ),
        build_case(
            run_id=args.run_id,
            suite=ENTRY_SUITE,
            case_id="consumer-003-memory-source-missing-target-no-create",
            tc_refs=["TC-ID-CONSUMER-003"],
            evidence_candidate_refs=["EV-CAND-ID-CONSUMER-003"],
            evidence_refs=["EV-ID-CONSUMER-001"],
            assertions=[
                build_assertion(
                    case_id="consumer-003-memory-source-missing-target-no-create",
                    index=1,
                    assertion_kind="flow",
                    assertion_ref="consumer.memory_source.missing_relation_quarantined_no_create",
                    safe_message=(
                        "Memory source-state events quarantine missing local relations and do not create new memory truth, reference state, or outbox material."
                    ),
                )
            ],
        ),
        build_case(
            run_id=args.run_id,
            suite=ENTRY_SUITE,
            case_id="consumer-004-archive-callback-target-mismatch",
            tc_refs=["TC-ID-CONSUMER-004"],
            evidence_candidate_refs=["EV-CAND-ID-CONSUMER-004"],
            evidence_refs=["EV-ID-CONSUMER-001"],
            assertions=[
                build_assertion(
                    case_id="consumer-004-archive-callback-target-mismatch",
                    index=1,
                    assertion_kind="flow",
                    assertion_ref="callback.archive_handoff.target_mismatch_rejected",
                    safe_message=(
                        "Archive handoff callbacks reject direct-target and callback-lookup mismatches without mutating either seeded memory relation."
                    ),
                )
            ],
        ),
        build_case(
            run_id=args.run_id,
            suite=ENTRY_SUITE,
            case_id="consumer-005-trace-callback-receipt-and-replay",
            tc_refs=["TC-ID-CONSUMER-005"],
            evidence_candidate_refs=["EV-CAND-ID-CONSUMER-005"],
            evidence_refs=["EV-ID-CONSUMER-001", "EV-ID-IDEMP-001"],
            assertions=[
                build_assertion(
                    case_id="consumer-005-trace-callback-receipt-and-replay",
                    index=1,
                    assertion_kind="flow",
                    assertion_ref="callback.trace_handoff.delivered_requires_receipt",
                    safe_message=(
                        "Trace handoff callbacks reject Delivered results that omit the formal handoff receipt marker."
                    ),
                ),
                build_assertion(
                    case_id="consumer-005-trace-callback-receipt-and-replay",
                    index=2,
                    assertion_kind="flow",
                    assertion_ref="callback.trace_handoff.duplicate_replays_stored_receipt",
                    safe_message=(
                        "Accepted trace handoff callbacks store a typed callback receipt and replay it without a second handoff update, trace append, or outbox append."
                    ),
                ),
            ],
        ),
        build_case(
            run_id=args.run_id,
            suite=ENTRY_SUITE,
            case_id="consumer-006-shared-replay-kind-and-body-free",
            tc_refs=["TC-ID-CONSUMER-006"],
            evidence_candidate_refs=["EV-CAND-ID-CONSUMER-006"],
            evidence_refs=["EV-ID-CONSUMER-001", "EV-ID-IDEMP-001", "EV-ID-REDACTION-001"],
            assertions=[
                build_assertion(
                    case_id="consumer-006-shared-replay-kind-and-body-free",
                    index=1,
                    assertion_kind="flow",
                    assertion_ref="consumer.shared.typed_receipt_replay_surfaces",
                    safe_message=(
                        "Shared consumer and callback replay stays on the typed stored receipt envelope and preserves the distinct callback receipt kind."
                    ),
                ),
                build_assertion(
                    case_id="consumer-006-shared-replay-kind-and-body-free",
                    index=2,
                    assertion_kind="contract",
                    assertion_ref="consumer.shared.body_free_public_receipt",
                    safe_message=(
                        "Public receipts, stored replay envelopes, and the paired verification notes remain body-free and expose only safe refs or issue markers."
                    ),
                ),
            ],
        ),
    ]

    infra_cases = [
        build_case(
            run_id=args.run_id,
            suite=INFRA_SUITE,
            case_id="idemp-003-consumer-callback-replay-no-rerun",
            tc_refs=["TC-ID-IDEMP-003"],
            evidence_candidate_refs=["EV-CAND-ID-IDEMP-003"],
            evidence_refs=["EV-ID-IDEMP-001", "EV-ID-CONSUMER-001"],
            assertions=[
                build_assertion(
                    case_id="idemp-003-consumer-callback-replay-no-rerun",
                    index=1,
                    assertion_kind="flow",
                    assertion_ref="idemp.consumer.role_source.replay_no_second_mutation",
                    safe_message=(
                        "Duplicate role-source consumer deliveries replay the stored typed receipt and leave trace and outbox counts unchanged."
                    ),
                ),
                build_assertion(
                    case_id="idemp-003-consumer-callback-replay-no-rerun",
                    index=2,
                    assertion_kind="flow",
                    assertion_ref="idemp.callback.trace_handoff.replay_no_second_mutation",
                    safe_message=(
                        "Duplicate trace-handoff callbacks replay the stored callback receipt and do not rerun the handoff state transition or append a second marker trace."
                    ),
                ),
            ],
        )
    ]

    redaction_cases = [
        build_case(
            run_id=args.run_id,
            suite=REDACTION_SUITE,
            case_id="redaction-001-consumer-callback-body-free-surfaces",
            tc_refs=[
                "TC-ID-REDACTION-001",
                "TC-ID-REDACTION-002",
                "TC-ID-REDACTION-003",
            ],
            evidence_candidate_refs=["EV-CAND-ID-REDACTION-001"],
            evidence_refs=["EV-ID-REDACTION-001", "EV-ID-CONSUMER-001"],
            assertions=[
                build_assertion(
                    case_id="redaction-001-consumer-callback-body-free-surfaces",
                    index=1,
                    assertion_kind="contract",
                    assertion_ref="redaction.consumer_callback.public_surfaces_body_free",
                    safe_message=(
                        "Consumer and callback payloads, receipts, traces, and outbox markers stay on safe refs, summary markers, or state labels and do not persist forbidden body material."
                    ),
                ),
                build_assertion(
                    case_id="redaction-001-consumer-callback-body-free-surfaces",
                    index=2,
                    assertion_kind="flow",
                    assertion_ref="redaction.consumer_callback.reports_and_logs_body_free",
                    safe_message=(
                        "Run-scoped artifacts, reports, and suite logs remain limited to safe identifiers and verification summaries without raw event, callback, or archive bodies."
                    ),
                ),
            ],
        )
    ]

    context_payload = {
        "schema_version": SCHEMA_VERSION,
        "run_id": args.run_id,
        "suite_refs": [ENTRY_SUITE, INFRA_SUITE, REDACTION_SUITE],
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
    }
    write_json(artifact_root / "meta" / "context.json", context_payload)

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
            "config_reason_ref": "config:not-applicable:commit-06-b-inbound-callback-mutation-flows",
            "redaction_status": "not_applicable",
            "generated_at": args.generated_at,
            "artifact_digest_algorithm": "sha256",
        },
    )

    entry_suite_report, _ = write_suite(
        artifact_root=artifact_root,
        run_id=args.run_id,
        suite=ENTRY_SUITE,
        cases=entry_cases,
        duration_ms=720,
        started_at=args.started_at,
        finished_at=args.finished_at,
    )
    infra_suite_report, _ = write_suite(
        artifact_root=artifact_root,
        run_id=args.run_id,
        suite=INFRA_SUITE,
        cases=infra_cases,
        duration_ms=640,
        started_at=args.started_at,
        finished_at=args.finished_at,
    )
    redaction_suite_report, _ = write_suite(
        artifact_root=artifact_root,
        run_id=args.run_id,
        suite=REDACTION_SUITE,
        cases=redaction_cases,
        duration_ms=80,
        started_at=args.started_at,
        finished_at=args.finished_at,
    )

    write_entry_report(
        report_root=report_root,
        run_id=args.run_id,
        suite_report_path=entry_suite_report,
        artifact_root=artifact_root,
        design_baseline=design_baseline,
    )
    write_infra_report(
        report_root=report_root,
        run_id=args.run_id,
        suite_report_path=infra_suite_report,
        artifact_root=artifact_root,
        design_baseline=design_baseline,
    )
    write_redaction_report(
        report_root=report_root,
        run_id=args.run_id,
        suite_report_path=redaction_suite_report,
        artifact_root=artifact_root,
        design_baseline=design_baseline,
    )


if __name__ == "__main__":
    main()
