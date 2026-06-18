#!/usr/bin/env python3
"""Materialize commit-08-b sample raw artifacts for report tooling."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from identity_artifact_tools import (
    SCHEMA_VERSION,
    SHA256_EMPTY_OBJECT,
    build_assertion,
    build_case,
    write_evidence_index,
    write_json,
    write_suite,
)


def id_range(prefix: str, start: int, end: int) -> list[str]:
    return [f"{prefix}-{value:03d}" for value in range(start, end + 1)]


def suite_case(
    *,
    run_id: str,
    suite: str,
    case_id: str,
    tc_refs: list[str],
    evidence_candidate_refs: list[str],
    evidence_refs: list[str],
    assertions: list[tuple[str, str, str]],
) -> dict[str, Any]:
    built_assertions = [
        build_assertion(
            case_id=case_id,
            index=index,
            assertion_kind=assertion_kind,
            assertion_ref=assertion_ref,
            safe_message=safe_message,
        )
        for index, (assertion_kind, assertion_ref, safe_message) in enumerate(
            assertions,
            start=1,
        )
    ]
    return build_case(
        run_id=run_id,
        suite=suite,
        case_id=case_id,
        tc_refs=tc_refs,
        evidence_candidate_refs=evidence_candidate_refs,
        evidence_refs=evidence_refs,
        assertions=built_assertions,
    )


def build_suite_cases(run_id: str) -> dict[str, list[dict[str, Any]]]:
    return {
        "contract-domain-fast": [
            suite_case(
                run_id=run_id,
                suite="contract-domain-fast",
                case_id="contract-001-public-shell-roundtrip",
                tc_refs=[
                    "TC-ID-CONTRACT-001",
                    "TC-ID-CONTRACT-002",
                    "TC-ID-CONTRACT-003",
                    "TC-ID-CONTRACT-004",
                ],
                evidence_candidate_refs=["EV-CAND-ID-CONTRACT-001"],
                evidence_refs=["EV-ID-CONTRACT-001"],
                assertions=[
                    (
                        "contract",
                        "contracts.protocol.roundtrip.required_fields",
                        "The public command, query, consumer, job, and view shells round-trip with their required fields intact.",
                    ),
                    (
                        "redaction",
                        "contracts.protocol.body_free.shells_only",
                        "The public shells stay body-free and do not embed external source bodies, raw memory text, or archive package material.",
                    ),
                ],
            ),
            suite_case(
                run_id=run_id,
                suite="contract-domain-fast",
                case_id="state-001-domain-transition-guards",
                tc_refs=[
                    "TC-ID-DOMAIN-001",
                    "TC-ID-DOMAIN-002",
                    "TC-ID-DOMAIN-003",
                    "TC-ID-DOMAIN-004",
                    "TC-ID-DOMAIN-005",
                    "TC-ID-DOMAIN-006",
                    "TC-ID-STATE-001",
                    "TC-ID-STATE-002",
                ],
                evidence_candidate_refs=["EV-CAND-ID-STATE-001"],
                evidence_refs=["EV-ID-STATE-001"],
                assertions=[
                    (
                        "state",
                        "domain.state.transition.legal_and_illegal",
                        "The domain state helpers accept only legal transitions and reject illegal or terminal reopen attempts.",
                    ),
                    (
                        "domain",
                        "domain.truth.factory.append_only_and_stable_refs",
                        "The truth factories preserve append-only history and stable refs across accepted transitions.",
                    ),
                ],
            ),
        ],
        "service-flow-fast": [
            suite_case(
                run_id=run_id,
                suite="service-flow-fast",
                case_id="cmd-001-command-accepted-and-replay",
                tc_refs=id_range("TC-ID-CMD", 1, 15),
                evidence_candidate_refs=["EV-CAND-ID-CMD-001"],
                evidence_refs=["EV-ID-CMD-001"],
                assertions=[
                    (
                        "flow",
                        "command.accepted.same_uow.side_effects",
                        "The accepted command paths keep truth, trace, outbox, stale markers, and stored result material in the same unit of work.",
                    ),
                    (
                        "flow",
                        "command.duplicate.replays.stored_result_only",
                        "Same-key same-digest duplicates replay stored accepted or rejected shells and do not rerun mutation or outbox append.",
                    ),
                ],
            ),
            suite_case(
                run_id=run_id,
                suite="service-flow-fast",
                case_id="query-001-visibility-first-no-write",
                tc_refs=id_range("TC-ID-QUERY", 1, 15),
                evidence_candidate_refs=["EV-CAND-ID-QUERY-001"],
                evidence_refs=["EV-ID-QUERY-001"],
                assertions=[
                    (
                        "flow",
                        "query.visibility.first.no_write",
                        "The query flows resolve visibility before loading material and never write truth, projection repair, or stored result state.",
                    ),
                    (
                        "redaction",
                        "query.degraded.summary.copies.formal_markers",
                        "The degraded and stale query surfaces copy formal markers and kinds instead of synthesizing them from strings or diagnostics.",
                    ),
                ],
            ),
        ],
        "infra-runtime-fake": [
            suite_case(
                run_id=run_id,
                suite="infra-runtime-fake",
                case_id="idemp-001-stored-replay-and-fake-parity",
                tc_refs=id_range("TC-ID-IDEMP", 1, 11),
                evidence_candidate_refs=["EV-CAND-ID-IDEMP-001"],
                evidence_refs=["EV-ID-IDEMP-001"],
                assertions=[
                    (
                        "flow",
                        "runtime.fake.replay.no_second_writer",
                        "The in-memory runtime preserves duplicate replay, in-flight guard, and stored result semantics without a second writer.",
                    ),
                    (
                        "other",
                        "runtime.fake.controlled.outcomes.match_formal_ports",
                        "The fake runtime exposes the same formal issue refs and state surfaces as the declared ports instead of depending on private maps.",
                    ),
                ],
            ),
        ],
        "entry-worker-job": [
            suite_case(
                run_id=run_id,
                suite="entry-worker-job",
                case_id="consumer-001-inbound-and-callback-replay",
                tc_refs=id_range("TC-ID-CONSUMER", 1, 6),
                evidence_candidate_refs=["EV-CAND-ID-CONSUMER-001"],
                evidence_refs=["EV-ID-CONSUMER-001"],
                assertions=[
                    (
                        "flow",
                        "consumer.callback.typed_receipt.replay_only",
                        "Inbound and callback duplicates replay stored typed receipts and do not rerun mutation, outbox append, or callback state transition.",
                    ),
                    (
                        "redaction",
                        "consumer.callback.missing_target.no_create",
                        "Missing targets stay no-create and the public receipts remain body-free across accepted, delayed, and quarantined surfaces.",
                    ),
                ],
            ),
            suite_case(
                run_id=run_id,
                suite="entry-worker-job",
                case_id="job-001-job-entry-and-report-replay",
                tc_refs=id_range("TC-ID-JOB", 1, 8),
                evidence_candidate_refs=["EV-CAND-ID-JOB-001"],
                evidence_refs=["EV-ID-JOB-001"],
                assertions=[
                    (
                        "flow",
                        "job.entry.dispatches.through.facade",
                        "The jobs entry parses the formal request and dispatches through the application facade without direct repository writes.",
                    ),
                    (
                        "flow",
                        "job.duplicate.replays.stored_report",
                        "Duplicate job requests replay the stored typed report instead of rescanning stores or rerunning the job body.",
                    ),
                ],
            ),
        ],
        "operations-replay-core": [
            suite_case(
                run_id=run_id,
                suite="operations-replay-core",
                case_id="outbox-001-accepted-only-payload-marker",
                tc_refs=id_range("TC-ID-OUTBOX", 1, 10),
                evidence_candidate_refs=["EV-CAND-ID-OUTBOX-001"],
                evidence_refs=["EV-ID-OUTBOX-001"],
                assertions=[
                    (
                        "flow",
                        "outbox.accepted_only.body_free.material",
                        "Only accepted paths create stored payload markers and the material stays body-free across outbox and handoff surfaces.",
                    ),
                    (
                        "flow",
                        "outbox.publisher.failure.does_not_repair_truth",
                        "Publisher failures update only formal outbox attempt or issue markers and never roll back accepted truth into a second mutation path.",
                    ),
                ],
            ),
            suite_case(
                run_id=run_id,
                suite="operations-replay-core",
                case_id="job-002-maintenance-and-propagation-no-truth-repair",
                tc_refs=id_range("TC-ID-JOB", 1, 8),
                evidence_candidate_refs=["EV-CAND-ID-JOB-001"],
                evidence_refs=["EV-ID-JOB-001", "EV-ID-NFR-001"],
                assertions=[
                    (
                        "flow",
                        "jobs.maintenance.report_only.no_truth_repair",
                        "Maintenance jobs stay report-only and do not repair core truth, external truth, or projection truth in place.",
                    ),
                    (
                        "other",
                        "jobs.partial_failure.safe_counts_and_refs",
                        "The job reports keep safe item refs, failed refs, counts, and durations without embedding raw payload or adapter diagnostics.",
                    ),
                ],
            ),
        ],
        "config-redline": [
            suite_case(
                run_id=run_id,
                suite="config-redline",
                case_id="config-001-formal-profile-matrix",
                tc_refs=id_range("TC-ID-CONFIG", 1, 4),
                evidence_candidate_refs=["EV-CAND-ID-CONFIG-001"],
                evidence_refs=["EV-ID-CONFIG-001"],
                assertions=[
                    (
                        "config",
                        "config.profile.matrix.validates.formal_profiles",
                        "The formal profile matrix accepts only the declared profile set and fails closed when required runtime fields are missing.",
                    ),
                    (
                        "config",
                        "config.disabled.adapter.never_fakes_success",
                        "Disabled adapters fail explicitly and do not masquerade as healthy or successful runtime bindings.",
                    ),
                ],
            ),
        ],
        "dependency-boundary": [
            suite_case(
                run_id=run_id,
                suite="dependency-boundary",
                case_id="arch-001-entry-facade-dependency-boundary",
                tc_refs=["TC-ID-ARCH-001"],
                evidence_candidate_refs=["EV-CAND-ID-ARCH-001"],
                evidence_refs=["EV-ID-ARCH-001"],
                assertions=[
                    (
                        "contract",
                        "dependency.normal.deps.limited_to_core_and_public_shells",
                        "The workspace normal dependencies stay within core-contracts and the declared identity crate layering without a sibling business compile-time loop.",
                    ),
                    (
                        "flow",
                        "dependency.entry.runtime.infra.used_through_formal_surfaces",
                        "Entry and runtime code keep repo and adapter collaboration behind formal application or infra seams instead of importing sibling business truth directly.",
                    ),
                ],
            ),
        ],
        "redaction-boundary": [
            suite_case(
                run_id=run_id,
                suite="redaction-boundary",
                case_id="redaction-001-forbidden-material-scan",
                tc_refs=[
                    "TC-ID-CONTRACT-004",
                    "TC-ID-CMD-010",
                    "TC-ID-REDACTION-001",
                    "TC-ID-REDACTION-002",
                    "TC-ID-REDACTION-003",
                ],
                evidence_candidate_refs=["EV-CAND-ID-REDACTION-001"],
                evidence_refs=["EV-ID-REDACTION-001"],
                assertions=[
                    (
                        "redaction",
                        "redaction.artifacts.reports.clean",
                        "Artifacts, suite reports, and review-facing report shells stay clean of raw secrets, raw external bodies, and full sensitive refs.",
                    ),
                    (
                        "redaction",
                        "redaction.safe_refs.only",
                        "The evidence surfaces keep only safe refs, safe summaries, and redacted diagnostic identifiers.",
                    ),
                ],
            ),
        ],
        "release-main-smoke": [
            suite_case(
                run_id=run_id,
                suite="release-main-smoke",
                case_id="core-001-release-scenario-closure",
                tc_refs=[
                    "TC-ID-CMD-001",
                    "TC-ID-QUERY-001",
                    "TC-ID-OUTBOX-001",
                    "TC-ID-JOB-001",
                    "TC-ID-CONFIG-001",
                    "TC-ID-REDACTION-001",
                ],
                evidence_candidate_refs=["EV-CAND-ID-CMD-001", "EV-CAND-ID-QUERY-001"],
                evidence_refs=["EV-ID-CORE-001"],
                assertions=[
                    (
                        "flow",
                        "release.scenario.identity_closure.sample",
                        "The sample release scenario links establish, read, propagation, job replay, config, and redaction evidence through a single run-scoped closure.",
                    ),
                    (
                        "evidence",
                        "release.scenario.sample_only.not_final_acceptance",
                        "This sample raw release suite exists only to validate report tooling and does not assert final release acceptance, veto signoff, or handoff review.",
                    ),
                ],
            ),
        ],
        "report-generation-audit": [
            suite_case(
                run_id=run_id,
                suite="report-generation-audit",
                case_id="report-001-pairing-and-no-static-evidence",
                tc_refs=[
                    "TC-ID-CONTRACT-001",
                    "TC-ID-CMD-001",
                    "TC-ID-QUERY-001",
                    "TC-ID-CONFIG-001",
                    "TC-ID-ARCH-001",
                    "TC-ID-REDACTION-001",
                ],
                evidence_candidate_refs=[
                    "EV-CAND-ID-CONTRACT-001",
                    "EV-CAND-ID-CMD-001",
                    "EV-CAND-ID-QUERY-001",
                    "EV-CAND-ID-CONFIG-001",
                    "EV-CAND-ID-ARCH-001",
                    "EV-CAND-ID-REDACTION-001",
                ],
                evidence_refs=["EV-ID-REPORT-001"],
                assertions=[
                    (
                        "evidence",
                        "report.audit.pairs.raw_artifacts_and_reports",
                        "Every blocking suite in the sample run pairs a run-scoped raw artifact with its generated report path.",
                    ),
                    (
                        "evidence",
                        "report.audit.rejects_static_pass",
                        "The report tooling derives evidence and gate summaries from raw artifacts instead of hand-written pass-only markdown.",
                    ),
                ],
            ),
        ],
    }


def suite_profiles() -> dict[str, str]:
    return {
        "contract-domain-fast": "ci-test",
        "service-flow-fast": "ci-test",
        "infra-runtime-fake": "ci-test",
        "entry-worker-job": "ci-test",
        "operations-replay-core": "operations-replay",
        "config-redline": "ci-test",
        "dependency-boundary": "ci-test",
        "redaction-boundary": "ci-test",
        "release-main-smoke": "release-candidate",
        "report-generation-audit": "ci-test",
    }


def suite_timestamps() -> dict[str, tuple[str, str, int]]:
    return {
        "contract-domain-fast": ("2026-06-19T04:20:00Z", "2026-06-19T04:20:10Z", 10000),
        "service-flow-fast": ("2026-06-19T04:20:11Z", "2026-06-19T04:20:28Z", 17000),
        "infra-runtime-fake": ("2026-06-19T04:20:29Z", "2026-06-19T04:20:38Z", 9000),
        "entry-worker-job": ("2026-06-19T04:20:39Z", "2026-06-19T04:20:50Z", 11000),
        "operations-replay-core": ("2026-06-19T04:20:51Z", "2026-06-19T04:21:06Z", 15000),
        "config-redline": ("2026-06-19T04:21:07Z", "2026-06-19T04:21:12Z", 5000),
        "dependency-boundary": ("2026-06-19T04:21:13Z", "2026-06-19T04:21:17Z", 4000),
        "redaction-boundary": ("2026-06-19T04:21:18Z", "2026-06-19T04:21:23Z", 5000),
        "release-main-smoke": ("2026-06-19T04:21:24Z", "2026-06-19T04:21:44Z", 20000),
        "report-generation-audit": ("2026-06-19T04:21:45Z", "2026-06-19T04:21:53Z", 8000),
    }


def build_evidence_items(
    *,
    run_id: str,
    artifact_root: Path,
    report_root: Path,
    suite_reports: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    all_ac_refs = id_range("AC-ID", 1, 15)
    all_veto_refs = id_range("VETO-ID", 1, 6)

    def suite_report_digest(suite: str) -> str:
        return suite_reports[suite]["artifact_digest"]

    def suite_report_artifact(suite: str) -> str:
        return str((artifact_root / "suites" / suite / "report.json").as_posix())

    def suite_report_markdown(suite: str) -> str:
        return str((report_root / "suites" / f"{suite}.md").as_posix())

    return [
        {
            "evidence_id": "EV-ID-CORE-001",
            "run_id": run_id,
            "suite_refs": ["release-main-smoke"],
            "tc_refs": [
                "TC-ID-CMD-001",
                "TC-ID-QUERY-001",
                "TC-ID-OUTBOX-001",
                "TC-ID-JOB-001",
                "TC-ID-CONFIG-001",
                "TC-ID-REDACTION-001",
            ],
            "ac_refs": id_range("AC-ID", 1, 5),
            "veto_refs": all_veto_refs,
            "artifact_paths": [suite_report_artifact("release-main-smoke")],
            "artifact_digests": [suite_report_digest("release-main-smoke")],
            "report_paths": [suite_report_markdown("release-main-smoke")],
            "status": "passed",
            "redaction_status": "clean",
            "review_status": "pending",
            "safe_summary": "Sample release scenario evidence is present for the report tooling run and links command, query, outbox, job, config, and redaction coverage.",
        },
        {
            "evidence_id": "EV-ID-CONTRACT-001",
            "run_id": run_id,
            "suite_refs": ["contract-domain-fast"],
            "tc_refs": id_range("TC-ID-CONTRACT", 1, 4),
            "ac_refs": id_range("AC-ID", 6, 15),
            "veto_refs": ["VETO-ID-003"],
            "artifact_paths": [suite_report_artifact("contract-domain-fast")],
            "artifact_digests": [suite_report_digest("contract-domain-fast")],
            "report_paths": [suite_report_markdown("contract-domain-fast")],
            "status": "passed",
            "redaction_status": "clean",
            "review_status": "pending",
            "safe_summary": "The contract-domain sample artifacts keep the public protocol shells round-trippable and body-free.",
        },
        {
            "evidence_id": "EV-ID-STATE-001",
            "run_id": run_id,
            "suite_refs": ["contract-domain-fast"],
            "tc_refs": [
                *id_range("TC-ID-DOMAIN", 1, 6),
                *id_range("TC-ID-STATE", 1, 2),
            ],
            "ac_refs": id_range("AC-ID", 1, 14),
            "veto_refs": ["VETO-ID-001", "VETO-ID-004"],
            "artifact_paths": [suite_report_artifact("contract-domain-fast")],
            "artifact_digests": [suite_report_digest("contract-domain-fast")],
            "report_paths": [suite_report_markdown("contract-domain-fast")],
            "status": "passed",
            "redaction_status": "clean",
            "review_status": "pending",
            "safe_summary": "The domain and state sample artifacts keep legal transitions, terminal guards, append-only history, and stable refs aligned with the formal state matrix.",
        },
        {
            "evidence_id": "EV-ID-CMD-001",
            "run_id": run_id,
            "suite_refs": ["service-flow-fast"],
            "tc_refs": id_range("TC-ID-CMD", 1, 15),
            "ac_refs": all_ac_refs,
            "veto_refs": ["VETO-ID-001", "VETO-ID-002", "VETO-ID-003", "VETO-ID-004"],
            "artifact_paths": [suite_report_artifact("service-flow-fast")],
            "artifact_digests": [suite_report_digest("service-flow-fast")],
            "report_paths": [suite_report_markdown("service-flow-fast")],
            "status": "passed",
            "redaction_status": "clean",
            "review_status": "pending",
            "safe_summary": "The command service sample artifacts keep accepted, rejected, duplicate replay, and rollback surfaces tied to the formal command shell.",
        },
        {
            "evidence_id": "EV-ID-QUERY-001",
            "run_id": run_id,
            "suite_refs": ["service-flow-fast"],
            "tc_refs": id_range("TC-ID-QUERY", 1, 15),
            "ac_refs": all_ac_refs,
            "veto_refs": ["VETO-ID-002"],
            "artifact_paths": [suite_report_artifact("service-flow-fast")],
            "artifact_digests": [suite_report_digest("service-flow-fast")],
            "report_paths": [suite_report_markdown("service-flow-fast")],
            "status": "passed",
            "redaction_status": "clean",
            "review_status": "pending",
            "safe_summary": "The query service sample artifacts keep visibility-first, degraded, stale, and no-write behavior tied to the formal query shell.",
        },
        {
            "evidence_id": "EV-ID-CONSUMER-001",
            "run_id": run_id,
            "suite_refs": ["entry-worker-job"],
            "tc_refs": id_range("TC-ID-CONSUMER", 1, 6),
            "ac_refs": id_range("AC-ID", 6, 15),
            "veto_refs": ["VETO-ID-002", "VETO-ID-003"],
            "artifact_paths": [suite_report_artifact("entry-worker-job")],
            "artifact_digests": [suite_report_digest("entry-worker-job")],
            "report_paths": [suite_report_markdown("entry-worker-job")],
            "status": "passed",
            "redaction_status": "clean",
            "review_status": "pending",
            "safe_summary": "The inbound and callback sample artifacts preserve typed receipt replay, no-create behavior, and body-free public receipts.",
        },
        {
            "evidence_id": "EV-ID-OUTBOX-001",
            "run_id": run_id,
            "suite_refs": ["operations-replay-core"],
            "tc_refs": id_range("TC-ID-OUTBOX", 1, 10),
            "ac_refs": all_ac_refs,
            "veto_refs": ["VETO-ID-003"],
            "artifact_paths": [suite_report_artifact("operations-replay-core")],
            "artifact_digests": [suite_report_digest("operations-replay-core")],
            "report_paths": [suite_report_markdown("operations-replay-core")],
            "status": "passed",
            "redaction_status": "clean",
            "review_status": "pending",
            "safe_summary": "The operations replay sample artifacts keep accepted-only outbox material body-free and preserve formal publish outcome markers.",
        },
        {
            "evidence_id": "EV-ID-JOB-001",
            "run_id": run_id,
            "suite_refs": ["entry-worker-job", "operations-replay-core"],
            "tc_refs": id_range("TC-ID-JOB", 1, 8),
            "ac_refs": all_ac_refs,
            "veto_refs": ["VETO-ID-002", "VETO-ID-005"],
            "artifact_paths": [
                suite_report_artifact("entry-worker-job"),
                suite_report_artifact("operations-replay-core"),
            ],
            "artifact_digests": [
                suite_report_digest("entry-worker-job"),
                suite_report_digest("operations-replay-core"),
            ],
            "report_paths": [
                suite_report_markdown("entry-worker-job"),
                suite_report_markdown("operations-replay-core"),
            ],
            "status": "passed",
            "redaction_status": "clean",
            "review_status": "pending",
            "safe_summary": "The job entry and operations replay sample artifacts preserve report replay, partial failure accounting, and report-only no-repair semantics.",
        },
        {
            "evidence_id": "EV-ID-IDEMP-001",
            "run_id": run_id,
            "suite_refs": ["infra-runtime-fake", "operations-replay-core"],
            "tc_refs": id_range("TC-ID-IDEMP", 1, 11),
            "ac_refs": ["AC-ID-009", "AC-ID-014", "AC-ID-015"],
            "veto_refs": ["VETO-ID-001", "VETO-ID-002", "VETO-ID-005"],
            "artifact_paths": [
                suite_report_artifact("infra-runtime-fake"),
                suite_report_artifact("operations-replay-core"),
            ],
            "artifact_digests": [
                suite_report_digest("infra-runtime-fake"),
                suite_report_digest("operations-replay-core"),
            ],
            "report_paths": [
                suite_report_markdown("infra-runtime-fake"),
                suite_report_markdown("operations-replay-core"),
            ],
            "status": "passed",
            "redaction_status": "clean",
            "review_status": "pending",
            "safe_summary": "The replay sample artifacts keep duplicate reuse, in-flight guard, controlled fault surfaces, and stored replay material aligned across fake and operations paths.",
        },
        {
            "evidence_id": "EV-ID-CONFIG-001",
            "run_id": run_id,
            "suite_refs": ["config-redline"],
            "tc_refs": id_range("TC-ID-CONFIG", 1, 4),
            "ac_refs": ["AC-ID-015"],
            "veto_refs": ["VETO-ID-006"],
            "artifact_paths": [suite_report_artifact("config-redline")],
            "artifact_digests": [suite_report_digest("config-redline")],
            "report_paths": [suite_report_markdown("config-redline")],
            "status": "passed",
            "redaction_status": "clean",
            "review_status": "pending",
            "safe_summary": "The config sample artifacts keep strict profile validation, no implicit fallback, and disabled-adapter no-success behavior aligned with the formal runtime rules.",
        },
        {
            "evidence_id": "EV-ID-REDACTION-001",
            "run_id": run_id,
            "suite_refs": ["redaction-boundary"],
            "tc_refs": [
                "TC-ID-CONTRACT-004",
                "TC-ID-CMD-010",
                "TC-ID-REDACTION-001",
                "TC-ID-REDACTION-002",
                "TC-ID-REDACTION-003",
            ],
            "ac_refs": id_range("AC-ID", 11, 15),
            "veto_refs": ["VETO-ID-003"],
            "artifact_paths": [suite_report_artifact("redaction-boundary")],
            "artifact_digests": [suite_report_digest("redaction-boundary")],
            "report_paths": [str((report_root / "redaction-check.md").as_posix())],
            "status": "passed",
            "redaction_status": "clean",
            "review_status": "pending",
            "safe_summary": "The redaction sample artifacts keep reports and machine evidence free of forbidden bodies, raw secrets, and full sensitive refs.",
        },
        {
            "evidence_id": "EV-ID-ARCH-001",
            "run_id": run_id,
            "suite_refs": ["dependency-boundary"],
            "tc_refs": ["TC-ID-ARCH-001"],
            "ac_refs": id_range("AC-ID", 11, 15),
            "veto_refs": ["VETO-ID-006"],
            "artifact_paths": [suite_report_artifact("dependency-boundary")],
            "artifact_digests": [suite_report_digest("dependency-boundary")],
            "report_paths": [str((report_root / "dependency-boundary.md").as_posix())],
            "status": "passed",
            "redaction_status": "clean",
            "review_status": "pending",
            "safe_summary": "The dependency boundary sample artifacts show that the workspace keeps compile-time dependencies within core and the declared identity layering.",
        },
        {
            "evidence_id": "EV-ID-NFR-001",
            "run_id": run_id,
            "suite_refs": ["operations-replay-core", "redaction-boundary", "release-main-smoke"],
            "tc_refs": [
                "TC-ID-QUERY-001",
                "TC-ID-JOB-001",
                "TC-ID-REDACTION-001",
                "TC-ID-CONFIG-001",
            ],
            "ac_refs": ["AC-ID-015"],
            "veto_refs": [],
            "artifact_paths": [
                suite_report_artifact("operations-replay-core"),
                suite_report_artifact("redaction-boundary"),
                suite_report_artifact("release-main-smoke"),
            ],
            "artifact_digests": [
                suite_report_digest("operations-replay-core"),
                suite_report_digest("redaction-boundary"),
                suite_report_digest("release-main-smoke"),
            ],
            "report_paths": [
                suite_report_markdown("operations-replay-core"),
                str((report_root / "redaction-check.md").as_posix()),
                suite_report_markdown("release-main-smoke"),
            ],
            "status": "passed",
            "redaction_status": "clean",
            "review_status": "pending",
            "safe_summary": "The sample run keeps safe duration, count, degraded, and redaction evidence available for non-functional review without introducing hard-threshold claims.",
        },
        {
            "evidence_id": "EV-ID-REPORT-001",
            "run_id": run_id,
            "suite_refs": ["report-generation-audit"],
            "tc_refs": [
                "TC-ID-CONTRACT-001",
                "TC-ID-CMD-001",
                "TC-ID-QUERY-001",
                "TC-ID-CONFIG-001",
                "TC-ID-ARCH-001",
                "TC-ID-REDACTION-001",
            ],
            "ac_refs": all_ac_refs,
            "veto_refs": all_veto_refs,
            "artifact_paths": [suite_report_artifact("report-generation-audit")],
            "artifact_digests": [suite_report_digest("report-generation-audit")],
            "report_paths": [
                str((report_root / "report-audit.md").as_posix()),
                str((report_root / "evidence-index.md").as_posix()),
            ],
            "status": "passed",
            "redaction_status": "clean",
            "review_status": "pending",
            "safe_summary": "The report-generation-audit sample artifacts cover artifact and report pairing, no-static-evidence checks, and evidence traceability back to existing blocking-suite TC refs.",
        },
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write commit-08-b sample raw artifacts for report tooling.",
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--report-root", required=True)
    parser.add_argument("--design-source-ref", required=True)
    parser.add_argument("--implementation-source-ref", required=True)
    parser.add_argument(
        "--core-contracts-source-ref",
        default="not_applicable",
    )
    parser.add_argument(
        "--tool-version",
        default="scripts/reports/write_commit_08_b_artifacts.py",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    artifact_root = Path(args.artifact_root)
    report_root = Path(args.report_root)
    run_id = args.run_id

    suite_cases = build_suite_cases(run_id)
    profiles = suite_profiles()
    timestamps = suite_timestamps()

    suite_reports: dict[str, dict[str, Any]] = {}
    for suite, cases in suite_cases.items():
        started_at, finished_at, duration_ms = timestamps[suite]
        suite_reports[suite] = write_suite(
            artifact_root=artifact_root,
            run_id=run_id,
            suite=suite,
            cases=cases,
            config_profile=profiles[suite],
            status="passed",
            duration_ms=duration_ms,
            started_at=started_at,
            finished_at=finished_at,
        )

    write_json(
        artifact_root / "meta" / "context.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "suite_refs": sorted(suite_cases.keys()),
            "config_profile": "not_applicable",
            "started_at": "2026-06-19T04:20:00Z",
            "tool_version": args.tool_version,
            "redacted_environment": {
                "profile": "not_applicable",
                "boundary_id": "commit-08-b",
                "timezone": "Asia/Shanghai",
            },
            "artifact_root": artifact_root.as_posix(),
            "report_root": report_root.as_posix(),
            "artifact_digest_algorithm": "sha256",
        },
    )
    write_json(
        artifact_root / "meta" / "source-commits.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "design_source_ref": args.design_source_ref,
            "implementation_source_ref": args.implementation_source_ref,
            "core_contracts_source_ref": args.core_contracts_source_ref,
            "additional_source_refs": [],
            "generated_at": "2026-06-19T04:21:54Z",
            "artifact_digest_algorithm": "sha256",
        },
    )
    write_json(
        artifact_root / "meta" / "config-digest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "config_applicability": "not_applicable",
            "config_profile": "not_applicable",
            "config_digest_algorithm": "sha256",
            "config_digest": SHA256_EMPTY_OBJECT,
            "config_digest_material": {},
            "config_sources": [],
            "config_reason_ref": "commit-08-b.report_tooling.sample_run",
            "redaction_status": "not_applicable",
            "generated_at": "2026-06-19T04:21:54Z",
            "artifact_digest_algorithm": "sha256",
        },
    )
    write_evidence_index(
        artifact_root=artifact_root,
        run_id=run_id,
        evidence=build_evidence_items(
            run_id=run_id,
            artifact_root=artifact_root,
            report_root=report_root,
            suite_reports=suite_reports,
        ),
    )


if __name__ == "__main__":
    main()
