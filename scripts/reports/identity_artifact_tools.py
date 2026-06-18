#!/usr/bin/env python3
"""Shared helpers for run-scoped identity test artifacts and reports."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "identity.artifact.v1"
SHA256_EMPTY_OBJECT = (
    "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
)


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def digest_bytes(payload: bytes) -> str:
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def with_digest(payload: dict[str, Any]) -> dict[str, Any]:
    body = dict(payload)
    digest = digest_bytes(canonical_json_bytes(body))
    body["artifact_digest"] = digest
    return body


def write_json(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    stored = with_digest(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(stored))
    return stored


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_text(path: Path, content: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def ensure_empty_log(path: Path) -> None:
    ensure_text(path, "")


def file_digest(path: Path) -> str:
    return digest_bytes(path.read_bytes())


def suite_report_artifact_path(artifact_root: Path, suite: str) -> Path:
    return artifact_root / "suites" / suite / "report.json"


def suite_cases_root(artifact_root: Path, suite: str) -> Path:
    return artifact_root / "suites" / suite / "cases"


def suite_case_artifact_path(artifact_root: Path, suite: str, case_id: str) -> Path:
    return suite_cases_root(artifact_root, suite) / f"{case_id}.json"


def suite_stdout_path(artifact_root: Path, suite: str) -> Path:
    return artifact_root / "suites" / suite / "stdout.log"


def suite_stderr_path(artifact_root: Path, suite: str) -> Path:
    return artifact_root / "suites" / suite / "stderr.log"


def suite_report_markdown_path(report_root: Path, suite: str) -> Path:
    return report_root / "suites" / f"{suite}.md"


def build_assertion(
    *,
    case_id: str,
    index: int,
    assertion_kind: str,
    assertion_ref: str,
    safe_message: str,
    safe_detail_refs: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "assertion_id": f"{case_id}-assertion-{index:02d}",
        "assertion_ref": assertion_ref,
        "assertion_status": "passed",
        "assertion_kind": assertion_kind,
        "safe_message": safe_message,
        "safe_message_ref": assertion_ref,
        "safe_detail_refs": safe_detail_refs or [],
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
    assertions: list[dict[str, Any]],
    status: str = "passed",
    duration_ms: int = 0,
    failure_reason_ref: str | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "suite": suite,
        "case_id": case_id,
        "tc_refs": tc_refs,
        "status": status,
        "assertions": assertions,
        "failure_reason_ref": failure_reason_ref,
        "evidence_candidate_refs": evidence_candidate_refs,
        "evidence_refs": evidence_refs,
        "duration_ms": duration_ms,
        "artifact_digest_algorithm": "sha256",
    }


def write_suite(
    *,
    artifact_root: Path,
    run_id: str,
    suite: str,
    cases: list[dict[str, Any]],
    config_profile: str,
    status: str,
    duration_ms: int,
    started_at: str,
    finished_at: str,
) -> dict[str, Any]:
    suite_root = artifact_root / "suites" / suite
    suite_root.mkdir(parents=True, exist_ok=True)

    stdout_path = suite_stdout_path(artifact_root, suite)
    stderr_path = suite_stderr_path(artifact_root, suite)
    ensure_empty_log(stdout_path)
    ensure_empty_log(stderr_path)

    case_refs: list[str] = []
    case_digests: dict[str, str] = {}
    for case_payload in cases:
        case_id = case_payload["case_id"]
        stored_case = write_json(
            suite_case_artifact_path(artifact_root, suite, case_id),
            case_payload,
        )
        case_refs.append(case_id)
        case_digests[case_id] = stored_case["artifact_digest"]

    return write_json(
        suite_report_artifact_path(artifact_root, suite),
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "suite": suite,
            "status": status,
            "case_refs": case_refs,
            "case_digests": case_digests,
            "failure_reason_ref": None,
            "duration_ms": duration_ms,
            "config_profile": config_profile,
            "started_at": started_at,
            "finished_at": finished_at,
            "stdout_digest": file_digest(stdout_path),
            "stderr_digest": file_digest(stderr_path),
            "artifact_digest_algorithm": "sha256",
        },
    )


def write_evidence_index(
    *,
    artifact_root: Path,
    run_id: str,
    evidence: list[dict[str, Any]],
) -> dict[str, Any]:
    return write_json(
        artifact_root / "evidence-index.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "evidence": evidence,
            "artifact_digest_algorithm": "sha256",
        },
    )
