# operations-replay-core

- run_id: `20260619T124900+0800`
- suite status: `passed`
- config profile: `operations-replay`
- started_at: `2026-06-19T04:20:51Z`
- finished_at: `2026-06-19T04:21:06Z`
- duration_ms: `15000`

## Cases

- `outbox-001-accepted-only-payload-marker`: status=`passed`; tc_refs=TC-ID-OUTBOX-001,TC-ID-OUTBOX-002,TC-ID-OUTBOX-003,TC-ID-OUTBOX-004,TC-ID-OUTBOX-005,TC-ID-OUTBOX-006,TC-ID-OUTBOX-007,TC-ID-OUTBOX-008,TC-ID-OUTBOX-009,TC-ID-OUTBOX-010; evidence_refs=EV-ID-OUTBOX-001
- `job-002-maintenance-and-propagation-no-truth-repair`: status=`passed`; tc_refs=TC-ID-JOB-001,TC-ID-JOB-002,TC-ID-JOB-003,TC-ID-JOB-004,TC-ID-JOB-005,TC-ID-JOB-006,TC-ID-JOB-007,TC-ID-JOB-008; evidence_refs=EV-ID-JOB-001,EV-ID-NFR-001

## Assertions

- `outbox.accepted_only.body_free.material`: passed - Only accepted paths create stored payload markers and the material stays body-free across outbox and handoff surfaces.
- `outbox.publisher.failure.does_not_repair_truth`: passed - Publisher failures update only formal outbox attempt or issue markers and never roll back accepted truth into a second mutation path.
- `jobs.maintenance.report_only.no_truth_repair`: passed - Maintenance jobs stay report-only and do not repair core truth, external truth, or projection truth in place.
- `jobs.partial_failure.safe_counts_and_refs`: passed - The job reports keep safe item refs, failed refs, counts, and durations without embedding raw payload or adapter diagnostics.

## Raw Artifacts

- suite raw artifact: `artifacts/test/20260619T124900+0800/suites/operations-replay-core/report.json`
- case artifacts: `artifacts/test/20260619T124900+0800/suites/operations-replay-core/cases/*.json`
- stdout digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- stderr digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
