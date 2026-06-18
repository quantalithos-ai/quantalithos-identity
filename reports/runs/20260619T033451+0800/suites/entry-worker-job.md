# entry-worker-job

- run_id: `20260619T033451+0800`
- suite status: `passed`
- config profile: `ci-test`
- started_at: `2026-06-18T19:35:38Z`
- finished_at: `2026-06-18T19:35:49Z`
- duration_ms: `11000`

## Cases

- `consumer-001-inbound-and-callback-replay`: status=`passed`; tc_refs=TC-ID-CONSUMER-001,TC-ID-CONSUMER-002,TC-ID-CONSUMER-003,TC-ID-CONSUMER-004,TC-ID-CONSUMER-005,TC-ID-CONSUMER-006; evidence_refs=EV-ID-CONSUMER-001
- `job-001-job-entry-and-report-replay`: status=`passed`; tc_refs=TC-ID-JOB-001,TC-ID-JOB-002,TC-ID-JOB-003,TC-ID-JOB-004,TC-ID-JOB-005,TC-ID-JOB-006,TC-ID-JOB-007,TC-ID-JOB-008; evidence_refs=EV-ID-JOB-001

## Assertions

- `consumer.callback.typed_receipt.replay_only`: passed - Inbound and callback duplicates replay stored typed receipts and do not rerun mutation, outbox append, or callback state transition.
- `consumer.callback.missing_target.no_create`: passed - Missing targets stay no-create and the public receipts remain body-free across accepted, delayed, and quarantined surfaces.
- `job.entry.dispatches.through.facade`: passed - The jobs entry parses the formal request and dispatches through the application facade without direct repository writes.
- `job.duplicate.replays.stored_report`: passed - Duplicate job requests replay the stored typed report instead of rescanning stores or rerunning the job body.

## Raw Artifacts

- suite raw artifact: `artifacts/test/20260619T033451+0800/suites/entry-worker-job/report.json`
- case artifacts: `artifacts/test/20260619T033451+0800/suites/entry-worker-job/cases/*.json`
- stdout digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- stderr digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
