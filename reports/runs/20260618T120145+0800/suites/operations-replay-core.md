# operations-replay-core

- run_id: `20260618T120145+0800`
- gate: `GATE-07`; `GATE-08`; `GATE-10` body-free subset
- commit boundary: `commit-07-c`
- design baseline: `725d3d0`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-OUTBOX-001`; `EV-ID-JOB-001`; `EV-ID-IDEMP-001`

## Scope

- Covered only the `commit-07-c` propagation subset: publish pending outbox records, deliver trace handoff intents, retry one retryable family, duplicate job report replay, and body-free issue or receipt or report invariants.
- Did not claim PH-08 runner, CLI, runtime, config binding, scheduler, broker ack or retry loops, release scripts, or any `commit-07-b` maintenance semantics.
- Publish and delivery verification stays on saved outbox or handoff material, formal topic binding or target resolution, and stored replay surfaces; it does not reconstruct payloads from current truth or rescan unrelated stores on duplicate.

## Cases

- `TC-ID-OUTBOX-009`: retryable publish failures move records to `RetryableFailed`, surface only formal issue refs, and leave accepted member truth unchanged.
- `TC-ID-OUTBOX-010`: unsupported topic outcomes stay terminal and visible on the failed surface; mixed publish runs stay partial instead of faking fallback publish success.
- `TC-ID-JOB-004`: delivered handoff requires formal attempt plus receipt refs, retryable failure keeps the attempt ref, and the default fake cancels unsupported targets instead of synthesizing delivery.
- `TC-ID-JOB-005`; `TC-ID-IDEMP-004`: retry runs touch one family only, skip terminal items, and duplicate propagation jobs replay stored typed output plus stored report without rerun.

## Transition Audit

- `in_memory::tests::publish_outbox_job_updates_state_and_preserves_member_truth` verifies publish success updates only outbox state and keeps persisted member truth unchanged.
- `in_memory::tests::publish_outbox_job_maps_retryable_and_unsupported_outcomes` verifies retryable failures land on `RetryableFailed`, unsupported topics land on `Failed`, and mixed runs surface partial status with safe issue refs.
- `in_memory::tests::deliver_trace_handoff_job_delivers_with_formal_receipt`, `deliver_trace_handoff_job_default_fake_does_not_synthesize_success`, and `deliver_trace_handoff_job_maps_retryable_failure_with_attempt` verify delivered, retryable, and cancelled handoff transitions stay on formal attempt, receipt, and issue markers only.
- `in_memory::tests::retry_propagation_job_retries_only_retryable_outbox_family` verifies retry selection excludes terminal published items and leaves non-selected families untouched.

## Replay And Body-Free Audit

- `in_memory::tests::publish_outbox_job_duplicate_replay_returns_stored_report_without_rerun`, `job_duplicate_replays_stored_report`, and `job_dispatch_duplicate_replay_does_not_run_handler` verify duplicate propagation jobs replay stored typed output plus stored job report and do not relist targets or re-enter adapters.
- `in_memory::tests::job_dispatch_replay_missing_report_is_consistency_defect` and `job_dispatch_replay_wrong_stored_kind_is_consistency_defect` verify replay defects fail closed instead of reconstructing from current truth or rescanning current stores.
- Propagation reports, issue refs, and handoff receipt surfaces stay body-free. No separate `redaction-check.md` was needed for this run because the stdout and stderr artifacts stayed blank and the suite report carries only refs, counts, timestamps, and issue or receipt refs.

## Evidence

- suite raw artifact: `artifacts/test/20260618T120145+0800/suites/operations-replay-core/report.json`
- source commits: `artifacts/test/20260618T120145+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260618T120145+0800/meta/config-digest.json`
- case artifacts: `artifacts/test/20260618T120145+0800/suites/operations-replay-core/cases/*.json`

## Verification

- `cargo fmt --all` passed
- `cargo check -p identity-contracts` passed
- `cargo check -p identity-application` passed
- `cargo check -p identity-infra` passed
- `cargo test -p identity-infra` passed
