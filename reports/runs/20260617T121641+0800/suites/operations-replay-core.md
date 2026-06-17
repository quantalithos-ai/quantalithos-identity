# operations-replay-core

- run_id: `20260617T121641+0800`
- gate: `GATE-08`
- commit boundary: `commit-07-a`
- design baseline: `b92ded4`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-JOB-001`

## Scope

- Covered only the commit-07-a shared job scaffold: replayable `IdentityJobRunReport` assembly, stored result and report persistence order, duplicate report replay, and body-free issue or ref invariants.
- Did not claim any rebuild, refresh, reconciliation, publish, deliver, or retry job body, publisher or handoff execution, runner or CLI wiring, scheduler loop, or runtime or config script behavior.
- Duplicate replay verification stays on stored idempotency plus stored `IdentityJobRunReport`; it does not rescan pending, stale, retryable, outbox, or handoff stores and does not reconstruct a response from current truth.

## Cases

- `TC-ID-JOB-006` and `TC-ID-IDEMP-004`: duplicate jobs load the stored result shell plus stored report and return a replayed response without rerunning handler logic.
- `TC-ID-JOB-008`: the shared job scaffold writes only report and replay surfaces, keeps partial or failed reports on non-empty issue refs, and exposes body-free public report summaries.

## Replay Audit

- `in_memory::tests::job_duplicate_replays_stored_report` verifies the same key and digest reserve path resolves to a stored job report replay root instead of allocating a fresh first-run record.
- `in_memory::tests::job_dispatch_duplicate_replay_does_not_run_handler` verifies duplicate dispatch reuses the stored report surface and never enters the job body closure.
- `in_memory::tests::job_dispatch_replay_missing_report_is_consistency_defect` and `job_dispatch_replay_wrong_stored_kind_is_consistency_defect` verify missing or mismatched replay surfaces fail closed instead of rescanning repositories or rerunning a job body.

## Transaction And Body-Free Audit

- `in_memory::tests::job_dispatch_saves_report_before_idempotency_completion` verifies the shared scaffold does not leave a completed idempotency record pointing at a missing stored report surface when completion fails.
- `jobs::tests::partial_result_requires_issue_refs` verifies partial report results require at least one formal maintenance issue ref before persistence or replay.
- `IdentityJobServiceDeps` in this boundary only contains unit-of-work, clock, id generation, idempotency, stored-result, and job-report ports, so the shared scaffold has no direct access to core identity truth repositories or propagation adapters.
- `IdentityJobReportSurface` and `IdentityJobRunReport::to_surface()` stay on refs, counts, timestamps, cursor refs, and issue refs; they do not carry raw job input, logs, config, payload, receipt, or adapter-response bodies.

## Evidence

- suite raw artifact: `artifacts/test/20260617T121641+0800/suites/operations-replay-core/report.json`
- source commits: `artifacts/test/20260617T121641+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260617T121641+0800/meta/config-digest.json`
- case artifacts: `artifacts/test/20260617T121641+0800/suites/operations-replay-core/cases/*.json`

## Verification

- `cargo fmt --all` passed
- `cargo check -p identity-application` passed
- `cargo check -p identity-infra` passed
- `cargo test -p identity-application jobs::tests` passed
- `cargo test -p identity-infra job_dispatch_` passed
- `cargo test -p identity-infra job_duplicate_replays_stored_report` passed
