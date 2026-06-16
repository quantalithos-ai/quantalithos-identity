# entry-worker-job

- run_id: `20260617T032600+0800`
- gate: `GATE-06`
- commit boundary: `commit-06-b`
- design baseline: `4a18d7a`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-CONSUMER-001`

## Scope

- Covered only the five commit-06-b inbound and callback mutation flows, their formal typed receipt replay surfaces, missing-target no-create handling, callback target mismatch handling, and body-free public receipts.
- Did not claim accepted outbound material factories, publish/deliver/retry jobs, API or worker entry wiring, transport retry loops, or any implicit creation of missing member, relation, or handoff target truth.
- Replay verification stays on stored typed consumer and callback receipts; it does not rebuild responses from current truth, effect summaries, or repository scans.

## Cases

- `TC-ID-CONSUMER-001`: role source change accepts a body-free snapshot update and duplicate replay returns the stored typed receipt without a second mutation
- `TC-ID-CONSUMER-002`: work participation source duplicates return a fresh `Noop` receipt and do not append another career record
- `TC-ID-CONSUMER-003`: memory source changes quarantine missing relations and do not create local memory truth, reference state, or outbox material
- `TC-ID-CONSUMER-004`: archive handoff callbacks reject direct-target and callback-lookup mismatches without mutating either relation
- `TC-ID-CONSUMER-005`: trace handoff callbacks require a formal receipt for `Delivered`, store the callback envelope on success, and replay it without a second state change
- `TC-ID-CONSUMER-006`: shared consumer and callback replay stays on typed stored receipts, keeps callback receipts on `HandoffCallbackReceipt`, and preserves body-free public surfaces

## No-Create And Replay Audit

- Missing relation branches are backed by `in_memory::tests::handle_memory_reference_source_state_changed_missing_relation_does_not_create`, which verifies the consumer returns `Quarantined` and leaves relation/outbox stores empty.
- Callback target mismatch is backed by `in_memory::tests::handle_archive_handoff_result_target_mismatch_rejects_without_mutation`, which verifies both seeded relations keep their original versions.
- Duplicate replay is backed by `in_memory::tests::handle_role_capability_source_changed_accepts_and_replays`, `in_memory::tests::handle_trace_handoff_result_delivered_replays_stored_receipt`, `in_memory::tests::inbound_consumer_scaffold_duplicate_replays_without_running_handler`, and `in_memory::tests::callback_scaffold_duplicate_replays_handoff_callback_receipt_without_handler`.

## Body-Free Audit

- The accepted role, work, memory, archive, and trace flows persist only refs, safe summary markers, state kinds, receipt refs, issue markers, trace refs, and outbox refs.
- No artifact, report, stored receipt, or verification note in this run contains raw event bodies, callback bodies, archive packages, adapter diagnostics, method-definition bodies, or memory text.

## Evidence

- suite raw artifact: `artifacts/test/20260617T032600+0800/suites/entry-worker-job/report.json`
- source commits: `artifacts/test/20260617T032600+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260617T032600+0800/meta/config-digest.json`
- case artifacts: `artifacts/test/20260617T032600+0800/suites/entry-worker-job/cases/*.json`

## Verification

- `cargo fmt --all` passed
- `cargo check -p identity-application` passed
- `cargo check -p identity-infra` passed
- `cargo test -p identity-infra` passed
- Targeted coverage inside the passing suite includes `handle_role_capability_source_changed_accepts_and_replays`, `handle_work_participation_accepted_source_duplicate_returns_noop`, `handle_memory_reference_source_state_changed_missing_relation_does_not_create`, `handle_archive_handoff_result_target_mismatch_rejects_without_mutation`, `handle_trace_handoff_result_delivered_requires_receipt`, and `handle_trace_handoff_result_delivered_replays_stored_receipt`.
