# operations-replay-core

- run_id: `20260617T112044+0800`
- gate: `GATE-07`
- commit boundary: `commit-06-c`
- design baseline: `7c78e38`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-OUTBOX-001`

## Scope

- Covered only the commit-06-c outbound accepted material subset: canonical payload marker snapshots, accepted-only outbox creation, immutable fake snapshot persistence, duplicate replay stability, and outbox-save rollback parity.
- Did not claim `TC-ID-OUTBOX-009~010`, publish/deliver/retry job execution, topic raw-string binding, publisher adapter execution, API/worker/jobs entry wiring, or any current-truth payload reconstruction at publish time.

## Cases

- `TC-ID-OUTBOX-001~004`: GlobalMemberEstablished, IdentityAnchorChanged, GlobalLifecycleChanged, and conditional GlobalMemberAvailabilityChanged material snapshots stay on canonical topic/schema/subject/trace bindings.
- `TC-ID-OUTBOX-005`: role summary and role source material snapshots stay body-free and replay-stable.
- `TC-ID-OUTBOX-006`: career append and correction material snapshots stay append-only and keep correction-specific material separate.
- `TC-ID-OUTBOX-007`: memory reference and archive-handoff state material snapshots stay on marker-only payloads.
- `TC-ID-OUTBOX-008`: rejected/query/retry-only branches do not create accepted material, and outbox save failure rolls back the owning accepted transaction.

## Snapshot Audit

- `in_memory::tests::establish_member_persists_member_lifecycle_trace_audit_and_replay` verifies the initial establish path stores GlobalMemberEstablished and IdentityAnchorChanged snapshots and keeps payload marker count stable on duplicate replay.
- `in_memory::tests::update_lifecycle_uses_member_key_and_replays_from_stored_envelope` verifies lifecycle material always exists and availability material is emitted only when derived availability changes.
- `in_memory::tests::maintain_role_capability_summary_accepts_and_replays`, `append_career_record_handles_append_correction_and_duplicate_conflict`, `maintain_memory_reference_link_archive_handoff_and_replay`, and `handle_trace_handoff_result_delivered_replays_stored_receipt` verify the remaining role, career, memory, and handoff material kinds use canonical topic/schema bindings and immutable payload marker snapshots.

## Accepted-Only And Replay Audit

- `in_memory::tests::append_career_record_pending_review_accepts_without_outbox`, `handle_memory_reference_source_state_changed_missing_relation_does_not_create`, and `handle_archive_handoff_result_target_mismatch_rejects_without_mutation` verify non-accepted branches do not create accepted outbox material.
- `in_memory::tests::establish_member_rolls_back_when_outbox_save_fails` verifies outbox-save failure clears staged writes, leaves member/lifecycle truth absent, and persists no payload marker snapshot.
- Replay stability is verified by the establish, lifecycle, role, memory, and trace-handoff replay tests, which keep the original outbox refs and payload marker counts instead of regenerating material from current truth.

## Evidence

- suite raw artifact: `artifacts/test/20260617T112044+0800/suites/operations-replay-core/report.json`
- source commits: `artifacts/test/20260617T112044+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260617T112044+0800/meta/config-digest.json`
- case artifacts: `artifacts/test/20260617T112044+0800/suites/operations-replay-core/cases/*.json`

## Verification

- `cargo fmt --all` passed
- `cargo check -p identity-application -p identity-domain -p identity-infra` passed
- `cargo test -p identity-domain` passed
- `cargo test -p identity-application` passed
- `cargo test -p identity-infra` passed
