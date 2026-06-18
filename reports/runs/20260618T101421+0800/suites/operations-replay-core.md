# operations-replay-core

- run_id: `20260618T101421+0800`
- gate: `GATE-08`; `GATE-05` no-repair audit
- commit boundary: `commit-07-b`
- design baseline: `09174b0`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-JOB-001`; `EV-ID-IDEMP-001`; `EV-ID-NFR-001`

## Scope

- Covered only the commit-07-b maintenance job subset: projection rebuild, external reference refresh, reconciliation report-only behavior, duplicate report replay, and body-free issue or report invariants.
- Did not claim publish, deliver, retry propagation jobs, runner or CLI wiring, scheduler behavior, runtime or config script behavior, or any commit-07-c propagation surface.
- Reference refresh evidence stays on formal ExternalReferenceRef bundle-key selection and loaded-bundle versioning; it does not derive bundle identity from state refs, state-row strings, sibling scans, business source refs, or fake-only maps.

## Cases

- `TC-ID-JOB-001`: projection rebuild uses the formal member-summary rebuild plan, saves the view from typed body-free inputs, preserves newer projection state on race, and leaves member truth unchanged.
- `TC-ID-JOB-002`: reference refresh selects formal bundle keys, uses the loaded bundle version for state and typed sidecar saves, preserves the last good snapshot on unavailable outcomes, and leaves member truth unchanged.
- `TC-ID-JOB-003`: reconciliation stays report-only when inspection context is missing, rejects forbidden finding material before persistence, and does not repair identity or adjacent truth.
- `TC-ID-JOB-008`; `TC-ID-IDEMP-004`: duplicate maintenance jobs replay stored typed output plus stored report, fail closed on missing or wrong-kind replay surfaces, and keep reports body-free.

## No-Repair Audit

- `in_memory::tests::rebuild_projection_job_saves_view_and_marks_projection_rebuilt` verifies rebuild writes projection and member-summary view state only and leaves the persisted member unchanged.
- `in_memory::tests::refresh_reference_job_by_owner_uses_loaded_bundle_version_for_state_and_sidecars` and `reference_refresh_preserves_last_good_snapshot` verify refresh mutates only reference bundle state plus sidecars and preserves the last good snapshot on unavailable outcomes.
- `in_memory::tests::reconciliation_job_missing_inspection_context_returns_partial_report_only` and `reconciliation_job_rejects_forbidden_finding_material` verify reconciliation emits report-only material or safe rejection and never repairs business truth.

## Replay And Consistency Audit

- `in_memory::tests::rebuild_projection_job_duplicate_replay_returns_typed_output_without_body_rerun` verifies the maintenance duplicate path returns the stored typed output and stored report surface instead of rerunning the job body.
- `in_memory::tests::job_duplicate_replays_stored_report` and `job_dispatch_duplicate_replay_does_not_run_handler` verify duplicate job dispatch reuses the stored report surface and does not re-expand targets or re-enter handler logic.
- `in_memory::tests::job_dispatch_replay_missing_report_is_consistency_defect` and `job_dispatch_replay_wrong_stored_kind_is_consistency_defect` verify replay defects fail closed instead of reconstructing from current state.

## Body-Free And Report-Only Audit

- `in_memory::tests::projection_rebuild_race_preserves_newer_state` shows rebuild conflict handling stays on formal state and cursor refs rather than hidden repair logic.
- `in_memory::tests::reference_refresh_preserves_last_good_snapshot` and `reconciliation_job_rejects_forbidden_finding_material` verify unavailable or forbidden branches keep only safe refs or issue markers and never persist raw external body.
- `in_memory::tests::job_dispatch_saves_report_before_idempotency_completion` verifies the replayable report is persisted before idempotency completion becomes visible, so completed replay never points at a missing report surface.

## Evidence

- suite raw artifact: `artifacts/test/20260618T101421+0800/suites/operations-replay-core/report.json`
- source commits: `artifacts/test/20260618T101421+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260618T101421+0800/meta/config-digest.json`
- case artifacts: `artifacts/test/20260618T101421+0800/suites/operations-replay-core/cases/*.json`

## Verification

- `cargo fmt --all` passed
- `cargo check -p identity-contracts -p identity-domain -p identity-application -p identity-infra` passed
- `cargo test -p identity-infra` passed
