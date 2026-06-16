# service-flow-fast

- run_id: `20260616T200831+0800`
- gate: `GATE-05`
- commit boundary: `commit-05-b`
- design baseline: `f91e72c`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-QUERY-001`; shared no-write audit includes `EV-ID-STATE-001`

## Scope

- Covered only core truth, member summary, trace, and audit query family evidence for `TC-ID-QUERY-001~008` and the shared no-write audit `TC-ID-QUERY-015`.
- This run does not claim `TC-ID-QUERY-009~014`; projection/reference/report/outbox/handoff operations reads remain reserved for `commit-05-c` by design baseline `f91e72c`.
- Query verification stays inside visibility-first resolution, stable lookup, degraded/redaction surface handling, and read-only execution.

## Cases

- `TC-ID-QUERY-001`: missing member anchor returns the formal missing surface and does not create truth
- `TC-ID-QUERY-002`: lifecycle summary reads return the formal lifecycle state through a read-only query surface
- `TC-ID-QUERY-003`: role capability summary reads stay body-free and visibility-first
- `TC-ID-QUERY-004`: career list reads return member-scoped body-free records without repair
- `TC-ID-QUERY-005`: memory reference list reads stay body-free and do not mutate reference state
- `TC-ID-QUERY-006`: stale or degraded member summary views missing freshness markers return degraded material through the dedicated mapper path
- `TC-ID-QUERY-007`: trace reads copy page access for empty and first-missing branches and redact restricted item fields without leaking raw trace bodies
- `TC-ID-QUERY-008`: audit trail reads use the formal member canonical audit subject and stay read-only
- `TC-ID-QUERY-015`: representative core/member/trace/audit queries keep active write transactions and staged writes at zero

## Write Audit

- No query flow in this run opened a write UoW, reserved idempotency, wrote stored results, or repaired projection/reference/report/outbox/handoff state.
- The no-write assertion is backed by targeted infra tests that verify `active_write_transactions() == 0` and `staged_write_count() == 0` before and after representative queries.

## Redaction Audit

- Trace redaction-sensitive branches are covered in the service-flow suite through `read_identity_trace_by_subject_redacts_item_fields_and_copies_visibility_result`.
- Separate `redaction-check.md` is `not_applicable` for this boundary because `commit-05-b` has no formal redaction report writer in the implementation repo and the boundary ledger marks the report optional when the writer is unavailable.
- Evidence source: `projects/L1-identity/design-calibration/implementation-boundaries/commit-05-b.md` required checks row `redaction audit` and `report evidence`.

## Evidence

- suite raw artifact: `artifacts/test/20260616T200831+0800/suites/service-flow-fast/report.json`
- source commits: `artifacts/test/20260616T200831+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260616T200831+0800/meta/config-digest.json`
- case artifacts: `artifacts/test/20260616T200831+0800/suites/service-flow-fast/cases/*.json`
- optional redaction report: `not_applicable` for this run because no formal `commit-05-b` redaction writer exists in the implementation repo

## Verification

- `cargo fmt --all` passed
- `cargo check -p identity-contracts` passed
- `cargo check -p identity-domain` passed
- `cargo check -p identity-application` passed
- `cargo check -p identity-infra` passed
- `cargo test -p identity-infra --lib` passed
- Targeted suite coverage within the passing infra run includes `get_global_member_anchor_missing_returns_missing_without_create`, `core_member_queries_return_body_free_material_without_write`, `read_member_summary_missing_freshness_returns_material_degraded_surface`, `read_identity_trace_by_member_empty_copies_page_access_without_write`, `read_identity_trace_by_member_first_missing_uses_page_access_degradation`, `read_identity_trace_by_subject_redacts_item_fields_and_copies_visibility_result`, and `read_audit_trail_uses_member_canonical_subject_and_stays_read_only`.