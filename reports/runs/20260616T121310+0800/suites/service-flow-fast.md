# service-flow-fast

- run_id: `20260616T121310+0800`
- gate: `GATE-05`; `GATE-03` subset
- commit boundary: `commit-05-a`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-QUERY-001`; `EV-ID-STATE-001`; `EV-ID-IDEMP-001`

## Scope

- Covered only `TC-ID-QUERY-015`, related `TC-ID-STATE-001`, and the fake stable-lookup/no-write parity subset for design baseline `c48b462`.
- Query foundation stays inside visibility-first preflight, stable member-summary lookup, and fake no-write spy support.
- This boundary does not implement the full 14 query bodies and does not save visibility decisions, idempotency records, stored results, trace, audit, outbox, projection, reference, report, or handoff state from query paths.

## Cases

- `TC-ID-QUERY-015`: query context rejects write-channel input and query preflight copies read subject and scope from the formal visibility access summary
- `TC-ID-STATE-001`: stable lookup consumes the persisted `(member_ref, visibility_scope_ref)` index and surfaces scope mismatch as projection integrity failure
- Related fake lookup cases: fake runtime uses the formal lookup index and shows zero staged writes during query preflight

## Write Audit

- Formal write-audit artifact writer is not yet present in the implementation repo for `commit-05-a`.
- Boundary ledger allows `not_applicable` when the current implementation boundary cannot generate write-audit artifacts formally.
- Evidence source: `projects/L1-identity/design-calibration/implementation-boundaries/commit-05-a.md` required checks row `report evidence` and allowed reports row.

## Evidence

- suite raw artifact: `artifacts/test/20260616T121310+0800/suites/service-flow-fast/report.json`
- source commits: `artifacts/test/20260616T121310+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260616T121310+0800/meta/config-digest.json`
- write-audit artifact: `not_applicable` for this run because no formal writer exists in the current boundary implementation

## Verification

- `cargo fmt --all` passed
- `cargo check -p identity-contracts` passed
- `cargo check -p identity-application` passed
- `cargo check -p identity-infra` passed
- `cargo test -p identity-infra read_visibility_repository_returns_formal_scope -- --nocapture` passed
- `cargo test -p identity-infra query_context_assertion_rejects_write_channel -- --nocapture` passed
- `cargo test -p identity-infra member_summary_preflight -- --nocapture` passed
- `cargo test -p identity-application assert_query_context -- --nocapture` ran with zero matching tests
