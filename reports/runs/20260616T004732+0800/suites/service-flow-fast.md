# service-flow-fast

- run_id: `20260616T004732+0800`
- gate: `GATE-04`
- commit boundary: `commit-04-c`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-CMD-001`; `EV-ID-IDEMP-001`

## Scope

- Covered only `TC-ID-CMD-011~015` for design baseline `2f0bfed`.
- `PrepareTraceHandoff` saves pending intent only and keeps `effect.outbox_refs = []`.
- Duplicate replay stays on stored accepted envelope and never calls delivery from the command path.

## Cases

- `TC-ID-CMD-011`: PrepareTraceHandoff accepted pending intent only
- `TC-ID-CMD-012`: PrepareTraceHandoff empty trace rejected
- `TC-ID-CMD-013`: duplicate same digest replays stored accepted result
- `TC-ID-CMD-014`: same key different digest returns formal duplicate conflict
- `TC-ID-CMD-015`: command rollback/conflict subset keeps truth, stored result, and idempotency completion consistent

## Evidence

- suite raw artifact: `artifacts/test/20260616T004732+0800/suites/service-flow-fast/report.json`
- source commits: `artifacts/test/20260616T004732+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260616T004732+0800/meta/config-digest.json`

## Verification

- `cargo fmt --all` passed
- `cargo check -p identity-contracts` passed
- `cargo check -p identity-domain` passed
- `cargo check -p identity-application` passed
- `cargo check -p identity-infra` passed
- `cargo test -p identity-contracts command_trace_handoff_dtos_roundtrip -- --nocapture` passed
- `cargo test -p identity-domain handoff -- --nocapture` passed
- `cargo test -p identity-infra prepare_trace_handoff -- --nocapture` passed
- `cargo test -p identity-infra handoff_delivered_requires_formal_receipt -- --nocapture` passed
