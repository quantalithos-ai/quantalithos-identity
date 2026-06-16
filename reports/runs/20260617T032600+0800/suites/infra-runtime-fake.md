# infra-runtime-fake

- run_id: `20260617T032600+0800`
- gate: `GATE-03` subset
- commit boundary: `commit-06-b`
- design baseline: `4a18d7a`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-IDEMP-001`

## Scope

- Covered the commit-06-b fake/runtime parity required for typed consumer and callback receipt replay, marker subject mapping, and same-UoW storage ordering.
- Did not claim worker entry dispatch wiring, transport retry loops, outbound material creation, publish/deliver/retry jobs, or any durable adapter implementation beyond the formal in-memory fake surface.

## Cases

- `TC-ID-IDEMP-003`: consumer and callback duplicates replay the stored typed receipt envelope and do not rerun mutation, trace append, outbox append, or handoff state transition

## Replay Audit

- `in_memory::tests::handle_role_capability_source_changed_accepts_and_replays` verifies duplicate role-source delivery reuses the stored `ConsumerReceipt` envelope and keeps trace count at one.
- `in_memory::tests::handle_trace_handoff_result_delivered_replays_stored_receipt` verifies duplicate callback replay reuses the stored `HandoffCallbackReceipt` envelope and keeps the handoff intent version, trace count, and outbox count unchanged.
- Shared scaffold replay tests remain in the same passing run to confirm the fake runtime still distinguishes normal consumer receipts from callback receipts before any payload-specific handler executes.

## Evidence

- suite raw artifact: `artifacts/test/20260617T032600+0800/suites/infra-runtime-fake/report.json`
- source commits: `artifacts/test/20260617T032600+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260617T032600+0800/meta/config-digest.json`
- case artifacts: `artifacts/test/20260617T032600+0800/suites/infra-runtime-fake/cases/*.json`

## Verification

- `cargo fmt --all` passed
- `cargo check -p identity-application` passed
- `cargo check -p identity-infra` passed
- `cargo test -p identity-infra` passed
