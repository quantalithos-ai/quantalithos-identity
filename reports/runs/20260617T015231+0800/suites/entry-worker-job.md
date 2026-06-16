# entry-worker-job

- run_id: `20260617T015231+0800`
- gate: `GATE-06`
- commit boundary: `commit-06-a`
- design baseline: `0f4d7a4`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-CONSUMER-001`

## Scope

- Covered only the shared consumer and callback scaffold for unsupported-schema receipt assembly, typed duplicate replay, callback replay kind isolation, and body-free public receipt surfaces.
- Did not claim concrete role/work/memory/archive/trace mutation handling, accepted outbound material creation, publish/deliver/retry jobs, or worker entry wiring.
- Unsupported-schema coverage in this run begins after safe envelope metadata is already available to the application scaffold; transport-level raw envelope parsing remains outside this boundary.

## Cases

- `TC-ID-CONSUMER-006`: shared consumer and callback scaffold returns body-free unsupported receipts, loads stored typed replay envelopes, and keeps callback replay on the formal `HandoffCallbackReceipt` kind

## Replay Audit

- Duplicate replay assertions are backed by `in_memory::tests::inbound_consumer_scaffold_duplicate_replays_without_running_handler` and `in_memory::tests::callback_scaffold_duplicate_replays_handoff_callback_receipt_without_handler`.
- Unsupported-schema assertions are backed by `in_memory::tests::unsupported_schema_scaffold_returns_fresh_receipt_and_persists_replay_surface`, which verifies the handler is not invoked and the next delivery replays from stored typed state.

## Body-Free Audit

- The stored shell, typed receipt envelope, and public receipt assertions keep only refs and safe issue markers; they do not store raw payloads, callback bodies, archive packages, or adapter diagnostics.
- Context/channel alignment is covered by `consumer::tests::inbound_context_must_match_public_envelope` and `consumer::tests::callback_context_must_match_public_envelope`.

## Evidence

- suite raw artifact: `artifacts/test/20260617T015231+0800/suites/entry-worker-job/report.json`
- source commits: `artifacts/test/20260617T015231+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260617T015231+0800/meta/config-digest.json`
- case artifact: `artifacts/test/20260617T015231+0800/suites/entry-worker-job/cases/consumer-006-shared-scaffold-and-replay.json`

## Verification

- `cargo fmt --all` passed
- `cargo check -p identity-application` passed
- `cargo check -p identity-infra` passed
- `cargo test -p identity-application` passed
- `cargo test -p identity-infra` passed
