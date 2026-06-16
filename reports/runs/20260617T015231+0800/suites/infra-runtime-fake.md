# infra-runtime-fake

- run_id: `20260617T015231+0800`
- gate: `GATE-03` subset
- commit boundary: `commit-06-a`
- design baseline: `0f4d7a4`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-IDEMP-001`

## Scope

- Covered only consumer and callback typed receipt save/get, duplicate replay, and unsupported-schema replay persistence for `TC-ID-IDEMP-003`, plus the related body-free receipt contract checks.
- Did not claim command/job replay breadth from `commit-03-c`, commit-unknown recovery, or any concrete inbound/callback mutation beyond the shared scaffold.
- Replay coverage in this run is limited to the formal stored shell plus typed receipt envelope path; it does not use current truth reconstruction or payload reparse.

## Cases

- `TC-ID-IDEMP-003`: consumer/callback duplicate replay uses typed receipt envelopes as the only replay source and persists unsupported-schema replay surfaces before re-entry

## Evidence

- suite raw artifact: `artifacts/test/20260617T015231+0800/suites/infra-runtime-fake/report.json`
- source commits: `artifacts/test/20260617T015231+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260617T015231+0800/meta/config-digest.json`
- case artifact: `artifacts/test/20260617T015231+0800/suites/infra-runtime-fake/cases/idemp-003-consumer-receipt-typed-replay.json`

## Verification

- `cargo fmt --all` passed
- `cargo check -p identity-application` passed
- `cargo check -p identity-infra` passed
- `cargo test -p identity-infra` passed
- Targeted infra coverage in this run includes `consumer_duplicate_replays_typed_receipt`, `inbound_consumer_scaffold_duplicate_replays_without_running_handler`, `unsupported_schema_scaffold_returns_fresh_receipt_and_persists_replay_surface`, and `callback_scaffold_duplicate_replays_handoff_callback_receipt_without_handler`.
