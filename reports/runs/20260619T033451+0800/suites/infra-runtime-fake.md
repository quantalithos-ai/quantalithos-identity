# infra-runtime-fake

- run_id: `20260619T033451+0800`
- suite status: `passed`
- config profile: `ci-test`
- started_at: `2026-06-18T19:35:28Z`
- finished_at: `2026-06-18T19:35:37Z`
- duration_ms: `9000`

## Cases

- `idemp-001-stored-replay-and-fake-parity`: status=`passed`; tc_refs=TC-ID-IDEMP-001,TC-ID-IDEMP-002,TC-ID-IDEMP-003,TC-ID-IDEMP-004,TC-ID-IDEMP-005,TC-ID-IDEMP-006,TC-ID-IDEMP-007,TC-ID-IDEMP-008,TC-ID-IDEMP-009,TC-ID-IDEMP-010,TC-ID-IDEMP-011; evidence_refs=EV-ID-IDEMP-001

## Assertions

- `runtime.fake.replay.no_second_writer`: passed - The in-memory runtime preserves duplicate replay, in-flight guard, and stored result semantics without a second writer.
- `runtime.fake.controlled.outcomes.match_formal_ports`: passed - The fake runtime exposes the same formal issue refs and state surfaces as the declared ports instead of depending on private maps.

## Raw Artifacts

- suite raw artifact: `artifacts/test/20260619T033451+0800/suites/infra-runtime-fake/report.json`
- case artifacts: `artifacts/test/20260619T033451+0800/suites/infra-runtime-fake/cases/*.json`
- stdout digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- stderr digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
