# config-redline

- run_id: `20260618T233256+0800`
- gate: `GATE-09`
- commit boundary: `commit-08-a`
- design baseline: `3e981ab`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-CONFIG-001`

## Scope

- Covered only the `commit-08-a` runtime config and redline subset: strict JSON parsing, source overlay precedence, formal profile validation, disabled adapter visibility, and topic or target completeness failure handling.
- Did not claim PH-08-b or PH-08-c script generation, report writer automation, release smoke, or acceptance handoff outputs.
- Runtime config evidence stays on formal startup config loading and runtime builder availability surfaces; it does not introduce ad hoc config keys, fallback env flags, or hidden default topics or targets.

## Cases

- `TC-ID-CONFIG-001`: formal profile matrix validates `local-dev`, `ci-test`, `integration-like`, and `operations-replay` only when each profile satisfies its declared constraints.
- `TC-ID-CONFIG-002`: malformed JSON, redline violations, and missing required adapter inputs fail closed before a dispatchable runtime surface is exposed.
- `TC-ID-CONFIG-003`: disabled optional adapters surface formal `Disabled` or degraded runtime availability instead of fake success.
- `TC-ID-CONFIG-004`: enabled publisher topic completeness and missing trace handoff target handling stay fail-fast or disabled; no fallback binding is invented.

## Validation Audit

- `config::tests::formal_profile_matrix_loads_under_formal_constraints` verifies the formal runtime profile set and the `operations-replay` replay-root requirements.
- `config::tests::strict_json_rejects_comments_and_trailing_commas`, `strict_json_rejects_duplicate_keys`, `environment_invalid_does_not_fall_back_to_defaults`, and `redline_false_fails_validation` verify malformed or unsafe config never assembles into a dispatchable runtime.
- `config::tests::enabled_publisher_without_topic_map_fails_validation` verifies an enabled publisher cannot proceed without a formal topic map binding.
- `runtime::tests::builder_publishes_degraded_state_when_optional_adapters_are_disabled`, `disabled_adapter_is_visible_in_runtime_registry`, and `durable_store_mode_fails_builder` verify disabled adapters and unwired durable store paths stay explicit and fail closed.

## Body-Free Audit

- The suite artifacts carry only safe issue refs, profile names, and boundary-safe summaries. No raw env dump, secret, request body, or adapter payload is recorded in this run.
- The stdout and stderr artifacts contain no payload, secret, or config body material.

## Evidence

- suite raw artifact: `artifacts/test/20260618T233256+0800/suites/config-redline/report.json`
- source commits: `artifacts/test/20260618T233256+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260618T233256+0800/meta/config-digest.json`
- case artifacts: `artifacts/test/20260618T233256+0800/suites/config-redline/cases/*.json`

## Verification

- `cargo fmt --all` passed
- `cargo check -p identity-infra` passed
- `cargo test -p identity-infra` passed

