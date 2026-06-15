# service-flow-fast

- run_id: `20260615T211637+0800`
- gate: `GATE-04`
- commit boundary: `commit-04-b`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-CMD-001`

## Scope

- Covered only `TC-ID-CMD-005~010` for design baseline `d9f9e71`.
- Did not claim `TC-ID-CMD-011~015`; those remain in `commit-04-c`.
- Related duplicate replay and stored-result invariants remain sourced from prior `GATE-03` evidence and are not re-claimed in this run.

## Cases

- `TC-ID-CMD-005`: MaintainRoleCapabilitySummary accepted
- `TC-ID-CMD-006`: MaintainRoleCapabilitySummary unavailable source rejected/degraded
- `TC-ID-CMD-007`: AppendCareerRecord accepted
- `TC-ID-CMD-008`: AppendCareerRecord duplicate source noop/conflict
- `TC-ID-CMD-009`: MaintainMemoryReference accepted
- `TC-ID-CMD-010`: MaintainMemoryReference forbidden body rejected

## Evidence

- suite raw artifact: `artifacts/test/20260615T211637+0800/suites/service-flow-fast/report.json`
- source commits: `artifacts/test/20260615T211637+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260615T211637+0800/meta/config-digest.json`

## Verification

- `cargo fmt --all --check` passed
- `cargo check -p identity-contracts` passed
- `cargo check -p identity-domain` passed
- `cargo check -p identity-application` passed
- `cargo check -p identity-infra` passed
- `cargo test -p identity-infra` passed
