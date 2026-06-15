# infra-runtime-fake

- run_id: `20260615T131333+0800`
- gate: `GATE-03`
- commit boundary: `commit-03-c`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-IDEMP-001`

## Scope

- Covered only `TC-ID-IDEMP-001~004`, `TC-ID-IDEMP-006`, and `TC-ID-IDEMP-008~011` per design baseline `3bee523`.
- Did not claim `TC-ID-IDEMP-005`; commit-unknown recovery remains a later service-level retry slice.
- Did not claim `TC-ID-IDEMP-007`; accepted command rollback after outbox append remains later command-flow evidence.

## Cases

- `TC-ID-IDEMP-001`: operation namespace isolation
- `TC-ID-IDEMP-002`: duplicate stored result missing no recompute
- `TC-ID-IDEMP-003`: consumer duplicate receipt replay
- `TC-ID-IDEMP-004`: job duplicate stored report replay
- `TC-ID-IDEMP-006`: stored result saved before idempotency complete
- `TC-ID-IDEMP-008`: projection rebuild race preserves newer state
- `TC-ID-IDEMP-009`: reference refresh preserves last good snapshot
- `TC-ID-IDEMP-010`: handoff delivered requires formal receipt
- `TC-ID-IDEMP-011`: rollback failure surfaces manual intervention

## Evidence

- suite raw artifact: `artifacts/test/20260615T131333+0800/suites/infra-runtime-fake/report.json`
- source commits: `artifacts/test/20260615T131333+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260615T131333+0800/meta/config-digest.json`

## Verification

- `cargo fmt --all --check` passed
- `cargo check -p identity-application` passed
- `cargo check -p identity-infra` passed
- `cargo test -p identity-infra` passed
