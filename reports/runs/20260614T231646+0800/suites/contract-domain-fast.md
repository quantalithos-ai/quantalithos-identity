# contract-domain-fast

- run_id: `20260614T231646+0800`
- gate: `GATE-02`
- commit boundary: `commit-02-c`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-STATE-001`

## Scope

- Covered only `TC-ID-STATE-001~002` per design baseline `9ae0569`.
- Did not claim `TC-ID-OUTBOX-*` or `TC-ID-JOB-*`; those remain later operations evidence families.

## Cases

- `TC-ID-STATE-001`: Projection / reference / report no repair
- `TC-ID-STATE-002`: outbox and handoff terminal guards

## Evidence

- suite raw artifact: `artifacts/test/20260614T231646+0800/suites/contract-domain-fast/report.json`
- source commits: `artifacts/test/20260614T231646+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260614T231646+0800/meta/config-digest.json`

## Verification

- `cargo fmt --all --check` passed
- `cargo check -p identity-contracts` passed
- `cargo check -p identity-domain` passed
- `cargo test -p identity-contracts` passed
- `cargo test -p identity-domain` passed
