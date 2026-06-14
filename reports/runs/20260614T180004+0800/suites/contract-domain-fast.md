# contract-domain-fast

- run_id: `20260614T180004+0800`
- gate: `GATE-02`
- commit boundary: `commit-02-b`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-STATE-001`

## Scope

- Covered only `TC-ID-DOMAIN-001~006` per design baseline `9ae0569`.
- Did not claim `TC-ID-STATE-001~002`; those belong to `commit-02-c`.

## Cases

- `TC-ID-DOMAIN-001`: GlobalMember establish invariant
- `TC-ID-DOMAIN-002`: Global lifecycle legal transitions
- `TC-ID-DOMAIN-003`: Global lifecycle illegal transitions
- `TC-ID-DOMAIN-004`: RoleCapabilitySummary source guard
- `TC-ID-DOMAIN-005`: CareerRecord append-only
- `TC-ID-DOMAIN-006`: MemoryReference state body-free

## Evidence

- suite raw artifact: `artifacts/test/20260614T180004+0800/suites/contract-domain-fast/report.json`
- source commits: `artifacts/test/20260614T180004+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260614T180004+0800/meta/config-digest.json`

## Verification

- `cargo fmt --all --check` passed
- `cargo check` passed
- `cargo test` passed
- `cargo test -p identity-domain -- --nocapture` passed
