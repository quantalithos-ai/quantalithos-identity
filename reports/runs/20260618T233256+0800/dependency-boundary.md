# dependency-boundary

- run_id: `20260618T233256+0800`
- gate: `GATE-01`
- commit boundary: `commit-08-a`
- design baseline: `3e981ab`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-ARCH-001`

## Scope

- Covered the `commit-08-a` entry dependency boundary for `identity-api`, `identity-worker`, and `identity-jobs`.
- Audited normal compile dependencies and the workspace path boundary introduced by the new entry crates and runtime-config support.
- Did not claim release script wiring, report writer dependencies, or non-entry runtime bootstrap binaries beyond the current PH-08-a scope.

## Cases

- `TC-ID-ARCH-001`: the entry crates keep normal compile dependencies on public shells only and do not create a sibling business implementation path dependency beyond allowed core contracts.

## Dependency Audit

- Workspace `Cargo.toml` keeps the only sibling path dependency on `../quantalithos-core/crates/contracts` as `core-contracts`.
- `cargo tree -p identity-api --depth 2`, `cargo tree -p identity-worker --depth 2`, and `cargo tree -p identity-jobs --depth 2` show the new entry crates depend normally on `core-contracts`, `identity-application`, and `identity-contracts` only.
- `identity-infra` remains a dev-dependency for entry tests, so entry runtime parity does not become a normal dependency loop.
- Entry adapters are constructed from the application facade plus shared ports for clock, id generation, operation context, runtime state, and dispatch catalog only; they do not link repositories, unit-of-work managers, publishers, or handoff adapters.

## Evidence

- suite raw artifact: `artifacts/test/20260618T233256+0800/suites/dependency-boundary/report.json`
- source commits: `artifacts/test/20260618T233256+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260618T233256+0800/meta/config-digest.json`
- case artifact: `artifacts/test/20260618T233256+0800/suites/dependency-boundary/cases/arch-001-entry-facade-dependency-boundary.json`

## Verification

- `cargo check -p identity-application -p identity-api -p identity-worker -p identity-jobs -p identity-infra` passed
- `cargo tree -p identity-api --depth 2` passed
- `cargo tree -p identity-worker --depth 2` passed
- `cargo tree -p identity-jobs --depth 2` passed

