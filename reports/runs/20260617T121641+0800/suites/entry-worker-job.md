# entry-worker-job

- run_id: `20260617T121641+0800`
- gate: `GATE-08`
- commit boundary: `commit-07-a`
- design baseline: `b92ded4`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-CONTRACT-001`

## Scope

- Covered only the commit-07-a job entry subset available in this boundary: application-facade dispatch, public context and idempotency alignment checks, and invalid-input rejection before any job write path begins.
- Did not claim any jobs crate runner, CLI parsing, scheduler, API or worker wiring, actual maintenance or propagation job bodies, or duplicate replay via repository scans.
- Dispatch stays constrained to `IdentityApplicationFacade::dispatch_job` forwarding into `IdentityJobService::dispatch_job_scaffold`; this boundary adds no direct repository, publisher, or handoff adapter execution path.

## Cases

- `TC-ID-JOB-007`: malformed job context, idempotency, channel, or run-ref metadata is rejected as `InvalidRequest` before unit-of-work start, idempotency reserve, report creation, or handler dispatch.

## Entry Validation Audit

- `jobs::tests::job_context_requires_matching_job_run_ref` verifies mismatched public request and operation context metadata fail the shared pre-dispatch validator.
- `IdentityJobService::dispatch_job_scaffold` invokes `assert_job_context()` before `clock.now()`, `begin()`, or idempotency reserve, so malformed requests stop before any report or stored replay surface can be written.

## Facade Boundary Audit

- `IdentityApplicationFacade::dispatch_job` only forwards to a configured shared job service and returns `InvalidRequest` when no job service is attached, preventing fallback scanning or ad-hoc execution.
- No `crates/jobs`, API, or worker entry wiring changed in this boundary, so there is still no direct entry path that could bypass the application facade or shared scaffold.

## Evidence

- suite raw artifact: `artifacts/test/20260617T121641+0800/suites/entry-worker-job/report.json`
- source commits: `artifacts/test/20260617T121641+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260617T121641+0800/meta/config-digest.json`
- case artifacts: `artifacts/test/20260617T121641+0800/suites/entry-worker-job/cases/*.json`

## Verification

- `cargo fmt --all` passed
- `cargo check -p identity-application` passed
- `cargo test -p identity-application jobs::tests` passed
- No runner, worker, or CLI entry code changed in this boundary; the entry subset is limited to facade and shared-service validation.
