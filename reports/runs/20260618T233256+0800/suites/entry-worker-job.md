# entry-worker-job

- run_id: `20260618T233256+0800`
- gate: `GATE-06`; `GATE-08` entry subset
- commit boundary: `commit-08-a`
- design baseline: `3e981ab`
- config profile: `ci-test`
- suite status: `passed`
- evidence: `EV-ID-CONSUMER-001`; `EV-ID-JOB-001`

## Scope

- Covered only the `commit-08-a` entry subset: API command pre-dispatch validation, worker consumer envelope dispatch, jobs entry scope validation, formal request digest or metadata derivation, and facade-only forwarding.
- Did not claim consumer mutation semantics, callback mutation flows, maintenance or propagation job bodies, duplicate replay store scans, runner or CLI wiring, or any PH-08-b or PH-08-c script or report writer behavior.
- Entry wiring stays constrained to `IdentityApplicationFacade` plus the formal shared ports and runtime assembly state. No entry crate reaches repositories, resolvers, publishers, handoff adapters, or unit-of-work internals directly.

## Cases

- `TC-ID-CONSUMER-001`: a valid role-capability source change envelope dispatches through the worker entry facade path, and binding mismatches are rejected before any consumer write path begins.
- `TC-ID-JOB-007`: malformed job scope metadata is rejected before report creation or write path start, while valid job requests still dispatch through the application facade only.

## Entry Validation Audit

- `api::tests::command_entry_dispatches_through_the_application_facade` and `command_entry_rejects_missing_idempotency_before_dispatch` verify API command entry dispatch and pre-dispatch rejection stay on formal body-free request metadata and idempotency guards.
- `worker::tests::worker_entry_dispatches_inbound_events_through_the_facade` and `worker_entry_rejects_binding_mismatch_before_dispatch` verify worker entry derives metadata from formal envelope markers and blocks invalid binding metadata before any consumer mutation path.
- `jobs::tests::jobs_entry_dispatches_through_the_application_facade` and `jobs_entry_rejects_scope_mismatch_before_dispatch` verify job entry dispatch stays facade-only and rejects invalid scope metadata before report or stored replay paths can start.

## Facade Boundary Audit

- The API, worker, and jobs adapters are constructed only from `IdentityApplicationFacade`, runtime assembly state, clock, id generator, operation-context factory, and dispatch target catalog ports.
- Request digests and metadata refs are derived from formal public markers or canonical body-free entry material. No adapter reconstructs digests from current truth, repository state, or raw payload body.
- The stdout and stderr artifacts contain no request body, payload body, config body, secret, or adapter diagnostic material.

## Evidence

- suite raw artifact: `artifacts/test/20260618T233256+0800/suites/entry-worker-job/report.json`
- source commits: `artifacts/test/20260618T233256+0800/meta/source-commits.json`
- config digest: `artifacts/test/20260618T233256+0800/meta/config-digest.json`
- case artifacts: `artifacts/test/20260618T233256+0800/suites/entry-worker-job/cases/*.json`

## Verification

- `cargo fmt --all` passed
- `cargo check -p identity-application -p identity-api -p identity-worker -p identity-jobs -p identity-infra` passed
- `cargo test -p identity-application -p identity-api -p identity-worker -p identity-jobs -p identity-infra` passed

