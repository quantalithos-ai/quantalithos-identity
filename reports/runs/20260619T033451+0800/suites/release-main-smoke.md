# release-main-smoke

- run_id: `20260619T033451+0800`
- suite status: `passed`
- config profile: `release-candidate`
- started_at: `2026-06-18T19:36:23Z`
- finished_at: `2026-06-18T19:36:43Z`
- duration_ms: `20000`

## Cases

- `core-001-release-scenario-closure`: status=`passed`; tc_refs=TC-ID-CMD-001,TC-ID-QUERY-001,TC-ID-OUTBOX-001,TC-ID-JOB-001,TC-ID-CONFIG-001,TC-ID-REDACTION-001; evidence_refs=EV-ID-CORE-001

## Assertions

- `release.scenario.identity_closure.final_gate`: passed - The release scenario closes establish, read, propagation, job replay, config, and redaction checks through a single run-scoped release gate.
- `release.scenario.acceptance.inputs.generated`: passed - The release suite feeds final acceptance inputs only through generated run-scoped artifacts, reports, handoff material, and veto review.

## Raw Artifacts

- suite raw artifact: `artifacts/test/20260619T033451+0800/suites/release-main-smoke/report.json`
- case artifacts: `artifacts/test/20260619T033451+0800/suites/release-main-smoke/cases/*.json`
- stdout digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- stderr digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
