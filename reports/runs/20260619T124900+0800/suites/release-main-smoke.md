# release-main-smoke

- run_id: `20260619T124900+0800`
- suite status: `passed`
- config profile: `release-candidate`
- started_at: `2026-06-19T04:21:24Z`
- finished_at: `2026-06-19T04:21:44Z`
- duration_ms: `20000`

## Cases

- `core-001-release-scenario-closure`: status=`passed`; tc_refs=TC-ID-CMD-001,TC-ID-QUERY-001,TC-ID-OUTBOX-001,TC-ID-JOB-001,TC-ID-CONFIG-001,TC-ID-REDACTION-001; evidence_refs=EV-ID-CORE-001

## Assertions

- `release.scenario.identity_closure.sample`: passed - The sample release scenario links establish, read, propagation, job replay, config, and redaction evidence through a single run-scoped closure.
- `release.scenario.sample_only.not_final_acceptance`: passed - This sample raw release suite exists only to validate report tooling and does not assert final release acceptance, veto signoff, or handoff review.

## Raw Artifacts

- suite raw artifact: `artifacts/test/20260619T124900+0800/suites/release-main-smoke/report.json`
- case artifacts: `artifacts/test/20260619T124900+0800/suites/release-main-smoke/cases/*.json`
- stdout digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- stderr digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
