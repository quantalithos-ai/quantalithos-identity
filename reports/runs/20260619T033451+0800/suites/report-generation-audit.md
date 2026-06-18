# report-generation-audit

- run_id: `20260619T033451+0800`
- suite status: `passed`
- config profile: `ci-test`
- started_at: `2026-06-18T19:36:44Z`
- finished_at: `2026-06-18T19:36:52Z`
- duration_ms: `8000`

## Cases

- `report-001-pairing-and-no-static-evidence`: status=`passed`; tc_refs=TC-ID-CONTRACT-001,TC-ID-CMD-001,TC-ID-QUERY-001,TC-ID-CONFIG-001,TC-ID-ARCH-001,TC-ID-REDACTION-001; evidence_refs=EV-ID-REPORT-001

## Assertions

- `report.audit.pairs.raw_artifacts_and_reports`: passed - Every blocking suite in the release run pairs a run-scoped raw artifact with its generated report, handoff, and review paths.
- `report.audit.rejects_static_pass`: passed - The final release evidence is derived from raw artifacts and generated reports instead of hand-written pass-only acceptance material.

## Raw Artifacts

- suite raw artifact: `artifacts/test/20260619T033451+0800/suites/report-generation-audit/report.json`
- case artifacts: `artifacts/test/20260619T033451+0800/suites/report-generation-audit/cases/*.json`
- stdout digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- stderr digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
