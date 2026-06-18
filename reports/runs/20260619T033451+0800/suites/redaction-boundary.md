# redaction-boundary

- run_id: `20260619T033451+0800`
- suite status: `passed`
- config profile: `ci-test`
- started_at: `2026-06-18T19:36:17Z`
- finished_at: `2026-06-18T19:36:22Z`
- duration_ms: `5000`

## Cases

- `redaction-001-forbidden-material-scan`: status=`passed`; tc_refs=TC-ID-CONTRACT-004,TC-ID-CMD-010,TC-ID-REDACTION-001,TC-ID-REDACTION-002,TC-ID-REDACTION-003; evidence_refs=EV-ID-REDACTION-001

## Assertions

- `redaction.artifacts.reports.clean`: passed - Artifacts, suite reports, and review-facing report shells stay clean of raw secrets, raw external bodies, and full sensitive refs.
- `redaction.safe_refs.only`: passed - The evidence surfaces keep only safe refs, safe summaries, and redacted diagnostic identifiers.

## Raw Artifacts

- suite raw artifact: `artifacts/test/20260619T033451+0800/suites/redaction-boundary/report.json`
- case artifacts: `artifacts/test/20260619T033451+0800/suites/redaction-boundary/cases/*.json`
- stdout digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- stderr digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
