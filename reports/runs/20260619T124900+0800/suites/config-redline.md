# config-redline

- run_id: `20260619T124900+0800`
- suite status: `passed`
- config profile: `ci-test`
- started_at: `2026-06-19T04:21:07Z`
- finished_at: `2026-06-19T04:21:12Z`
- duration_ms: `5000`

## Cases

- `config-001-formal-profile-matrix`: status=`passed`; tc_refs=TC-ID-CONFIG-001,TC-ID-CONFIG-002,TC-ID-CONFIG-003,TC-ID-CONFIG-004; evidence_refs=EV-ID-CONFIG-001

## Assertions

- `config.profile.matrix.validates.formal_profiles`: passed - The formal profile matrix accepts only the declared profile set and fails closed when required runtime fields are missing.
- `config.disabled.adapter.never_fakes_success`: passed - Disabled adapters fail explicitly and do not masquerade as healthy or successful runtime bindings.

## Raw Artifacts

- suite raw artifact: `artifacts/test/20260619T124900+0800/suites/config-redline/report.json`
- case artifacts: `artifacts/test/20260619T124900+0800/suites/config-redline/cases/*.json`
- stdout digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- stderr digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
