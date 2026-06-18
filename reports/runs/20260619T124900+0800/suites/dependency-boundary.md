# dependency-boundary

- run_id: `20260619T124900+0800`
- suite status: `passed`
- config profile: `ci-test`
- started_at: `2026-06-19T04:21:13Z`
- finished_at: `2026-06-19T04:21:17Z`
- duration_ms: `4000`

## Cases

- `arch-001-entry-facade-dependency-boundary`: status=`passed`; tc_refs=TC-ID-ARCH-001; evidence_refs=EV-ID-ARCH-001

## Assertions

- `dependency.normal.deps.limited_to_core_and_public_shells`: passed - The workspace normal dependencies stay within core-contracts and the declared identity crate layering without a sibling business compile-time loop.
- `dependency.entry.runtime.infra.used_through_formal_surfaces`: passed - Entry and runtime code keep repo and adapter collaboration behind formal application or infra seams instead of importing sibling business truth directly.

## Raw Artifacts

- suite raw artifact: `artifacts/test/20260619T124900+0800/suites/dependency-boundary/report.json`
- case artifacts: `artifacts/test/20260619T124900+0800/suites/dependency-boundary/cases/*.json`
- stdout digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- stderr digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
