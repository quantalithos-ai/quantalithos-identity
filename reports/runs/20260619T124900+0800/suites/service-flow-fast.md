# service-flow-fast

- run_id: `20260619T124900+0800`
- suite status: `passed`
- config profile: `ci-test`
- started_at: `2026-06-19T04:20:11Z`
- finished_at: `2026-06-19T04:20:28Z`
- duration_ms: `17000`

## Cases

- `cmd-001-command-accepted-and-replay`: status=`passed`; tc_refs=TC-ID-CMD-001,TC-ID-CMD-002,TC-ID-CMD-003,TC-ID-CMD-004,TC-ID-CMD-005,TC-ID-CMD-006,TC-ID-CMD-007,TC-ID-CMD-008,TC-ID-CMD-009,TC-ID-CMD-010,TC-ID-CMD-011,TC-ID-CMD-012,TC-ID-CMD-013,TC-ID-CMD-014,TC-ID-CMD-015; evidence_refs=EV-ID-CMD-001
- `query-001-visibility-first-no-write`: status=`passed`; tc_refs=TC-ID-QUERY-001,TC-ID-QUERY-002,TC-ID-QUERY-003,TC-ID-QUERY-004,TC-ID-QUERY-005,TC-ID-QUERY-006,TC-ID-QUERY-007,TC-ID-QUERY-008,TC-ID-QUERY-009,TC-ID-QUERY-010,TC-ID-QUERY-011,TC-ID-QUERY-012,TC-ID-QUERY-013,TC-ID-QUERY-014,TC-ID-QUERY-015; evidence_refs=EV-ID-QUERY-001

## Assertions

- `command.accepted.same_uow.side_effects`: passed - The accepted command paths keep truth, trace, outbox, stale markers, and stored result material in the same unit of work.
- `command.duplicate.replays.stored_result_only`: passed - Same-key same-digest duplicates replay stored accepted or rejected shells and do not rerun mutation or outbox append.
- `query.visibility.first.no_write`: passed - The query flows resolve visibility before loading material and never write truth, projection repair, or stored result state.
- `query.degraded.summary.copies.formal_markers`: passed - The degraded and stale query surfaces copy formal markers and kinds instead of synthesizing them from strings or diagnostics.

## Raw Artifacts

- suite raw artifact: `artifacts/test/20260619T124900+0800/suites/service-flow-fast/report.json`
- case artifacts: `artifacts/test/20260619T124900+0800/suites/service-flow-fast/cases/*.json`
- stdout digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- stderr digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
