# evidence-index

- run_id: `20260619T033451+0800`
- raw artifact: `artifacts/test/20260619T033451+0800/evidence-index.json`
- evidence item count: `14`

## EV-ID-CORE-001

- status: `passed`
- redaction_status: `clean`
- review_status: `reviewed`
- suite_refs: `release-main-smoke`
- tc_refs: `TC-ID-CMD-001,TC-ID-QUERY-001,TC-ID-OUTBOX-001,TC-ID-JOB-001,TC-ID-CONFIG-001,TC-ID-REDACTION-001`
- ac_refs: `AC-ID-001,AC-ID-002,AC-ID-003,AC-ID-004,AC-ID-005`
- veto_refs: `VETO-ID-001,VETO-ID-002,VETO-ID-003,VETO-ID-004,VETO-ID-005,VETO-ID-006`
- safe_summary: Release gate evidence is present for the current run and links command, query, outbox, job, config, and redaction coverage.
- artifact paths:
  - `artifacts/test/20260619T033451+0800/suites/release-main-smoke/report.json`
- artifact digests:
  - `sha256:a7b0069d0237869df5fa91d2a53d27bd8c7de6d35273901c4e81ba1569338189`
- report paths:
  - `reports/runs/20260619T033451+0800/suites/release-main-smoke.md`
- detail page: `reports/runs/20260619T033451+0800/evidence/EV-ID-CORE-001.md`

## EV-ID-CONTRACT-001

- status: `passed`
- redaction_status: `clean`
- review_status: `reviewed`
- suite_refs: `contract-domain-fast`
- tc_refs: `TC-ID-CONTRACT-001,TC-ID-CONTRACT-002,TC-ID-CONTRACT-003,TC-ID-CONTRACT-004`
- ac_refs: `AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-003`
- safe_summary: The contract-domain artifacts keep the public protocol shells round-trippable and body-free.
- artifact paths:
  - `artifacts/test/20260619T033451+0800/suites/contract-domain-fast/report.json`
- artifact digests:
  - `sha256:99499f92926e1c4d906775f379e8bc1030b712bb0d3334ba642ea3ec12ff7953`
- report paths:
  - `reports/runs/20260619T033451+0800/suites/contract-domain-fast.md`
- detail page: `reports/runs/20260619T033451+0800/evidence/EV-ID-CONTRACT-001.md`

## EV-ID-STATE-001

- status: `passed`
- redaction_status: `clean`
- review_status: `reviewed`
- suite_refs: `contract-domain-fast`
- tc_refs: `TC-ID-DOMAIN-001,TC-ID-DOMAIN-002,TC-ID-DOMAIN-003,TC-ID-DOMAIN-004,TC-ID-DOMAIN-005,TC-ID-DOMAIN-006,TC-ID-STATE-001,TC-ID-STATE-002`
- ac_refs: `AC-ID-001,AC-ID-002,AC-ID-003,AC-ID-004,AC-ID-005,AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014`
- veto_refs: `VETO-ID-001,VETO-ID-004`
- safe_summary: The domain and state artifacts keep legal transitions, terminal guards, append-only history, and stable refs aligned with the formal state matrix.
- artifact paths:
  - `artifacts/test/20260619T033451+0800/suites/contract-domain-fast/report.json`
- artifact digests:
  - `sha256:99499f92926e1c4d906775f379e8bc1030b712bb0d3334ba642ea3ec12ff7953`
- report paths:
  - `reports/runs/20260619T033451+0800/suites/contract-domain-fast.md`
- detail page: `reports/runs/20260619T033451+0800/evidence/EV-ID-STATE-001.md`

## EV-ID-CMD-001

- status: `passed`
- redaction_status: `clean`
- review_status: `reviewed`
- suite_refs: `service-flow-fast`
- tc_refs: `TC-ID-CMD-001,TC-ID-CMD-002,TC-ID-CMD-003,TC-ID-CMD-004,TC-ID-CMD-005,TC-ID-CMD-006,TC-ID-CMD-007,TC-ID-CMD-008,TC-ID-CMD-009,TC-ID-CMD-010,TC-ID-CMD-011,TC-ID-CMD-012,TC-ID-CMD-013,TC-ID-CMD-014,TC-ID-CMD-015`
- ac_refs: `AC-ID-001,AC-ID-002,AC-ID-003,AC-ID-004,AC-ID-005,AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-001,VETO-ID-002,VETO-ID-003,VETO-ID-004`
- safe_summary: The command service artifacts keep accepted, rejected, duplicate replay, and rollback surfaces tied to the formal command shell.
- artifact paths:
  - `artifacts/test/20260619T033451+0800/suites/service-flow-fast/report.json`
- artifact digests:
  - `sha256:4091483a0aa8400a1195367e18748e56de3b27531dcc39b917496e63aaa727f2`
- report paths:
  - `reports/runs/20260619T033451+0800/suites/service-flow-fast.md`
- detail page: `reports/runs/20260619T033451+0800/evidence/EV-ID-CMD-001.md`

## EV-ID-QUERY-001

- status: `passed`
- redaction_status: `clean`
- review_status: `reviewed`
- suite_refs: `service-flow-fast`
- tc_refs: `TC-ID-QUERY-001,TC-ID-QUERY-002,TC-ID-QUERY-003,TC-ID-QUERY-004,TC-ID-QUERY-005,TC-ID-QUERY-006,TC-ID-QUERY-007,TC-ID-QUERY-008,TC-ID-QUERY-009,TC-ID-QUERY-010,TC-ID-QUERY-011,TC-ID-QUERY-012,TC-ID-QUERY-013,TC-ID-QUERY-014,TC-ID-QUERY-015`
- ac_refs: `AC-ID-001,AC-ID-002,AC-ID-003,AC-ID-004,AC-ID-005,AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-002`
- safe_summary: The query service artifacts keep visibility-first, degraded, stale, and no-write behavior tied to the formal query shell.
- artifact paths:
  - `artifacts/test/20260619T033451+0800/suites/service-flow-fast/report.json`
- artifact digests:
  - `sha256:4091483a0aa8400a1195367e18748e56de3b27531dcc39b917496e63aaa727f2`
- report paths:
  - `reports/runs/20260619T033451+0800/suites/service-flow-fast.md`
- detail page: `reports/runs/20260619T033451+0800/evidence/EV-ID-QUERY-001.md`

## EV-ID-CONSUMER-001

- status: `passed`
- redaction_status: `clean`
- review_status: `reviewed`
- suite_refs: `entry-worker-job`
- tc_refs: `TC-ID-CONSUMER-001,TC-ID-CONSUMER-002,TC-ID-CONSUMER-003,TC-ID-CONSUMER-004,TC-ID-CONSUMER-005,TC-ID-CONSUMER-006`
- ac_refs: `AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-002,VETO-ID-003`
- safe_summary: The inbound and callback artifacts preserve typed receipt replay, no-create behavior, and body-free public receipts.
- artifact paths:
  - `artifacts/test/20260619T033451+0800/suites/entry-worker-job/report.json`
- artifact digests:
  - `sha256:c4fa8bf7ebc0d15e8801db87595858da0cc534eb1536cbd80e85c7ec0d039ba6`
- report paths:
  - `reports/runs/20260619T033451+0800/suites/entry-worker-job.md`
- detail page: `reports/runs/20260619T033451+0800/evidence/EV-ID-CONSUMER-001.md`

## EV-ID-OUTBOX-001

- status: `passed`
- redaction_status: `clean`
- review_status: `reviewed`
- suite_refs: `operations-replay-core`
- tc_refs: `TC-ID-OUTBOX-001,TC-ID-OUTBOX-002,TC-ID-OUTBOX-003,TC-ID-OUTBOX-004,TC-ID-OUTBOX-005,TC-ID-OUTBOX-006,TC-ID-OUTBOX-007,TC-ID-OUTBOX-008,TC-ID-OUTBOX-009,TC-ID-OUTBOX-010`
- ac_refs: `AC-ID-001,AC-ID-002,AC-ID-003,AC-ID-004,AC-ID-005,AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-003`
- safe_summary: The operations replay artifacts keep accepted-only outbox material body-free and preserve formal publish outcome markers.
- artifact paths:
  - `artifacts/test/20260619T033451+0800/suites/operations-replay-core/report.json`
- artifact digests:
  - `sha256:f46a0093633d29036099eda845c82f6e722018077d07a8f8a8e5c143aa6ad6d9`
- report paths:
  - `reports/runs/20260619T033451+0800/suites/operations-replay-core.md`
- detail page: `reports/runs/20260619T033451+0800/evidence/EV-ID-OUTBOX-001.md`

## EV-ID-JOB-001

- status: `passed`
- redaction_status: `clean`
- review_status: `reviewed`
- suite_refs: `entry-worker-job,operations-replay-core`
- tc_refs: `TC-ID-JOB-001,TC-ID-JOB-002,TC-ID-JOB-003,TC-ID-JOB-004,TC-ID-JOB-005,TC-ID-JOB-006,TC-ID-JOB-007,TC-ID-JOB-008`
- ac_refs: `AC-ID-001,AC-ID-002,AC-ID-003,AC-ID-004,AC-ID-005,AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-002,VETO-ID-005`
- safe_summary: The job entry and operations replay artifacts preserve report replay, partial failure accounting, and report-only no-repair semantics.
- artifact paths:
  - `artifacts/test/20260619T033451+0800/suites/entry-worker-job/report.json`
  - `artifacts/test/20260619T033451+0800/suites/operations-replay-core/report.json`
- artifact digests:
  - `sha256:c4fa8bf7ebc0d15e8801db87595858da0cc534eb1536cbd80e85c7ec0d039ba6`
  - `sha256:f46a0093633d29036099eda845c82f6e722018077d07a8f8a8e5c143aa6ad6d9`
- report paths:
  - `reports/runs/20260619T033451+0800/suites/entry-worker-job.md`
  - `reports/runs/20260619T033451+0800/suites/operations-replay-core.md`
- detail page: `reports/runs/20260619T033451+0800/evidence/EV-ID-JOB-001.md`

## EV-ID-IDEMP-001

- status: `passed`
- redaction_status: `clean`
- review_status: `reviewed`
- suite_refs: `infra-runtime-fake,operations-replay-core`
- tc_refs: `TC-ID-IDEMP-001,TC-ID-IDEMP-002,TC-ID-IDEMP-003,TC-ID-IDEMP-004,TC-ID-IDEMP-005,TC-ID-IDEMP-006,TC-ID-IDEMP-007,TC-ID-IDEMP-008,TC-ID-IDEMP-009,TC-ID-IDEMP-010,TC-ID-IDEMP-011`
- ac_refs: `AC-ID-009,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-001,VETO-ID-002,VETO-ID-005`
- safe_summary: The replay artifacts keep duplicate reuse, in-flight guard, controlled fault surfaces, and stored replay material aligned across fake and operations paths.
- artifact paths:
  - `artifacts/test/20260619T033451+0800/suites/infra-runtime-fake/report.json`
  - `artifacts/test/20260619T033451+0800/suites/operations-replay-core/report.json`
- artifact digests:
  - `sha256:232694cc513e8d5e9e249fda72bfa71932ba40f14ca5f7e95f952e300d298827`
  - `sha256:f46a0093633d29036099eda845c82f6e722018077d07a8f8a8e5c143aa6ad6d9`
- report paths:
  - `reports/runs/20260619T033451+0800/suites/infra-runtime-fake.md`
  - `reports/runs/20260619T033451+0800/suites/operations-replay-core.md`
- detail page: `reports/runs/20260619T033451+0800/evidence/EV-ID-IDEMP-001.md`

## EV-ID-CONFIG-001

- status: `passed`
- redaction_status: `clean`
- review_status: `reviewed`
- suite_refs: `config-redline`
- tc_refs: `TC-ID-CONFIG-001,TC-ID-CONFIG-002,TC-ID-CONFIG-003,TC-ID-CONFIG-004`
- ac_refs: `AC-ID-015`
- veto_refs: `VETO-ID-006`
- safe_summary: The config artifacts keep strict profile validation, no implicit fallback, and disabled-adapter no-success behavior aligned with the formal runtime rules.
- artifact paths:
  - `artifacts/test/20260619T033451+0800/suites/config-redline/report.json`
- artifact digests:
  - `sha256:bef97d5487010043b2ecbe920290179f25b7228fed1fab182a3b77e7a2d051b9`
- report paths:
  - `reports/runs/20260619T033451+0800/suites/config-redline.md`
- detail page: `reports/runs/20260619T033451+0800/evidence/EV-ID-CONFIG-001.md`

## EV-ID-REDACTION-001

- status: `passed`
- redaction_status: `clean`
- review_status: `reviewed`
- suite_refs: `redaction-boundary`
- tc_refs: `TC-ID-CONTRACT-004,TC-ID-CMD-010,TC-ID-REDACTION-001,TC-ID-REDACTION-002,TC-ID-REDACTION-003`
- ac_refs: `AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-003`
- safe_summary: The redaction artifacts keep reports and machine evidence free of forbidden bodies, raw secrets, and full sensitive refs.
- artifact paths:
  - `artifacts/test/20260619T033451+0800/suites/redaction-boundary/report.json`
- artifact digests:
  - `sha256:26dd906080db11596ff7f95490c5fb175215f85cb56652531961453582395da2`
- report paths:
  - `reports/runs/20260619T033451+0800/redaction-check.md`
- detail page: `reports/runs/20260619T033451+0800/evidence/EV-ID-REDACTION-001.md`

## EV-ID-ARCH-001

- status: `passed`
- redaction_status: `clean`
- review_status: `reviewed`
- suite_refs: `dependency-boundary`
- tc_refs: `TC-ID-ARCH-001`
- ac_refs: `AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-006`
- safe_summary: The dependency boundary artifacts show that compile-time dependencies stay within core and the declared identity layering.
- artifact paths:
  - `artifacts/test/20260619T033451+0800/suites/dependency-boundary/report.json`
- artifact digests:
  - `sha256:b3cbfb5253dece2d216eb9bce5f5b95c4fda4630bae71324ddd6a32c585d37ad`
- report paths:
  - `reports/runs/20260619T033451+0800/dependency-boundary.md`
- detail page: `reports/runs/20260619T033451+0800/evidence/EV-ID-ARCH-001.md`

## EV-ID-NFR-001

- status: `passed`
- redaction_status: `clean`
- review_status: `reviewed`
- suite_refs: `operations-replay-core,redaction-boundary,release-main-smoke`
- tc_refs: `TC-ID-QUERY-001,TC-ID-JOB-001,TC-ID-REDACTION-001,TC-ID-CONFIG-001`
- ac_refs: `AC-ID-015`
- veto_refs: `none`
- safe_summary: The release run keeps safe duration, count, degraded, and redaction evidence available for non-functional review without introducing hard-threshold claims.
- artifact paths:
  - `artifacts/test/20260619T033451+0800/suites/operations-replay-core/report.json`
  - `artifacts/test/20260619T033451+0800/suites/redaction-boundary/report.json`
  - `artifacts/test/20260619T033451+0800/suites/release-main-smoke/report.json`
- artifact digests:
  - `sha256:f46a0093633d29036099eda845c82f6e722018077d07a8f8a8e5c143aa6ad6d9`
  - `sha256:26dd906080db11596ff7f95490c5fb175215f85cb56652531961453582395da2`
  - `sha256:a7b0069d0237869df5fa91d2a53d27bd8c7de6d35273901c4e81ba1569338189`
- report paths:
  - `reports/runs/20260619T033451+0800/suites/operations-replay-core.md`
  - `reports/runs/20260619T033451+0800/redaction-check.md`
  - `reports/runs/20260619T033451+0800/suites/release-main-smoke.md`
- detail page: `reports/runs/20260619T033451+0800/evidence/EV-ID-NFR-001.md`

## EV-ID-REPORT-001

- status: `passed`
- redaction_status: `clean`
- review_status: `reviewed`
- suite_refs: `report-generation-audit`
- tc_refs: `TC-ID-CONTRACT-001,TC-ID-CMD-001,TC-ID-QUERY-001,TC-ID-CONFIG-001,TC-ID-ARCH-001,TC-ID-REDACTION-001`
- ac_refs: `AC-ID-001,AC-ID-002,AC-ID-003,AC-ID-004,AC-ID-005,AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-001,VETO-ID-002,VETO-ID-003,VETO-ID-004,VETO-ID-005,VETO-ID-006`
- safe_summary: The report-generation-audit artifacts cover artifact and report pairing, no-static-evidence checks, and evidence traceability back to blocking-suite TC refs.
- artifact paths:
  - `artifacts/test/20260619T033451+0800/suites/report-generation-audit/report.json`
- artifact digests:
  - `sha256:f8dcc5bdc002d0a8a1e8c53d227e30861e7ce61b0eb5d925d8b0713140da2e5b`
- report paths:
  - `reports/runs/20260619T033451+0800/report-audit.md`
  - `reports/runs/20260619T033451+0800/evidence-index.md`
- detail page: `reports/runs/20260619T033451+0800/evidence/EV-ID-REPORT-001.md`
