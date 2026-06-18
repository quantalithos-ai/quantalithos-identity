# evidence-index

- run_id: `20260619T124900+0800`
- raw artifact: `artifacts/test/20260619T124900+0800/evidence-index.json`
- evidence item count: `14`

## EV-ID-CORE-001

- status: `passed`
- redaction_status: `clean`
- review_status: `pending`
- suite_refs: `release-main-smoke`
- tc_refs: `TC-ID-CMD-001,TC-ID-QUERY-001,TC-ID-OUTBOX-001,TC-ID-JOB-001,TC-ID-CONFIG-001,TC-ID-REDACTION-001`
- ac_refs: `AC-ID-001,AC-ID-002,AC-ID-003,AC-ID-004,AC-ID-005`
- veto_refs: `VETO-ID-001,VETO-ID-002,VETO-ID-003,VETO-ID-004,VETO-ID-005,VETO-ID-006`
- safe_summary: Sample release scenario evidence is present for the report tooling run and links command, query, outbox, job, config, and redaction coverage.
- artifact paths:
  - `artifacts/test/20260619T124900+0800/suites/release-main-smoke/report.json`
- artifact digests:
  - `sha256:3bc8a2e6655dd6d71114e36643b1ebcdac9d0203d8e98268677e83bd57b91c6b`
- report paths:
  - `reports/runs/20260619T124900+0800/suites/release-main-smoke.md`

## EV-ID-CONTRACT-001

- status: `passed`
- redaction_status: `clean`
- review_status: `pending`
- suite_refs: `contract-domain-fast`
- tc_refs: `TC-ID-CONTRACT-001,TC-ID-CONTRACT-002,TC-ID-CONTRACT-003,TC-ID-CONTRACT-004`
- ac_refs: `AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-003`
- safe_summary: The contract-domain sample artifacts keep the public protocol shells round-trippable and body-free.
- artifact paths:
  - `artifacts/test/20260619T124900+0800/suites/contract-domain-fast/report.json`
- artifact digests:
  - `sha256:b212966baf23378e0ed7199d1370bac6f25d35b30d6b423a51899e1c68a5d678`
- report paths:
  - `reports/runs/20260619T124900+0800/suites/contract-domain-fast.md`

## EV-ID-STATE-001

- status: `passed`
- redaction_status: `clean`
- review_status: `pending`
- suite_refs: `contract-domain-fast`
- tc_refs: `TC-ID-DOMAIN-001,TC-ID-DOMAIN-002,TC-ID-DOMAIN-003,TC-ID-DOMAIN-004,TC-ID-DOMAIN-005,TC-ID-DOMAIN-006,TC-ID-STATE-001,TC-ID-STATE-002`
- ac_refs: `AC-ID-001,AC-ID-002,AC-ID-003,AC-ID-004,AC-ID-005,AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014`
- veto_refs: `VETO-ID-001,VETO-ID-004`
- safe_summary: The domain and state sample artifacts keep legal transitions, terminal guards, append-only history, and stable refs aligned with the formal state matrix.
- artifact paths:
  - `artifacts/test/20260619T124900+0800/suites/contract-domain-fast/report.json`
- artifact digests:
  - `sha256:b212966baf23378e0ed7199d1370bac6f25d35b30d6b423a51899e1c68a5d678`
- report paths:
  - `reports/runs/20260619T124900+0800/suites/contract-domain-fast.md`

## EV-ID-CMD-001

- status: `passed`
- redaction_status: `clean`
- review_status: `pending`
- suite_refs: `service-flow-fast`
- tc_refs: `TC-ID-CMD-001,TC-ID-CMD-002,TC-ID-CMD-003,TC-ID-CMD-004,TC-ID-CMD-005,TC-ID-CMD-006,TC-ID-CMD-007,TC-ID-CMD-008,TC-ID-CMD-009,TC-ID-CMD-010,TC-ID-CMD-011,TC-ID-CMD-012,TC-ID-CMD-013,TC-ID-CMD-014,TC-ID-CMD-015`
- ac_refs: `AC-ID-001,AC-ID-002,AC-ID-003,AC-ID-004,AC-ID-005,AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-001,VETO-ID-002,VETO-ID-003,VETO-ID-004`
- safe_summary: The command service sample artifacts keep accepted, rejected, duplicate replay, and rollback surfaces tied to the formal command shell.
- artifact paths:
  - `artifacts/test/20260619T124900+0800/suites/service-flow-fast/report.json`
- artifact digests:
  - `sha256:d7e2c7cd76fa2523a084f532b14531cd02be2cb33067c0949a66a07693f39986`
- report paths:
  - `reports/runs/20260619T124900+0800/suites/service-flow-fast.md`

## EV-ID-QUERY-001

- status: `passed`
- redaction_status: `clean`
- review_status: `pending`
- suite_refs: `service-flow-fast`
- tc_refs: `TC-ID-QUERY-001,TC-ID-QUERY-002,TC-ID-QUERY-003,TC-ID-QUERY-004,TC-ID-QUERY-005,TC-ID-QUERY-006,TC-ID-QUERY-007,TC-ID-QUERY-008,TC-ID-QUERY-009,TC-ID-QUERY-010,TC-ID-QUERY-011,TC-ID-QUERY-012,TC-ID-QUERY-013,TC-ID-QUERY-014,TC-ID-QUERY-015`
- ac_refs: `AC-ID-001,AC-ID-002,AC-ID-003,AC-ID-004,AC-ID-005,AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-002`
- safe_summary: The query service sample artifacts keep visibility-first, degraded, stale, and no-write behavior tied to the formal query shell.
- artifact paths:
  - `artifacts/test/20260619T124900+0800/suites/service-flow-fast/report.json`
- artifact digests:
  - `sha256:d7e2c7cd76fa2523a084f532b14531cd02be2cb33067c0949a66a07693f39986`
- report paths:
  - `reports/runs/20260619T124900+0800/suites/service-flow-fast.md`

## EV-ID-CONSUMER-001

- status: `passed`
- redaction_status: `clean`
- review_status: `pending`
- suite_refs: `entry-worker-job`
- tc_refs: `TC-ID-CONSUMER-001,TC-ID-CONSUMER-002,TC-ID-CONSUMER-003,TC-ID-CONSUMER-004,TC-ID-CONSUMER-005,TC-ID-CONSUMER-006`
- ac_refs: `AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-002,VETO-ID-003`
- safe_summary: The inbound and callback sample artifacts preserve typed receipt replay, no-create behavior, and body-free public receipts.
- artifact paths:
  - `artifacts/test/20260619T124900+0800/suites/entry-worker-job/report.json`
- artifact digests:
  - `sha256:7ba22bafb5d413a681e1b30f5174ebfaa4b68fbfbd765790fbadb4881f0e6184`
- report paths:
  - `reports/runs/20260619T124900+0800/suites/entry-worker-job.md`

## EV-ID-OUTBOX-001

- status: `passed`
- redaction_status: `clean`
- review_status: `pending`
- suite_refs: `operations-replay-core`
- tc_refs: `TC-ID-OUTBOX-001,TC-ID-OUTBOX-002,TC-ID-OUTBOX-003,TC-ID-OUTBOX-004,TC-ID-OUTBOX-005,TC-ID-OUTBOX-006,TC-ID-OUTBOX-007,TC-ID-OUTBOX-008,TC-ID-OUTBOX-009,TC-ID-OUTBOX-010`
- ac_refs: `AC-ID-001,AC-ID-002,AC-ID-003,AC-ID-004,AC-ID-005,AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-003`
- safe_summary: The operations replay sample artifacts keep accepted-only outbox material body-free and preserve formal publish outcome markers.
- artifact paths:
  - `artifacts/test/20260619T124900+0800/suites/operations-replay-core/report.json`
- artifact digests:
  - `sha256:db73adf474db2bf4d831a78030144d438ab758738deea231d4d698f2b7c2a761`
- report paths:
  - `reports/runs/20260619T124900+0800/suites/operations-replay-core.md`

## EV-ID-JOB-001

- status: `passed`
- redaction_status: `clean`
- review_status: `pending`
- suite_refs: `entry-worker-job,operations-replay-core`
- tc_refs: `TC-ID-JOB-001,TC-ID-JOB-002,TC-ID-JOB-003,TC-ID-JOB-004,TC-ID-JOB-005,TC-ID-JOB-006,TC-ID-JOB-007,TC-ID-JOB-008`
- ac_refs: `AC-ID-001,AC-ID-002,AC-ID-003,AC-ID-004,AC-ID-005,AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-002,VETO-ID-005`
- safe_summary: The job entry and operations replay sample artifacts preserve report replay, partial failure accounting, and report-only no-repair semantics.
- artifact paths:
  - `artifacts/test/20260619T124900+0800/suites/entry-worker-job/report.json`
  - `artifacts/test/20260619T124900+0800/suites/operations-replay-core/report.json`
- artifact digests:
  - `sha256:7ba22bafb5d413a681e1b30f5174ebfaa4b68fbfbd765790fbadb4881f0e6184`
  - `sha256:db73adf474db2bf4d831a78030144d438ab758738deea231d4d698f2b7c2a761`
- report paths:
  - `reports/runs/20260619T124900+0800/suites/entry-worker-job.md`
  - `reports/runs/20260619T124900+0800/suites/operations-replay-core.md`

## EV-ID-IDEMP-001

- status: `passed`
- redaction_status: `clean`
- review_status: `pending`
- suite_refs: `infra-runtime-fake,operations-replay-core`
- tc_refs: `TC-ID-IDEMP-001,TC-ID-IDEMP-002,TC-ID-IDEMP-003,TC-ID-IDEMP-004,TC-ID-IDEMP-005,TC-ID-IDEMP-006,TC-ID-IDEMP-007,TC-ID-IDEMP-008,TC-ID-IDEMP-009,TC-ID-IDEMP-010,TC-ID-IDEMP-011`
- ac_refs: `AC-ID-009,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-001,VETO-ID-002,VETO-ID-005`
- safe_summary: The replay sample artifacts keep duplicate reuse, in-flight guard, controlled fault surfaces, and stored replay material aligned across fake and operations paths.
- artifact paths:
  - `artifacts/test/20260619T124900+0800/suites/infra-runtime-fake/report.json`
  - `artifacts/test/20260619T124900+0800/suites/operations-replay-core/report.json`
- artifact digests:
  - `sha256:4bd943f36b76acaf3a60c7df07d9d6d98569963b367b9450aa1e83810530e8b4`
  - `sha256:db73adf474db2bf4d831a78030144d438ab758738deea231d4d698f2b7c2a761`
- report paths:
  - `reports/runs/20260619T124900+0800/suites/infra-runtime-fake.md`
  - `reports/runs/20260619T124900+0800/suites/operations-replay-core.md`

## EV-ID-CONFIG-001

- status: `passed`
- redaction_status: `clean`
- review_status: `pending`
- suite_refs: `config-redline`
- tc_refs: `TC-ID-CONFIG-001,TC-ID-CONFIG-002,TC-ID-CONFIG-003,TC-ID-CONFIG-004`
- ac_refs: `AC-ID-015`
- veto_refs: `VETO-ID-006`
- safe_summary: The config sample artifacts keep strict profile validation, no implicit fallback, and disabled-adapter no-success behavior aligned with the formal runtime rules.
- artifact paths:
  - `artifacts/test/20260619T124900+0800/suites/config-redline/report.json`
- artifact digests:
  - `sha256:2b38e5826d13fe97bf6637f76f1dd50afbef9b44154b8da536dc0b59fa78fdb1`
- report paths:
  - `reports/runs/20260619T124900+0800/suites/config-redline.md`

## EV-ID-REDACTION-001

- status: `passed`
- redaction_status: `clean`
- review_status: `pending`
- suite_refs: `redaction-boundary`
- tc_refs: `TC-ID-CONTRACT-004,TC-ID-CMD-010,TC-ID-REDACTION-001,TC-ID-REDACTION-002,TC-ID-REDACTION-003`
- ac_refs: `AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-003`
- safe_summary: The redaction sample artifacts keep reports and machine evidence free of forbidden bodies, raw secrets, and full sensitive refs.
- artifact paths:
  - `artifacts/test/20260619T124900+0800/suites/redaction-boundary/report.json`
- artifact digests:
  - `sha256:cce851c67773c6bec7fd22d5f9a25eca7965290e410a32ef82b384f6caa14258`
- report paths:
  - `reports/runs/20260619T124900+0800/redaction-check.md`

## EV-ID-ARCH-001

- status: `passed`
- redaction_status: `clean`
- review_status: `pending`
- suite_refs: `dependency-boundary`
- tc_refs: `TC-ID-ARCH-001`
- ac_refs: `AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-006`
- safe_summary: The dependency boundary sample artifacts show that the workspace keeps compile-time dependencies within core and the declared identity layering.
- artifact paths:
  - `artifacts/test/20260619T124900+0800/suites/dependency-boundary/report.json`
- artifact digests:
  - `sha256:5bb538bd267da5816ee2309b483c752a4363afc95f960b9305084996c288e570`
- report paths:
  - `reports/runs/20260619T124900+0800/dependency-boundary.md`

## EV-ID-NFR-001

- status: `passed`
- redaction_status: `clean`
- review_status: `pending`
- suite_refs: `operations-replay-core,redaction-boundary,release-main-smoke`
- tc_refs: `TC-ID-QUERY-001,TC-ID-JOB-001,TC-ID-REDACTION-001,TC-ID-CONFIG-001`
- ac_refs: `AC-ID-015`
- veto_refs: `none`
- safe_summary: The sample run keeps safe duration, count, degraded, and redaction evidence available for non-functional review without introducing hard-threshold claims.
- artifact paths:
  - `artifacts/test/20260619T124900+0800/suites/operations-replay-core/report.json`
  - `artifacts/test/20260619T124900+0800/suites/redaction-boundary/report.json`
  - `artifacts/test/20260619T124900+0800/suites/release-main-smoke/report.json`
- artifact digests:
  - `sha256:db73adf474db2bf4d831a78030144d438ab758738deea231d4d698f2b7c2a761`
  - `sha256:cce851c67773c6bec7fd22d5f9a25eca7965290e410a32ef82b384f6caa14258`
  - `sha256:3bc8a2e6655dd6d71114e36643b1ebcdac9d0203d8e98268677e83bd57b91c6b`
- report paths:
  - `reports/runs/20260619T124900+0800/suites/operations-replay-core.md`
  - `reports/runs/20260619T124900+0800/redaction-check.md`
  - `reports/runs/20260619T124900+0800/suites/release-main-smoke.md`

## EV-ID-REPORT-001

- status: `passed`
- redaction_status: `clean`
- review_status: `pending`
- suite_refs: `report-generation-audit`
- tc_refs: `TC-ID-CONTRACT-001,TC-ID-CMD-001,TC-ID-QUERY-001,TC-ID-CONFIG-001,TC-ID-ARCH-001,TC-ID-REDACTION-001`
- ac_refs: `AC-ID-001,AC-ID-002,AC-ID-003,AC-ID-004,AC-ID-005,AC-ID-006,AC-ID-007,AC-ID-008,AC-ID-009,AC-ID-010,AC-ID-011,AC-ID-012,AC-ID-013,AC-ID-014,AC-ID-015`
- veto_refs: `VETO-ID-001,VETO-ID-002,VETO-ID-003,VETO-ID-004,VETO-ID-005,VETO-ID-006`
- safe_summary: The report-generation-audit sample artifacts cover artifact and report pairing, no-static-evidence checks, and evidence traceability back to existing blocking-suite TC refs.
- artifact paths:
  - `artifacts/test/20260619T124900+0800/suites/report-generation-audit/report.json`
- artifact digests:
  - `sha256:a8555ddc4f981b71f81ba923f5a8bfa7b4fbb19f29bc8fb015374ea6e28c87cb`
- report paths:
  - `reports/runs/20260619T124900+0800/report-audit.md`
  - `reports/runs/20260619T124900+0800/evidence-index.md`
