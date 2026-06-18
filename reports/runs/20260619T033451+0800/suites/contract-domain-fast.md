# contract-domain-fast

- run_id: `20260619T033451+0800`
- suite status: `passed`
- config profile: `ci-test`
- started_at: `2026-06-18T19:34:59Z`
- finished_at: `2026-06-18T19:35:09Z`
- duration_ms: `10000`

## Cases

- `contract-001-public-shell-roundtrip`: status=`passed`; tc_refs=TC-ID-CONTRACT-001,TC-ID-CONTRACT-002,TC-ID-CONTRACT-003,TC-ID-CONTRACT-004; evidence_refs=EV-ID-CONTRACT-001
- `state-001-domain-transition-guards`: status=`passed`; tc_refs=TC-ID-DOMAIN-001,TC-ID-DOMAIN-002,TC-ID-DOMAIN-003,TC-ID-DOMAIN-004,TC-ID-DOMAIN-005,TC-ID-DOMAIN-006,TC-ID-STATE-001,TC-ID-STATE-002; evidence_refs=EV-ID-STATE-001

## Assertions

- `contracts.protocol.roundtrip.required_fields`: passed - The public command, query, consumer, job, and view shells round-trip with their required fields intact.
- `contracts.protocol.body_free.shells_only`: passed - The public shells stay body-free and do not embed external source bodies, raw memory text, or archive package material.
- `domain.state.transition.legal_and_illegal`: passed - The domain state helpers accept only legal transitions and reject illegal or terminal reopen attempts.
- `domain.truth.factory.append_only_and_stable_refs`: passed - The truth factories preserve append-only history and stable refs across accepted transitions.

## Raw Artifacts

- suite raw artifact: `artifacts/test/20260619T033451+0800/suites/contract-domain-fast/report.json`
- case artifacts: `artifacts/test/20260619T033451+0800/suites/contract-domain-fast/cases/*.json`
- stdout digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- stderr digest: `sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
