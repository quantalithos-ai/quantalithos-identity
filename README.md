# quantalithos-identity

`quantalithos-identity` is the L1 identity domain service for Quantalithos AI.

The service will own the platform-level identity truth for `GlobalMember`,
role catalog references, capability profiles, career history, memory
references, audit records, and identity event publication.

Current status: database-backed domain/application/operations flows are implemented,
and the first real HTTP slice now exposes health, `HireGlobalMember`, and
`GetMemberSummary`.
