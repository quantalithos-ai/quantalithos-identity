//! Runtime builder that composes validated config into the in-memory runtime.

use identity_application::mapper::{
    DefaultIdentityMaintenanceIssueMapper, DefaultIdentityQueryMaterialDegradationMapper,
};
use identity_application::ports::{IdentityAdapterAvailabilityPort, IdentityClockPort};
use identity_application::support::{
    IdentityAdapterAvailability, IdentityAdapterAvailabilityIssueRef, IdentityAdapterModeRef,
    IdentityAdapterRef, IdentityApiRouteCatalogRef, IdentityConfigEvidenceRef,
    IdentityConfigIssueRef, IdentityConsumerBindingCatalogRef, IdentityJobCatalogRef,
    IdentityRepositoryPage, IdentityRuntimeAssemblyRef, IdentityRuntimeAssemblyState,
    IdentityRuntimeConfigShell, IdentityRuntimeProfileRef,
};
use identity_application::{
    IdentityApplicationFacade, IdentityCommandService, IdentityCommandServiceDeps,
    IdentityConsumerService, IdentityConsumerServiceDeps, IdentityJobService,
    IdentityJobServiceDeps, IdentityQueryService, IdentityQueryServiceDeps,
};
use identity_contracts::refs::IdentityTimestamp;

use crate::config::{
    IdentityAdapterMode, IdentityAuditSinkMode, IdentityGovernanceMode,
    IdentityRuntimeConfigSources, IdentityRuntimeStartupConfig, IdentityStoreMode,
};
use crate::in_memory::{IdentityInMemoryRuntime, IdentityInMemoryRuntimeBuilder};

const ADAPTER_ROLE_CATALOG: &str = "adapter.role-catalog";
const ADAPTER_PUBLISHER: &str = "adapter.publisher";
const ADAPTER_ARTIFACT_EVIDENCE: &str = "adapter.artifact-evidence";
const ADAPTER_MEMORY_ARCHIVE: &str = "adapter.memory-archive";
const ADAPTER_GOVERNANCE_BASIS: &str = "adapter.governance-basis";
const ADAPTER_WORK_SOURCE: &str = "adapter.work-source";
const ADAPTER_TRACE_HANDOFF: &str = "adapter.trace-handoff";
const ADAPTER_AUDIT_SINK: &str = "adapter.audit-sink";

pub enum IdentityInMemoryRuntimeBuildOutcome {
    Ready(IdentityInMemoryRuntimeAssembly),
    Failed(IdentityRuntimeAssemblyState),
}

pub struct IdentityInMemoryRuntimeAssembly {
    runtime: IdentityInMemoryRuntime,
    config_shell: IdentityRuntimeConfigShell,
    assembly_state: IdentityRuntimeAssemblyState,
    maintenance_issue_mapper: DefaultIdentityMaintenanceIssueMapper,
}

impl IdentityInMemoryRuntimeAssembly {
    pub fn runtime(&self) -> &IdentityInMemoryRuntime {
        &self.runtime
    }

    pub fn config_shell(&self) -> &IdentityRuntimeConfigShell {
        &self.config_shell
    }

    pub fn assembly_state(&self) -> &IdentityRuntimeAssemblyState {
        &self.assembly_state
    }

    pub fn application_facade(&self) -> IdentityApplicationFacade<'_> {
        let command_service = IdentityCommandService::new(IdentityCommandServiceDeps {
            unit_of_work_manager: &self.runtime,
            clock: &self.runtime,
            id_generator: &self.runtime,
            cursor_assigner: &self.runtime,
            operation_context_factory: &self.runtime,
            idempotency_repository: &self.runtime,
            stored_result_repository: &self.runtime,
            effect_summary_repository: &self.runtime,
            truth_change_subject_mapper: &self.runtime,
            accepted_audit_trail_marker_mapper: &self.runtime,
            member_repository: &self.runtime,
            lifecycle_repository: &self.runtime,
            role_capability_repository: &self.runtime,
            career_record_repository: &self.runtime,
            memory_reference_repository: &self.runtime,
            trace_record_repository: &self.runtime,
            audit_trail_repository: &self.runtime,
            outbox_repository: &self.runtime,
            projection_repository: &self.runtime,
            handoff_intent_repository: &self.runtime,
            handoff_target_port: &self.runtime,
            external_source_resolver: &self.runtime,
        });
        let query_service = IdentityQueryService::new(IdentityQueryServiceDeps {
            clock: &self.runtime,
            id_generator: &self.runtime,
            operation_context_factory: &self.runtime,
            read_visibility_repository: &self.runtime,
            projection_repository: &self.runtime,
            member_repository: &self.runtime,
            lifecycle_repository: &self.runtime,
            role_capability_repository: &self.runtime,
            career_record_repository: &self.runtime,
            memory_reference_repository: &self.runtime,
            trace_record_repository: &self.runtime,
            audit_trail_repository: &self.runtime,
            reference_state_repository: &self.runtime,
            reconciliation_report_repository: &self.runtime,
            outbox_repository: &self.runtime,
            handoff_intent_repository: &self.runtime,
            truth_change_subject_mapper: &self.runtime,
            degradation_mapper: &DefaultIdentityQueryMaterialDegradationMapper,
            unit_of_work_manager: &self.runtime,
        });
        let consumer_service = IdentityConsumerService::new(IdentityConsumerServiceDeps {
            unit_of_work_manager: &self.runtime,
            clock: &self.runtime,
            id_generator: &self.runtime,
            cursor_assigner: &self.runtime,
            operation_context_factory: &self.runtime,
            idempotency_repository: &self.runtime,
            stored_result_repository: &self.runtime,
            truth_change_subject_mapper: &self.runtime,
            marker_subject_mapper: &self.runtime,
            accepted_audit_trail_marker_mapper: &self.runtime,
            member_repository: &self.runtime,
            role_capability_repository: &self.runtime,
            career_record_repository: &self.runtime,
            memory_reference_repository: &self.runtime,
            reference_state_repository: &self.runtime,
            external_reference_resolver: &self.runtime,
            trace_record_repository: &self.runtime,
            audit_trail_repository: &self.runtime,
            outbox_repository: &self.runtime,
            projection_repository: &self.runtime,
            handoff_intent_repository: &self.runtime,
        });
        let job_service = IdentityJobService::new(IdentityJobServiceDeps {
            unit_of_work_manager: &self.runtime,
            clock: &self.runtime,
            id_generator: &self.runtime,
            idempotency_repository: &self.runtime,
            stored_result_repository: &self.runtime,
            job_report_repository: &self.runtime,
            projection_repository: &self.runtime,
            maintenance_repository: &self.runtime,
            reference_state_repository: &self.runtime,
            external_reference_resolver: &self.runtime,
            reconciliation_report_repository: &self.runtime,
            outbox_repository: &self.runtime,
            topic_binding_port: &self.runtime,
            outbox_publisher_port: &self.runtime,
            handoff_intent_repository: &self.runtime,
            handoff_target_port: &self.runtime,
            handoff_delivery_port: &self.runtime,
            maintenance_issue_mapper: &self.maintenance_issue_mapper,
        });

        IdentityApplicationFacade::new(command_service)
            .with_query_service(query_service)
            .with_consumer_service(consumer_service)
            .with_job_service(job_service)
    }
}

pub struct IdentityInMemoryRuntimeAssemblyBuilder {
    sources: IdentityRuntimeConfigSources,
}

impl IdentityInMemoryRuntimeAssemblyBuilder {
    pub fn new(sources: IdentityRuntimeConfigSources) -> Self {
        Self { sources }
    }

    pub fn build(self) -> IdentityInMemoryRuntimeBuildOutcome {
        let defaults = self.sources.code_defaults.clone();
        let default_profile_ref = IdentityRuntimeProfileRef::new(defaults.profile.name.as_str());
        let assembly_ref =
            IdentityRuntimeAssemblyRef::new(format!("runtime-{}", digest_hex(&defaults)));

        let effective = match self.sources.load() {
            Ok(config) => config,
            Err(issue_refs) => {
                let shell = IdentityRuntimeConfigShell::invalid(
                    default_profile_ref,
                    IdentityConfigEvidenceRef::new("config-invalid"),
                    issue_refs.clone(),
                );
                return IdentityInMemoryRuntimeBuildOutcome::Failed(
                    IdentityRuntimeAssemblyState::failed(assembly_ref, &shell, issue_refs),
                );
            }
        };

        let profile_ref = IdentityRuntimeProfileRef::new(effective.profile.name.as_str());
        let config_evidence_ref =
            IdentityConfigEvidenceRef::new(format!("config-digest:{}", digest_hex(&effective)));
        let config_shell = IdentityRuntimeConfigShell::validated(
            profile_ref,
            config_evidence_ref,
            adapter_mode_refs(&effective),
            Some(IdentityApiRouteCatalogRef::new("identity.api.routes")),
            Some(IdentityConsumerBindingCatalogRef::new(
                "identity.worker.bindings",
            )),
            Some(IdentityJobCatalogRef::new("identity.jobs.catalog")),
        );

        if effective.store.mode == IdentityStoreMode::Durable {
            let issue_refs = vec![config_issue(
                "store.mode",
                "durable-store-not-wired-in-commit-08-a",
            )];
            return IdentityInMemoryRuntimeBuildOutcome::Failed(
                IdentityRuntimeAssemblyState::failed(assembly_ref, &config_shell, issue_refs),
            );
        }

        let (runtime, adapter_issue_refs) = build_runtime(&effective);
        let assembled_at = runtime
            .now()
            .unwrap_or_else(|_| IdentityTimestamp::from_clock(0).expect("valid zero timestamp"));
        let adapter_refs = runtime
            .list_adapter_availability(IdentityRepositoryPage::new(None, 100))
            .map(|page| {
                page.items
                    .into_iter()
                    .map(|availability| availability.adapter_ref)
                    .collect()
            })
            .unwrap_or_default();
        let assembly_state = if adapter_issue_refs.is_empty() {
            IdentityRuntimeAssemblyState::assembled(
                assembly_ref,
                &config_shell,
                adapter_refs,
                assembled_at,
            )
        } else {
            IdentityRuntimeAssemblyState::degraded(
                assembly_ref,
                &config_shell,
                adapter_refs,
                adapter_issue_refs,
                assembled_at,
            )
        };

        IdentityInMemoryRuntimeBuildOutcome::Ready(IdentityInMemoryRuntimeAssembly {
            runtime,
            config_shell,
            assembly_state,
            maintenance_issue_mapper: DefaultIdentityMaintenanceIssueMapper,
        })
    }
}

fn build_runtime(
    config: &IdentityRuntimeStartupConfig,
) -> (IdentityInMemoryRuntime, Vec<IdentityConfigIssueRef>) {
    let checked_at = IdentityTimestamp::from_clock(1).expect("valid timestamp");
    let mut runtime_builder = IdentityInMemoryRuntimeBuilder::new();
    let mut issue_refs = Vec::new();

    for availability in adapter_availability(config, checked_at, &mut issue_refs) {
        runtime_builder = runtime_builder.seed_adapter_availability(availability);
    }

    (runtime_builder.build(), issue_refs)
}

fn adapter_availability(
    config: &IdentityRuntimeStartupConfig,
    checked_at: IdentityTimestamp,
    issue_refs: &mut Vec<IdentityConfigIssueRef>,
) -> Vec<IdentityAdapterAvailability> {
    let mut availability = Vec::new();
    availability.push(from_adapter_mode(
        ADAPTER_ROLE_CATALOG,
        &config.role_catalog.source_mode,
        checked_at,
        issue_refs,
    ));
    availability.push(from_adapter_mode(
        ADAPTER_PUBLISHER,
        &config.bus.publisher_mode,
        checked_at,
        issue_refs,
    ));
    availability.push(from_adapter_mode(
        ADAPTER_ARTIFACT_EVIDENCE,
        &config.external_refs.artifact_evidence.mode,
        checked_at,
        issue_refs,
    ));
    availability.push(from_adapter_mode(
        ADAPTER_MEMORY_ARCHIVE,
        &config.external_refs.memory_archive.mode,
        checked_at,
        issue_refs,
    ));
    availability.push(from_governance_mode(
        ADAPTER_GOVERNANCE_BASIS,
        &config.external_refs.governance_basis.mode,
        checked_at,
        issue_refs,
    ));
    availability.push(from_adapter_mode(
        ADAPTER_WORK_SOURCE,
        &config.external_refs.work_source.mode,
        checked_at,
        issue_refs,
    ));
    availability.push(trace_handoff_availability(
        &config.external_refs.trace_handoff.target_ref,
        checked_at,
        issue_refs,
    ));
    availability.push(from_audit_mode(
        ADAPTER_AUDIT_SINK,
        &config.audit.sink_mode,
        checked_at,
        issue_refs,
    ));
    availability
}

fn from_adapter_mode(
    adapter_name: &str,
    mode: &IdentityAdapterMode,
    checked_at: IdentityTimestamp,
    issue_refs: &mut Vec<IdentityConfigIssueRef>,
) -> IdentityAdapterAvailability {
    let adapter_ref = IdentityAdapterRef::new(adapter_name);
    let adapter_mode_ref = IdentityAdapterModeRef::new(mode.as_str());
    match mode {
        IdentityAdapterMode::Fake | IdentityAdapterMode::Controlled => {
            IdentityAdapterAvailability::available(adapter_ref, adapter_mode_ref, checked_at)
        }
        IdentityAdapterMode::Endpoint => {
            issue_refs.push(config_issue(
                adapter_name,
                "endpoint-adapter-not-wired-in-commit-08-a",
            ));
            IdentityAdapterAvailability::unavailable(
                adapter_ref,
                adapter_mode_ref,
                IdentityAdapterAvailabilityIssueRef::new(format!(
                    "{adapter_name}:endpoint-unavailable"
                )),
                checked_at,
            )
        }
        IdentityAdapterMode::Disabled => {
            issue_refs.push(config_issue(adapter_name, "adapter-disabled"));
            IdentityAdapterAvailability::disabled(
                adapter_ref,
                adapter_mode_ref,
                IdentityAdapterAvailabilityIssueRef::new(format!("{adapter_name}:disabled")),
                checked_at,
            )
        }
    }
}

fn from_governance_mode(
    adapter_name: &str,
    mode: &IdentityGovernanceMode,
    checked_at: IdentityTimestamp,
    issue_refs: &mut Vec<IdentityConfigIssueRef>,
) -> IdentityAdapterAvailability {
    let adapter_ref = IdentityAdapterRef::new(adapter_name);
    let adapter_mode_ref = IdentityAdapterModeRef::new(mode.as_str());
    match mode {
        IdentityGovernanceMode::Controlled => {
            IdentityAdapterAvailability::available(adapter_ref, adapter_mode_ref, checked_at)
        }
        IdentityGovernanceMode::Endpoint => {
            issue_refs.push(config_issue(
                adapter_name,
                "endpoint-adapter-not-wired-in-commit-08-a",
            ));
            IdentityAdapterAvailability::unavailable(
                adapter_ref,
                adapter_mode_ref,
                IdentityAdapterAvailabilityIssueRef::new(format!(
                    "{adapter_name}:endpoint-unavailable"
                )),
                checked_at,
            )
        }
        IdentityGovernanceMode::Disabled => {
            issue_refs.push(config_issue(adapter_name, "adapter-disabled"));
            IdentityAdapterAvailability::disabled(
                adapter_ref,
                adapter_mode_ref,
                IdentityAdapterAvailabilityIssueRef::new(format!("{adapter_name}:disabled")),
                checked_at,
            )
        }
    }
}

fn from_audit_mode(
    adapter_name: &str,
    mode: &IdentityAuditSinkMode,
    checked_at: IdentityTimestamp,
    issue_refs: &mut Vec<IdentityConfigIssueRef>,
) -> IdentityAdapterAvailability {
    let adapter_ref = IdentityAdapterRef::new(adapter_name);
    let adapter_mode_ref = IdentityAdapterModeRef::new(mode.as_str());
    match mode {
        IdentityAuditSinkMode::Local
        | IdentityAuditSinkMode::Captured
        | IdentityAuditSinkMode::Controlled => {
            IdentityAdapterAvailability::available(adapter_ref, adapter_mode_ref, checked_at)
        }
        IdentityAuditSinkMode::Endpoint => {
            issue_refs.push(config_issue(
                adapter_name,
                "endpoint-adapter-not-wired-in-commit-08-a",
            ));
            IdentityAdapterAvailability::unavailable(
                adapter_ref,
                adapter_mode_ref,
                IdentityAdapterAvailabilityIssueRef::new(format!(
                    "{adapter_name}:endpoint-unavailable"
                )),
                checked_at,
            )
        }
    }
}

fn trace_handoff_availability(
    target_ref: &Option<String>,
    checked_at: IdentityTimestamp,
    issue_refs: &mut Vec<IdentityConfigIssueRef>,
) -> IdentityAdapterAvailability {
    let adapter_ref = IdentityAdapterRef::new(ADAPTER_TRACE_HANDOFF);
    let adapter_mode_ref = if target_ref.is_some() {
        IdentityAdapterModeRef::new("configured")
    } else {
        IdentityAdapterModeRef::new("disabled")
    };

    if target_ref.is_some() {
        IdentityAdapterAvailability::available(adapter_ref, adapter_mode_ref, checked_at)
    } else {
        issue_refs.push(config_issue(
            "external_refs.trace_handoff.target_ref",
            "handoff-target-disabled",
        ));
        IdentityAdapterAvailability::disabled(
            adapter_ref,
            adapter_mode_ref,
            IdentityAdapterAvailabilityIssueRef::new("trace-handoff:disabled"),
            checked_at,
        )
    }
}

fn adapter_mode_refs(config: &IdentityRuntimeStartupConfig) -> Vec<IdentityAdapterModeRef> {
    vec![
        IdentityAdapterModeRef::new(config.role_catalog.source_mode.as_str()),
        IdentityAdapterModeRef::new(config.bus.publisher_mode.as_str()),
        IdentityAdapterModeRef::new(config.external_refs.artifact_evidence.mode.as_str()),
        IdentityAdapterModeRef::new(config.external_refs.memory_archive.mode.as_str()),
        IdentityAdapterModeRef::new(config.external_refs.governance_basis.mode.as_str()),
        IdentityAdapterModeRef::new(config.external_refs.work_source.mode.as_str()),
        IdentityAdapterModeRef::new(config.audit.sink_mode.as_str()),
    ]
}

fn config_issue(path: &str, code: &str) -> IdentityConfigIssueRef {
    IdentityConfigIssueRef::new(format!("config:{}:{code}", path))
}

fn digest_hex(config: &IdentityRuntimeStartupConfig) -> String {
    let bytes = serde_json::to_vec(config).unwrap_or_default();
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::{IdentityInMemoryRuntimeAssemblyBuilder, IdentityInMemoryRuntimeBuildOutcome};
    use crate::config::{
        IdentityConfigSourceKind, IdentityRuntimeConfigSources, IdentityRuntimeStartupConfig,
    };
    use identity_application::ports::IdentityAdapterAvailabilityPort;
    use identity_application::support::{
        IdentityAdapterAvailabilityKind, IdentityAdapterRef, IdentityRuntimeAssemblyStateKind,
    };

    fn valid_defaults() -> IdentityRuntimeStartupConfig {
        IdentityRuntimeStartupConfig::from_strict_json(
            r#"{
              "profile": {
                "name": "ci-test",
                "adapter_mode_policy": "p0-safe",
                "allow_test_override": true
              },
              "store": {
                "mode": "in-memory",
                "dsn_ref": null,
                "migration": { "required_version": "identity-schema-p0" },
                "transaction_mode": "single-uow",
                "idempotency": { "enabled": true },
                "dead_letter": { "retention_days": 30 }
              },
              "actor_context": {
                "required": true,
                "require_trace_id": true,
                "trusted_context_profile": "trusted-upstream",
                "idempotency_key_required": true
              },
              "role_catalog": {
                "source_mode": "fake",
                "snapshot_ref": null,
                "fixture_ref": "fixture://identity/roles/p0",
                "fingerprint_required": true,
                "unknown_role_strategy": "reject-write"
              },
              "bus": {
                "publisher_mode": "disabled",
                "endpoint_ref": null,
                "topic_map_ref": "fixture://identity/topic-map/p0",
                "require_known_event_kind": true
              },
              "outbox": {
                "store_name": "identity-outbox",
                "publish": {
                  "batch_size": 50,
                  "max_attempts": 5,
                  "backoff_policy_ref": "retry:identity-outbox:p0",
                  "failure_mode": "mark-failed-no-rollback"
                }
              },
              "projection": {
                "store_name": "member-summary-projection",
                "checkpoint_name": "member-summary",
                "rebuild": { "batch_size": 100 },
                "query": { "not_ready_strategy": "return-not-ready" }
              },
              "operations": {
                "run_id_required": true,
                "replay": {
                  "report_root_ref": null,
                  "input_root_ref": null
                },
                "propagation_retry": { "enabled": true }
              },
              "external_refs": {
                "artifact_evidence": { "mode": "disabled", "endpoint_ref": null },
                "memory_archive": { "mode": "disabled", "endpoint_ref": null },
                "governance_basis": { "mode": "disabled", "endpoint_ref": null },
                "work_source": { "mode": "disabled" },
                "trace_handoff": { "target_ref": null }
              },
              "audit": {
                "sink_mode": "captured",
                "sink_ref": null,
                "compensation_enabled": true,
                "redaction_profile": "identity-safe"
              },
              "redline": {
                "no_auth_in_identity": true,
                "ref_only_guard": true,
                "projection_no_write_guard": true,
                "outbox_no_event_creation_guard": true,
                "stored_replay_guard": true
              },
              "fixture": {
                "clock_mode": "fixed",
                "id_sequence_mode": "deterministic",
                "seed_ref": "fixture://identity/seeds/p0"
              }
            }"#,
            IdentityConfigSourceKind::CodeDefaults,
        )
        .expect("valid defaults")
    }

    #[test]
    fn builder_publishes_degraded_state_when_optional_adapters_are_disabled() {
        let outcome = IdentityInMemoryRuntimeAssemblyBuilder::new(IdentityRuntimeConfigSources {
            code_defaults: valid_defaults(),
            config_file_json: None,
            environment_json: None,
        })
        .build();

        let IdentityInMemoryRuntimeBuildOutcome::Ready(assembly) = outcome else {
            panic!("runtime should assemble");
        };
        assert_eq!(
            assembly.assembly_state().state_kind,
            IdentityRuntimeAssemblyStateKind::Degraded
        );
    }

    #[test]
    fn disabled_adapter_is_visible_in_runtime_registry() {
        let outcome = IdentityInMemoryRuntimeAssemblyBuilder::new(IdentityRuntimeConfigSources {
            code_defaults: valid_defaults(),
            config_file_json: None,
            environment_json: None,
        })
        .build();

        let IdentityInMemoryRuntimeBuildOutcome::Ready(assembly) = outcome else {
            panic!("runtime should assemble");
        };
        let publisher = assembly
            .runtime()
            .get_adapter_availability(IdentityAdapterRef::new("adapter.publisher"))
            .expect("publisher availability");
        assert_eq!(
            publisher.availability_kind,
            IdentityAdapterAvailabilityKind::Disabled
        );
    }

    #[test]
    fn durable_store_mode_fails_builder() {
        let outcome = IdentityInMemoryRuntimeAssemblyBuilder::new(IdentityRuntimeConfigSources {
            code_defaults: valid_defaults(),
            config_file_json: Some(
                r#"{ "store": { "mode": "durable", "dsn_ref": "ref://dsn" } }"#.to_owned(),
            ),
            environment_json: None,
        })
        .build();

        let IdentityInMemoryRuntimeBuildOutcome::Failed(state) = outcome else {
            panic!("durable store should fail");
        };
        assert_eq!(
            state.issue_refs[0].as_str(),
            "config:store.mode:durable-store-not-wired-in-commit-08-a"
        );
    }
}
