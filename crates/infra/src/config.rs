//! Infra-local runtime config parsing, source merge, and validation.

use std::collections::BTreeMap;

use identity_application::support::IdentityConfigIssueRef;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Debug, PartialEq)]
enum JsonNode {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<JsonNode>),
    Object(BTreeMap<String, JsonNode>),
}

impl<'de> Deserialize<'de> for JsonNode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct JsonNodeVisitor;

        impl<'de> Visitor<'de> for JsonNodeVisitor {
            type Value = JsonNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("strict JSON value")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(JsonNode::Bool(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(JsonNode::Number(serde_json::Number::from(value)))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(JsonNode::Number(serde_json::Number::from(value)))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                serde_json::Number::from_f64(value)
                    .map(JsonNode::Number)
                    .ok_or_else(|| E::custom("invalid JSON number"))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(JsonNode::String(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(JsonNode::String(value))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(JsonNode::Null)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(JsonNode::Null)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                JsonNode::deserialize(deserializer)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element::<JsonNode>()? {
                    items.push(item);
                }
                Ok(JsonNode::Array(items))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut object = BTreeMap::new();
                while let Some((key, value)) = map.next_entry::<String, JsonNode>()? {
                    if object.insert(key.clone(), value).is_some() {
                        return Err(de::Error::custom(format!("duplicate key `{key}`")));
                    }
                }
                Ok(JsonNode::Object(object))
            }
        }

        deserializer.deserialize_any(JsonNodeVisitor)
    }
}

impl JsonNode {
    fn into_value(self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(value) => serde_json::Value::Bool(value),
            Self::Number(value) => serde_json::Value::Number(value),
            Self::String(value) => serde_json::Value::String(value),
            Self::Array(items) => {
                serde_json::Value::Array(items.into_iter().map(Self::into_value).collect())
            }
            Self::Object(entries) => serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, value.into_value()))
                    .collect(),
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityConfigSourceKind {
    CodeDefaults,
    ConfigFile,
    Environment,
}

impl IdentityConfigSourceKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::CodeDefaults => "defaults",
            Self::ConfigFile => "file",
            Self::Environment => "environment",
        }
    }
}

#[derive(Clone, Debug)]
pub struct IdentityRuntimeConfigSources {
    pub code_defaults: IdentityRuntimeStartupConfig,
    pub config_file_json: Option<String>,
    pub environment_json: Option<String>,
}

impl IdentityRuntimeConfigSources {
    pub fn load(self) -> Result<IdentityRuntimeStartupConfig, Vec<IdentityConfigIssueRef>> {
        let mut effective = self.code_defaults;

        if let Some(config_file_json) = self.config_file_json {
            let patch = parse_patch(&config_file_json, IdentityConfigSourceKind::ConfigFile)?;
            patch.apply_to(&mut effective);
        }

        if let Some(environment_json) = self.environment_json {
            let patch = parse_patch(&environment_json, IdentityConfigSourceKind::Environment)?;
            patch.apply_to(&mut effective);
        }

        let issue_refs = effective.validate();
        if issue_refs.is_empty() {
            Ok(effective)
        } else {
            Err(issue_refs)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityRuntimeProfileName {
    LocalDev,
    CiTest,
    IntegrationLike,
    OperationsReplay,
}

impl IdentityRuntimeProfileName {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LocalDev => "local-dev",
            Self::CiTest => "ci-test",
            Self::IntegrationLike => "integration-like",
            Self::OperationsReplay => "operations-replay",
        }
    }

    fn is_test_profile(&self) -> bool {
        matches!(self, Self::LocalDev | Self::CiTest)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityAdapterModePolicy {
    P0Safe,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityStoreMode {
    InMemory,
    Durable,
    Controlled,
}

impl IdentityStoreMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InMemory => "in-memory",
            Self::Durable => "durable",
            Self::Controlled => "controlled",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityTransactionMode {
    SingleUow,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityTrustedContextProfile {
    TrustedUpstream,
    Fixture,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityAdapterMode {
    Fake,
    Controlled,
    Endpoint,
    Disabled,
}

impl IdentityAdapterMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Fake => "fake",
            Self::Controlled => "controlled",
            Self::Endpoint => "endpoint",
            Self::Disabled => "disabled",
        }
    }

    fn uses_fixture(&self) -> bool {
        matches!(self, Self::Fake)
    }

    fn requires_endpoint_ref(&self) -> bool {
        matches!(self, Self::Endpoint)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityGovernanceMode {
    Controlled,
    Endpoint,
    Disabled,
}

impl IdentityGovernanceMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Controlled => "controlled",
            Self::Endpoint => "endpoint",
            Self::Disabled => "disabled",
        }
    }

    fn requires_endpoint_ref(&self) -> bool {
        matches!(self, Self::Endpoint)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityUnknownRoleStrategy {
    RejectWrite,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityOutboxFailureMode {
    MarkFailedNoRollback,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityProjectionNotReadyStrategy {
    ReturnNotReady,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityAuditSinkMode {
    Local,
    Captured,
    Controlled,
    Endpoint,
}

impl IdentityAuditSinkMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Captured => "captured",
            Self::Controlled => "controlled",
            Self::Endpoint => "endpoint",
        }
    }

    fn requires_endpoint_ref(&self) -> bool {
        matches!(self, Self::Endpoint)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityRedactionProfile {
    IdentitySafe,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityFixtureClockMode {
    System,
    Fixed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityFixtureIdSequenceMode {
    Runtime,
    Deterministic,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityRuntimeStartupConfig {
    pub profile: IdentityProfileConfig,
    pub store: IdentityStoreConfig,
    pub actor_context: IdentityActorContextConfig,
    pub role_catalog: IdentityRoleCatalogConfig,
    pub bus: IdentityBusConfig,
    pub outbox: IdentityOutboxConfig,
    pub projection: IdentityProjectionConfig,
    pub operations: IdentityOperationsConfig,
    pub external_refs: IdentityExternalRefsConfig,
    pub audit: IdentityAuditConfig,
    pub redline: IdentityRedlineConfig,
    pub fixture: IdentityFixtureConfig,
}

impl IdentityRuntimeStartupConfig {
    pub fn from_strict_json(
        json: &str,
        source_kind: IdentityConfigSourceKind,
    ) -> Result<Self, Vec<IdentityConfigIssueRef>> {
        let node: JsonNode = serde_json::from_str(json)
            .map_err(|_| vec![issue(source_kind, "document", "parse-failed")])?;
        let config: Self = serde_json::from_value(node.into_value())
            .map_err(|_| vec![issue(source_kind, "document", "shape-invalid")])?;
        let issue_refs = config.validate();
        if issue_refs.is_empty() {
            Ok(config)
        } else {
            Err(issue_refs)
        }
    }

    pub fn validate(&self) -> Vec<IdentityConfigIssueRef> {
        let mut issue_refs = Vec::new();

        if self.profile.allow_test_override && !self.profile.name.is_test_profile() {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "profile.allow_test_override",
                "profile-incompatible",
            ));
        }

        if self.store.mode == IdentityStoreMode::Durable && missing(&self.store.dsn_ref) {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "store.dsn_ref",
                "required-for-durable",
            ));
        }
        if self.store.migration.required_version.trim().is_empty() {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "store.migration.required_version",
                "empty",
            ));
        }
        if !self.store.idempotency.enabled {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "store.idempotency.enabled",
                "must-be-true",
            ));
        }
        if self.store.dead_letter.retention_days == 0 {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "store.dead_letter.retention_days",
                "must-be-positive",
            ));
        }

        if !self.actor_context.required {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "actor_context.required",
                "must-be-true",
            ));
        }
        if !self.actor_context.idempotency_key_required {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "actor_context.idempotency_key_required",
                "must-be-true",
            ));
        }
        if self.actor_context.trusted_context_profile == IdentityTrustedContextProfile::Fixture
            && !self.profile.name.is_test_profile()
        {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "actor_context.trusted_context_profile",
                "fixture-profile-forbidden",
            ));
        }

        if self.role_catalog.source_mode.uses_fixture() {
            if !self.profile.name.is_test_profile() {
                issue_refs.push(issue(
                    IdentityConfigSourceKind::CodeDefaults,
                    "role_catalog.source_mode",
                    "fixture-profile-forbidden",
                ));
            }
            if missing(&self.role_catalog.fixture_ref) {
                issue_refs.push(issue(
                    IdentityConfigSourceKind::CodeDefaults,
                    "role_catalog.fixture_ref",
                    "required-for-fake",
                ));
            }
        }
        if matches!(
            self.role_catalog.source_mode,
            IdentityAdapterMode::Controlled | IdentityAdapterMode::Endpoint
        ) && missing(&self.role_catalog.snapshot_ref)
        {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "role_catalog.snapshot_ref",
                "required-for-controlled-or-endpoint",
            ));
        }
        if !self.role_catalog.fingerprint_required {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "role_catalog.fingerprint_required",
                "must-be-true",
            ));
        }

        validate_adapter_mode(
            &mut issue_refs,
            IdentityConfigSourceKind::CodeDefaults,
            "bus.publisher_mode",
            &self.bus.publisher_mode,
            self.bus.endpoint_ref.as_ref(),
        );
        if self.bus.publisher_mode != IdentityAdapterMode::Disabled
            && missing(&self.bus.topic_map_ref)
        {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "bus.topic_map_ref",
                "required-when-enabled",
            ));
        }
        if !self.bus.require_known_event_kind {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "bus.require_known_event_kind",
                "must-be-true",
            ));
        }

        if self.outbox.store_name.trim().is_empty() {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "outbox.store_name",
                "empty",
            ));
        }
        if self.outbox.publish.batch_size == 0 {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "outbox.publish.batch_size",
                "must-be-positive",
            ));
        }
        if self.outbox.publish.max_attempts == 0 {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "outbox.publish.max_attempts",
                "must-be-positive",
            ));
        }
        if self.outbox.publish.backoff_policy_ref.trim().is_empty() {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "outbox.publish.backoff_policy_ref",
                "empty",
            ));
        }

        if self.projection.store_name.trim().is_empty() {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "projection.store_name",
                "empty",
            ));
        }
        if self.projection.checkpoint_name.trim().is_empty() {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "projection.checkpoint_name",
                "empty",
            ));
        }
        if self.projection.rebuild.batch_size == 0 {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "projection.rebuild.batch_size",
                "must-be-positive",
            ));
        }

        if self.profile.name == IdentityRuntimeProfileName::OperationsReplay {
            if self.operations.replay.report_root_ref.is_none() {
                issue_refs.push(issue(
                    IdentityConfigSourceKind::CodeDefaults,
                    "operations.replay.report_root_ref",
                    "required-for-operations-replay",
                ));
            }
            if self.operations.replay.input_root_ref.is_none() {
                issue_refs.push(issue(
                    IdentityConfigSourceKind::CodeDefaults,
                    "operations.replay.input_root_ref",
                    "required-for-operations-replay",
                ));
            }
        }

        validate_adapter_mode(
            &mut issue_refs,
            IdentityConfigSourceKind::CodeDefaults,
            "external_refs.artifact_evidence.mode",
            &self.external_refs.artifact_evidence.mode,
            self.external_refs.artifact_evidence.endpoint_ref.as_ref(),
        );
        validate_adapter_mode(
            &mut issue_refs,
            IdentityConfigSourceKind::CodeDefaults,
            "external_refs.memory_archive.mode",
            &self.external_refs.memory_archive.mode,
            self.external_refs.memory_archive.endpoint_ref.as_ref(),
        );
        validate_governance_mode(
            &mut issue_refs,
            IdentityConfigSourceKind::CodeDefaults,
            "external_refs.governance_basis.mode",
            &self.external_refs.governance_basis.mode,
            self.external_refs.governance_basis.endpoint_ref.as_ref(),
        );
        if self.external_refs.work_source.mode.uses_fixture()
            && !self.profile.name.is_test_profile()
        {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "external_refs.work_source.mode",
                "fixture-profile-forbidden",
            ));
        }

        validate_audit_mode(
            &mut issue_refs,
            IdentityConfigSourceKind::CodeDefaults,
            &self.audit,
        );

        if !self.redline.no_auth_in_identity {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "redline.no_auth_in_identity",
                "must-be-true",
            ));
        }
        if !self.redline.ref_only_guard {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "redline.ref_only_guard",
                "must-be-true",
            ));
        }
        if !self.redline.projection_no_write_guard {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "redline.projection_no_write_guard",
                "must-be-true",
            ));
        }
        if !self.redline.outbox_no_event_creation_guard {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "redline.outbox_no_event_creation_guard",
                "must-be-true",
            ));
        }
        if !self.redline.stored_replay_guard {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "redline.stored_replay_guard",
                "must-be-true",
            ));
        }

        if self.fixture.clock_mode == IdentityFixtureClockMode::Fixed
            && !self.profile.name.is_test_profile()
        {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "fixture.clock_mode",
                "profile-incompatible",
            ));
        }
        if self.fixture.id_sequence_mode == IdentityFixtureIdSequenceMode::Deterministic
            && !self.profile.name.is_test_profile()
        {
            issue_refs.push(issue(
                IdentityConfigSourceKind::CodeDefaults,
                "fixture.id_sequence_mode",
                "profile-incompatible",
            ));
        }

        issue_refs
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityProfileConfig {
    pub name: IdentityRuntimeProfileName,
    pub adapter_mode_policy: IdentityAdapterModePolicy,
    pub allow_test_override: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityStoreConfig {
    pub mode: IdentityStoreMode,
    pub dsn_ref: Option<String>,
    pub migration: IdentityMigrationConfig,
    pub transaction_mode: IdentityTransactionMode,
    pub idempotency: IdentityStoreIdempotencyConfig,
    pub dead_letter: IdentityDeadLetterConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityMigrationConfig {
    pub required_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityStoreIdempotencyConfig {
    pub enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityDeadLetterConfig {
    pub retention_days: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityActorContextConfig {
    pub required: bool,
    pub require_trace_id: bool,
    pub trusted_context_profile: IdentityTrustedContextProfile,
    pub idempotency_key_required: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityRoleCatalogConfig {
    pub source_mode: IdentityAdapterMode,
    pub snapshot_ref: Option<String>,
    pub fixture_ref: Option<String>,
    pub fingerprint_required: bool,
    pub unknown_role_strategy: IdentityUnknownRoleStrategy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityBusConfig {
    pub publisher_mode: IdentityAdapterMode,
    pub endpoint_ref: Option<String>,
    pub topic_map_ref: Option<String>,
    pub require_known_event_kind: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityOutboxConfig {
    pub store_name: String,
    pub publish: IdentityOutboxPublishConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityOutboxPublishConfig {
    pub batch_size: u32,
    pub max_attempts: u32,
    pub backoff_policy_ref: String,
    pub failure_mode: IdentityOutboxFailureMode,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityProjectionConfig {
    pub store_name: String,
    pub checkpoint_name: String,
    pub rebuild: IdentityProjectionRebuildConfig,
    pub query: IdentityProjectionQueryConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityProjectionRebuildConfig {
    pub batch_size: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityProjectionQueryConfig {
    pub not_ready_strategy: IdentityProjectionNotReadyStrategy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityOperationsConfig {
    pub run_id_required: bool,
    pub replay: IdentityOperationsReplayConfig,
    pub propagation_retry: IdentityPropagationRetryConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityOperationsReplayConfig {
    pub report_root_ref: Option<String>,
    pub input_root_ref: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityPropagationRetryConfig {
    pub enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityExternalRefsConfig {
    pub artifact_evidence: IdentityEndpointAdapterConfig,
    pub memory_archive: IdentityEndpointAdapterConfig,
    pub governance_basis: IdentityGovernanceAdapterConfig,
    pub work_source: IdentityWorkSourceConfig,
    pub trace_handoff: IdentityTraceHandoffConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityEndpointAdapterConfig {
    pub mode: IdentityAdapterMode,
    pub endpoint_ref: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityGovernanceAdapterConfig {
    pub mode: IdentityGovernanceMode,
    pub endpoint_ref: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityWorkSourceConfig {
    pub mode: IdentityAdapterMode,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityTraceHandoffConfig {
    pub target_ref: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityAuditConfig {
    pub sink_mode: IdentityAuditSinkMode,
    pub sink_ref: Option<String>,
    pub compensation_enabled: bool,
    pub redaction_profile: IdentityRedactionProfile,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityRedlineConfig {
    pub no_auth_in_identity: bool,
    pub ref_only_guard: bool,
    pub projection_no_write_guard: bool,
    pub outbox_no_event_creation_guard: bool,
    pub stored_replay_guard: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityFixtureConfig {
    pub clock_mode: IdentityFixtureClockMode,
    pub id_sequence_mode: IdentityFixtureIdSequenceMode,
    pub seed_ref: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityRuntimeStartupConfigPatch {
    profile: Option<IdentityProfileConfigPatch>,
    store: Option<IdentityStoreConfigPatch>,
    actor_context: Option<IdentityActorContextConfigPatch>,
    role_catalog: Option<IdentityRoleCatalogConfigPatch>,
    bus: Option<IdentityBusConfigPatch>,
    outbox: Option<IdentityOutboxConfigPatch>,
    projection: Option<IdentityProjectionConfigPatch>,
    operations: Option<IdentityOperationsConfigPatch>,
    external_refs: Option<IdentityExternalRefsConfigPatch>,
    audit: Option<IdentityAuditConfigPatch>,
    redline: Option<IdentityRedlineConfigPatch>,
    fixture: Option<IdentityFixtureConfigPatch>,
}

impl IdentityRuntimeStartupConfigPatch {
    fn apply_to(self, target: &mut IdentityRuntimeStartupConfig) {
        if let Some(profile) = self.profile {
            profile.apply_to(&mut target.profile);
        }
        if let Some(store) = self.store {
            store.apply_to(&mut target.store);
        }
        if let Some(actor_context) = self.actor_context {
            actor_context.apply_to(&mut target.actor_context);
        }
        if let Some(role_catalog) = self.role_catalog {
            role_catalog.apply_to(&mut target.role_catalog);
        }
        if let Some(bus) = self.bus {
            bus.apply_to(&mut target.bus);
        }
        if let Some(outbox) = self.outbox {
            outbox.apply_to(&mut target.outbox);
        }
        if let Some(projection) = self.projection {
            projection.apply_to(&mut target.projection);
        }
        if let Some(operations) = self.operations {
            operations.apply_to(&mut target.operations);
        }
        if let Some(external_refs) = self.external_refs {
            external_refs.apply_to(&mut target.external_refs);
        }
        if let Some(audit) = self.audit {
            audit.apply_to(&mut target.audit);
        }
        if let Some(redline) = self.redline {
            redline.apply_to(&mut target.redline);
        }
        if let Some(fixture) = self.fixture {
            fixture.apply_to(&mut target.fixture);
        }
    }
}

macro_rules! apply_option {
    ($patch:expr, $target:expr, $field:ident) => {
        if let Some(value) = $patch.$field {
            $target.$field = value;
        }
    };
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityProfileConfigPatch {
    name: Option<IdentityRuntimeProfileName>,
    adapter_mode_policy: Option<IdentityAdapterModePolicy>,
    allow_test_override: Option<bool>,
}

impl IdentityProfileConfigPatch {
    fn apply_to(self, target: &mut IdentityProfileConfig) {
        apply_option!(self, target, name);
        apply_option!(self, target, adapter_mode_policy);
        apply_option!(self, target, allow_test_override);
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityStoreConfigPatch {
    mode: Option<IdentityStoreMode>,
    dsn_ref: Option<Option<String>>,
    migration: Option<IdentityMigrationConfigPatch>,
    transaction_mode: Option<IdentityTransactionMode>,
    idempotency: Option<IdentityStoreIdempotencyConfigPatch>,
    dead_letter: Option<IdentityDeadLetterConfigPatch>,
}

impl IdentityStoreConfigPatch {
    fn apply_to(self, target: &mut IdentityStoreConfig) {
        apply_option!(self, target, mode);
        if let Some(dsn_ref) = self.dsn_ref {
            target.dsn_ref = dsn_ref;
        }
        if let Some(migration) = self.migration {
            migration.apply_to(&mut target.migration);
        }
        apply_option!(self, target, transaction_mode);
        if let Some(idempotency) = self.idempotency {
            idempotency.apply_to(&mut target.idempotency);
        }
        if let Some(dead_letter) = self.dead_letter {
            dead_letter.apply_to(&mut target.dead_letter);
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityMigrationConfigPatch {
    required_version: Option<String>,
}

impl IdentityMigrationConfigPatch {
    fn apply_to(self, target: &mut IdentityMigrationConfig) {
        apply_option!(self, target, required_version);
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityStoreIdempotencyConfigPatch {
    enabled: Option<bool>,
}

impl IdentityStoreIdempotencyConfigPatch {
    fn apply_to(self, target: &mut IdentityStoreIdempotencyConfig) {
        apply_option!(self, target, enabled);
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityDeadLetterConfigPatch {
    retention_days: Option<u32>,
}

impl IdentityDeadLetterConfigPatch {
    fn apply_to(self, target: &mut IdentityDeadLetterConfig) {
        apply_option!(self, target, retention_days);
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityActorContextConfigPatch {
    required: Option<bool>,
    require_trace_id: Option<bool>,
    trusted_context_profile: Option<IdentityTrustedContextProfile>,
    idempotency_key_required: Option<bool>,
}

impl IdentityActorContextConfigPatch {
    fn apply_to(self, target: &mut IdentityActorContextConfig) {
        apply_option!(self, target, required);
        apply_option!(self, target, require_trace_id);
        apply_option!(self, target, trusted_context_profile);
        apply_option!(self, target, idempotency_key_required);
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityRoleCatalogConfigPatch {
    source_mode: Option<IdentityAdapterMode>,
    snapshot_ref: Option<Option<String>>,
    fixture_ref: Option<Option<String>>,
    fingerprint_required: Option<bool>,
    unknown_role_strategy: Option<IdentityUnknownRoleStrategy>,
}

impl IdentityRoleCatalogConfigPatch {
    fn apply_to(self, target: &mut IdentityRoleCatalogConfig) {
        apply_option!(self, target, source_mode);
        if let Some(snapshot_ref) = self.snapshot_ref {
            target.snapshot_ref = snapshot_ref;
        }
        if let Some(fixture_ref) = self.fixture_ref {
            target.fixture_ref = fixture_ref;
        }
        apply_option!(self, target, fingerprint_required);
        apply_option!(self, target, unknown_role_strategy);
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityBusConfigPatch {
    publisher_mode: Option<IdentityAdapterMode>,
    endpoint_ref: Option<Option<String>>,
    topic_map_ref: Option<Option<String>>,
    require_known_event_kind: Option<bool>,
}

impl IdentityBusConfigPatch {
    fn apply_to(self, target: &mut IdentityBusConfig) {
        apply_option!(self, target, publisher_mode);
        if let Some(endpoint_ref) = self.endpoint_ref {
            target.endpoint_ref = endpoint_ref;
        }
        if let Some(topic_map_ref) = self.topic_map_ref {
            target.topic_map_ref = topic_map_ref;
        }
        apply_option!(self, target, require_known_event_kind);
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityOutboxConfigPatch {
    store_name: Option<String>,
    publish: Option<IdentityOutboxPublishConfigPatch>,
}

impl IdentityOutboxConfigPatch {
    fn apply_to(self, target: &mut IdentityOutboxConfig) {
        apply_option!(self, target, store_name);
        if let Some(publish) = self.publish {
            publish.apply_to(&mut target.publish);
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityOutboxPublishConfigPatch {
    batch_size: Option<u32>,
    max_attempts: Option<u32>,
    backoff_policy_ref: Option<String>,
    failure_mode: Option<IdentityOutboxFailureMode>,
}

impl IdentityOutboxPublishConfigPatch {
    fn apply_to(self, target: &mut IdentityOutboxPublishConfig) {
        apply_option!(self, target, batch_size);
        apply_option!(self, target, max_attempts);
        apply_option!(self, target, backoff_policy_ref);
        apply_option!(self, target, failure_mode);
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityProjectionConfigPatch {
    store_name: Option<String>,
    checkpoint_name: Option<String>,
    rebuild: Option<IdentityProjectionRebuildConfigPatch>,
    query: Option<IdentityProjectionQueryConfigPatch>,
}

impl IdentityProjectionConfigPatch {
    fn apply_to(self, target: &mut IdentityProjectionConfig) {
        apply_option!(self, target, store_name);
        apply_option!(self, target, checkpoint_name);
        if let Some(rebuild) = self.rebuild {
            rebuild.apply_to(&mut target.rebuild);
        }
        if let Some(query) = self.query {
            query.apply_to(&mut target.query);
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityProjectionRebuildConfigPatch {
    batch_size: Option<u32>,
}

impl IdentityProjectionRebuildConfigPatch {
    fn apply_to(self, target: &mut IdentityProjectionRebuildConfig) {
        apply_option!(self, target, batch_size);
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityProjectionQueryConfigPatch {
    not_ready_strategy: Option<IdentityProjectionNotReadyStrategy>,
}

impl IdentityProjectionQueryConfigPatch {
    fn apply_to(self, target: &mut IdentityProjectionQueryConfig) {
        apply_option!(self, target, not_ready_strategy);
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityOperationsConfigPatch {
    run_id_required: Option<bool>,
    replay: Option<IdentityOperationsReplayConfigPatch>,
    propagation_retry: Option<IdentityPropagationRetryConfigPatch>,
}

impl IdentityOperationsConfigPatch {
    fn apply_to(self, target: &mut IdentityOperationsConfig) {
        apply_option!(self, target, run_id_required);
        if let Some(replay) = self.replay {
            replay.apply_to(&mut target.replay);
        }
        if let Some(propagation_retry) = self.propagation_retry {
            propagation_retry.apply_to(&mut target.propagation_retry);
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityOperationsReplayConfigPatch {
    report_root_ref: Option<Option<String>>,
    input_root_ref: Option<Option<String>>,
}

impl IdentityOperationsReplayConfigPatch {
    fn apply_to(self, target: &mut IdentityOperationsReplayConfig) {
        if let Some(report_root_ref) = self.report_root_ref {
            target.report_root_ref = report_root_ref;
        }
        if let Some(input_root_ref) = self.input_root_ref {
            target.input_root_ref = input_root_ref;
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityPropagationRetryConfigPatch {
    enabled: Option<bool>,
}

impl IdentityPropagationRetryConfigPatch {
    fn apply_to(self, target: &mut IdentityPropagationRetryConfig) {
        apply_option!(self, target, enabled);
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityExternalRefsConfigPatch {
    artifact_evidence: Option<IdentityEndpointAdapterConfigPatch>,
    memory_archive: Option<IdentityEndpointAdapterConfigPatch>,
    governance_basis: Option<IdentityGovernanceAdapterConfigPatch>,
    work_source: Option<IdentityWorkSourceConfigPatch>,
    trace_handoff: Option<IdentityTraceHandoffConfigPatch>,
}

impl IdentityExternalRefsConfigPatch {
    fn apply_to(self, target: &mut IdentityExternalRefsConfig) {
        if let Some(artifact_evidence) = self.artifact_evidence {
            artifact_evidence.apply_to(&mut target.artifact_evidence);
        }
        if let Some(memory_archive) = self.memory_archive {
            memory_archive.apply_to(&mut target.memory_archive);
        }
        if let Some(governance_basis) = self.governance_basis {
            governance_basis.apply_to(&mut target.governance_basis);
        }
        if let Some(work_source) = self.work_source {
            work_source.apply_to(&mut target.work_source);
        }
        if let Some(trace_handoff) = self.trace_handoff {
            trace_handoff.apply_to(&mut target.trace_handoff);
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityEndpointAdapterConfigPatch {
    mode: Option<IdentityAdapterMode>,
    endpoint_ref: Option<Option<String>>,
}

impl IdentityEndpointAdapterConfigPatch {
    fn apply_to(self, target: &mut IdentityEndpointAdapterConfig) {
        apply_option!(self, target, mode);
        if let Some(endpoint_ref) = self.endpoint_ref {
            target.endpoint_ref = endpoint_ref;
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityGovernanceAdapterConfigPatch {
    mode: Option<IdentityGovernanceMode>,
    endpoint_ref: Option<Option<String>>,
}

impl IdentityGovernanceAdapterConfigPatch {
    fn apply_to(self, target: &mut IdentityGovernanceAdapterConfig) {
        apply_option!(self, target, mode);
        if let Some(endpoint_ref) = self.endpoint_ref {
            target.endpoint_ref = endpoint_ref;
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityWorkSourceConfigPatch {
    mode: Option<IdentityAdapterMode>,
}

impl IdentityWorkSourceConfigPatch {
    fn apply_to(self, target: &mut IdentityWorkSourceConfig) {
        apply_option!(self, target, mode);
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityTraceHandoffConfigPatch {
    target_ref: Option<Option<String>>,
}

impl IdentityTraceHandoffConfigPatch {
    fn apply_to(self, target: &mut IdentityTraceHandoffConfig) {
        if let Some(target_ref) = self.target_ref {
            target.target_ref = target_ref;
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityAuditConfigPatch {
    sink_mode: Option<IdentityAuditSinkMode>,
    sink_ref: Option<Option<String>>,
    compensation_enabled: Option<bool>,
    redaction_profile: Option<IdentityRedactionProfile>,
}

impl IdentityAuditConfigPatch {
    fn apply_to(self, target: &mut IdentityAuditConfig) {
        apply_option!(self, target, sink_mode);
        if let Some(sink_ref) = self.sink_ref {
            target.sink_ref = sink_ref;
        }
        apply_option!(self, target, compensation_enabled);
        apply_option!(self, target, redaction_profile);
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityRedlineConfigPatch {
    no_auth_in_identity: Option<bool>,
    ref_only_guard: Option<bool>,
    projection_no_write_guard: Option<bool>,
    outbox_no_event_creation_guard: Option<bool>,
    stored_replay_guard: Option<bool>,
}

impl IdentityRedlineConfigPatch {
    fn apply_to(self, target: &mut IdentityRedlineConfig) {
        apply_option!(self, target, no_auth_in_identity);
        apply_option!(self, target, ref_only_guard);
        apply_option!(self, target, projection_no_write_guard);
        apply_option!(self, target, outbox_no_event_creation_guard);
        apply_option!(self, target, stored_replay_guard);
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityFixtureConfigPatch {
    clock_mode: Option<IdentityFixtureClockMode>,
    id_sequence_mode: Option<IdentityFixtureIdSequenceMode>,
    seed_ref: Option<Option<String>>,
}

impl IdentityFixtureConfigPatch {
    fn apply_to(self, target: &mut IdentityFixtureConfig) {
        apply_option!(self, target, clock_mode);
        apply_option!(self, target, id_sequence_mode);
        if let Some(seed_ref) = self.seed_ref {
            target.seed_ref = seed_ref;
        }
    }
}

fn parse_patch(
    json: &str,
    source_kind: IdentityConfigSourceKind,
) -> Result<IdentityRuntimeStartupConfigPatch, Vec<IdentityConfigIssueRef>> {
    let node: JsonNode = serde_json::from_str(json)
        .map_err(|_| vec![issue(source_kind, "document", "parse-failed")])?;
    serde_json::from_value(node.into_value())
        .map_err(|_| vec![issue(source_kind, "document", "shape-invalid")])
}

fn validate_adapter_mode(
    issue_refs: &mut Vec<IdentityConfigIssueRef>,
    source_kind: IdentityConfigSourceKind,
    path: &str,
    mode: &IdentityAdapterMode,
    endpoint_ref: Option<&String>,
) {
    if mode.requires_endpoint_ref() && missing_ref(endpoint_ref) {
        issue_refs.push(issue(source_kind, path, "endpoint-ref-required"));
    }
}

fn validate_governance_mode(
    issue_refs: &mut Vec<IdentityConfigIssueRef>,
    source_kind: IdentityConfigSourceKind,
    path: &str,
    mode: &IdentityGovernanceMode,
    endpoint_ref: Option<&String>,
) {
    if mode.requires_endpoint_ref() && missing_ref(endpoint_ref) {
        issue_refs.push(issue(source_kind, path, "endpoint-ref-required"));
    }
}

fn validate_audit_mode(
    issue_refs: &mut Vec<IdentityConfigIssueRef>,
    source_kind: IdentityConfigSourceKind,
    audit: &IdentityAuditConfig,
) {
    if audit.sink_mode.requires_endpoint_ref() && missing_ref(audit.sink_ref.as_ref()) {
        issue_refs.push(issue(
            source_kind,
            "audit.sink_ref",
            "required-for-endpoint",
        ));
    }
    if !audit.compensation_enabled {
        issue_refs.push(issue(
            source_kind,
            "audit.compensation_enabled",
            "must-be-true",
        ));
    }
}

fn issue(source_kind: IdentityConfigSourceKind, path: &str, code: &str) -> IdentityConfigIssueRef {
    IdentityConfigIssueRef::new(format!("config:{}:{path}:{code}", source_kind.as_str()))
}

fn missing(value: &Option<String>) -> bool {
    value.as_ref().is_none_or(|value| value.trim().is_empty())
}

fn missing_ref(value: Option<&String>) -> bool {
    value.is_none_or(|value| value.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::{
        IdentityConfigSourceKind, IdentityRuntimeConfigSources, IdentityRuntimeStartupConfig,
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
                "publisher_mode": "fake",
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
    fn strict_json_rejects_comments_and_trailing_commas() {
        let error = IdentityRuntimeStartupConfig::from_strict_json(
            r#"{ "profile": { "name": "ci-test", }, // comment
            }"#,
            IdentityConfigSourceKind::ConfigFile,
        )
        .expect_err("strict json failure");
        assert_eq!(error.len(), 1);
        assert_eq!(error[0].as_str(), "config:file:document:parse-failed");
    }

    #[test]
    fn strict_json_rejects_duplicate_keys() {
        let error = IdentityRuntimeStartupConfig::from_strict_json(
            r#"{ "profile": { "name": "ci-test" }, "profile": { "name": "local-dev" } }"#,
            IdentityConfigSourceKind::ConfigFile,
        )
        .expect_err("duplicate keys fail");
        assert_eq!(error[0].as_str(), "config:file:document:parse-failed");
    }

    #[test]
    fn environment_invalid_does_not_fall_back_to_defaults() {
        let error = IdentityRuntimeConfigSources {
            code_defaults: valid_defaults(),
            config_file_json: None,
            environment_json: Some(
                r#"{ "bus": { "publisher_mode": "endpoint", "endpoint_ref": null } }"#.to_owned(),
            ),
        }
        .load()
        .expect_err("invalid environment");
        assert!(error.contains(&super::IdentityConfigIssueRef::new(
            "config:defaults:bus.publisher_mode:endpoint-ref-required"
        )));
    }

    #[test]
    fn overlay_merges_file_then_environment() {
        let config = IdentityRuntimeConfigSources {
            code_defaults: valid_defaults(),
            config_file_json: Some(
                r#"{ "profile": { "name": "local-dev" }, "bus": { "publisher_mode": "disabled" } }"#
                    .to_owned(),
            ),
            environment_json: Some(r#"{ "profile": { "name": "ci-test" } }"#.to_owned()),
        }
        .load()
        .expect("merged config");

        assert_eq!(config.profile.name.as_str(), "ci-test");
        assert_eq!(config.bus.publisher_mode.as_str(), "disabled");
    }

    #[test]
    fn redline_false_fails_validation() {
        let error = IdentityRuntimeConfigSources {
            code_defaults: valid_defaults(),
            config_file_json: Some(
                r#"{ "redline": { "projection_no_write_guard": false } }"#.to_owned(),
            ),
            environment_json: None,
        }
        .load()
        .expect_err("redline false");

        assert!(error.contains(&super::IdentityConfigIssueRef::new(
            "config:defaults:redline.projection_no_write_guard:must-be-true"
        )));
    }
}
