//! Standardized inbound event DTOs and parsers for external facts entering identity.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::PrimitiveDateTime;

use crate::domain::shared::ids::{EventId, RoleId};
use crate::error::IdentityError;

/// Normalized inbound event envelope used before dispatching into application services.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InboundEventEnvelope {
    /// Stable source event id used as the inbound idempotency key.
    pub source_event_id: EventId,
    /// Stable source module identifier such as `method-library`.
    pub source_module: String,
    /// Source event type string retained for routing and diagnostics.
    pub event_type: String,
    /// Timestamp when the source event originally occurred.
    pub occurred_at: PrimitiveDateTime,
    /// Stable hash of the inbound payload used for idempotency conflict detection.
    pub payload_hash: String,
    /// Raw payload snapshot passed to the event-specific parser.
    pub payload: Value,
}

/// Minimal method-library role snapshot consumable by identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoleDefinitionSnapshot {
    /// Stable role id used as the local role catalog primary key.
    pub role_id: RoleId,
    /// Display name cached locally for query and validation flows.
    pub role_name: String,
    /// Source role version retained for compatibility and reconciliation.
    pub role_version: String,
    /// Upstream source reference stored as ref-only JSON.
    pub source_ref: Value,
    /// Source fingerprint used for drift detection.
    pub fingerprint: String,
    /// Source status mapped onto local role catalog status.
    pub status: String,
}

/// Raw inbound role-catalog event emitted by method-library.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InboundRoleCatalogEvent {
    /// Standardized envelope metadata extracted by the transport boundary.
    pub envelope: InboundEventEnvelope,
}

/// Parses role-catalog event payloads into the minimal identity-local snapshot view.
#[derive(Debug, Default, Clone, Copy)]
pub struct RoleCatalogEventParser;

impl RoleCatalogEventParser {
    /// Parses the inbound payload into a validated role definition snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload is malformed or missing `role_snapshot`.
    pub fn parse(&self, payload: Value) -> Result<RoleDefinitionSnapshot, IdentityError> {
        let role_snapshot =
            payload
                .get("role_snapshot")
                .cloned()
                .ok_or(IdentityError::InvalidConfiguration {
                    key: "InboundRoleCatalogEvent.payload.role_snapshot".to_string(),
                    reason: "field is required".to_string(),
                })?;

        serde_json::from_value(role_snapshot).map_err(|error| IdentityError::PersistenceData {
            message: format!("decode role catalog snapshot payload: {error}"),
        })
    }
}
