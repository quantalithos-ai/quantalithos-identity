//! Query projection records and checkpoint rows used by rebuild operations.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::PrimitiveDateTime;

use crate::domain::member::GlobalMemberLifecycle;
use crate::domain::outbox::OutboxEvent;
use crate::domain::shared::ids::{GlobalMemberId, OutboxEventId, RoleId};
use crate::error::IdentityError;

/// Represents the read-optimized member summary projection row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberSummaryProjection {
    /// Projection primary key matching the underlying global member id.
    pub global_member_id: GlobalMemberId,
    /// User-facing member display name cached for query responses.
    pub display_name: String,
    /// Lifecycle snapshot derived from the write model or outbox event stream.
    pub lifecycle: GlobalMemberLifecycle,
    /// Optional main role id summary for list and detail queries.
    pub main_role_id: Option<RoleId>,
    /// Optional cached main role display name for queries.
    pub main_role_name: Option<String>,
    /// Projection-safe capability summary JSON that excludes external truth bodies.
    pub capability_summary_json: Value,
    /// Projection-safe career summary JSON derived from append-only history.
    pub career_summary_json: Value,
    /// Projection-safe memory summary JSON that excludes memory payload bodies.
    pub memory_ref_summary_json: Value,
    /// Rebuild-controlled projection version for idempotent replay and diagnostics.
    pub projection_version: i64,
    /// Timestamp when the projection row was last refreshed.
    pub updated_at: PrimitiveDateTime,
}

impl MemberSummaryProjection {
    /// Applies a supported outbox event and returns the resulting projection snapshot when
    /// the event affects member summary reads.
    ///
    /// # Errors
    ///
    /// Returns an error when a member-summary event payload is malformed or when the event type
    /// is currently unsupported by the projection rebuild flow.
    pub fn apply_outbox_event(
        event: &OutboxEvent,
        existing_projection: Option<Self>,
        rebuilt_at: PrimitiveDateTime,
    ) -> Result<Option<Self>, IdentityError> {
        match event.event_type.as_str() {
            "identity.member.created" => {
                let payload: MemberCreatedProjectionPayload =
                    serde_json::from_value(event.payload_json.clone()).map_err(|error| {
                        IdentityError::PersistenceData {
                            message: format!(
                                "invalid member-created outbox payload for `{}`: {error}",
                                event.outbox_event_id.as_str()
                            ),
                        }
                    })?;
                let lifecycle = GlobalMemberLifecycle::from_db(payload.lifecycle.as_str()).ok_or(
                    IdentityError::PersistenceData {
                        message: format!(
                            "invalid lifecycle `{}` in member-created outbox payload for `{}`",
                            payload.lifecycle,
                            event.outbox_event_id.as_str()
                        ),
                    },
                )?;

                Ok(Some(Self {
                    global_member_id: GlobalMemberId::new(payload.global_member_id),
                    display_name: payload.display_name,
                    lifecycle,
                    main_role_id: Some(RoleId::new(payload.main_role_id)),
                    main_role_name: None,
                    capability_summary_json: json!({}),
                    career_summary_json: json!({}),
                    memory_ref_summary_json: json!({}),
                    projection_version: payload.version,
                    updated_at: rebuilt_at,
                }))
            }
            "identity.member.lifecycle_changed" | "identity.member.tombstoned" => {
                let payload: MemberCreatedProjectionPayload =
                    serde_json::from_value(event.payload_json.clone()).map_err(|error| {
                        IdentityError::PersistenceData {
                            message: format!(
                                "invalid member-lifecycle outbox payload for `{}`: {error}",
                                event.outbox_event_id.as_str()
                            ),
                        }
                    })?;
                let lifecycle = GlobalMemberLifecycle::from_db(payload.lifecycle.as_str()).ok_or(
                    IdentityError::PersistenceData {
                        message: format!(
                            "invalid lifecycle `{}` in member-lifecycle outbox payload for `{}`",
                            payload.lifecycle,
                            event.outbox_event_id.as_str()
                        ),
                    },
                )?;

                let mut projection = existing_projection.unwrap_or(Self {
                    global_member_id: GlobalMemberId::new(payload.global_member_id.clone()),
                    display_name: payload.display_name.clone(),
                    lifecycle,
                    main_role_id: Some(RoleId::new(payload.main_role_id.clone())),
                    main_role_name: None,
                    capability_summary_json: json!({}),
                    career_summary_json: json!({}),
                    memory_ref_summary_json: json!({}),
                    projection_version: payload.version,
                    updated_at: rebuilt_at,
                });
                projection.global_member_id = GlobalMemberId::new(payload.global_member_id);
                projection.display_name = payload.display_name;
                projection.lifecycle = lifecycle;
                projection.main_role_id = Some(RoleId::new(payload.main_role_id));
                if event.event_type == "identity.member.tombstoned" {
                    if let Some(memory_ref_summary_json) =
                        event.payload_json.get("memory_ref_summary_json").cloned()
                    {
                        projection.memory_ref_summary_json = memory_ref_summary_json;
                    }
                }
                projection.projection_version = payload.version;
                projection.updated_at = rebuilt_at;

                Ok(Some(projection))
            }
            "identity.capability_profile.updated" => {
                let payload: CapabilityProfileUpdatedProjectionPayload =
                    serde_json::from_value(event.payload_json.clone()).map_err(|error| {
                        IdentityError::PersistenceData {
                            message: format!(
                                "invalid capability-profile outbox payload for `{}`: {error}",
                                event.outbox_event_id.as_str()
                            ),
                        }
                    })?;
                let lifecycle = GlobalMemberLifecycle::from_db(payload.lifecycle.as_str()).ok_or(
                    IdentityError::PersistenceData {
                        message: format!(
                            "invalid lifecycle `{}` in capability-profile outbox payload for `{}`",
                            payload.lifecycle,
                            event.outbox_event_id.as_str()
                        ),
                    },
                )?;

                let mut projection = existing_projection.unwrap_or(Self {
                    global_member_id: GlobalMemberId::new(payload.global_member_id.clone()),
                    display_name: payload.display_name.clone(),
                    lifecycle,
                    main_role_id: Some(RoleId::new(payload.main_role_id.clone())),
                    main_role_name: None,
                    capability_summary_json: json!({}),
                    career_summary_json: json!({}),
                    memory_ref_summary_json: json!({}),
                    projection_version: payload.version,
                    updated_at: rebuilt_at,
                });
                projection.global_member_id = GlobalMemberId::new(payload.global_member_id);
                projection.display_name = payload.display_name;
                projection.lifecycle = lifecycle;
                projection.main_role_id = Some(RoleId::new(payload.main_role_id));
                projection.capability_summary_json = payload.capability_summary_json;
                projection.projection_version = payload.version;
                projection.updated_at = rebuilt_at;

                Ok(Some(projection))
            }
            "identity.memory_refs.updated" => {
                let payload: MemoryRefsUpdatedProjectionPayload =
                    serde_json::from_value(event.payload_json.clone()).map_err(|error| {
                        IdentityError::PersistenceData {
                            message: format!(
                                "invalid memory-refs outbox payload for `{}`: {error}",
                                event.outbox_event_id.as_str()
                            ),
                        }
                    })?;
                let lifecycle = GlobalMemberLifecycle::from_db(payload.lifecycle.as_str()).ok_or(
                    IdentityError::PersistenceData {
                        message: format!(
                            "invalid lifecycle `{}` in memory-refs outbox payload for `{}`",
                            payload.lifecycle,
                            event.outbox_event_id.as_str()
                        ),
                    },
                )?;

                let mut projection = existing_projection.unwrap_or(Self {
                    global_member_id: GlobalMemberId::new(payload.global_member_id.clone()),
                    display_name: payload.display_name.clone(),
                    lifecycle,
                    main_role_id: Some(RoleId::new(payload.main_role_id.clone())),
                    main_role_name: None,
                    capability_summary_json: json!({}),
                    career_summary_json: json!({}),
                    memory_ref_summary_json: json!({}),
                    projection_version: payload.version,
                    updated_at: rebuilt_at,
                });
                projection.global_member_id = GlobalMemberId::new(payload.global_member_id);
                projection.display_name = payload.display_name;
                projection.lifecycle = lifecycle;
                projection.main_role_id = Some(RoleId::new(payload.main_role_id));
                projection.memory_ref_summary_json = payload.memory_ref_summary_json;
                projection.projection_version = payload.version;
                projection.updated_at = rebuilt_at;

                Ok(Some(projection))
            }
            "identity.career_history.appended" => {
                let payload: CareerHistoryAppendedProjectionPayload =
                    serde_json::from_value(event.payload_json.clone()).map_err(|error| {
                        IdentityError::PersistenceData {
                            message: format!(
                                "invalid career-history outbox payload for `{}`: {error}",
                                event.outbox_event_id.as_str()
                            ),
                        }
                    })?;
                let lifecycle = GlobalMemberLifecycle::from_db(payload.lifecycle.as_str()).ok_or(
                    IdentityError::PersistenceData {
                        message: format!(
                            "invalid lifecycle `{}` in career-history outbox payload for `{}`",
                            payload.lifecycle,
                            event.outbox_event_id.as_str()
                        ),
                    },
                )?;

                let mut projection = existing_projection.unwrap_or(Self {
                    global_member_id: GlobalMemberId::new(payload.global_member_id.clone()),
                    display_name: payload.display_name.clone(),
                    lifecycle,
                    main_role_id: Some(RoleId::new(payload.main_role_id.clone())),
                    main_role_name: None,
                    capability_summary_json: json!({}),
                    career_summary_json: json!({}),
                    memory_ref_summary_json: json!({}),
                    projection_version: payload.version,
                    updated_at: rebuilt_at,
                });
                projection.global_member_id = GlobalMemberId::new(payload.global_member_id);
                projection.display_name = payload.display_name;
                projection.lifecycle = lifecycle;
                projection.main_role_id = Some(RoleId::new(payload.main_role_id));
                projection.career_summary_json = payload.career_summary_json;
                projection.projection_version = payload.version;
                projection.updated_at = rebuilt_at;

                Ok(Some(projection))
            }
            "identity.role_catalog.synced" | "identity.gate_decision.recorded" => Ok(None),
            other => Err(IdentityError::PersistenceData {
                message: format!(
                    "unsupported member summary projection event type `{other}` for `{}`",
                    event.outbox_event_id.as_str()
                ),
            }),
        }
    }
}

/// Enumerates the states allowed for projection checkpoint rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionCheckpointStatus {
    /// No rebuild worker currently owns or is failing this checkpoint.
    Idle,
    /// A rebuild worker is actively advancing the checkpoint.
    Running,
    /// The most recent rebuild attempt failed and requires recovery.
    Failed,
}

impl ProjectionCheckpointStatus {
    /// Parses the persisted database string into the typed checkpoint status enum.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "idle" => Some(Self::Idle),
            "running" => Some(Self::Running),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    /// Returns the canonical persisted string for the checkpoint status enum.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Failed => "failed",
        }
    }
}

/// Represents a durable rebuild checkpoint stored in `projection_checkpoints`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionCheckpoint {
    /// Stable checkpoint name, usually the projection or job identifier.
    pub checkpoint_name: String,
    /// Most recent outbox event processed successfully by the rebuild workflow.
    pub last_processed_event_id: Option<OutboxEventId>,
    /// Current rebuild status for the checkpoint row.
    pub status: ProjectionCheckpointStatus,
    /// Optional failure reason for the most recent rebuild error.
    pub failure_reason: Option<String>,
    /// Timestamp when the checkpoint row was last updated.
    pub updated_at: PrimitiveDateTime,
}

impl ProjectionCheckpoint {
    /// Creates a brand-new idle checkpoint for the provided operations name.
    pub fn initial(checkpoint_name: impl Into<String>, updated_at: PrimitiveDateTime) -> Self {
        Self {
            checkpoint_name: checkpoint_name.into(),
            last_processed_event_id: None,
            status: ProjectionCheckpointStatus::Idle,
            failure_reason: None,
            updated_at,
        }
    }

    /// Marks the checkpoint as actively running and clears any previous failure evidence.
    pub fn mark_running(&mut self, updated_at: PrimitiveDateTime) {
        self.status = ProjectionCheckpointStatus::Running;
        self.failure_reason = None;
        self.updated_at = updated_at;
    }

    /// Marks the checkpoint as idle after a rebuild pass finishes.
    pub fn mark_idle(&mut self, updated_at: PrimitiveDateTime) {
        self.status = ProjectionCheckpointStatus::Idle;
        self.failure_reason = None;
        self.updated_at = updated_at;
    }

    /// Advances the checkpoint after one outbox event has been applied successfully.
    pub fn advance_to(&mut self, outbox_event_id: OutboxEventId, updated_at: PrimitiveDateTime) {
        self.last_processed_event_id = Some(outbox_event_id);
        self.status = ProjectionCheckpointStatus::Running;
        self.failure_reason = None;
        self.updated_at = updated_at;
    }

    /// Records a rebuild failure without moving the last successful event cursor.
    pub fn mark_failed(
        &mut self,
        failure_reason: impl Into<String>,
        updated_at: PrimitiveDateTime,
    ) {
        self.status = ProjectionCheckpointStatus::Failed;
        self.failure_reason = Some(failure_reason.into());
        self.updated_at = updated_at;
    }
}

#[derive(Debug, Deserialize)]
struct MemberCreatedProjectionPayload {
    global_member_id: String,
    display_name: String,
    lifecycle: String,
    main_role_id: String,
    version: i64,
}

#[derive(Debug, Deserialize)]
struct CapabilityProfileUpdatedProjectionPayload {
    global_member_id: String,
    display_name: String,
    lifecycle: String,
    main_role_id: String,
    capability_summary_json: Value,
    version: i64,
}

#[derive(Debug, Deserialize)]
struct MemoryRefsUpdatedProjectionPayload {
    global_member_id: String,
    display_name: String,
    lifecycle: String,
    main_role_id: String,
    memory_ref_summary_json: Value,
    version: i64,
}

#[derive(Debug, Deserialize)]
struct CareerHistoryAppendedProjectionPayload {
    global_member_id: String,
    display_name: String,
    lifecycle: String,
    main_role_id: String,
    career_summary_json: Value,
    version: i64,
}
