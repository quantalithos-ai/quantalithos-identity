//! Append-only career history records derived from external work and process facts.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::PrimitiveDateTime;

use crate::domain::shared::ids::{CareerEntryId, EventId, GlobalMemberId, ProjectId};
use crate::error::IdentityError;

/// Stable source-module marker used for work-derived career entries.
pub const SOURCE_MODULE_WORK: &str = "work";
/// Stable source-module marker used for process-derived career entries.
pub const SOURCE_MODULE_PROCESS: &str = "process";

/// Ref-only pointer to a work-domain fact retained by identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkRef {
    /// Stable work-domain identifier.
    pub work_id: String,
    /// Work kind or collection identifier.
    pub work_kind: String,
    /// Optional immutable work revision reference.
    pub work_version: Option<String>,
}

impl WorkRef {
    /// Returns an error when the work ref is missing required fields.
    pub fn validate(&self) -> Result<(), IdentityError> {
        if self.work_id.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "work_id must not be blank".to_string(),
            });
        }
        if self.work_kind.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "work_kind must not be blank".to_string(),
            });
        }

        Ok(())
    }
}

/// Ref-only pointer to a process-domain fact retained by identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessRef {
    /// Stable process-domain identifier.
    pub process_id: String,
    /// Process kind or collection identifier.
    pub process_kind: String,
    /// Optional immutable process revision reference.
    pub process_version: Option<String>,
}

impl ProcessRef {
    /// Returns an error when the process ref is missing required fields.
    pub fn validate(&self) -> Result<(), IdentityError> {
        if self.process_id.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "process_id must not be blank".to_string(),
            });
        }
        if self.process_kind.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "process_kind must not be blank".to_string(),
            });
        }

        Ok(())
    }
}

/// Minimal work-fact payload consumable by identity for career history updates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkFactEvent {
    /// Member referenced by the upstream work fact.
    pub global_member_id: GlobalMemberId,
    /// Optional project reference retained as weak identity-local context.
    pub project_id: Option<ProjectId>,
    /// Ref-only pointer back to the originating work fact.
    pub work_ref: WorkRef,
    /// Upstream semantic kind such as `assigned` or `completed`.
    pub entry_kind: String,
    /// Optional upstream start timestamp that identity must not infer.
    pub started_at: Option<PrimitiveDateTime>,
    /// Optional upstream end timestamp that identity must not infer.
    pub ended_at: Option<PrimitiveDateTime>,
    /// Optional minimal JSON summary used for read models and audit context.
    pub payload_summary: Option<Value>,
}

/// Minimal process-fact payload consumable by identity for career history updates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessFactEvent {
    /// Member referenced by the upstream process fact.
    pub global_member_id: GlobalMemberId,
    /// Optional project reference retained as weak identity-local context.
    pub project_id: Option<ProjectId>,
    /// Ref-only pointer back to the originating process fact.
    pub process_ref: ProcessRef,
    /// Upstream semantic kind such as `participated` or `reviewed`.
    pub entry_kind: String,
    /// Optional upstream start timestamp that identity must not infer.
    pub started_at: Option<PrimitiveDateTime>,
    /// Optional upstream end timestamp that identity must not infer.
    pub ended_at: Option<PrimitiveDateTime>,
    /// Optional minimal JSON summary used for read models and audit context.
    pub payload_summary: Option<Value>,
}

/// One append-only career fact retained locally by identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CareerEntry {
    /// Stable career entry id derived by identity.
    pub career_entry_id: CareerEntryId,
    /// Member owning the appended career fact.
    pub global_member_id: GlobalMemberId,
    /// Stable upstream event id used for idempotency and traceability.
    pub source_event_id: EventId,
    /// Source module retained for diagnostics and persistence.
    pub source_module: String,
    /// Optional project reference carried through from the source fact.
    pub project_id: Option<ProjectId>,
    /// Optional work ref when the source fact originated from the work domain.
    pub work_ref: Option<WorkRef>,
    /// Optional process ref when the source fact originated from the process domain.
    pub process_ref: Option<ProcessRef>,
    /// Stable fact kind retained as source-controlled text.
    pub entry_kind: String,
    /// Optional upstream start timestamp.
    pub started_at: Option<PrimitiveDateTime>,
    /// Optional upstream end timestamp.
    pub ended_at: Option<PrimitiveDateTime>,
    /// Optional minimal summary JSON retained for projection and audit use.
    pub payload_summary: Option<Value>,
    /// Timestamp when identity appended this entry.
    pub created_at: PrimitiveDateTime,
}

impl CareerEntry {
    /// Builds a career entry from a work-domain fact event.
    pub fn from_work_event(
        career_entry_id: CareerEntryId,
        source_event_id: EventId,
        event: WorkFactEvent,
        created_at: PrimitiveDateTime,
    ) -> Result<Self, IdentityError> {
        event.work_ref.validate()?;
        validate_entry_kind(&event.entry_kind)?;
        validate_time_range(event.started_at, event.ended_at)?;

        Ok(Self {
            career_entry_id,
            global_member_id: event.global_member_id,
            source_event_id,
            source_module: SOURCE_MODULE_WORK.to_string(),
            project_id: event.project_id,
            work_ref: Some(event.work_ref),
            process_ref: None,
            entry_kind: event.entry_kind.trim().to_string(),
            started_at: event.started_at,
            ended_at: event.ended_at,
            payload_summary: event.payload_summary,
            created_at,
        })
    }

    /// Builds a career entry from a process-domain fact event.
    pub fn from_process_event(
        career_entry_id: CareerEntryId,
        source_event_id: EventId,
        event: ProcessFactEvent,
        created_at: PrimitiveDateTime,
    ) -> Result<Self, IdentityError> {
        event.process_ref.validate()?;
        validate_entry_kind(&event.entry_kind)?;
        validate_time_range(event.started_at, event.ended_at)?;

        Ok(Self {
            career_entry_id,
            global_member_id: event.global_member_id,
            source_event_id,
            source_module: SOURCE_MODULE_PROCESS.to_string(),
            project_id: event.project_id,
            work_ref: None,
            process_ref: Some(event.process_ref),
            entry_kind: event.entry_kind.trim().to_string(),
            started_at: event.started_at,
            ended_at: event.ended_at,
            payload_summary: event.payload_summary,
            created_at,
        })
    }

    /// Returns true when the entry was produced by the same upstream source event.
    pub fn same_source_event(&self, event_id: &EventId) -> bool {
        &self.source_event_id == event_id
    }

    /// Returns a projection-safe JSON summary of the career entry.
    pub fn summarize(&self) -> Value {
        json!({
            "career_entry_id": self.career_entry_id.as_str(),
            "source_event_id": self.source_event_id.as_str(),
            "source_module": self.source_module,
            "project_id": self.project_id.as_ref().map(ProjectId::as_str),
            "work_ref": self.work_ref,
            "process_ref": self.process_ref,
            "entry_kind": self.entry_kind,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "payload_summary": self.payload_summary,
            "created_at": self.created_at,
        })
    }
}

/// Append-only collection of one member's career entries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CareerHistory {
    /// Member owning this append-only history collection.
    pub global_member_id: GlobalMemberId,
    /// Chronological append-only list of retained career entries.
    pub entries: Vec<CareerEntry>,
}

impl CareerHistory {
    /// Creates an empty append-only history for one member.
    pub fn empty_for_member(global_member_id: GlobalMemberId) -> Self {
        Self {
            global_member_id,
            entries: Vec::new(),
        }
    }

    /// Rehydrates an append-only history from persisted entries.
    pub fn rehydrate(
        global_member_id: GlobalMemberId,
        mut entries: Vec<CareerEntry>,
    ) -> Result<Self, IdentityError> {
        for entry in &entries {
            if entry.global_member_id != global_member_id {
                return Err(IdentityError::PersistenceData {
                    message: format!(
                        "career entry `{}` does not belong to member `{}`",
                        entry.career_entry_id.as_str(),
                        global_member_id.as_str()
                    ),
                });
            }
        }
        entries.sort_by_key(|entry| entry.created_at);

        Ok(Self {
            global_member_id,
            entries,
        })
    }

    /// Returns true when a source event was already appended into this history.
    pub fn contains_source_event(&self, event_id: &EventId) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.same_source_event(event_id))
    }

    /// Appends one new career entry while preserving append-only and member-boundary rules.
    pub fn append(&mut self, entry: CareerEntry) -> Result<(), IdentityError> {
        if entry.global_member_id != self.global_member_id {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: format!(
                    "career entry `{}` cannot be appended to member `{}`",
                    entry.career_entry_id.as_str(),
                    self.global_member_id.as_str()
                ),
            });
        }
        if self.contains_source_event(&entry.source_event_id) {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_IDEMPOTENCY_CONFLICT",
                message: format!(
                    "career entry source event `{}` was already appended",
                    entry.source_event_id.as_str()
                ),
            });
        }

        self.entries.push(entry);
        Ok(())
    }

    /// Returns the current append-only entry count as a projection-friendly version value.
    pub fn version(&self) -> i64 {
        self.entries.len() as i64
    }

    /// Returns the timestamp of the most recently appended career entry.
    pub fn latest_created_at(&self) -> Option<PrimitiveDateTime> {
        self.entries.last().map(|entry| entry.created_at)
    }

    /// Returns a projection-safe JSON summary of the current history.
    pub fn summary_json(&self) -> Value {
        json!({
            "global_member_id": self.global_member_id.as_str(),
            "entry_count": self.entries.len(),
            "entries": self.entries.iter().map(CareerEntry::summarize).collect::<Vec<_>>(),
        })
    }
}

fn validate_entry_kind(entry_kind: &str) -> Result<(), IdentityError> {
    if entry_kind.trim().is_empty() {
        return Err(IdentityError::RuleViolation {
            code: "IDENTITY_INVALID_ARGUMENT",
            message: "entry_kind must not be blank".to_string(),
        });
    }

    Ok(())
}

fn validate_time_range(
    started_at: Option<PrimitiveDateTime>,
    ended_at: Option<PrimitiveDateTime>,
) -> Result<(), IdentityError> {
    if let (Some(started_at), Some(ended_at)) = (started_at, ended_at)
        && ended_at < started_at
    {
        return Err(IdentityError::RuleViolation {
            code: "IDENTITY_INVALID_ARGUMENT",
            message: "ended_at must not be earlier than started_at".to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use time::macros::datetime;

    use super::{CareerEntry, CareerHistory, ProcessFactEvent, ProcessRef, WorkFactEvent, WorkRef};
    use crate::domain::shared::ids::{CareerEntryId, EventId, GlobalMemberId, ProjectId};
    use crate::error::IdentityError;

    #[test]
    fn from_process_event_sets_process_ref_only() {
        let entry = CareerEntry::from_process_event(
            CareerEntryId::new("career-entry:event-1"),
            EventId::new("event-1"),
            ProcessFactEvent {
                global_member_id: GlobalMemberId::new("member-1"),
                project_id: Some(ProjectId::new("project-1")),
                process_ref: ProcessRef {
                    process_id: "process-1".to_string(),
                    process_kind: "activity".to_string(),
                    process_version: Some("v1".to_string()),
                },
                entry_kind: "reviewed".to_string(),
                started_at: Some(datetime!(2026-05-18 09:00:00)),
                ended_at: Some(datetime!(2026-05-18 10:00:00)),
                payload_summary: None,
            },
            datetime!(2026-05-18 10:30:00),
        )
        .expect("process fact event should build a career entry");

        assert!(entry.work_ref.is_none());
        assert!(entry.process_ref.is_some());
        assert_eq!(entry.source_module, "process");
    }

    #[test]
    fn append_rejects_duplicate_source_event() {
        let member_id = GlobalMemberId::new("member-2");
        let mut history = CareerHistory::empty_for_member(member_id.clone());
        let entry = CareerEntry::from_work_event(
            CareerEntryId::new("career-entry:event-2"),
            EventId::new("event-2"),
            WorkFactEvent {
                global_member_id: member_id.clone(),
                project_id: None,
                work_ref: WorkRef {
                    work_id: "work-2".to_string(),
                    work_kind: "task".to_string(),
                    work_version: None,
                },
                entry_kind: "assigned".to_string(),
                started_at: None,
                ended_at: None,
                payload_summary: None,
            },
            datetime!(2026-05-18 11:00:00),
        )
        .expect("work fact event should build a career entry");

        history
            .append(entry.clone())
            .expect("first append should succeed");
        let error = history
            .append(entry)
            .expect_err("duplicate source event should be rejected");

        assert!(matches!(
            error,
            IdentityError::RuleViolation {
                code: "IDENTITY_IDEMPOTENCY_CONFLICT",
                ..
            }
        ));
    }

    #[test]
    fn from_work_event_rejects_invalid_time_range() {
        let error = CareerEntry::from_work_event(
            CareerEntryId::new("career-entry:event-3"),
            EventId::new("event-3"),
            WorkFactEvent {
                global_member_id: GlobalMemberId::new("member-3"),
                project_id: None,
                work_ref: WorkRef {
                    work_id: "work-3".to_string(),
                    work_kind: "task".to_string(),
                    work_version: None,
                },
                entry_kind: "completed".to_string(),
                started_at: Some(datetime!(2026-05-18 13:00:00)),
                ended_at: Some(datetime!(2026-05-18 12:00:00)),
                payload_summary: None,
            },
            datetime!(2026-05-18 13:05:00),
        )
        .expect_err("invalid time range should be rejected");

        assert!(matches!(
            error,
            IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                ..
            }
        ));
    }
}
