//! Memory refs aggregate and ref-only archive collaboration types owned by identity.

use serde::{Deserialize, Serialize};
use serde_json::json;
use time::{OffsetDateTime, PrimitiveDateTime};

use crate::domain::shared::context::ActorContext;
use crate::domain::shared::ids::{GlobalMemberId, MemoryRefsId};
use crate::error::IdentityError;

/// Ref-only pointer to semantic or episodic memory retained by identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryRef {
    /// Stable memory identifier owned by the memory domain.
    pub memory_id: String,
    /// Memory kind or collection identifier used for downstream routing.
    pub memory_kind: String,
    /// Optional immutable version or revision reference.
    pub memory_version: Option<String>,
}

impl MemoryRef {
    /// Returns an error when the ref is missing required fields.
    pub fn validate(&self) -> Result<(), IdentityError> {
        if self.memory_id.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "memory_id must not be blank".to_string(),
            });
        }
        if self.memory_kind.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "memory_kind must not be blank".to_string(),
            });
        }

        Ok(())
    }
}

/// Ref-only pointer to an archive result retained by identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveRef {
    /// Stable archive identifier owned by the archive domain.
    pub archive_id: String,
    /// Archive kind or collection identifier used for downstream routing.
    pub archive_kind: String,
    /// Optional immutable version or revision reference.
    pub archive_version: Option<String>,
}

impl ArchiveRef {
    /// Returns an error when the ref is missing required fields.
    pub fn validate(&self) -> Result<(), IdentityError> {
        if self.archive_id.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "archive_id must not be blank".to_string(),
            });
        }
        if self.archive_kind.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "archive_kind must not be blank".to_string(),
            });
        }

        Ok(())
    }
}

/// Enumerates the archive states retained by the memory refs aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveStatus {
    /// No archive collaboration has been started.
    None,
    /// Archive collaboration has been requested but is not complete yet.
    Pending,
    /// Archive collaboration finished successfully.
    Archived,
    /// Archive collaboration finished unsuccessfully.
    Failed,
}

impl ArchiveStatus {
    /// Parses the persisted database string into the strongly-typed archive status enum.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "pending" => Some(Self::Pending),
            "archived" => Some(Self::Archived),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    /// Returns the canonical persisted string for the archive status enum.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Pending => "pending",
            Self::Archived => "archived",
            Self::Failed => "failed",
        }
    }
}

/// Write-model aggregate that stores ref-only memory and archive pointers for one member.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryRefs {
    /// Stable memory refs id owned by identity.
    pub memory_refs_id: MemoryRefsId,
    /// Member owning this memory refs aggregate.
    pub global_member_id: GlobalMemberId,
    /// Optional semantic memory ref for the member.
    pub semantic_memory_ref: Option<MemoryRef>,
    /// Append-only collection of episodic memory refs.
    pub episodic_memory_refs: Vec<MemoryRef>,
    /// Optional archive ref written after archive collaboration completes or starts.
    pub archive_ref: Option<ArchiveRef>,
    /// Current archive collaboration status.
    pub archive_status: ArchiveStatus,
    /// Optimistic-lock version incremented after each successful update.
    pub version: i64,
    /// Timestamp when the aggregate was last updated.
    pub updated_at: PrimitiveDateTime,
}

/// Command payload used to update one member memory refs aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateMemoryRefsCommand {
    /// Target member id.
    pub global_member_id: GlobalMemberId,
    /// Optional semantic memory ref to replace.
    pub semantic_memory_ref: Option<MemoryRef>,
    /// Episodic memory refs to append.
    pub episodic_memory_refs: Vec<MemoryRef>,
}

/// Summary returned by memory refs write paths.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryRefsSummary {
    /// Stable memory refs id.
    pub memory_refs_id: MemoryRefsId,
    /// Target member id.
    pub global_member_id: GlobalMemberId,
    /// Current semantic memory ref when present.
    pub semantic_memory_ref: Option<MemoryRef>,
    /// Current episodic memory refs.
    pub episodic_memory_refs: Vec<MemoryRef>,
    /// Current archive ref when present.
    pub archive_ref: Option<ArchiveRef>,
    /// Current archive collaboration status.
    pub archive_status: ArchiveStatus,
    /// Current memory refs version.
    pub version: i64,
}

impl MemoryRefs {
    /// Creates an empty memory refs aggregate for the provided member.
    pub fn create_empty(global_member_id: GlobalMemberId) -> Self {
        let now = current_timestamp();

        Self {
            memory_refs_id: MemoryRefsId::new(format!("memory-refs:{}", global_member_id.as_str())),
            global_member_id,
            semantic_memory_ref: None,
            episodic_memory_refs: Vec::new(),
            archive_ref: None,
            archive_status: ArchiveStatus::None,
            version: 0,
            updated_at: now,
        }
    }

    /// Replaces the semantic memory ref for the member.
    pub fn replace_semantic_ref(
        &mut self,
        memory_ref: MemoryRef,
        _actor: &ActorContext,
    ) -> Result<(), IdentityError> {
        memory_ref.validate()?;

        self.semantic_memory_ref = Some(memory_ref);
        self.touch();
        Ok(())
    }

    /// Appends a new episodic memory ref when it is not already retained.
    pub fn add_episodic_ref(
        &mut self,
        memory_ref: MemoryRef,
        _actor: &ActorContext,
    ) -> Result<(), IdentityError> {
        memory_ref.validate()?;

        if self
            .episodic_memory_refs
            .iter()
            .all(|existing| existing != &memory_ref)
        {
            self.episodic_memory_refs.push(memory_ref);
            self.touch();
        }
        Ok(())
    }

    /// Marks the memory refs aggregate as waiting on archive completion.
    pub fn mark_archive_pending(&mut self, archive_ref: ArchiveRef) -> Result<(), IdentityError> {
        archive_ref.validate()?;

        self.archive_ref = Some(archive_ref);
        self.archive_status = ArchiveStatus::Pending;
        self.touch();
        Ok(())
    }

    /// Marks the memory refs aggregate as archived successfully.
    pub fn mark_archived(&mut self, archive_ref: ArchiveRef) -> Result<(), IdentityError> {
        archive_ref.validate()?;

        self.archive_ref = Some(archive_ref);
        self.archive_status = ArchiveStatus::Archived;
        self.touch();
        Ok(())
    }

    /// Returns the command-side summary for the current memory refs aggregate.
    pub fn summary(&self) -> MemoryRefsSummary {
        MemoryRefsSummary {
            memory_refs_id: self.memory_refs_id.clone(),
            global_member_id: self.global_member_id.clone(),
            semantic_memory_ref: self.semantic_memory_ref.clone(),
            episodic_memory_refs: self.episodic_memory_refs.clone(),
            archive_ref: self.archive_ref.clone(),
            archive_status: self.archive_status,
            version: self.version,
        }
    }

    /// Returns a projection-safe JSON summary of the current memory refs aggregate.
    pub fn summary_json(&self) -> serde_json::Value {
        json!({
            "memory_refs_id": self.memory_refs_id.as_str(),
            "semantic_memory_ref": self.semantic_memory_ref,
            "episodic_memory_refs": self.episodic_memory_refs,
            "archive_ref": self.archive_ref,
            "archive_status": self.archive_status.as_db(),
            "version": self.version,
        })
    }

    fn touch(&mut self) {
        self.version += 1;
        self.updated_at = current_timestamp();
    }
}

fn current_timestamp() -> PrimitiveDateTime {
    let now = OffsetDateTime::now_utc();
    PrimitiveDateTime::new(now.date(), now.time())
}
