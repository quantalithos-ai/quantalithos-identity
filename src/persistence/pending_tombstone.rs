//! SQLx-backed repository implementation for pending tombstone-flow records.

use sqlx::{Postgres, Row, Transaction};

use crate::application::persistence::PendingTombstoneRepository;
use crate::domain::shared::context::ActorContext;
use crate::domain::shared::ids::{GateDecisionId, GlobalMemberId, PendingFlowId};
use crate::domain::tombstone::{GateDecisionRef, PendingTombstoneFlow, PendingTombstoneFlowStatus};
use crate::error::IdentityError;

/// Pending tombstone-flow repository bound to an open SQL transaction.
pub struct SqlxPendingTombstoneRepository<'tx, 'db> {
    transaction: &'tx mut Transaction<'db, Postgres>,
}

impl<'tx, 'db> SqlxPendingTombstoneRepository<'tx, 'db> {
    /// Creates a repository facade over the provided SQL transaction.
    pub fn new(transaction: &'tx mut Transaction<'db, Postgres>) -> Self {
        Self { transaction }
    }
}

impl PendingTombstoneRepository for SqlxPendingTombstoneRepository<'_, '_> {
    async fn get_by_member_for_update(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> Result<Option<PendingTombstoneFlow>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT
                pending_flow_id,
                global_member_id,
                action_name,
                requested_by_json,
                requested_reason,
                expected_gate_decision_id,
                gate_decision_ref_json,
                status,
                cancel_reason,
                opened_at,
                updated_at
            FROM pending_tombstone_flows
            WHERE global_member_id = $1
              AND status IN ('waiting_gate', 'gate_recorded')
            ORDER BY opened_at DESC
            LIMIT 1
            FOR UPDATE
            "#,
        )
        .bind(global_member_id.as_str())
        .fetch_optional(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        row.map(map_pending_tombstone_flow_row).transpose()
    }

    async fn get_by_gate_decision(
        &mut self,
        gate_decision_id: &GateDecisionId,
    ) -> Result<Option<PendingTombstoneFlow>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT
                pending_flow_id,
                global_member_id,
                action_name,
                requested_by_json,
                requested_reason,
                expected_gate_decision_id,
                gate_decision_ref_json,
                status,
                cancel_reason,
                opened_at,
                updated_at
            FROM pending_tombstone_flows
            WHERE expected_gate_decision_id = $1
            "#,
        )
        .bind(gate_decision_id.as_str())
        .fetch_optional(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        row.map(map_pending_tombstone_flow_row).transpose()
    }

    async fn insert(&mut self, flow: &PendingTombstoneFlow) -> Result<(), IdentityError> {
        let requested_by_json = serde_json::to_value(&flow.requested_by).map_err(|error| {
            IdentityError::PersistenceData {
                message: format!("serialize requested_by actor context: {error}"),
            }
        })?;
        let gate_decision_ref_json = flow
            .gate_decision_ref
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| IdentityError::PersistenceData {
                message: format!("serialize gate decision ref: {error}"),
            })?;

        sqlx::query(
            r#"
            INSERT INTO pending_tombstone_flows (
                pending_flow_id,
                global_member_id,
                action_name,
                requested_by_json,
                requested_reason,
                expected_gate_decision_id,
                gate_decision_ref_json,
                status,
                cancel_reason,
                opened_at,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(flow.pending_flow_id.as_str())
        .bind(flow.global_member_id.as_str())
        .bind(flow.action_name.as_str())
        .bind(requested_by_json)
        .bind(flow.requested_reason.as_str())
        .bind(
            flow.expected_gate_decision_id
                .as_ref()
                .map(|value| value.as_str()),
        )
        .bind(gate_decision_ref_json)
        .bind(flow.status.as_db())
        .bind(flow.cancel_reason.as_deref())
        .bind(flow.opened_at)
        .bind(flow.updated_at)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }

    async fn save(&mut self, flow: &PendingTombstoneFlow) -> Result<(), IdentityError> {
        let requested_by_json = serde_json::to_value(&flow.requested_by).map_err(|error| {
            IdentityError::PersistenceData {
                message: format!("serialize requested_by actor context: {error}"),
            }
        })?;
        let gate_decision_ref_json = flow
            .gate_decision_ref
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| IdentityError::PersistenceData {
                message: format!("serialize gate decision ref: {error}"),
            })?;

        sqlx::query(
            r#"
            UPDATE pending_tombstone_flows
            SET
                global_member_id = $2,
                action_name = $3,
                requested_by_json = $4,
                requested_reason = $5,
                expected_gate_decision_id = $6,
                gate_decision_ref_json = $7,
                status = $8,
                cancel_reason = $9,
                opened_at = $10,
                updated_at = $11
            WHERE pending_flow_id = $1
            "#,
        )
        .bind(flow.pending_flow_id.as_str())
        .bind(flow.global_member_id.as_str())
        .bind(flow.action_name.as_str())
        .bind(requested_by_json)
        .bind(flow.requested_reason.as_str())
        .bind(
            flow.expected_gate_decision_id
                .as_ref()
                .map(|value| value.as_str()),
        )
        .bind(gate_decision_ref_json)
        .bind(flow.status.as_db())
        .bind(flow.cancel_reason.as_deref())
        .bind(flow.opened_at)
        .bind(flow.updated_at)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }
}

fn map_pending_tombstone_flow_row(
    row: sqlx::postgres::PgRow,
) -> Result<PendingTombstoneFlow, IdentityError> {
    let requested_by: ActorContext =
        serde_json::from_value(row.get("requested_by_json")).map_err(|error| {
            IdentityError::PersistenceData {
                message: format!("decode pending flow actor context: {error}"),
            }
        })?;
    let gate_decision_ref = row
        .get::<Option<serde_json::Value>, _>("gate_decision_ref_json")
        .map(serde_json::from_value::<GateDecisionRef>)
        .transpose()
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("decode pending flow gate decision ref: {error}"),
        })?;
    let status: String = row.get("status");

    Ok(PendingTombstoneFlow {
        pending_flow_id: PendingFlowId::new(row.get::<String, _>("pending_flow_id")),
        global_member_id: GlobalMemberId::new(row.get::<String, _>("global_member_id")),
        action_name: row.get("action_name"),
        requested_by,
        requested_reason: row.get("requested_reason"),
        expected_gate_decision_id: row
            .get::<Option<String>, _>("expected_gate_decision_id")
            .map(GateDecisionId::new),
        gate_decision_ref,
        status: PendingTombstoneFlowStatus::from_db(status.as_str()).ok_or_else(|| {
            IdentityError::PersistenceData {
                message: format!("invalid pending tombstone flow status `{status}`"),
            }
        })?,
        cancel_reason: row.get("cancel_reason"),
        opened_at: row.get("opened_at"),
        updated_at: row.get("updated_at"),
    })
}
