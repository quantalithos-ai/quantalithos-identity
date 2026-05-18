CREATE TABLE pending_tombstone_flows (
    pending_flow_id TEXT PRIMARY KEY,
    global_member_id TEXT NOT NULL REFERENCES global_members (global_member_id),
    action_name TEXT NOT NULL,
    requested_by_json JSONB NOT NULL,
    requested_reason TEXT NOT NULL,
    expected_gate_decision_id TEXT NULL,
    gate_decision_ref_json JSONB NULL,
    status TEXT NOT NULL CHECK (
        status IN ('waiting_gate', 'gate_recorded', 'completed', 'cancelled')
    ),
    cancel_reason TEXT NULL,
    opened_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX uq_pending_tombstone_flows_active_member
    ON pending_tombstone_flows (global_member_id)
    WHERE status IN ('waiting_gate', 'gate_recorded');

CREATE UNIQUE INDEX uq_pending_tombstone_flows_expected_gate_decision_id
    ON pending_tombstone_flows (expected_gate_decision_id)
    WHERE expected_gate_decision_id IS NOT NULL;

CREATE INDEX idx_pending_tombstone_flows_status_updated_at_desc
    ON pending_tombstone_flows (status, updated_at DESC);
