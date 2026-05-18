CREATE TABLE role_catalog_entries (
    role_id TEXT PRIMARY KEY,
    role_name TEXT NOT NULL,
    role_version TEXT NOT NULL,
    source_ref_json JSONB NOT NULL,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'deprecated', 'source_drift')),
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_role_catalog_entries_status
    ON role_catalog_entries (status);

CREATE TABLE global_members (
    global_member_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    lifecycle TEXT NOT NULL CHECK (
        lifecycle IN ('hired', 'active', 'paused', 'retired', 'tombstoned')
    ),
    main_role_id TEXT NOT NULL REFERENCES role_catalog_entries (role_id),
    secondary_role_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    capability_profile_id TEXT NULL,
    memory_refs_id TEXT NULL,
    version BIGINT NOT NULL CHECK (version >= 0),
    created_by_json JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_global_members_lifecycle
    ON global_members (lifecycle);

CREATE INDEX idx_global_members_main_role_id
    ON global_members (main_role_id);

CREATE TABLE lifecycle_history_entries (
    history_entry_id TEXT PRIMARY KEY,
    global_member_id TEXT NOT NULL REFERENCES global_members (global_member_id),
    event_type TEXT NOT NULL CHECK (
        event_type IN ('created', 'lifecycle_changed', 'tombstoned')
    ),
    from_lifecycle TEXT NULL CHECK (
        from_lifecycle IS NULL
        OR from_lifecycle IN ('hired', 'active', 'paused', 'retired', 'tombstoned')
    ),
    to_lifecycle TEXT NOT NULL CHECK (
        to_lifecycle IN ('hired', 'active', 'paused', 'retired', 'tombstoned')
    ),
    actor_json JSONB NOT NULL,
    gate_decision_ref_json JSONB NULL,
    metadata_json JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL,
    CONSTRAINT lifecycle_history_entries_tombstone_gate_check CHECK (
        event_type <> 'tombstoned'
        OR gate_decision_ref_json IS NOT NULL
    )
);

CREATE INDEX idx_lifecycle_history_entries_global_member_id
    ON lifecycle_history_entries (global_member_id);

CREATE TABLE audit_trace_entries (
    audit_trace_id TEXT PRIMARY KEY,
    trace_id TEXT NOT NULL,
    action TEXT NOT NULL,
    actor_json JSONB NULL,
    target_ref_json JSONB NULL,
    source_module TEXT NULL,
    result TEXT NOT NULL CHECK (result IN ('success', 'failed', 'skipped')),
    reason TEXT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_audit_trace_entries_trace_id
    ON audit_trace_entries (trace_id);

CREATE INDEX idx_audit_trace_entries_action_created_at_desc
    ON audit_trace_entries (action, created_at DESC);
