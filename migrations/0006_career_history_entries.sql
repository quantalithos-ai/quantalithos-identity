CREATE TABLE career_history_entries (
    career_entry_id TEXT PRIMARY KEY,
    global_member_id TEXT NOT NULL REFERENCES global_members (global_member_id),
    source_event_id TEXT NOT NULL UNIQUE,
    source_module TEXT NOT NULL CHECK (source_module IN ('work', 'process')),
    project_id TEXT NULL,
    work_ref_json JSONB NULL,
    process_ref_json JSONB NULL,
    entry_kind TEXT NOT NULL,
    started_at TIMESTAMP NULL,
    ended_at TIMESTAMP NULL,
    payload_summary_json JSONB NULL,
    created_at TIMESTAMP NOT NULL,
    CONSTRAINT career_history_entries_source_ref_check CHECK (
        (work_ref_json IS NOT NULL AND process_ref_json IS NULL)
        OR (work_ref_json IS NULL AND process_ref_json IS NOT NULL)
    ),
    CONSTRAINT career_history_entries_time_range_check CHECK (
        ended_at IS NULL OR started_at IS NULL OR ended_at >= started_at
    )
);

CREATE INDEX idx_career_history_entries_global_member_created_at_desc
    ON career_history_entries (global_member_id, created_at DESC);

CREATE INDEX idx_career_history_entries_source_event_id
    ON career_history_entries (source_event_id);
