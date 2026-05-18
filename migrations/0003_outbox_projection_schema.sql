CREATE TABLE outbox_events (
    outbox_event_id TEXT PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('pending', 'published', 'failed')),
    retry_count INT NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    next_retry_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL,
    published_at TIMESTAMP NULL,
    failure_reason TEXT NULL
);

CREATE INDEX idx_outbox_events_status_next_retry_at
    ON outbox_events (status, next_retry_at);

CREATE INDEX idx_outbox_events_aggregate_id_created_at
    ON outbox_events (aggregate_id, created_at);

CREATE TABLE idempotency_records (
    idempotency_key TEXT PRIMARY KEY,
    scope TEXT NOT NULL CHECK (scope IN ('command', 'inbound_event', 'outbox_publish')),
    request_hash TEXT NOT NULL,
    result_ref_json JSONB NULL,
    status TEXT NOT NULL CHECK (status IN ('processing', 'succeeded', 'failed')),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_idempotency_records_scope_status_updated_at_desc
    ON idempotency_records (scope, status, updated_at DESC);

CREATE TABLE member_summary_projection (
    global_member_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    lifecycle TEXT NOT NULL CHECK (
        lifecycle IN ('hired', 'active', 'paused', 'retired', 'tombstoned')
    ),
    main_role_id TEXT NULL,
    main_role_name TEXT NULL,
    capability_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    career_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    memory_ref_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    projection_version BIGINT NOT NULL CHECK (projection_version >= 0),
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_member_summary_projection_lifecycle_main_role_id
    ON member_summary_projection (lifecycle, main_role_id);

CREATE TABLE projection_checkpoints (
    checkpoint_name TEXT PRIMARY KEY,
    last_processed_event_id TEXT NULL REFERENCES outbox_events (outbox_event_id),
    status TEXT NOT NULL CHECK (status IN ('idle', 'running', 'failed')),
    failure_reason TEXT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE inbound_dead_letters (
    dead_letter_id TEXT PRIMARY KEY,
    source_event_id TEXT NULL,
    source_module TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    failure_reason TEXT NOT NULL,
    replay_status TEXT NOT NULL CHECK (replay_status IN ('pending', 'replayed', 'ignored')),
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_inbound_dead_letters_source_event_id
    ON inbound_dead_letters (source_event_id);

CREATE INDEX idx_inbound_dead_letters_source_module_event_type_replay_status
    ON inbound_dead_letters (source_module, event_type, replay_status);
