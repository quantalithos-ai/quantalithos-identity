ALTER TABLE outbox_events
    ADD COLUMN trace_id TEXT NOT NULL DEFAULT 'trace-unknown';

UPDATE outbox_events
SET trace_id = idempotency_key
WHERE trace_id = 'trace-unknown';

ALTER TABLE outbox_events
    ALTER COLUMN trace_id DROP DEFAULT;
