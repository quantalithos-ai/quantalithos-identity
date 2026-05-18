ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_status_check;

ALTER TABLE outbox_events
    ADD CONSTRAINT outbox_events_status_check
    CHECK (status IN ('pending', 'published', 'failed', 'dead'));
