CREATE TABLE memory_refs (
    memory_refs_id TEXT PRIMARY KEY,
    global_member_id TEXT NOT NULL UNIQUE REFERENCES global_members (global_member_id),
    semantic_memory_ref_json JSONB NULL,
    episodic_memory_refs_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    archive_ref_json JSONB NULL,
    archive_status TEXT NOT NULL CHECK (archive_status IN ('none', 'pending', 'archived', 'failed')),
    version BIGINT NOT NULL CHECK (version >= 0),
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_memory_refs_global_member_id
    ON memory_refs (global_member_id);

CREATE INDEX idx_memory_refs_archive_status
    ON memory_refs (archive_status);
