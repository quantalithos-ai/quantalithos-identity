CREATE TABLE capability_profiles (
    capability_profile_id TEXT PRIMARY KEY,
    global_member_id TEXT NOT NULL UNIQUE REFERENCES global_members (global_member_id),
    capabilities_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    evidence_refs_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    version BIGINT NOT NULL CHECK (version >= 0),
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_capability_profiles_global_member_id
    ON capability_profiles (global_member_id);
