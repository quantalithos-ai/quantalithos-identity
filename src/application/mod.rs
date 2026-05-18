//! Application layer modules for repository ports and use-case orchestration.

pub mod member_lifecycle;
pub mod persistence;
pub mod query_projection;
pub mod role_catalog_sync;

#[cfg(test)]
mod p0_smoke;
