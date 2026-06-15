//! Domain model crate for the identity workspace.
//!
//! This crate holds the core truth, state, policy, and guard foundations.

pub mod audit;
pub mod career;
pub mod errors;
pub mod handoff;
pub mod lifecycle;
pub mod member_identity;
pub mod memory_reference;
pub mod outbox;
pub mod projection_state;
pub mod reconciliation;
pub mod reference_state;
pub mod role_capability;
pub mod trace;
