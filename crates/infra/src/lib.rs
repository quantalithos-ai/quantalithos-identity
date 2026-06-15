//! Infrastructure adapters crate for the identity workspace.
//!
//! This boundary provides an in-memory runtime skeleton that implements the
//! formal application port surface used by fake/runtime parity tests.

pub mod in_memory;

pub use crate::in_memory::{FaultCase, IdentityInMemoryRuntime, IdentityInMemoryRuntimeBuilder};
