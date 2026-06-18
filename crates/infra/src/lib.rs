//! Infrastructure adapters crate for the identity workspace.
//!
//! This boundary provides an in-memory runtime skeleton that implements the
//! formal application port surface used by fake/runtime parity tests.

pub mod config;
pub mod in_memory;
pub mod runtime;

pub use crate::in_memory::{FaultCase, IdentityInMemoryRuntime, IdentityInMemoryRuntimeBuilder};
pub use crate::runtime::{
    IdentityInMemoryRuntimeAssembly, IdentityInMemoryRuntimeAssemblyBuilder,
    IdentityInMemoryRuntimeBuildOutcome,
};
