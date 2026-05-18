//! Shared persistence test helpers used by database-backed integration tests.

#[cfg(test)]
use std::sync::{Arc, LazyLock};

#[cfg(test)]
use tokio::sync::Mutex;

/// Global async mutex used to serialize integration tests against the shared local database.
#[cfg(test)]
pub static DB_TEST_MUTEX: LazyLock<Arc<Mutex<()>>> = LazyLock::new(|| Arc::new(Mutex::new(())));
