//! Service configuration primitives for the bootstrap phase.

use std::env;

use crate::error::IdentityError;

/// Holds the minimum process configuration required by the service skeleton.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfig {
    /// Network address used by the future HTTP or RPC server.
    pub listen_addr: String,
    /// Optional database connection string reserved for persistence integration.
    pub database_url: Option<String>,
    /// Maximum database connections allowed in the SQLx pool.
    pub database_max_connections: u32,
}

impl AppConfig {
    /// Loads the service configuration from process environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error if a configured environment variable is present but empty.
    pub fn from_env() -> Result<Self, IdentityError> {
        let listen_addr =
            optional_env("IDENTITY_LISTEN_ADDR")?.unwrap_or_else(|| "127.0.0.1:8080".to_string());
        let database_url = optional_env("DATABASE_URL")?;
        let database_max_connections = optional_env("IDENTITY_DATABASE_MAX_CONNECTIONS")?
            .map(|value| {
                value.parse::<u32>().map_err(|_| IdentityError::InvalidConfiguration {
                    key: "IDENTITY_DATABASE_MAX_CONNECTIONS".to_string(),
                    reason: "value must be a valid u32".to_string(),
                })
            })
            .transpose()?
            .unwrap_or(10);

        Ok(Self {
            listen_addr,
            database_url,
            database_max_connections,
        })
    }
}

fn optional_env(key: &str) -> Result<Option<String>, IdentityError> {
    match env::var(key) {
        Ok(value) if value.trim().is_empty() => Err(IdentityError::InvalidConfiguration {
            key: key.to_string(),
            reason: "value must not be empty".to_string(),
        }),
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(IdentityError::InvalidConfiguration {
            key: key.to_string(),
            reason: "value must be valid UTF-8".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::AppConfig;

    #[test]
    fn uses_default_listen_addr_when_env_is_absent() {
        unsafe {
            std::env::remove_var("IDENTITY_LISTEN_ADDR");
            std::env::remove_var("DATABASE_URL");
        }

        let config = AppConfig::from_env().expect("config should load");
        assert_eq!(config.listen_addr, "127.0.0.1:8080");
        assert_eq!(config.database_url, None);
        assert_eq!(config.database_max_connections, 10);
    }
}
