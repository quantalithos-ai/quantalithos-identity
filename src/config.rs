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
    /// Enables the embedded outbox publisher worker without affecting write-model commits.
    pub outbox_publisher_enabled: bool,
    /// Number of outbox rows scanned per publisher pass.
    pub outbox_publisher_batch_size: u32,
    /// Delay in milliseconds between embedded publisher passes.
    pub outbox_publisher_poll_interval_ms: u64,
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
                value
                    .parse::<u32>()
                    .map_err(|_| IdentityError::InvalidConfiguration {
                        key: "IDENTITY_DATABASE_MAX_CONNECTIONS".to_string(),
                        reason: "value must be a valid u32".to_string(),
                    })
            })
            .transpose()?
            .unwrap_or(10);
        let outbox_publisher_enabled =
            optional_env("IDENTITY_OUTBOX_PUBLISHER_ENABLED")?.map_or(Ok(false), parse_bool_env)?;
        let outbox_publisher_batch_size = optional_env("IDENTITY_OUTBOX_PUBLISHER_BATCH_SIZE")?
            .map(|value| parse_u32_env("IDENTITY_OUTBOX_PUBLISHER_BATCH_SIZE", &value))
            .transpose()?
            .unwrap_or(50);
        let outbox_publisher_poll_interval_ms =
            optional_env("IDENTITY_OUTBOX_PUBLISHER_POLL_INTERVAL_MS")?
                .map(|value| parse_u64_env("IDENTITY_OUTBOX_PUBLISHER_POLL_INTERVAL_MS", &value))
                .transpose()?
                .unwrap_or(1_000);

        Ok(Self {
            listen_addr,
            database_url,
            database_max_connections,
            outbox_publisher_enabled,
            outbox_publisher_batch_size,
            outbox_publisher_poll_interval_ms,
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

fn parse_bool_env(value: String) -> Result<bool, IdentityError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(IdentityError::InvalidConfiguration {
            key: "IDENTITY_OUTBOX_PUBLISHER_ENABLED".to_string(),
            reason: "value must be a valid boolean".to_string(),
        }),
    }
}

fn parse_u32_env(key: &str, value: &str) -> Result<u32, IdentityError> {
    value
        .parse::<u32>()
        .map_err(|_| IdentityError::InvalidConfiguration {
            key: key.to_string(),
            reason: "value must be a valid u32".to_string(),
        })
}

fn parse_u64_env(key: &str, value: &str) -> Result<u64, IdentityError> {
    value
        .parse::<u64>()
        .map_err(|_| IdentityError::InvalidConfiguration {
            key: key.to_string(),
            reason: "value must be a valid u64".to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::AppConfig;

    #[test]
    fn uses_default_listen_addr_when_env_is_absent() {
        unsafe {
            std::env::remove_var("IDENTITY_LISTEN_ADDR");
            std::env::remove_var("DATABASE_URL");
            std::env::remove_var("IDENTITY_DATABASE_MAX_CONNECTIONS");
            std::env::remove_var("IDENTITY_OUTBOX_PUBLISHER_ENABLED");
            std::env::remove_var("IDENTITY_OUTBOX_PUBLISHER_BATCH_SIZE");
            std::env::remove_var("IDENTITY_OUTBOX_PUBLISHER_POLL_INTERVAL_MS");
        }

        let config = AppConfig::from_env().expect("config should load");
        assert_eq!(config.listen_addr, "127.0.0.1:8080");
        assert_eq!(config.database_url, None);
        assert_eq!(config.database_max_connections, 10);
        assert!(!config.outbox_publisher_enabled);
        assert_eq!(config.outbox_publisher_batch_size, 50);
        assert_eq!(config.outbox_publisher_poll_interval_ms, 1_000);
    }
}
