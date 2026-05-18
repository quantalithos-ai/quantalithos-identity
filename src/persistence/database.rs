//! SQLx database bootstrap helpers for the persistence layer.

use sqlx::ConnectOptions;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};

use crate::config::AppConfig;
use crate::error::IdentityError;

/// Shared migration source compiled into the binary for bootstrap validation.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Creates the shared PostgreSQL pool used by later repositories and units of work.
///
/// # Errors
///
/// Returns an error if `DATABASE_URL` is missing or if SQLx cannot connect.
pub async fn connect_pool(config: &AppConfig) -> Result<PgPool, IdentityError> {
    let database_url = config
        .database_url
        .as_deref()
        .ok_or(IdentityError::MissingDatabaseUrl)?;

    let connect_options = database_url
        .parse::<PgConnectOptions>()
        .map_err(IdentityError::DatabasePool)?
        .disable_statement_logging();

    PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .connect_with(connect_options)
        .await
        .map_err(IdentityError::DatabasePool)
}

/// Applies embedded SQLx migrations against the provided database pool.
///
/// # Errors
///
/// Returns an error if the migration runner cannot apply the migration set.
pub async fn run_migrations(pool: &PgPool) -> Result<(), IdentityError> {
    MIGRATOR
        .run(pool)
        .await
        .map_err(IdentityError::DatabaseMigration)
}

#[cfg(test)]
mod tests {
    use crate::config::AppConfig;
    use crate::error::IdentityError;

    use super::connect_pool;

    #[tokio::test]
    async fn connect_pool_requires_database_url() {
        let config = AppConfig {
            listen_addr: "127.0.0.1:8080".to_string(),
            database_url: None,
            database_max_connections: 10,
        };

        let error = connect_pool(&config)
            .await
            .expect_err("missing database url should fail");

        assert!(matches!(error, IdentityError::MissingDatabaseUrl));
    }
}
