//! Binary entrypoint for the quantalithos-identity service.

use quantalithos_identity::config::AppConfig;
use quantalithos_identity::inbound::http::{HttpAppState, router};
use quantalithos_identity::persistence::database::{connect_pool, run_migrations};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::from_env()?;
    let listen_addr: SocketAddr = config.listen_addr.parse()?;
    let pool = connect_pool(&config).await?;
    run_migrations(&pool).await?;
    let app_state = HttpAppState::new(pool.clone());

    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    println!("identity service listening on {}", listener.local_addr()?);

    axum::serve(listener, router(app_state)).await?;
    Ok(())
}
