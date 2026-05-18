//! Binary entrypoint for the quantalithos-identity service skeleton.

use std::net::SocketAddr;

use quantalithos_identity::config::AppConfig;
use quantalithos_identity::inbound::http::{health_check, router};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::from_env()?;
    let listen_addr: SocketAddr = config.listen_addr.parse()?;

    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    println!("identity skeleton listening on {}", listener.local_addr()?);

    axum::serve(listener, router().fallback(get_health_fallback)).await?;
    Ok(())
}

async fn get_health_fallback() -> impl axum::response::IntoResponse {
    health_check().await
}
