use anyhow::Result;
use rspm_core::config::{HealthCheck, HealthCheckKind};
use tokio::net::TcpStream;
use tokio::process::Command;

pub async fn check_health(check: &HealthCheck) -> Result<bool> {
    match check.kind {
        HealthCheckKind::Tcp => {
            let Some(address) = &check.address else {
                return Ok(false);
            };
            Ok(TcpStream::connect(address).await.is_ok())
        }
        HealthCheckKind::Http => {
            let Some(url) = &check.url else {
                return Ok(false);
            };
            Ok(check_http_url(url).await)
        }
        HealthCheckKind::Command => {
            let Some(command) = &check.command else {
                return Ok(false);
            };
            let status = if cfg!(target_os = "windows") {
                Command::new("cmd").args(["/C", command]).status().await?
            } else {
                Command::new("sh").args(["-c", command]).status().await?
            };
            Ok(status.success())
        }
        HealthCheckKind::File => {
            let Some(path) = &check.path else {
                return Ok(false);
            };
            Ok(tokio::fs::metadata(path).await.is_ok())
        }
    }
}

async fn check_http_url(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("http://") else {
        return false;
    };
    let Some((host_port, path)) = rest.split_once('/') else {
        return false;
    };
    let path = format!("/{path}");
    let Ok(mut stream) = TcpStream::connect(host_port).await else {
        return false;
    };
    let request = format!("GET {path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    if stream.write_all(request.as_bytes()).await.is_err() {
        return false;
    }

    let mut response = String::new();
    if stream.read_to_string(&mut response).await.is_err() {
        return false;
    }

    response.starts_with("HTTP/1.1 2") || response.starts_with("HTTP/1.0 2")
}
