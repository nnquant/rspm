pub mod api;
pub mod client;
pub mod transport;

pub use client::RspmClient;
#[cfg(windows)]
pub use transport::NamedPipeRspmClient;
pub use transport::TcpRspmClient;
#[cfg(unix)]
pub use transport::UnixRspmClient;
