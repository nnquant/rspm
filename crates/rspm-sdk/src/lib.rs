pub mod api;
pub mod client;
pub mod render;
pub mod supervisor;
pub mod transport;

pub use client::RspmClient;
pub use supervisor::{DaemonCommandSpec, DaemonOwnership, RspmSupervisor};
#[cfg(windows)]
pub use transport::NamedPipeRspmClient;
pub use transport::TcpRspmClient;
#[cfg(unix)]
pub use transport::UnixRspmClient;
