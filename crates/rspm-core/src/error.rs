use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to parse toml: {0}")]
    TomlParse(#[from] toml::de::Error),

    #[error("invalid config: {0}")]
    Validation(String),

    #[error("task [{0}] not found")]
    TaskNotFound(String),
}
