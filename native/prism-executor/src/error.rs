use thiserror::Error;

pub type Result<T> = std::result::Result<T, PrismError>;

#[derive(Error, Debug)]
pub enum PrismError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Join error: {0}")]
    Join(String),

    #[error("Aggregation error: unsupported function {0}")]
    UnsupportedAggregation(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
