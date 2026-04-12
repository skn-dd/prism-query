use thiserror::Error;

pub type Result<T> = std::result::Result<T, SubstraitError>;

#[derive(Error, Debug)]
pub enum SubstraitError {
    #[error("Protobuf decode error: {0}")]
    ProtobufDecode(#[from] prost::DecodeError),

    #[error("Unsupported Substrait relation: {0}")]
    UnsupportedRelation(String),

    #[error("Unsupported Substrait expression: {0}")]
    UnsupportedExpression(String),

    #[error("Unsupported aggregate function: {0}")]
    UnsupportedAggregateFunction(String),

    #[error("Unsupported join type: {0}")]
    UnsupportedJoinType(String),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Executor error: {0}")]
    Executor(#[from] prism_executor::PrismError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Internal error: {0}")]
    Internal(String),
}
