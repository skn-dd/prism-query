//! Prism Substrait — Substrait plan consumer.
//!
//! Deserializes Substrait IR (protobuf) into Prism execution plans that
//! operate on Arrow RecordBatches. This is the bridge between Trino's
//! query planner (which produces Substrait via Java serialization) and
//! the native Rust execution engine.

pub mod consumer;
pub mod error;
pub mod plan;

pub use error::{SubstraitError, Result};
pub use plan::{ExecutionPlan, PlanNode};
