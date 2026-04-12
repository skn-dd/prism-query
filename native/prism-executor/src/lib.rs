//! Prism Executor — Arrow-native SIMD execution operators.
//!
//! Replaces Trino's JVM-heap Page/Block model with Apache Arrow RecordBatch
//! operations using vectorized SIMD kernels.

pub mod error;
pub mod filter_project;
pub mod hash_aggregate;
pub mod hash_join;
pub mod sort;
pub mod string_ops;

pub use error::{PrismError, Result};
