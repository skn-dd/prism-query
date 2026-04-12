//! Prism OSI — Open Semantic Interchange format parser and compiler.
//!
//! Parses OSI model definitions (JSON/YAML) following the OSI v0.1.1 spec,
//! and exposes them as virtual catalog tables in the Prism query engine.
//! Metrics are compiled to Substrait expressions for execution.

pub mod catalog;
pub mod model;

pub use catalog::OsiCatalog;
pub use model::{OsiModel, Dataset, OsiField, Metric, Relationship, load_model, load_model_yaml};
