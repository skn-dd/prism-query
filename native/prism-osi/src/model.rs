//! OSI (Open Semantic Interchange) model definitions.
//!
//! Follows the OSI v0.1.1 specification:
//! - SemanticModel: top-level container
//! - Datasets: logical tables with fields, metrics, and relationships
//! - Fields: typed columns in a dataset
//! - Metrics: calculated measures (SUM, COUNT, AVG, etc.)
//! - Relationships: foreign-key joins between datasets

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Top-level OSI semantic model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OsiModel {
    /// Model name.
    pub name: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Version of the OSI spec this model follows.
    #[serde(default = "default_version")]
    pub version: String,
    /// Datasets in this model.
    pub datasets: Vec<Dataset>,
    /// Global relationships between datasets.
    #[serde(default)]
    pub relationships: Vec<Relationship>,
}

fn default_version() -> String {
    "0.1.1".to_string()
}

/// A dataset (logical table) in the semantic model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dataset {
    /// Dataset name (used as virtual table name).
    pub name: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Physical source table (catalog.schema.table).
    pub source: DatasetSource,
    /// Fields (columns) in this dataset.
    pub fields: Vec<OsiField>,
    /// Calculated metrics.
    #[serde(default)]
    pub metrics: Vec<Metric>,
}

/// Physical source mapping for a dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetSource {
    /// Catalog name (e.g., "hive", "iceberg").
    #[serde(default)]
    pub catalog: String,
    /// Schema name.
    #[serde(default)]
    pub schema: String,
    /// Table name.
    pub table: String,
}

impl DatasetSource {
    pub fn fully_qualified(&self) -> String {
        let mut parts = Vec::new();
        if !self.catalog.is_empty() {
            parts.push(self.catalog.as_str());
        }
        if !self.schema.is_empty() {
            parts.push(self.schema.as_str());
        }
        parts.push(self.table.as_str());
        parts.join(".")
    }
}

/// A field (column) in a dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OsiField {
    /// Field name.
    pub name: String,
    /// Data type (string representation: "integer", "varchar", "decimal", etc.).
    #[serde(rename = "type")]
    pub data_type: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Whether this field is a primary key.
    #[serde(default)]
    pub primary_key: bool,
    /// SQL expression if this is a calculated field.
    #[serde(default)]
    pub expression: Option<String>,
    /// Tags for categorization.
    #[serde(default)]
    pub tags: Vec<String>,
}

/// A metric (calculated measure) in a dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    /// Metric name.
    pub name: String,
    /// SQL expression defining the metric.
    pub expression: String,
    /// Aggregate function: "sum", "count", "avg", "min", "max", "count_distinct".
    pub aggregate: String,
    /// The field this metric operates on.
    pub field: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Filters to apply before aggregation.
    #[serde(default)]
    pub filters: Vec<MetricFilter>,
}

/// A filter condition for a metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricFilter {
    pub field: String,
    pub operator: String,
    pub value: String,
}

/// A relationship (join) between two datasets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relationship {
    /// Name of this relationship.
    #[serde(default)]
    pub name: String,
    /// Source dataset and field.
    pub from: RelationshipEnd,
    /// Target dataset and field.
    pub to: RelationshipEnd,
    /// Relationship type: "one_to_one", "one_to_many", "many_to_one", "many_to_many".
    #[serde(default = "default_relationship_type")]
    pub relationship_type: String,
}

fn default_relationship_type() -> String {
    "many_to_one".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipEnd {
    pub dataset: String,
    pub field: String,
}

/// Load an OSI model from a JSON file.
pub fn load_model(path: &Path) -> anyhow::Result<OsiModel> {
    let content = std::fs::read_to_string(path)?;
    let model: OsiModel = serde_json::from_str(&content)?;
    validate_model(&model)?;
    Ok(model)
}

/// Load an OSI model from a YAML file.
pub fn load_model_yaml(path: &Path) -> anyhow::Result<OsiModel> {
    let content = std::fs::read_to_string(path)?;
    let model: OsiModel = serde_yaml::from_str(&content)?;
    validate_model(&model)?;
    Ok(model)
}

/// Validate an OSI model for internal consistency.
fn validate_model(model: &OsiModel) -> anyhow::Result<()> {
    let dataset_names: HashMap<&str, &Dataset> = model
        .datasets
        .iter()
        .map(|d| (d.name.as_str(), d))
        .collect();

    // Check that relationship endpoints reference valid datasets and fields
    for rel in &model.relationships {
        if !dataset_names.contains_key(rel.from.dataset.as_str()) {
            anyhow::bail!(
                "Relationship '{}' references unknown source dataset '{}'",
                rel.name,
                rel.from.dataset
            );
        }
        if !dataset_names.contains_key(rel.to.dataset.as_str()) {
            anyhow::bail!(
                "Relationship '{}' references unknown target dataset '{}'",
                rel.name,
                rel.to.dataset
            );
        }

        // Verify fields exist
        let from_ds = dataset_names[rel.from.dataset.as_str()];
        if !from_ds.fields.iter().any(|f| f.name == rel.from.field) {
            anyhow::bail!(
                "Relationship '{}' references unknown field '{}' in dataset '{}'",
                rel.name,
                rel.from.field,
                rel.from.dataset
            );
        }

        let to_ds = dataset_names[rel.to.dataset.as_str()];
        if !to_ds.fields.iter().any(|f| f.name == rel.to.field) {
            anyhow::bail!(
                "Relationship '{}' references unknown field '{}' in dataset '{}'",
                rel.name,
                rel.to.field,
                rel.to.dataset
            );
        }
    }

    // Check that metrics reference valid fields in their dataset
    for ds in &model.datasets {
        for metric in &ds.metrics {
            if !ds.fields.iter().any(|f| f.name == metric.field) {
                anyhow::bail!(
                    "Metric '{}' in dataset '{}' references unknown field '{}'",
                    metric.name,
                    ds.name,
                    metric.field
                );
            }
        }
    }

    Ok(())
}

/// Generate SQL for a metric (used to resolve metric queries at planning time).
pub fn metric_to_sql(dataset: &Dataset, metric: &Metric) -> String {
    let agg_expr = match metric.aggregate.as_str() {
        "sum" => format!("SUM({})", metric.field),
        "count" => format!("COUNT({})", metric.field),
        "avg" => format!("AVG({})", metric.field),
        "min" => format!("MIN({})", metric.field),
        "max" => format!("MAX({})", metric.field),
        "count_distinct" => format!("COUNT(DISTINCT {})", metric.field),
        _ => metric.expression.clone(),
    };

    if metric.filters.is_empty() {
        agg_expr
    } else {
        let conditions: Vec<String> = metric
            .filters
            .iter()
            .map(|f| format!("{} {} {}", f.field, f.operator, f.value))
            .collect();
        format!(
            "CASE WHEN {} THEN {} END",
            conditions.join(" AND "),
            agg_expr
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_model() -> OsiModel {
        OsiModel {
            name: "ecommerce".to_string(),
            description: "E-commerce analytics model".to_string(),
            version: "0.1.1".to_string(),
            datasets: vec![
                Dataset {
                    name: "orders".to_string(),
                    description: "Customer orders".to_string(),
                    source: DatasetSource {
                        catalog: "hive".to_string(),
                        schema: "analytics".to_string(),
                        table: "orders".to_string(),
                    },
                    fields: vec![
                        OsiField {
                            name: "order_id".to_string(),
                            data_type: "integer".to_string(),
                            description: "Order identifier".to_string(),
                            primary_key: true,
                            expression: None,
                            tags: vec![],
                        },
                        OsiField {
                            name: "customer_id".to_string(),
                            data_type: "integer".to_string(),
                            description: "Customer FK".to_string(),
                            primary_key: false,
                            expression: None,
                            tags: vec![],
                        },
                        OsiField {
                            name: "amount".to_string(),
                            data_type: "decimal".to_string(),
                            description: "Order total".to_string(),
                            primary_key: false,
                            expression: None,
                            tags: vec![],
                        },
                    ],
                    metrics: vec![Metric {
                        name: "total_revenue".to_string(),
                        expression: "SUM(amount)".to_string(),
                        aggregate: "sum".to_string(),
                        field: "amount".to_string(),
                        description: "Total revenue across orders".to_string(),
                        filters: vec![],
                    }],
                },
                Dataset {
                    name: "customers".to_string(),
                    description: "Customer profiles".to_string(),
                    source: DatasetSource {
                        catalog: "hive".to_string(),
                        schema: "analytics".to_string(),
                        table: "customers".to_string(),
                    },
                    fields: vec![
                        OsiField {
                            name: "customer_id".to_string(),
                            data_type: "integer".to_string(),
                            description: "Customer identifier".to_string(),
                            primary_key: true,
                            expression: None,
                            tags: vec![],
                        },
                        OsiField {
                            name: "name".to_string(),
                            data_type: "varchar".to_string(),
                            description: "Customer name".to_string(),
                            primary_key: false,
                            expression: None,
                            tags: vec![],
                        },
                    ],
                    metrics: vec![],
                },
            ],
            relationships: vec![Relationship {
                name: "order_customer".to_string(),
                from: RelationshipEnd {
                    dataset: "orders".to_string(),
                    field: "customer_id".to_string(),
                },
                to: RelationshipEnd {
                    dataset: "customers".to_string(),
                    field: "customer_id".to_string(),
                },
                relationship_type: "many_to_one".to_string(),
            }],
        }
    }

    #[test]
    fn test_validate_model() {
        let model = sample_model();
        assert!(validate_model(&model).is_ok());
    }

    #[test]
    fn test_invalid_relationship() {
        let mut model = sample_model();
        model.relationships[0].from.dataset = "nonexistent".to_string();
        assert!(validate_model(&model).is_err());
    }

    #[test]
    fn test_metric_to_sql() {
        let model = sample_model();
        let orders = &model.datasets[0];
        let metric = &orders.metrics[0];
        assert_eq!(metric_to_sql(orders, metric), "SUM(amount)");
    }

    #[test]
    fn test_metric_with_filter() {
        let metric = Metric {
            name: "us_revenue".to_string(),
            expression: "SUM(amount)".to_string(),
            aggregate: "sum".to_string(),
            field: "amount".to_string(),
            description: "US-only revenue".to_string(),
            filters: vec![MetricFilter {
                field: "country".to_string(),
                operator: "=".to_string(),
                value: "'US'".to_string(),
            }],
        };
        let ds = &Dataset {
            name: "test".to_string(),
            description: "".to_string(),
            source: DatasetSource {
                catalog: "".to_string(),
                schema: "".to_string(),
                table: "test".to_string(),
            },
            fields: vec![],
            metrics: vec![],
        };
        let sql = metric_to_sql(ds, &metric);
        assert!(sql.contains("CASE WHEN country = 'US'"));
        assert!(sql.contains("SUM(amount)"));
    }

    #[test]
    fn test_source_fully_qualified() {
        let src = DatasetSource {
            catalog: "hive".to_string(),
            schema: "analytics".to_string(),
            table: "orders".to_string(),
        };
        assert_eq!(src.fully_qualified(), "hive.analytics.orders");
    }
}
