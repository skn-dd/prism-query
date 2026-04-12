//! OSI Catalog Connector — resolves OSI semantic models into SQL at query time.
//!
//! ## How it works
//!
//! The catalog connector bridges the semantic layer (OSI models) with the physical
//! layer (Trino's connector ecosystem). It works in three stages:
//!
//! ### 1. Model Loading & Registration
//!
//! At startup, the coordinator loads `.osi.yaml` files from a configured directory
//! (e.g., `/etc/prism/models/` or an S3 bucket). Each model is parsed, validated,
//! and registered in the `OsiCatalog`. Models can also be hot-reloaded at runtime.
//!
//! ### 2. Virtual Catalog Exposure
//!
//! Each OSI model becomes a *virtual catalog* in Trino's namespace:
//!
//! ```text
//! osi.<model_name>.<dataset_name>          → virtual table (fields)
//! osi.<model_name>.<dataset_name>._metrics → virtual metrics table
//! ```
//!
//! When a user queries `SELECT * FROM osi.ecommerce.orders`, the catalog connector
//! rewrites it to `SELECT * FROM hive.analytics.orders` (the physical source).
//!
//! ### 3. Metric Resolution
//!
//! Metrics are the key differentiator. When a user writes:
//!
//! ```sql
//! SELECT region, METRIC(total_revenue), METRIC(order_count)
//! FROM osi.ecommerce.orders
//! GROUP BY region
//! ```
//!
//! The metric resolver expands `METRIC(total_revenue)` into `SUM(amount)` and
//! `METRIC(order_count)` into `COUNT(order_id)`, producing:
//!
//! ```sql
//! SELECT region, SUM(amount) AS total_revenue, COUNT(order_id) AS order_count
//! FROM hive.analytics.orders
//! GROUP BY region
//! ```
//!
//! Metrics with filters (like `delivered_revenue`) get CASE-wrapped:
//!
//! ```sql
//! CASE WHEN status = 'delivered' THEN SUM(amount) END AS delivered_revenue
//! ```
//!
//! ### 4. Cross-Dataset Joins (via Relationships)
//!
//! When a query references fields from related datasets, the catalog auto-generates
//! JOINs based on the relationship definitions:
//!
//! ```sql
//! -- User writes:
//! SELECT c.name, METRIC(total_revenue)
//! FROM osi.ecommerce.orders o
//! JOIN osi.ecommerce.customers c USING (customer_id)
//! GROUP BY c.name
//!
//! -- Resolves to:
//! SELECT c.name, SUM(o.amount) AS total_revenue
//! FROM hive.analytics.orders o
//! JOIN hive.analytics.customers c ON o.customer_id = c.customer_id
//! GROUP BY c.name
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::model::{
    MetricFilter, OsiModel, load_model, load_model_yaml,
};

/// The OSI catalog — holds loaded models and provides query resolution.
#[derive(Debug)]
pub struct OsiCatalog {
    /// Loaded models keyed by model name.
    models: HashMap<String, CatalogEntry>,
    /// Directory to watch for model files.
    model_dir: Option<PathBuf>,
}

/// A cached model entry with metadata.
#[derive(Debug, Clone)]
pub struct CatalogEntry {
    pub model: OsiModel,
    /// File path the model was loaded from (if file-based).
    pub source_path: Option<PathBuf>,
    /// When the model was last loaded.
    pub loaded_at: SystemTime,
    /// Precomputed lookup indexes.
    pub index: ModelIndex,
}

/// Precomputed indexes for fast lookups during query resolution.
#[derive(Debug, Clone)]
pub struct ModelIndex {
    /// dataset_name → index in model.datasets
    pub datasets: HashMap<String, usize>,
    /// (dataset_name, field_name) → field index
    pub fields: HashMap<(String, String), usize>,
    /// (dataset_name, metric_name) → metric index
    pub metrics: HashMap<(String, String), usize>,
    /// (from_dataset, to_dataset) → relationship index
    pub relationships: HashMap<(String, String), usize>,
}

impl ModelIndex {
    fn build(model: &OsiModel) -> Self {
        let mut datasets = HashMap::new();
        let mut fields = HashMap::new();
        let mut metrics = HashMap::new();
        let mut relationships = HashMap::new();

        for (i, ds) in model.datasets.iter().enumerate() {
            datasets.insert(ds.name.clone(), i);
            for (j, field) in ds.fields.iter().enumerate() {
                fields.insert((ds.name.clone(), field.name.clone()), j);
            }
            for (j, metric) in ds.metrics.iter().enumerate() {
                metrics.insert((ds.name.clone(), metric.name.clone()), j);
            }
        }

        for (i, rel) in model.relationships.iter().enumerate() {
            relationships.insert(
                (rel.from.dataset.clone(), rel.to.dataset.clone()),
                i,
            );
            // Also index the reverse direction for lookups
            relationships.insert(
                (rel.to.dataset.clone(), rel.from.dataset.clone()),
                i,
            );
        }

        Self {
            datasets,
            fields,
            metrics,
            relationships,
        }
    }
}

impl OsiCatalog {
    /// Create an empty catalog.
    pub fn new() -> Self {
        Self {
            models: HashMap::new(),
            model_dir: None,
        }
    }

    /// Create a catalog that watches a directory for model files.
    pub fn with_model_dir(dir: impl Into<PathBuf>) -> Self {
        Self {
            models: HashMap::new(),
            model_dir: Some(dir.into()),
        }
    }

    /// Register a model programmatically.
    pub fn register_model(&mut self, model: OsiModel) {
        let index = ModelIndex::build(&model);
        let entry = CatalogEntry {
            model: model.clone(),
            source_path: None,
            loaded_at: SystemTime::now(),
            index,
        };
        self.models.insert(model.name.clone(), entry);
    }

    /// Load a single model file and register it.
    pub fn load_model_file(&mut self, path: &Path) -> anyhow::Result<String> {
        let model = if path.extension().map_or(false, |e| e == "yaml" || e == "yml") {
            load_model_yaml(path)?
        } else {
            load_model(path)?
        };
        let name = model.name.clone();
        let index = ModelIndex::build(&model);
        let entry = CatalogEntry {
            model,
            source_path: Some(path.to_owned()),
            loaded_at: SystemTime::now(),
            index,
        };
        self.models.insert(name.clone(), entry);
        Ok(name)
    }

    /// Load all model files from the configured directory.
    pub fn load_all_models(&mut self) -> anyhow::Result<Vec<String>> {
        let dir = self
            .model_dir
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no model directory configured"))?
            .clone();

        let mut loaded = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |e| {
                e == "yaml" || e == "yml" || e == "json"
            }) {
                match self.load_model_file(&path) {
                    Ok(name) => loaded.push(name),
                    Err(e) => {
                        tracing::warn!("Failed to load model {:?}: {}", path, e);
                    }
                }
            }
        }
        Ok(loaded)
    }

    /// Get a registered model by name.
    pub fn get_model(&self, name: &str) -> Option<&CatalogEntry> {
        self.models.get(name)
    }

    /// List all registered model names.
    pub fn list_models(&self) -> Vec<&str> {
        self.models.keys().map(|k| k.as_str()).collect()
    }

    /// List all datasets in a model.
    pub fn list_datasets(&self, model_name: &str) -> Option<Vec<&str>> {
        self.models.get(model_name).map(|entry| {
            entry
                .model
                .datasets
                .iter()
                .map(|d| d.name.as_str())
                .collect()
        })
    }

    /// Resolve a fully-qualified OSI reference to its physical source.
    ///
    /// Input: `osi.ecommerce.orders` → Output: `hive.analytics.orders`
    pub fn resolve_table(&self, model_name: &str, dataset_name: &str) -> Option<String> {
        let entry = self.models.get(model_name)?;
        let ds_idx = entry.index.datasets.get(dataset_name)?;
        let ds = &entry.model.datasets[*ds_idx];
        Some(ds.source.fully_qualified())
    }

    /// Resolve a metric name to its SQL expression.
    ///
    /// Input: `("ecommerce", "orders", "total_revenue")`
    /// Output: `ResolvedMetric { sql: "SUM(amount)", alias: "total_revenue", ... }`
    pub fn resolve_metric(
        &self,
        model_name: &str,
        dataset_name: &str,
        metric_name: &str,
    ) -> Option<ResolvedMetric> {
        let entry = self.models.get(model_name)?;
        let ds_idx = *entry.index.datasets.get(dataset_name)?;
        let m_idx = *entry
            .index
            .metrics
            .get(&(dataset_name.to_string(), metric_name.to_string()))?;

        let ds = &entry.model.datasets[ds_idx];
        let metric = &ds.metrics[m_idx];
        let sql = crate::model::metric_to_sql(ds, metric);

        Some(ResolvedMetric {
            sql_expression: sql,
            alias: metric.name.clone(),
            aggregate: metric.aggregate.clone(),
            source_field: metric.field.clone(),
            filters: metric.filters.clone(),
            description: metric.description.clone(),
        })
    }

    /// Resolve all metrics for a dataset.
    pub fn resolve_all_metrics(
        &self,
        model_name: &str,
        dataset_name: &str,
    ) -> Option<Vec<ResolvedMetric>> {
        let entry = self.models.get(model_name)?;
        let ds_idx = *entry.index.datasets.get(dataset_name)?;
        let ds = &entry.model.datasets[ds_idx];

        Some(
            ds.metrics
                .iter()
                .map(|m| {
                    let sql = crate::model::metric_to_sql(ds, m);
                    ResolvedMetric {
                        sql_expression: sql,
                        alias: m.name.clone(),
                        aggregate: m.aggregate.clone(),
                        source_field: m.field.clone(),
                        filters: m.filters.clone(),
                        description: m.description.clone(),
                    }
                })
                .collect(),
        )
    }

    /// Resolve a relationship between two datasets for automatic JOIN generation.
    ///
    /// Returns the join condition SQL fragment.
    pub fn resolve_join(
        &self,
        model_name: &str,
        from_dataset: &str,
        to_dataset: &str,
    ) -> Option<ResolvedJoin> {
        let entry = self.models.get(model_name)?;
        let rel_idx = *entry
            .index
            .relationships
            .get(&(from_dataset.to_string(), to_dataset.to_string()))?;
        let rel = &entry.model.relationships[rel_idx];

        let from_ds_idx = *entry.index.datasets.get(from_dataset)?;
        let to_ds_idx = *entry.index.datasets.get(to_dataset)?;
        let from_source = entry.model.datasets[from_ds_idx].source.fully_qualified();
        let to_source = entry.model.datasets[to_ds_idx].source.fully_qualified();

        Some(ResolvedJoin {
            from_table: from_source,
            to_table: to_source,
            from_field: rel.from.field.clone(),
            to_field: rel.to.field.clone(),
            relationship_type: rel.relationship_type.clone(),
            join_sql: format!(
                "{}.{} = {}.{}",
                from_dataset, rel.from.field, to_dataset, rel.to.field
            ),
        })
    }

    /// Rewrite a full query that uses OSI references.
    ///
    /// Takes a parsed query context and produces the rewritten SQL.
    pub fn rewrite_query(&self, query: &OsiQuery) -> anyhow::Result<RewrittenQuery> {
        let entry = self
            .models
            .get(&query.model_name)
            .ok_or_else(|| anyhow::anyhow!("unknown model: {}", query.model_name))?;

        let ds_idx = *entry
            .index
            .datasets
            .get(&query.dataset_name)
            .ok_or_else(|| {
                anyhow::anyhow!("unknown dataset: {}", query.dataset_name)
            })?;
        let ds = &entry.model.datasets[ds_idx];
        let physical_table = ds.source.fully_qualified();

        // Resolve SELECT columns
        let mut select_parts = Vec::new();
        let mut group_by_parts = Vec::new();

        for col in &query.select_columns {
            match col {
                SelectColumn::Field(name) => {
                    select_parts.push(name.clone());
                    if query.has_aggregates() {
                        group_by_parts.push(name.clone());
                    }
                }
                SelectColumn::Metric(metric_name) => {
                    let resolved = self
                        .resolve_metric(&query.model_name, &query.dataset_name, metric_name)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "unknown metric '{}' in dataset '{}'",
                                metric_name,
                                query.dataset_name
                            )
                        })?;
                    select_parts.push(format!("{} AS {}", resolved.sql_expression, resolved.alias));
                }
                SelectColumn::AllFields => {
                    for field in &ds.fields {
                        select_parts.push(field.name.clone());
                    }
                }
                SelectColumn::AllMetrics => {
                    let metrics = self
                        .resolve_all_metrics(&query.model_name, &query.dataset_name)
                        .unwrap_or_default();
                    for m in metrics {
                        select_parts.push(format!("{} AS {}", m.sql_expression, m.alias));
                    }
                }
            }
        }

        // Build WHERE clause
        let where_clause = if query.where_conditions.is_empty() {
            None
        } else {
            Some(query.where_conditions.join(" AND "))
        };

        // Build JOINs
        let mut join_clauses = Vec::new();
        for join in &query.joins {
            if let Some(resolved) = self.resolve_join(
                &query.model_name,
                &query.dataset_name,
                &join.target_dataset,
            ) {
                join_clauses.push(format!(
                    "JOIN {} {} ON {}",
                    resolved.to_table, join.alias, resolved.join_sql
                ));
            }
        }

        // Assemble SQL
        let mut sql = format!("SELECT {}", select_parts.join(", "));
        sql.push_str(&format!("\nFROM {}", physical_table));

        for join in &join_clauses {
            sql.push_str(&format!("\n{}", join));
        }

        if let Some(ref w) = where_clause {
            sql.push_str(&format!("\nWHERE {}", w));
        }

        if !group_by_parts.is_empty() {
            sql.push_str(&format!("\nGROUP BY {}", group_by_parts.join(", ")));
        }

        if let Some(ref order_by) = query.order_by {
            sql.push_str(&format!("\nORDER BY {}", order_by));
        }

        if let Some(limit) = query.limit {
            sql.push_str(&format!("\nLIMIT {}", limit));
        }

        Ok(RewrittenQuery {
            sql,
            physical_tables: vec![physical_table],
            metrics_used: query
                .select_columns
                .iter()
                .filter_map(|c| match c {
                    SelectColumn::Metric(name) => Some(name.clone()),
                    _ => None,
                })
                .collect(),
        })
    }

    /// Get schema information for a dataset (fields + metrics as virtual columns).
    pub fn describe_dataset(
        &self,
        model_name: &str,
        dataset_name: &str,
    ) -> Option<DatasetSchema> {
        let entry = self.models.get(model_name)?;
        let ds_idx = *entry.index.datasets.get(dataset_name)?;
        let ds = &entry.model.datasets[ds_idx];

        let fields: Vec<ColumnInfo> = ds
            .fields
            .iter()
            .map(|f| ColumnInfo {
                name: f.name.clone(),
                data_type: f.data_type.clone(),
                is_metric: false,
                description: f.description.clone(),
                expression: f.expression.clone(),
            })
            .collect();

        let metrics: Vec<ColumnInfo> = ds
            .metrics
            .iter()
            .map(|m| ColumnInfo {
                name: m.name.clone(),
                data_type: match m.aggregate.as_str() {
                    "count" | "count_distinct" => "bigint".to_string(),
                    "avg" => "double".to_string(),
                    _ => "decimal".to_string(),
                },
                is_metric: true,
                description: m.description.clone(),
                expression: Some(m.expression.clone()),
            })
            .collect();

        Some(DatasetSchema {
            model_name: model_name.to_string(),
            dataset_name: dataset_name.to_string(),
            physical_source: ds.source.fully_qualified(),
            fields,
            metrics,
        })
    }
}

// ─── Query representation ─────────────────────────────────────────────────────

/// A parsed OSI query (input to the rewriter).
#[derive(Debug, Clone)]
pub struct OsiQuery {
    pub model_name: String,
    pub dataset_name: String,
    pub select_columns: Vec<SelectColumn>,
    pub where_conditions: Vec<String>,
    pub joins: Vec<OsiJoinRef>,
    pub order_by: Option<String>,
    pub limit: Option<usize>,
}

impl OsiQuery {
    pub fn has_aggregates(&self) -> bool {
        self.select_columns.iter().any(|c| matches!(c, SelectColumn::Metric(_) | SelectColumn::AllMetrics))
    }
}

/// A column reference in a SELECT clause.
#[derive(Debug, Clone)]
pub enum SelectColumn {
    /// A regular field: `SELECT region`
    Field(String),
    /// A metric reference: `SELECT METRIC(total_revenue)`
    Metric(String),
    /// All fields: `SELECT *`
    AllFields,
    /// All metrics: `SELECT METRICS(*)`
    AllMetrics,
}

/// A join reference in an OSI query.
#[derive(Debug, Clone)]
pub struct OsiJoinRef {
    pub target_dataset: String,
    pub alias: String,
}

// ─── Resolution results ───────────────────────────────────────────────────────

/// A resolved metric ready for SQL generation.
#[derive(Debug, Clone)]
pub struct ResolvedMetric {
    /// The SQL expression (e.g., "SUM(amount)").
    pub sql_expression: String,
    /// The metric name used as alias.
    pub alias: String,
    /// Aggregate function name.
    pub aggregate: String,
    /// Source field name.
    pub source_field: String,
    /// Applied filters.
    pub filters: Vec<MetricFilter>,
    /// Description.
    pub description: String,
}

/// A resolved join between two datasets.
#[derive(Debug, Clone)]
pub struct ResolvedJoin {
    pub from_table: String,
    pub to_table: String,
    pub from_field: String,
    pub to_field: String,
    pub relationship_type: String,
    /// SQL join condition fragment.
    pub join_sql: String,
}

/// The output of query rewriting.
#[derive(Debug, Clone)]
pub struct RewrittenQuery {
    /// The final physical SQL.
    pub sql: String,
    /// Physical tables referenced.
    pub physical_tables: Vec<String>,
    /// Metrics that were resolved.
    pub metrics_used: Vec<String>,
}

/// Schema description of a dataset (for DESCRIBE / SHOW COLUMNS).
#[derive(Debug, Clone)]
pub struct DatasetSchema {
    pub model_name: String,
    pub dataset_name: String,
    pub physical_source: String,
    pub fields: Vec<ColumnInfo>,
    pub metrics: Vec<ColumnInfo>,
}

/// A column in the schema description.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_metric: bool,
    pub description: String,
    pub expression: Option<String>,
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;

    fn sample_model() -> OsiModel {
        OsiModel {
            name: "ecommerce".to_string(),
            description: "E-commerce analytics".to_string(),
            version: "0.1.1".to_string(),
            datasets: vec![
                Dataset {
                    name: "orders".to_string(),
                    description: "".to_string(),
                    source: DatasetSource {
                        catalog: "hive".to_string(),
                        schema: "analytics".to_string(),
                        table: "orders".to_string(),
                    },
                    fields: vec![
                        OsiField {
                            name: "order_id".into(),
                            data_type: "integer".into(),
                            description: "".into(),
                            primary_key: true,
                            expression: None,
                            tags: vec![],
                        },
                        OsiField {
                            name: "customer_id".into(),
                            data_type: "integer".into(),
                            description: "".into(),
                            primary_key: false,
                            expression: None,
                            tags: vec![],
                        },
                        OsiField {
                            name: "amount".into(),
                            data_type: "decimal".into(),
                            description: "".into(),
                            primary_key: false,
                            expression: None,
                            tags: vec![],
                        },
                        OsiField {
                            name: "region".into(),
                            data_type: "varchar".into(),
                            description: "".into(),
                            primary_key: false,
                            expression: None,
                            tags: vec![],
                        },
                        OsiField {
                            name: "status".into(),
                            data_type: "varchar".into(),
                            description: "".into(),
                            primary_key: false,
                            expression: None,
                            tags: vec![],
                        },
                    ],
                    metrics: vec![
                        Metric {
                            name: "total_revenue".into(),
                            expression: "SUM(amount)".into(),
                            aggregate: "sum".into(),
                            field: "amount".into(),
                            description: "Total revenue".into(),
                            filters: vec![],
                        },
                        Metric {
                            name: "order_count".into(),
                            expression: "COUNT(order_id)".into(),
                            aggregate: "count".into(),
                            field: "order_id".into(),
                            description: "Number of orders".into(),
                            filters: vec![],
                        },
                        Metric {
                            name: "delivered_revenue".into(),
                            expression: "SUM(amount)".into(),
                            aggregate: "sum".into(),
                            field: "amount".into(),
                            description: "Revenue from delivered orders".into(),
                            filters: vec![MetricFilter {
                                field: "status".into(),
                                operator: "=".into(),
                                value: "'delivered'".into(),
                            }],
                        },
                    ],
                },
                Dataset {
                    name: "customers".to_string(),
                    description: "".to_string(),
                    source: DatasetSource {
                        catalog: "hive".to_string(),
                        schema: "analytics".to_string(),
                        table: "customers".to_string(),
                    },
                    fields: vec![
                        OsiField {
                            name: "customer_id".into(),
                            data_type: "integer".into(),
                            description: "".into(),
                            primary_key: true,
                            expression: None,
                            tags: vec![],
                        },
                        OsiField {
                            name: "name".into(),
                            data_type: "varchar".into(),
                            description: "".into(),
                            primary_key: false,
                            expression: None,
                            tags: vec![],
                        },
                    ],
                    metrics: vec![],
                },
            ],
            relationships: vec![Relationship {
                name: "order_customer".into(),
                from: RelationshipEnd {
                    dataset: "orders".into(),
                    field: "customer_id".into(),
                },
                to: RelationshipEnd {
                    dataset: "customers".into(),
                    field: "customer_id".into(),
                },
                relationship_type: "many_to_one".into(),
            }],
        }
    }

    #[test]
    fn test_catalog_register_and_list() {
        let mut catalog = OsiCatalog::new();
        catalog.register_model(sample_model());

        assert_eq!(catalog.list_models(), vec!["ecommerce"]);
        assert_eq!(
            catalog.list_datasets("ecommerce").unwrap(),
            vec!["orders", "customers"]
        );
    }

    #[test]
    fn test_resolve_table() {
        let mut catalog = OsiCatalog::new();
        catalog.register_model(sample_model());

        assert_eq!(
            catalog.resolve_table("ecommerce", "orders").unwrap(),
            "hive.analytics.orders"
        );
        assert_eq!(
            catalog.resolve_table("ecommerce", "customers").unwrap(),
            "hive.analytics.customers"
        );
        assert!(catalog.resolve_table("ecommerce", "nonexistent").is_none());
    }

    #[test]
    fn test_resolve_metric() {
        let mut catalog = OsiCatalog::new();
        catalog.register_model(sample_model());

        let resolved = catalog
            .resolve_metric("ecommerce", "orders", "total_revenue")
            .unwrap();
        assert_eq!(resolved.sql_expression, "SUM(amount)");
        assert_eq!(resolved.alias, "total_revenue");
        assert_eq!(resolved.aggregate, "sum");
    }

    #[test]
    fn test_resolve_filtered_metric() {
        let mut catalog = OsiCatalog::new();
        catalog.register_model(sample_model());

        let resolved = catalog
            .resolve_metric("ecommerce", "orders", "delivered_revenue")
            .unwrap();
        assert!(resolved.sql_expression.contains("CASE WHEN"));
        assert!(resolved.sql_expression.contains("status = 'delivered'"));
        assert!(resolved.sql_expression.contains("SUM(amount)"));
    }

    #[test]
    fn test_resolve_join() {
        let mut catalog = OsiCatalog::new();
        catalog.register_model(sample_model());

        let join = catalog
            .resolve_join("ecommerce", "orders", "customers")
            .unwrap();
        assert_eq!(join.from_table, "hive.analytics.orders");
        assert_eq!(join.to_table, "hive.analytics.customers");
        assert_eq!(join.from_field, "customer_id");
        assert_eq!(join.to_field, "customer_id");
        assert!(join.join_sql.contains("customer_id"));
    }

    #[test]
    fn test_rewrite_simple_query() {
        let mut catalog = OsiCatalog::new();
        catalog.register_model(sample_model());

        let query = OsiQuery {
            model_name: "ecommerce".into(),
            dataset_name: "orders".into(),
            select_columns: vec![
                SelectColumn::Field("region".into()),
                SelectColumn::Metric("total_revenue".into()),
                SelectColumn::Metric("order_count".into()),
            ],
            where_conditions: vec![],
            joins: vec![],
            order_by: None,
            limit: None,
        };

        let result = catalog.rewrite_query(&query).unwrap();
        assert!(result.sql.contains("SELECT region, SUM(amount) AS total_revenue, COUNT(order_id) AS order_count"));
        assert!(result.sql.contains("FROM hive.analytics.orders"));
        assert!(result.sql.contains("GROUP BY region"));
        assert_eq!(result.metrics_used, vec!["total_revenue", "order_count"]);
    }

    #[test]
    fn test_rewrite_with_where_and_limit() {
        let mut catalog = OsiCatalog::new();
        catalog.register_model(sample_model());

        let query = OsiQuery {
            model_name: "ecommerce".into(),
            dataset_name: "orders".into(),
            select_columns: vec![
                SelectColumn::Field("region".into()),
                SelectColumn::Metric("total_revenue".into()),
            ],
            where_conditions: vec!["region = 'EAST'".into()],
            joins: vec![],
            order_by: Some("total_revenue DESC".into()),
            limit: Some(10),
        };

        let result = catalog.rewrite_query(&query).unwrap();
        assert!(result.sql.contains("WHERE region = 'EAST'"));
        assert!(result.sql.contains("ORDER BY total_revenue DESC"));
        assert!(result.sql.contains("LIMIT 10"));
    }

    #[test]
    fn test_rewrite_with_join() {
        let mut catalog = OsiCatalog::new();
        catalog.register_model(sample_model());

        let query = OsiQuery {
            model_name: "ecommerce".into(),
            dataset_name: "orders".into(),
            select_columns: vec![
                SelectColumn::Field("region".into()),
                SelectColumn::Metric("total_revenue".into()),
            ],
            joins: vec![OsiJoinRef {
                target_dataset: "customers".into(),
                alias: "c".into(),
            }],
            where_conditions: vec![],
            order_by: None,
            limit: None,
        };

        let result = catalog.rewrite_query(&query).unwrap();
        assert!(result.sql.contains("JOIN hive.analytics.customers c ON"));
    }

    #[test]
    fn test_describe_dataset() {
        let mut catalog = OsiCatalog::new();
        catalog.register_model(sample_model());

        let schema = catalog.describe_dataset("ecommerce", "orders").unwrap();
        assert_eq!(schema.physical_source, "hive.analytics.orders");
        assert_eq!(schema.fields.len(), 5);
        assert_eq!(schema.metrics.len(), 3);

        // Metrics should be flagged
        assert!(schema.metrics.iter().all(|m| m.is_metric));
        assert!(schema.fields.iter().all(|f| !f.is_metric));
    }

    #[test]
    fn test_rewrite_all_fields() {
        let mut catalog = OsiCatalog::new();
        catalog.register_model(sample_model());

        let query = OsiQuery {
            model_name: "ecommerce".into(),
            dataset_name: "orders".into(),
            select_columns: vec![SelectColumn::AllFields],
            where_conditions: vec![],
            joins: vec![],
            order_by: None,
            limit: None,
        };

        let result = catalog.rewrite_query(&query).unwrap();
        assert!(result.sql.contains("order_id"));
        assert!(result.sql.contains("customer_id"));
        assert!(result.sql.contains("amount"));
        assert!(result.sql.contains("region"));
        assert!(result.sql.contains("status"));
    }

    #[test]
    fn test_rewrite_all_metrics() {
        let mut catalog = OsiCatalog::new();
        catalog.register_model(sample_model());

        let query = OsiQuery {
            model_name: "ecommerce".into(),
            dataset_name: "orders".into(),
            select_columns: vec![
                SelectColumn::Field("region".into()),
                SelectColumn::AllMetrics,
            ],
            where_conditions: vec![],
            joins: vec![],
            order_by: None,
            limit: None,
        };

        let result = catalog.rewrite_query(&query).unwrap();
        assert!(result.sql.contains("SUM(amount) AS total_revenue"));
        assert!(result.sql.contains("COUNT(order_id) AS order_count"));
        assert!(result.sql.contains("CASE WHEN"));
        assert!(result.sql.contains("GROUP BY region"));
    }
}
