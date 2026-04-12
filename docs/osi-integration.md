# OSI Integration

## What is OSI?

Open Semantic Interchange (OSI) is a standard format for defining semantic models — structured descriptions of data assets with business-friendly metrics, relationships, and field descriptions. Think of it as a universal schema for analytics.

## Spec Version

Prism supports OSI v0.1.1. The working group includes AtScale, dbt Labs, Databricks, Snowflake, Starburst (Trino), and others.

## Model Structure

```yaml
name: model_name
version: "0.1.1"

datasets:
  - name: orders
    source:
      catalog: hive
      schema: analytics
      table: orders
    fields:
      - name: amount
        type: decimal
        description: Order total
    metrics:
      - name: total_revenue
        aggregate: sum
        field: amount
    
relationships:
  - from: { dataset: orders, field: customer_id }
    to: { dataset: customers, field: customer_id }
```

## How Prism Uses OSI

### 1. Virtual Catalog

OSI models are exposed as a virtual `osi` catalog in Trino:

```sql
-- Query using OSI model
SELECT region, total_revenue
FROM osi.ecommerce.orders
GROUP BY region;
```

The `prism-osi` crate resolves `total_revenue` to `SUM(amount)` and rewrites the query against the physical source table `hive.analytics.orders`.

### 2. Metric Resolution

Metrics can have filters:

```yaml
metrics:
  - name: us_revenue
    aggregate: sum
    field: amount
    filters:
      - field: country
        operator: "="
        value: "'US'"
```

Resolves to: `SUM(CASE WHEN country = 'US' THEN amount END)`

### 3. Relationship-Aware Joins

When a query spans datasets connected by relationships, Prism automatically generates the JOIN:

```sql
-- User writes:
SELECT c.name, o.total_revenue
FROM osi.ecommerce.orders o, osi.ecommerce.customers c
WHERE o.customer_id = c.customer_id
GROUP BY c.name;

-- Prism resolves to:
SELECT c.name, SUM(o.amount) as total_revenue
FROM hive.analytics.orders o
JOIN hive.analytics.customers c ON o.customer_id = c.customer_id
GROUP BY c.name;
```

### 4. Substrait Compilation

The `osi-substrait` pattern compiles OSI metrics into Substrait aggregate expressions, which the native executor handles directly.

## Loading Models

Place `.osi.yaml` or `.osi.json` files in the `osi-models/` directory. They are automatically loaded at startup and registered in the virtual catalog.

## Validation

Models are validated on load:
- All relationship endpoints reference existing datasets and fields
- All metrics reference existing fields in their dataset
- Field types are valid
