//! TPC-H data generators with scale factor support.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow_array::{Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SortOptions};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

/// Generate TPC-H lineitem table as a single RecordBatch.
/// SF1 = 6_000_000 rows, SF0.1 = 600_000 rows.
pub fn make_lineitem(sf: f64) -> RecordBatch {
    let batches = make_lineitem_chunked(sf, usize::MAX);
    let schema = batches[0].schema();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

/// Generate TPC-H lineitem table as chunked RecordBatches.
/// Each chunk has at most `chunk_size` rows (default 5M for SF100 compatibility).
pub fn make_lineitem_chunked(sf: f64, chunk_size: usize) -> Vec<RecordBatch> {
    let n = (6_000_000.0 * sf) as usize;
    make_lineitem_shard(n, 0, chunk_size)
}

/// Generate a shard of TPC-H lineitem table.
/// `row_count` rows starting from global `row_offset`, in chunks of `chunk_size`.
pub fn make_lineitem_shard(row_count: usize, row_offset: usize, chunk_size: usize) -> Vec<RecordBatch> {
    let schema = lineitem_schema();
    let flags = ["A", "N", "R"];
    let statuses = ["F", "O"];

    let mut batches = Vec::new();
    let mut local = 0usize;
    while local < row_count {
        let chunk_end = (local + chunk_size).min(row_count);
        let g_start = row_offset + local;
        let g_end = row_offset + chunk_end;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from((g_start..g_end).map(|i| i as i64).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(
                    (g_start..g_end).map(|i| (i % 200_000) as i64).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    (g_start..g_end).map(|i| (i % 10_000) as i64).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    (g_start..g_end).map(|i| (i % 7 + 1) as i32).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (g_start..g_end).map(|i| (i % 50 + 1) as f64).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (g_start..g_end)
                        .map(|i| ((i % 100_000) as f64) * 0.99 + 900.0)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (g_start..g_end).map(|i| (i % 11) as f64 * 0.01).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (g_start..g_end).map(|i| (i % 9) as f64 * 0.01).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (g_start..g_end).map(|i| flags[i % 3]).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (g_start..g_end).map(|i| statuses[i % 2]).collect::<Vec<_>>(),
                )),
                // l_shipdate: dates in range 1992-01-01 to 1998-12-01 (days since epoch)
                // 1992-01-01 = 8035 days since 1970-01-01, 1998-12-01 = 10562
                Arc::new(Date32Array::from(
                    (g_start..g_end).map(|i| 8035 + (i % 2527) as i32).collect::<Vec<_>>(),
                )),
                // l_commitdate: shipdate + 30-90 days
                Arc::new(Date32Array::from(
                    (g_start..g_end).map(|i| 8035 + (i % 2527) as i32 + 30 + (i % 60) as i32).collect::<Vec<_>>(),
                )),
                // l_receiptdate: commitdate + 1-30 days
                Arc::new(Date32Array::from(
                    (g_start..g_end).map(|i| 8035 + (i % 2527) as i32 + 30 + (i % 60) as i32 + 1 + (i % 30) as i32).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        batches.push(batch);
        local = chunk_end;
    }
    batches
}

pub fn lineitem_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
    ]))
}

/// Generate TPC-H orders table as a single RecordBatch.
/// SF1 = 1_500_000 rows, SF0.1 = 150_000 rows.
pub fn make_orders(sf: f64) -> RecordBatch {
    let batches = make_orders_chunked(sf, usize::MAX);
    let schema = batches[0].schema();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

/// Generate TPC-H orders table as chunked RecordBatches.
pub fn make_orders_chunked(sf: f64, chunk_size: usize) -> Vec<RecordBatch> {
    let n = (1_500_000.0 * sf) as usize;
    make_orders_shard(n, 0, chunk_size)
}

/// Generate a shard of TPC-H orders table.
/// `row_count` rows starting from global `row_offset`, in chunks of `chunk_size`.
pub fn make_orders_shard(row_count: usize, row_offset: usize, chunk_size: usize) -> Vec<RecordBatch> {
    let schema = orders_schema();
    let statuses = ["F", "O", "P"];
    let priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];

    let mut batches = Vec::new();
    let mut local = 0usize;
    while local < row_count {
        let chunk_end = (local + chunk_size).min(row_count);
        let g_start = row_offset + local;
        let g_end = row_offset + chunk_end;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from((g_start..g_end).map(|i| i as i64).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(
                    (g_start..g_end).map(|i| (i % 150_000) as i64).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (g_start..g_end).map(|i| statuses[i % 3]).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (g_start..g_end)
                        .map(|i| (i as f64) * 2.5 + 100.0)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (g_start..g_end).map(|i| priorities[i % 5]).collect::<Vec<_>>(),
                )),
                // o_orderdate: dates in range 1992-01-01 to 1998-08-02
                Arc::new(Date32Array::from(
                    (g_start..g_end).map(|i| 8035 + (i % 2405) as i32).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        batches.push(batch);
        local = chunk_end;
    }
    batches
}

pub fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_orderdate", DataType::Date32, false),
    ]))
}

// ─── Parquet writers ─────────────────────────────────────────────────

/// Write lineitem data as a sorted Parquet file.
/// Sorts by (l_discount, l_quantity) so row groups have tight min/max ranges
/// for effective row group skipping on filter queries.
pub fn write_lineitem_parquet(
    output_dir: &Path,
    row_count: usize,
    row_offset: usize,
    row_group_size: usize,
) -> anyhow::Result<PathBuf> {
    fs::create_dir_all(output_dir)?;
    let file_path = output_dir.join("lineitem.parquet");

    // Generate data in memory, then sort
    let batches = make_lineitem_shard(row_count, row_offset, row_count.min(5_000_000));
    let schema = lineitem_schema();
    let all_data = concat_batches(&schema, &batches)?;

    // Sort by l_discount (col 6) then l_quantity (col 4) for filter pushdown
    let sort_cols = vec![
        arrow::compute::SortColumn {
            values: all_data.column(6).clone(),
            options: Some(SortOptions::default()),
        },
        arrow::compute::SortColumn {
            values: all_data.column(4).clone(),
            options: Some(SortOptions::default()),
        },
    ];
    let indices = arrow::compute::lexsort_to_indices(&sort_cols, None)?;
    let sorted_columns: Vec<_> = all_data
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c, &indices, None).unwrap())
        .collect();
    let sorted = RecordBatch::try_new(schema.clone(), sorted_columns)?;

    // Write as Parquet with configured row group size
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(row_group_size)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
        .build();

    let file = fs::File::create(&file_path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&sorted)?;
    writer.close()?;

    tracing::info!(
        "Wrote {} rows to {:?} (sorted by l_discount, l_quantity, row_group_size={})",
        row_count,
        file_path,
        row_group_size,
    );

    Ok(file_path)
}

/// Write orders data as a sorted Parquet file.
/// Sorts by (o_orderstatus) for predicate pushdown on status filters.
pub fn write_orders_parquet(
    output_dir: &Path,
    row_count: usize,
    row_offset: usize,
    row_group_size: usize,
) -> anyhow::Result<PathBuf> {
    fs::create_dir_all(output_dir)?;
    let file_path = output_dir.join("orders.parquet");

    let batches = make_orders_shard(row_count, row_offset, row_count.min(5_000_000));
    let schema = orders_schema();
    let all_data = concat_batches(&schema, &batches)?;

    // Sort by o_orderstatus (col 2)
    let sort_cols = vec![arrow::compute::SortColumn {
        values: all_data.column(2).clone(),
        options: Some(SortOptions::default()),
    }];
    let indices = arrow::compute::lexsort_to_indices(&sort_cols, None)?;
    let sorted_columns: Vec<_> = all_data
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c, &indices, None).unwrap())
        .collect();
    let sorted = RecordBatch::try_new(schema.clone(), sorted_columns)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(row_group_size)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
        .build();

    let file = fs::File::create(&file_path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&sorted)?;
    writer.close()?;

    tracing::info!(
        "Wrote {} rows to {:?} (sorted by o_orderstatus, row_group_size={})",
        row_count,
        file_path,
        row_group_size,
    );

    Ok(file_path)
}
