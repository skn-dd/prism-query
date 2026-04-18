//! Async Parquet reader with column pruning, row-group-level predicate
//! skipping, and per-file parallelism via `tokio::task::spawn`.
//!
//! All file I/O flows through `object_store::ObjectStore`, so the same code
//! handles `file://`, `s3://`, `gs://`, `abfs[s]://` and bare local paths.
//! Per-file fan-out runs on the tokio runtime; row-group statistics are still
//! consulted to skip unmatched groups before any column data is fetched.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_array::{new_null_array, ArrayRef};
use arrow_cast::cast;
use arrow_schema::{Schema, SchemaRef};
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;

use crate::error::{PrismError, Result};
use crate::filter_project::{Predicate, ScalarValue};
use crate::object_source::{list_files, parse_uri};

/// Configuration for a Parquet table scan.
pub struct ParquetScanConfig {
    /// List of URIs to scan. Each URI may reference a directory of Parquet
    /// files or a single Parquet file. Schemes: `file://`, `s3://`, `gs://`,
    /// `abfs[s]://`, or bare local paths. An empty list yields an empty
    /// result (not an error) — the caller decides what to do.
    pub uris: Vec<String>,
    /// Optional predicate for row group skipping.
    pub predicate: Option<Predicate>,
    /// Column projection — only read these column indices from Parquet.
    /// None means read all columns.
    pub projection: Option<Vec<usize>>,
    /// Batch size for the Parquet reader.
    pub batch_size: usize,
    /// If true, skip the expand_projected_batches step and return batches
    /// with only the projected columns. The caller must remap column indices.
    pub skip_expand: bool,
    /// Optional target schema to unify batches across files with differing
    /// schemas (schema evolution). When set, every file's batches are
    /// adapted to this schema: missing columns become nulls, extra columns
    /// are dropped, promotable types are cast, and column order is matched.
    /// When `None`, the first file's schema is used as the reference.
    pub target_schema: Option<SchemaRef>,
}

/// Scan Parquet files asynchronously, skipping row groups whose statistics
/// prove the predicate cannot match.
pub async fn parquet_scan(config: &ParquetScanConfig) -> Result<Vec<RecordBatch>> {
    if config.uris.is_empty() {
        return Ok(Vec::new());
    }

    // Resolve every URI to a (store, file_path) pair. A URI may be a
    // directory prefix (producing multiple files) or a single file.
    let mut resolved: Vec<(Arc<dyn ObjectStore + 'static>, ObjectPath)> = Vec::new();
    for uri in &config.uris {
        let (store, prefix) = parse_uri(uri)?;
        let files = list_files(&*store, &prefix, "parquet").await?;
        if files.is_empty() {
            return Err(PrismError::Internal(format!(
                "no .parquet files found at {}",
                uri
            )));
        }
        for f in files {
            resolved.push((store.clone(), f));
        }
    }

    if resolved.is_empty() {
        return Ok(Vec::new());
    }

    // Peek at the first file for the full schema (used for projection-expansion
    // when `target_schema` is None). When `target_schema` is set, we use it
    // instead — it is the authoritative full-table schema across all files.
    let full_schema: SchemaRef = match config.target_schema.clone() {
        Some(ts) => ts,
        None => {
            let peek_meta = read_file_metadata(resolved[0].0.clone(), &resolved[0].1).await?;
            peek_meta.schema().clone()
        }
    };

    // Per-file fan-out via tokio. We bound concurrency at num_cpus to mimic
    // the previous rayon behaviour without saturating the runtime.
    let parallelism = std::cmp::max(1, num_cpus_or_default());
    let predicate = config.predicate.clone();
    let projection = config.projection.clone();
    let batch_size = config.batch_size;
    let target_schema = config.target_schema.clone();
    let total_files = resolved.len();

    type FileResult = std::result::Result<(Vec<RecordBatch>, usize, usize), String>;
    // Pre-build each future eagerly so the adapter closure doesn't need to be
    // generic over `dyn ObjectStore` lifetimes (which trips HRTB inference
    // when this function is called from an async trait method).
    let mut futures: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = FileResult> + Send>>> =
        Vec::with_capacity(resolved.len());
    for (store, file_path) in resolved.into_iter() {
        let predicate = predicate.clone();
        let projection = projection.clone();
        let target_schema = target_schema.clone();
        futures.push(Box::pin(async move {
            read_parquet_file(
                store,
                file_path,
                predicate,
                projection,
                batch_size,
                target_schema,
            )
            .await
        }));
    }
    let file_results: Vec<FileResult> = stream::iter(futures)
        .buffer_unordered(parallelism)
        .collect()
        .await;

    let mut all_batches = Vec::new();
    let mut total_skipped = 0usize;
    let mut total_groups = 0usize;
    for result in file_results {
        match result {
            Ok((batches, skipped, total)) => {
                total_skipped += skipped;
                total_groups += total;
                all_batches.extend(batches);
            }
            Err(e) => return Err(PrismError::Internal(e)),
        }
    }

    let total_cols = full_schema.fields().len();
    let proj_desc = match &config.projection {
        Some(cols) => format!("{}/{} cols", cols.len(), total_cols),
        None => "all cols".to_string(),
    };
    tracing::info!(
        "Parquet scan: {}/{} row groups read ({} skipped), {} batches, {} from {} files",
        total_groups - total_skipped,
        total_groups,
        total_skipped,
        all_batches.len(),
        proj_desc,
        total_files,
    );

    // If projection was applied, expand batches back to the full schema
    // (unless skip_expand is set, in which case the caller handles remapping).
    if !config.skip_expand {
        if let Some(ref proj_cols) = config.projection {
            if proj_cols.len() < full_schema.fields().len() {
                let expanded = expand_projected_batches(&all_batches, proj_cols, &full_schema)?;
                return Ok(expanded);
            }
        }
    }

    Ok(all_batches)
}

/// Read a single Parquet file, applying row group skipping and projection.
async fn read_parquet_file(
    store: Arc<dyn ObjectStore>,
    file_path: ObjectPath,
    predicate: Option<Predicate>,
    projection: Option<Vec<usize>>,
    batch_size: usize,
    target_schema: Option<SchemaRef>,
) -> std::result::Result<(Vec<RecordBatch>, usize, usize), String> {
    // Build a ParquetObjectReader and load metadata.
    let reader = ParquetObjectReader::new(store, file_path.clone());
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| format!("parquet builder for {}: {}", file_path, e))?;

    let metadata = builder.metadata().clone();
    let schema = builder.schema().clone();

    let num_row_groups = metadata.num_row_groups();
    let mut skipped = 0usize;

    let row_groups_to_read: Vec<usize> = (0..num_row_groups)
        .filter(|&i| {
            if let Some(ref pred) = predicate {
                let rg_meta = metadata.row_group(i);
                let should_read = row_group_might_match(pred, rg_meta, &schema);
                if !should_read {
                    skipped += 1;
                }
                should_read
            } else {
                true
            }
        })
        .collect();

    if row_groups_to_read.is_empty() {
        return Ok((Vec::new(), skipped, num_row_groups));
    }

    if let Some(ref proj_cols) = projection {
        let parquet_schema = builder.parquet_schema();
        let num_cols = parquet_schema.num_columns();
        let valid_cols: Vec<usize> = proj_cols.iter().copied().filter(|&c| c < num_cols).collect();
        if !valid_cols.is_empty() {
            let mask = ProjectionMask::roots(parquet_schema, valid_cols.into_iter());
            builder = builder.with_projection(mask);
        }
    }

    let stream = builder
        .with_row_groups(row_groups_to_read)
        .with_batch_size(batch_size)
        .build()
        .map_err(|e| format!("parquet stream build {}: {}", file_path, e))?;

    let mut batches = Vec::new();
    let collected: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .map_err(|e| format!("parquet read {}: {}", file_path, e))?;
    for batch in collected {
        if batch.num_rows() == 0 {
            continue;
        }
        // Apply schema adaptation if a target schema was provided.
        // With a projection the parent runs with `skip_expand` + external
        // column remapping and expects file-native layout, so we intentionally
        // skip adaptation in that case — schema evolution across projected
        // columns must be handled at the Iceberg/Delta metadata layer (which
        // is where column IDs live). For full scans, adapt here.
        let out = if let (Some(ref ts), None) = (&target_schema, &projection) {
            adapt_batch_to_schema(&batch, ts).map_err(|e| {
                format!("adapt batch for {}: {}", file_path, e)
            })?
        } else {
            batch
        };
        batches.push(out);
    }

    Ok((batches, skipped, num_row_groups))
}

/// Get the total row count from Parquet metadata without reading any data.
/// This is used for COUNT(*) optimization.
pub async fn parquet_row_count(uris: Vec<String>, predicate: Option<&Predicate>) -> Result<usize> {
    let mut total = 0usize;

    for uri in &uris {
        let (store, prefix) = parse_uri(uri)?;
        let files = list_files(&*store, &prefix, "parquet").await?;

        for file_path in &files {
            let meta = read_file_metadata(store.clone(), file_path).await?;
            let metadata = meta.metadata();
            let schema = meta.schema();

            for i in 0..metadata.num_row_groups() {
                let rg_meta = metadata.row_group(i);
                let should_count = match predicate {
                    Some(pred) => row_group_might_match(pred, rg_meta, schema),
                    None => true,
                };
                if should_count {
                    total += rg_meta.num_rows() as usize;
                }
            }
        }
    }

    Ok(total)
}

async fn read_file_metadata(
    store: Arc<dyn ObjectStore>,
    file_path: &ObjectPath,
) -> Result<ArrowReaderMetadata> {
    let mut reader = ParquetObjectReader::new(store, file_path.clone());
    ArrowReaderMetadata::load_async(&mut reader, Default::default())
        .await
        .map_err(|e| PrismError::Internal(format!("parquet metadata {}: {}", file_path, e)))
}

fn num_cpus_or_default() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
}

/// Adapt a record batch so its schema matches `target`.
///
/// Rules (all applied in a single pass):
/// - **Missing columns**: a field present in `target` but not in the batch
///   becomes a null column of the target type.
/// - **Extra columns**: a field present in the batch but not in `target` is
///   dropped.
/// - **Column ordering**: the output column order matches `target` exactly.
/// - **Type promotion**: when a field name matches but the batch's data type
///   differs, `arrow_cast::cast` is invoked. If the cast fails the caller
///   gets a loud `PrismError::Internal` rather than silent miscomputation.
pub fn adapt_batch_to_schema(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let src_schema = batch.schema();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for tgt_field in target.fields() {
        let tgt_name = tgt_field.name();
        let tgt_dtype = tgt_field.data_type();
        match src_schema.index_of(tgt_name) {
            Ok(src_idx) => {
                let src_col = batch.column(src_idx);
                if src_col.data_type() == tgt_dtype {
                    columns.push(src_col.clone());
                } else {
                    let casted = cast(src_col.as_ref(), tgt_dtype).map_err(|e| {
                        PrismError::Internal(format!(
                            "adapt_batch_to_schema: cannot cast column '{}' from {:?} to {:?}: {}",
                            tgt_name,
                            src_col.data_type(),
                            tgt_dtype,
                            e
                        ))
                    })?;
                    columns.push(casted);
                }
            }
            Err(_) => {
                // Missing column — inject nulls of the target type.
                columns.push(new_null_array(tgt_dtype, num_rows));
            }
        }
    }

    RecordBatch::try_new(target.clone(), columns)
        .map_err(|e| PrismError::Internal(format!("adapt_batch_to_schema: {}", e)))
}

/// Check if a row group might contain rows matching the predicate.
/// Returns `false` if the row group can be definitively skipped.
pub fn row_group_might_match(
    predicate: &Predicate,
    rg_meta: &RowGroupMetaData,
    schema: &Schema,
) -> bool {
    match predicate {
        Predicate::Eq(col, val) => col_stats_might_match(*col, CmpOp::Eq, val, rg_meta, schema),
        Predicate::Ne(_col, _val) => true,
        Predicate::Lt(col, val) => col_stats_might_match(*col, CmpOp::Lt, val, rg_meta, schema),
        Predicate::Le(col, val) => col_stats_might_match(*col, CmpOp::Le, val, rg_meta, schema),
        Predicate::Gt(col, val) => col_stats_might_match(*col, CmpOp::Gt, val, rg_meta, schema),
        Predicate::Ge(col, val) => col_stats_might_match(*col, CmpOp::Ge, val, rg_meta, schema),
        Predicate::IsNull(_) | Predicate::IsNotNull(_) => true,
        Predicate::And(left, right) => {
            row_group_might_match(left, rg_meta, schema)
                && row_group_might_match(right, rg_meta, schema)
        }
        Predicate::Or(left, right) => {
            row_group_might_match(left, rg_meta, schema)
                || row_group_might_match(right, rg_meta, schema)
        }
        Predicate::Not(_) | Predicate::Like(_, _) | Predicate::ILike(_, _) => true,
    }
}

#[derive(Debug, Clone, Copy)]
enum CmpOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

fn col_stats_might_match(
    col_idx: usize,
    op: CmpOp,
    val: &ScalarValue,
    rg_meta: &RowGroupMetaData,
    schema: &Schema,
) -> bool {
    let field_name = match schema.fields().get(col_idx) {
        Some(f) => f.name().as_str(),
        None => return true,
    };

    let col_chunk = rg_meta
        .columns()
        .iter()
        .find(|c| c.column_descr().name() == field_name);

    let col_chunk = match col_chunk {
        Some(c) => c,
        None => return true,
    };

    // Arrow/Parquet 57 removed `Statistics::has_min_max_set`; the recommended check
    // is to consult `min_opt()`/`max_opt()` directly. `stats_might_match` already
    // tolerates a None min/max via `unwrap_or` fallbacks, so we just pass stats
    // through and let the per-branch logic decide whether the row group might match.
    let stats = match col_chunk.statistics() {
        Some(s) => s,
        None => return true,
    };

    stats_might_match(op, val, stats)
}

fn stats_might_match(op: CmpOp, val: &ScalarValue, stats: &Statistics) -> bool {
    match (val, stats) {
        (ScalarValue::Int64(v), Statistics::Int64(s)) => {
            let min = *s.min_opt().unwrap_or(&i64::MIN);
            let max = *s.max_opt().unwrap_or(&i64::MAX);
            check_range(*v as f64, min as f64, max as f64, op)
        }
        (ScalarValue::Int32(v), Statistics::Int32(s)) => {
            let min = *s.min_opt().unwrap_or(&i32::MIN);
            let max = *s.max_opt().unwrap_or(&i32::MAX);
            check_range(*v as f64, min as f64, max as f64, op)
        }
        (ScalarValue::Float64(v), Statistics::Double(s)) => {
            let min = *s.min_opt().unwrap_or(&f64::MIN);
            let max = *s.max_opt().unwrap_or(&f64::MAX);
            check_range(*v, min, max, op)
        }
        (ScalarValue::Utf8(v), Statistics::ByteArray(s)) => {
            let min_bytes = match s.min_opt() {
                Some(b) => String::from_utf8_lossy(b.data()).to_string(),
                None => return true,
            };
            let max_bytes = match s.max_opt() {
                Some(b) => String::from_utf8_lossy(b.data()).to_string(),
                None => return true,
            };
            check_range_str(v.as_str(), &min_bytes, &max_bytes, op)
        }
        _ => true,
    }
}

fn check_range(val: f64, min: f64, max: f64, op: CmpOp) -> bool {
    match op {
        CmpOp::Eq => val >= min && val <= max,
        CmpOp::Lt => min < val,
        CmpOp::Le => min <= val,
        CmpOp::Gt => max > val,
        CmpOp::Ge => max >= val,
    }
}

fn check_range_str(val: &str, min: &str, max: &str, op: CmpOp) -> bool {
    match op {
        CmpOp::Eq => val >= min && val <= max,
        CmpOp::Lt => min < val,
        CmpOp::Le => min <= val,
        CmpOp::Gt => max > val,
        CmpOp::Ge => max >= val,
    }
}

/// Expand projected batches back to the full schema.
fn expand_projected_batches(
    batches: &[RecordBatch],
    proj_cols: &[usize],
    full_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let num_full_cols = full_schema.fields().len();

    // Build a mapping: full_col_index -> projected_batch_index
    let mut proj_map: Vec<Option<usize>> = vec![None; num_full_cols];
    for (batch_idx, &orig_idx) in proj_cols.iter().enumerate() {
        if orig_idx < num_full_cols {
            proj_map[orig_idx] = Some(batch_idx);
        }
    }

    // Build schema where non-projected columns are nullable
    let expanded_fields: Vec<_> = full_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            if proj_map[i].is_some() {
                f.as_ref().clone()
            } else {
                arrow_schema::Field::new(f.name(), f.data_type().clone(), true)
            }
        })
        .collect();
    let expanded_schema: SchemaRef = Arc::new(Schema::new(expanded_fields));

    let mut expanded = Vec::with_capacity(batches.len());
    for batch in batches {
        let num_rows = batch.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_full_cols);

        for (col_idx, mapping) in proj_map.iter().enumerate() {
            match mapping {
                Some(batch_col) => {
                    columns.push(batch.column(*batch_col).clone());
                }
                None => {
                    let dt = full_schema.field(col_idx).data_type().clone();
                    columns.push(new_null_array(&dt, num_rows));
                }
            }
        }

        let expanded_batch = RecordBatch::try_new(expanded_schema.clone(), columns)
            .map_err(|e| PrismError::Internal(format!("expand batch: {}", e)))?;
        expanded.push(expanded_batch);
    }

    Ok(expanded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path as FsPath, PathBuf};
    use std::sync::Arc;

    use arrow_array::{
        Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
        TimestampMillisecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStore, PutPayload};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn build_test_parquet_bytes(row_group_size: usize) -> Vec<u8> {
        let schema = test_schema();
        let n = 500;
        let ids: Vec<i64> = (0..n).collect();
        let values: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let names: Vec<&str> = (0..n)
            .map(|i| {
                if i < 100 {
                    "alpha"
                } else if i < 200 {
                    "beta"
                } else if i < 300 {
                    "gamma"
                } else if i < 400 {
                    "delta"
                } else {
                    "epsilon"
                }
            })
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Float64Array::from(values)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(row_group_size)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
            .build();

        let mut buf: Vec<u8> = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buf
    }

    fn write_test_parquet(path: &FsPath, row_group_size: usize) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let bytes = build_test_parquet_bytes(row_group_size);
        std::fs::write(path, bytes).unwrap();
    }

    /// Write a parquet file with an arbitrary batch to the given in-memory store.
    async fn put_parquet_batch(
        store: &Arc<dyn ObjectStore>,
        path: &str,
        batch: &RecordBatch,
    ) {
        let mut buf: Vec<u8> = Vec::new();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer =
            ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        store
            .put(&ObjectPath::from(path), PutPayload::from(buf))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_parquet_statistics_are_written() {
        let dir = PathBuf::from("/tmp/test_parquet_stats_written");
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("test.parquet");
        write_test_parquet(&path, 100);

        // Round-trip metadata through the async API.
        let (store, prefix) = parse_uri(dir.to_str().unwrap()).unwrap();
        let files = list_files(&*store, &prefix, "parquet").await.unwrap();
        assert_eq!(files.len(), 1);
        let meta = read_file_metadata(store.clone(), &files[0]).await.unwrap();
        assert_eq!(meta.metadata().num_row_groups(), 5);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_row_group_skipping() {
        let dir = PathBuf::from("/tmp/test_parquet_rg_skipping");
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("test.parquet");
        write_test_parquet(&path, 100);

        let pred = Predicate::Lt(1, ScalarValue::Float64(150.0));
        let config = ParquetScanConfig {
            uris: vec![dir.to_string_lossy().into_owned()],
            predicate: Some(pred),
            projection: None,
            batch_size: 8192,
            skip_expand: false,
            target_schema: None,
        };

        let batches = parquet_scan(&config).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 200);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_string_stats_skipping() {
        let dir = PathBuf::from("/tmp/test_parquet_string_skip");
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("test.parquet");
        write_test_parquet(&path, 100);

        let pred = Predicate::Eq(2, ScalarValue::Utf8("gamma".to_string()));
        let config = ParquetScanConfig {
            uris: vec![dir.to_string_lossy().into_owned()],
            predicate: Some(pred),
            projection: None,
            batch_size: 8192,
            skip_expand: false,
            target_schema: None,
        };

        let batches = parquet_scan(&config).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_row_count_metadata() {
        let dir = PathBuf::from("/tmp/test_parquet_row_count");
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("test.parquet");
        write_test_parquet(&path, 100);

        let count = parquet_row_count(vec![dir.to_string_lossy().into_owned()], None)
            .await
            .unwrap();
        assert_eq!(count, 500);

        // With predicate that skips some row groups
        let pred = Predicate::Lt(1, ScalarValue::Float64(150.0));
        let count =
            parquet_row_count(vec![dir.to_string_lossy().into_owned()], Some(&pred))
                .await
                .unwrap();
        assert_eq!(count, 200);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Integration test: prove the abstraction works against a non-local
    /// `ObjectStore` by reading from `InMemory`.
    #[tokio::test]
    async fn test_parquet_scan_from_in_memory_store() {
        let bytes = build_test_parquet_bytes(100);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = ObjectPath::from("tables/sample");
        let file = ObjectPath::from("tables/sample/data.parquet");
        store
            .put(&file, PutPayload::from(bytes))
            .await
            .unwrap();

        // Verify list_files sees the upload.
        let listed = list_files(&*store, &prefix, "parquet").await.unwrap();
        assert_eq!(listed, vec![file.clone()]);

        // Drive a real scan through ParquetObjectReader against InMemory.
        let meta = read_file_metadata(store.clone(), &file).await.unwrap();
        assert_eq!(meta.metadata().num_row_groups(), 5);

        let predicate = Predicate::Lt(1, ScalarValue::Float64(150.0));
        let (batches, skipped, total) = read_parquet_file(
            store,
            file,
            Some(predicate),
            None,
            8192,
            None,
        )
        .await
        .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 200);
        assert_eq!(total, 5);
        assert!(skipped >= 3, "expected >=3 skipped row groups, got {}", skipped);
    }

    #[tokio::test]
    async fn test_parquet_scan_empty_uris_returns_empty() {
        let config = ParquetScanConfig {
            uris: Vec::new(),
            predicate: None,
            projection: None,
            batch_size: 8192,
            skip_expand: false,
            target_schema: None,
        };
        let batches = parquet_scan(&config).await.unwrap();
        assert!(batches.is_empty());
    }

    // --- adapt_batch_to_schema unit tests ---

    #[test]
    fn test_adapt_missing_column_injects_nulls() {
        let src_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
        ]));
        let src = RecordBatch::try_new(
            src_schema,
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();

        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        let out = adapt_batch_to_schema(&src, &target).unwrap();
        assert_eq!(out.schema(), target);
        assert_eq!(out.num_rows(), 3);
        assert_eq!(out.num_columns(), 2);
        let b = out.column(1);
        assert_eq!(b.null_count(), 3);
    }

    #[test]
    fn test_adapt_type_promotion_i32_to_i64() {
        let src_schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, false),
        ]));
        let src = RecordBatch::try_new(
            src_schema,
            vec![Arc::new(Int32Array::from(vec![1i32, 2, 3]))],
        )
        .unwrap();
        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
        ]));
        let out = adapt_batch_to_schema(&src, &target).unwrap();
        assert_eq!(out.schema(), target);
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.values(), &[1i64, 2, 3]);
    }

    #[test]
    fn test_adapt_type_promotion_date32_to_timestamp() {
        let src_schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, false),
        ]));
        // 0 = 1970-01-01, 1 = 1970-01-02
        let src = RecordBatch::try_new(
            src_schema,
            vec![Arc::new(Date32Array::from(vec![0, 1]))],
        )
        .unwrap();
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));
        let out = adapt_batch_to_schema(&src, &target).unwrap();
        assert_eq!(out.schema(), target);
        let ts = out
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        // 1970-01-02 in millis
        assert_eq!(ts.value(1), 86_400_000);
    }

    #[test]
    fn test_adapt_column_reordering() {
        let src_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let src = RecordBatch::try_new(
            src_schema,
            vec![
                Arc::new(Int64Array::from(vec![10i64, 20])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Utf8, false),
            Field::new("a", DataType::Int64, false),
        ]));
        let out = adapt_batch_to_schema(&src, &target).unwrap();
        assert_eq!(out.schema(), target);
        let b = out.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(b.value(0), "x");
        let a = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.values(), &[10i64, 20]);
    }

    #[test]
    fn test_adapt_drops_extra_columns() {
        let src_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("extra", DataType::Utf8, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let src = RecordBatch::try_new(
            src_schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(StringArray::from(vec!["drop", "me"])),
                Arc::new(Int64Array::from(vec![10i64, 20])),
            ],
        )
        .unwrap();

        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let out = adapt_batch_to_schema(&src, &target).unwrap();
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.schema(), target);
    }

    #[test]
    fn test_adapt_invalid_cast_errors() {
        let src_schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
        ]));
        let src = RecordBatch::try_new(
            src_schema,
            vec![Arc::new(StringArray::from(vec!["not-a-number"]))],
        )
        .unwrap();
        // arrow_cast will fail to parse "not-a-number" as Int64 with safe=true.
        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Int64, false),
        ]));
        let result = adapt_batch_to_schema(&src, &target);
        assert!(result.is_err(), "expected invalid cast to fail loudly");
    }

    /// Scan across two files with differing schemas (schema evolution),
    /// target_schema specified, verify unified batches.
    #[tokio::test]
    async fn test_parquet_scan_multi_uri_schema_evolution() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // File A: columns [id Int32, name Utf8]
        let schema_a = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch_a = RecordBatch::try_new(
            schema_a,
            vec![
                Arc::new(Int32Array::from(vec![1i32, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        put_parquet_batch(&store, "data/file_a.parquet", &batch_a).await;

        // File B: columns [id Int64, name Utf8, value Float64] — new column + promoted id
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch_b = RecordBatch::try_new(
            schema_b,
            vec![
                Arc::new(Int64Array::from(vec![3i64, 4])),
                Arc::new(StringArray::from(vec!["c", "d"])),
                Arc::new(Float64Array::from(vec![1.5, 2.5])),
            ],
        )
        .unwrap();
        put_parquet_batch(&store, "data/file_b.parquet", &batch_b).await;

        // Target schema — unified view: id Int64, name Utf8, value Float64
        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]));

        // We scan through InMemory via memory URIs. Each file is wrapped by
        // going through parse_uri with its memory:// equivalent, but parse_uri
        // doesn't support memory://. Instead, exercise read_parquet_file
        // directly for each file with the target schema applied.

        let files = [
            ObjectPath::from("data/file_a.parquet"),
            ObjectPath::from("data/file_b.parquet"),
        ];
        let mut all = Vec::new();
        for f in files {
            let (batches, _s, _t) = read_parquet_file(
                store.clone(),
                f,
                None,
                None,
                8192,
                Some(target.clone()),
            )
            .await
            .unwrap();
            all.extend(batches);
        }

        assert_eq!(all.len(), 2);
        for b in &all {
            assert_eq!(b.schema(), target);
        }
        // File A's 'value' column should be all nulls.
        assert_eq!(all[0].column(2).null_count(), 2);
        // File A's id cast to Int64
        let id_a = all[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_a.values(), &[1i64, 2]);
        // File B unchanged
        assert_eq!(all[1].column(2).null_count(), 0);
    }
}
