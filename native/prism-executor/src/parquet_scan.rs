//! Parquet reader with column pruning, row-group-level predicate skipping,
//! and parallel I/O via rayon.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_array::{ArrayRef, new_null_array};
use arrow_schema::{Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use rayon::prelude::*;

use crate::error::Result;
use crate::filter_project::{Predicate, ScalarValue};

/// Configuration for a Parquet table scan.
pub struct ParquetScanConfig {
    /// Path to a directory containing .parquet files (or a single file).
    pub path: PathBuf,
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
}

/// Scan Parquet files with parallel I/O, skipping row groups whose statistics
/// prove the predicate cannot match.
pub fn parquet_scan(config: &ParquetScanConfig) -> Result<Vec<RecordBatch>> {
    let files = list_parquet_files(&config.path)?;
    if files.is_empty() {
        return Err(crate::PrismError::Internal(format!(
            "no .parquet files found at {:?}",
            config.path
        )));
    }

    // Peek at first file for schema info
    let first_file = fs::File::open(&files[0]).map_err(|e| {
        crate::PrismError::Internal(format!("failed to open {:?}: {}", files[0], e))
    })?;
    let peek_builder = ParquetRecordBatchReaderBuilder::try_new(first_file).map_err(|e| {
        crate::PrismError::Internal(format!("failed to read parquet {:?}: {}", files[0], e))
    })?;
    let full_schema: SchemaRef = peek_builder.schema().clone();

    // Read files in parallel using rayon
    let file_results: Vec<std::result::Result<(Vec<RecordBatch>, usize, usize), String>> = files
        .par_iter()
        .map(|file_path| {
            read_parquet_file(file_path, config)
        })
        .collect();

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
            Err(e) => return Err(crate::PrismError::Internal(e)),
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
        files.len(),
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
fn read_parquet_file(
    file_path: &Path,
    config: &ParquetScanConfig,
) -> std::result::Result<(Vec<RecordBatch>, usize, usize), String> {
    let file = fs::File::open(file_path)
        .map_err(|e| format!("failed to open {:?}: {}", file_path, e))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("failed to read parquet {:?}: {}", file_path, e))?;

    let metadata = builder.metadata().clone();
    let schema = builder.schema().clone();

    let num_row_groups = metadata.num_row_groups();
    let mut skipped = 0usize;

    let row_groups_to_read: Vec<usize> = (0..num_row_groups)
        .filter(|&i| {
            if let Some(ref pred) = config.predicate {
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

    // Re-open file for reading
    let file = fs::File::open(file_path)
        .map_err(|e| format!("failed to reopen {:?}: {}", file_path, e))?;

    let mut reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("parquet builder: {}", e))?;

    // Apply column projection — filter out virtual column indices that exceed the schema
    if let Some(ref proj_cols) = config.projection {
        let parquet_schema = reader_builder.parquet_schema();
        let num_cols = parquet_schema.num_columns();
        let valid_cols: Vec<usize> = proj_cols.iter().copied().filter(|&c| c < num_cols).collect();
        if !valid_cols.is_empty() {
            let mask = ProjectionMask::roots(&parquet_schema, valid_cols.into_iter());
            reader_builder = reader_builder.with_projection(mask);
        }
    }

    let reader = reader_builder
        .with_row_groups(row_groups_to_read)
        .with_batch_size(config.batch_size)
        .build()
        .map_err(|e| format!("parquet reader: {}", e))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("parquet read: {}", e))?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }

    Ok((batches, skipped, num_row_groups))
}

/// Get the total row count from Parquet metadata without reading any data.
/// This is used for COUNT(*) optimization.
pub fn parquet_row_count(path: &Path, predicate: Option<&Predicate>) -> Result<usize> {
    let files = list_parquet_files(path)?;
    let mut total = 0usize;

    for file_path in &files {
        let file = fs::File::open(file_path).map_err(|e| {
            crate::PrismError::Internal(format!("failed to open {:?}: {}", file_path, e))
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            crate::PrismError::Internal(format!("failed to read parquet {:?}: {}", file_path, e))
        })?;
        let metadata = builder.metadata();
        let schema = builder.schema();

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

    Ok(total)
}

/// Check if a row group might contain rows matching the predicate.
/// Returns `false` if the row group can be definitively skipped.
pub fn row_group_might_match(
    predicate: &Predicate,
    rg_meta: &RowGroupMetaData,
    schema: &Schema,
) -> bool {
    match predicate {
        Predicate::Eq(col, val) => {
            col_stats_might_match(*col, CmpOp::Eq, val, rg_meta, schema)
        }
        Predicate::Ne(_col, _val) => true,
        Predicate::Lt(col, val) => {
            col_stats_might_match(*col, CmpOp::Lt, val, rg_meta, schema)
        }
        Predicate::Le(col, val) => {
            col_stats_might_match(*col, CmpOp::Le, val, rg_meta, schema)
        }
        Predicate::Gt(col, val) => {
            col_stats_might_match(*col, CmpOp::Gt, val, rg_meta, schema)
        }
        Predicate::Ge(col, val) => {
            col_stats_might_match(*col, CmpOp::Ge, val, rg_meta, schema)
        }
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

    let stats = match col_chunk.statistics() {
        Some(s) if s.has_min_max_set() => s,
        _ => return true,
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
            .map_err(|e| crate::PrismError::Internal(format!("expand batch: {}", e)))?;
        expanded.push(expanded_batch);
    }

    Ok(expanded)
}

/// List all .parquet files in a directory (or return the single file).
fn list_parquet_files(path: &Path) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }

    if !path.is_dir() {
        return Err(crate::PrismError::Internal(format!(
            "path {:?} is neither a file nor a directory",
            path
        )));
    }

    let mut files: Vec<PathBuf> = fs::read_dir(path)
        .map_err(|e| crate::PrismError::Internal(format!("readdir {:?}: {}", path, e)))?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                Some(p)
            } else {
                None
            }
        })
        .collect();

    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
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

    fn write_test_parquet(path: &Path, row_group_size: usize) {
        let schema = test_schema();
        fs::create_dir_all(path.parent().unwrap()).unwrap();

        let n = 500;
        let ids: Vec<i64> = (0..n).collect();
        let values: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let names: Vec<&str> = (0..n).map(|i| {
            if i < 100 { "alpha" }
            else if i < 200 { "beta" }
            else if i < 300 { "gamma" }
            else if i < 400 { "delta" }
            else { "epsilon" }
        }).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Float64Array::from(values)),
                Arc::new(StringArray::from(names)),
            ],
        ).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(row_group_size)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
            .build();

        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn test_parquet_statistics_are_written() {
        let dir = PathBuf::from("/tmp/test_parquet_stats_written");
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("test.parquet");
        write_test_parquet(&path, 100);

        let file = fs::File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = builder.metadata();
        assert_eq!(metadata.num_row_groups(), 5);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_row_group_skipping() {
        let dir = PathBuf::from("/tmp/test_parquet_rg_skipping");
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("test.parquet");
        write_test_parquet(&path, 100);

        let pred = Predicate::Lt(1, ScalarValue::Float64(150.0));
        let config = ParquetScanConfig {
            path: path.clone(),
            predicate: Some(pred),
            projection: None,
            batch_size: 8192,
            skip_expand: false,
        };

        let batches = parquet_scan(&config).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 200);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_string_stats_skipping() {
        let dir = PathBuf::from("/tmp/test_parquet_string_skip");
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("test.parquet");
        write_test_parquet(&path, 100);

        let pred = Predicate::Eq(2, ScalarValue::Utf8("gamma".to_string()));
        let config = ParquetScanConfig {
            path: path.clone(),
            predicate: Some(pred),
            projection: None,
            batch_size: 8192,
            skip_expand: false,
        };

        let batches = parquet_scan(&config).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_row_count_metadata() {
        let dir = PathBuf::from("/tmp/test_parquet_row_count");
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("test.parquet");
        write_test_parquet(&path, 100);

        let count = parquet_row_count(&path, None).unwrap();
        assert_eq!(count, 500);

        // With predicate that skips some row groups
        let pred = Predicate::Lt(1, ScalarValue::Float64(150.0));
        let count = parquet_row_count(&path, Some(&pred)).unwrap();
        assert_eq!(count, 200);

        let _ = fs::remove_dir_all(&dir);
    }
}
