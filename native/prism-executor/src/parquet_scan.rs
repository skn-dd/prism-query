//! Parquet reader with row-group-level predicate skipping.
//!
//! Reads Parquet files from a directory, evaluating column min/max statistics
//! per row group to skip groups that cannot match the predicate.

use std::fs;
use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;

use crate::error::Result;
use crate::filter_project::{Predicate, ScalarValue};

/// Configuration for a Parquet table scan.
pub struct ParquetScanConfig {
    /// Path to a directory containing .parquet files (or a single file).
    pub path: PathBuf,
    /// Optional predicate for row group skipping.
    pub predicate: Option<Predicate>,
    /// Batch size for the Parquet reader.
    pub batch_size: usize,
}

/// Scan Parquet files, skipping row groups whose statistics
/// prove the predicate cannot match.
pub fn parquet_scan(config: &ParquetScanConfig) -> Result<Vec<RecordBatch>> {
    let files = list_parquet_files(&config.path)?;
    if files.is_empty() {
        return Err(crate::PrismError::Internal(format!(
            "no .parquet files found at {:?}",
            config.path
        )));
    }

    let mut all_batches = Vec::new();
    let mut skipped_groups = 0usize;
    let mut total_groups = 0usize;

    for file_path in &files {
        let file = fs::File::open(file_path).map_err(|e| {
            crate::PrismError::Internal(format!("failed to open {:?}: {}", file_path, e))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            crate::PrismError::Internal(format!("failed to read parquet {:?}: {}", file_path, e))
        })?;

        let metadata = builder.metadata().clone();
        let schema = builder.schema().clone();

        // Determine which row groups to read based on predicate statistics
        let num_row_groups = metadata.num_row_groups();
        total_groups += num_row_groups;

        let row_groups_to_read: Vec<usize> = (0..num_row_groups)
            .filter(|&i| {
                if let Some(ref pred) = config.predicate {
                    let rg_meta = metadata.row_group(i);
                    let should_read = row_group_might_match(pred, rg_meta, &schema);
                    if !should_read {
                        skipped_groups += 1;
                    }
                    should_read
                } else {
                    true
                }
            })
            .collect();

        if row_groups_to_read.is_empty() {
            continue;
        }

        // Re-open file for reading (builder consumed the first handle)
        let file = fs::File::open(file_path).map_err(|e| {
            crate::PrismError::Internal(format!("failed to reopen {:?}: {}", file_path, e))
        })?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| crate::PrismError::Internal(format!("parquet builder: {}", e)))?
            .with_row_groups(row_groups_to_read)
            .with_batch_size(config.batch_size)
            .build()
            .map_err(|e| crate::PrismError::Internal(format!("parquet reader: {}", e)))?;

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| crate::PrismError::Internal(format!("parquet read: {}", e)))?;
            if batch.num_rows() > 0 {
                all_batches.push(batch);
            }
        }
    }

    tracing::info!(
        "Parquet scan: {}/{} row groups read ({} skipped) from {} files, {} batches loaded",
        total_groups - skipped_groups,
        total_groups,
        skipped_groups,
        files.len(),
        all_batches.len(),
    );

    Ok(all_batches)
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
        Predicate::Ne(_col, _val) => true, // can't skip for !=
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
            // Both must potentially match — skip if either says skip
            row_group_might_match(left, rg_meta, schema)
                && row_group_might_match(right, rg_meta, schema)
        }
        Predicate::Or(left, right) => {
            // Either could match — skip only if BOTH say skip
            row_group_might_match(left, rg_meta, schema)
                || row_group_might_match(right, rg_meta, schema)
        }
        Predicate::Not(_) => true, // conservatively don't skip
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

/// Check if a column's statistics in a row group are compatible with a comparison.
fn col_stats_might_match(
    col_idx: usize,
    op: CmpOp,
    val: &ScalarValue,
    rg_meta: &RowGroupMetaData,
    schema: &Schema,
) -> bool {
    // Find the column in the row group metadata by matching schema field name
    let field_name = match schema.fields().get(col_idx) {
        Some(f) => f.name().as_str(),
        None => return true, // unknown column, don't skip
    };

    // Find the column chunk by name
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
        _ => return true, // no stats available, can't skip
    };

    stats_might_match(op, val, stats)
}

/// Core statistics matching: compare a scalar value against column min/max.
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
        _ => true, // type mismatch or unsupported, don't skip
    }
}

/// Check if a value is compatible with a [min, max] range for the given operator.
fn check_range(val: f64, min: f64, max: f64, op: CmpOp) -> bool {
    match op {
        CmpOp::Eq => val >= min && val <= max,
        CmpOp::Lt => min < val,       // skip if min >= val (all rows >= val)
        CmpOp::Le => min <= val,       // skip if min > val
        CmpOp::Gt => max > val,        // skip if max <= val (all rows <= val)
        CmpOp::Ge => max >= val,       // skip if max < val
    }
}

/// String comparison for row group skipping.
fn check_range_str(val: &str, min: &str, max: &str, op: CmpOp) -> bool {
    match op {
        CmpOp::Eq => val >= min && val <= max,
        CmpOp::Lt => min < val,
        CmpOp::Le => min <= val,
        CmpOp::Gt => max > val,
        CmpOp::Ge => max >= val,
    }
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
    use parquet::file::reader::FileReader;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReader;

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

        // Create sorted data: 5 row groups of 100 rows each
        // Row group 0: value 0.0-99.0
        // Row group 1: value 100.0-199.0
        // Row group 2: value 200.0-299.0
        // Row group 3: value 300.0-399.0
        // Row group 4: value 400.0-499.0
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

        // Verify stats are present
        let file = fs::File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = builder.metadata();
        
        assert_eq!(metadata.num_row_groups(), 5, "expected 5 row groups");
        
        for i in 0..5 {
            let rg = metadata.row_group(i);
            println!("Row group {}: {} rows", i, rg.num_rows());
            assert_eq!(rg.num_rows(), 100, "each row group should have 100 rows");
            
            for col in rg.columns() {
                let stats = col.statistics().expect("statistics should be present");
                assert!(stats.has_min_max_set(), 
                    "min/max should be set for column {}", col.column_descr().name());
                println!("  Column {}: has_min_max={}", 
                    col.column_descr().name(), stats.has_min_max_set());
                
                match stats {
                    Statistics::Int64(s) => {
                        println!("    Int64 min={:?} max={:?}", s.min_opt(), s.max_opt());
                    }
                    Statistics::Double(s) => {
                        println!("    Double min={:?} max={:?}", s.min_opt(), s.max_opt());
                    }
                    Statistics::ByteArray(s) => {
                        let min = s.min_opt().map(|b| String::from_utf8_lossy(b.data()).to_string());
                        let max = s.max_opt().map(|b| String::from_utf8_lossy(b.data()).to_string());
                        println!("    ByteArray min={:?} max={:?}", min, max);
                    }
                    _ => {}
                }
            }
        }
        
        // Verify specific stats for value column (col 1)
        let rg0 = metadata.row_group(0);
        let value_stats = rg0.columns().iter()
            .find(|c| c.column_descr().name() == "value")
            .unwrap()
            .statistics()
            .unwrap();
        
        match value_stats {
            Statistics::Double(s) => {
                assert_eq!(*s.min_opt().unwrap(), 0.0, "RG0 value min should be 0.0");
                assert_eq!(*s.max_opt().unwrap(), 99.0, "RG0 value max should be 99.0");
            }
            _ => panic!("expected Double statistics for value column"),
        }
        
        let rg4 = metadata.row_group(4);
        let value_stats = rg4.columns().iter()
            .find(|c| c.column_descr().name() == "value")
            .unwrap()
            .statistics()
            .unwrap();
        
        match value_stats {
            Statistics::Double(s) => {
                assert_eq!(*s.min_opt().unwrap(), 400.0, "RG4 value min should be 400.0");
                assert_eq!(*s.max_opt().unwrap(), 499.0, "RG4 value max should be 499.0");
            }
            _ => panic!("expected Double statistics for value column"),
        }
        
        println!("\n✓ All statistics verified!");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_row_group_skipping() {
        let dir = PathBuf::from("/tmp/test_parquet_rg_skipping");
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("test.parquet");
        
        write_test_parquet(&path, 100);

        // Predicate: value < 150.0 — should read RG0 (0-99) and RG1 (100-199), skip RG2-4
        let pred = Predicate::Lt(1, ScalarValue::Float64(150.0));
        
        let config = ParquetScanConfig {
            path: path.clone(),
            predicate: Some(pred),
            batch_size: 8192,
        };
        
        let batches = parquet_scan(&config).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        
        // Should read 200 rows (RG0 + RG1), not all 500
        // The filter node will then further reduce to 150 rows
        println!("Rows loaded with Lt(value, 150.0): {}", total_rows);
        assert_eq!(total_rows, 200, "should load only RG0+RG1 (200 rows), not all 500");
        
        // Predicate: value >= 300.0 — should read RG3 (300-399) and RG4 (400-499), skip RG0-2
        let pred = Predicate::Ge(1, ScalarValue::Float64(300.0));
        let config = ParquetScanConfig {
            path: path.clone(),
            predicate: Some(pred),
            batch_size: 8192,
        };
        
        let batches = parquet_scan(&config).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("Rows loaded with Ge(value, 300.0): {}", total_rows);
        assert_eq!(total_rows, 200, "should load only RG3+RG4 (200 rows)");
        
        // Predicate: value == 250.0 — should read only RG2 (200-299)
        let pred = Predicate::Eq(1, ScalarValue::Float64(250.0));
        let config = ParquetScanConfig {
            path: path.clone(),
            predicate: Some(pred),
            batch_size: 8192,
        };
        
        let batches = parquet_scan(&config).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("Rows loaded with Eq(value, 250.0): {}", total_rows);
        assert_eq!(total_rows, 100, "should load only RG2 (100 rows)");
        
        // No predicate — should read all 500 rows
        let config = ParquetScanConfig {
            path: path.clone(),
            predicate: None,
            batch_size: 8192,
        };
        
        let batches = parquet_scan(&config).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("Rows loaded with no predicate: {}", total_rows);
        assert_eq!(total_rows, 500, "should load all 500 rows");
        
        // AND predicate: value >= 100.0 AND value < 300.0 — should read RG1 + RG2
        let pred = Predicate::And(
            Box::new(Predicate::Ge(1, ScalarValue::Float64(100.0))),
            Box::new(Predicate::Lt(1, ScalarValue::Float64(300.0))),
        );
        let config = ParquetScanConfig {
            path: path.clone(),
            predicate: Some(pred),
            batch_size: 8192,
        };
        
        let batches = parquet_scan(&config).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("Rows loaded with Ge(100) AND Lt(300): {}", total_rows);
        assert_eq!(total_rows, 200, "should load only RG1+RG2 (200 rows)");
        
        println!("\n✓ All row group skipping tests passed!");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_string_stats_skipping() {
        let dir = PathBuf::from("/tmp/test_parquet_string_skip");
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("test.parquet");
        
        write_test_parquet(&path, 100);
        
        // name column (col 2): RG0=alpha, RG1=beta, RG2=gamma, RG3=delta, RG4=epsilon
        // Predicate: name == "gamma" — should read only RG2
        let pred = Predicate::Eq(2, ScalarValue::Utf8("gamma".to_string()));
        let config = ParquetScanConfig {
            path: path.clone(),
            predicate: Some(pred),
            batch_size: 8192,
        };
        
        let batches = parquet_scan(&config).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("Rows loaded with Eq(name, gamma): {}", total_rows);
        assert_eq!(total_rows, 100, "should load only RG2 with name=gamma");
        
        println!("\n✓ String stats skipping test passed!");
        let _ = fs::remove_dir_all(&dir);
    }
}
