//! Vectorized filter + projection on Arrow RecordBatches.
//!
//! Replaces Trino's `ScanFilterAndProjectOperator`. Evaluates predicates
//! using Arrow compute kernels with SIMD bitmasking, then projects selected columns.

use std::sync::Arc;

use arrow::compute::{self, FilterBuilder};
use arrow_array::{
    cast::AsArray, Array, ArrayRef, BooleanArray, Datum, Float64Array, RecordBatch,
};
use arrow_array::types::{Float64Type, Int32Type, Int64Type};
use arrow_cast::cast;
use arrow_ord::cmp;
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{PrismError, Result};

/// A predicate that can be evaluated against a RecordBatch.
#[derive(Debug, Clone)]
pub enum Predicate {
    /// column_index == literal
    Eq(usize, ScalarValue),
    /// column_index != literal
    Ne(usize, ScalarValue),
    /// column_index < literal
    Lt(usize, ScalarValue),
    /// column_index <= literal
    Le(usize, ScalarValue),
    /// column_index > literal
    Gt(usize, ScalarValue),
    /// column_index >= literal
    Ge(usize, ScalarValue),
    /// column IS NULL
    IsNull(usize),
    /// column IS NOT NULL
    IsNotNull(usize),
    /// AND of two predicates
    And(Box<Predicate>, Box<Predicate>),
    /// OR of two predicates
    Or(Box<Predicate>, Box<Predicate>),
    /// NOT of a predicate
    Not(Box<Predicate>),
    /// SQL LIKE pattern match
    Like(usize, String),
    /// SQL ILIKE (case-insensitive LIKE)
    ILike(usize, String),
}

/// Scalar value for predicate comparison.
#[derive(Debug, Clone)]
pub enum ScalarValue {
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Boolean(bool),
    Date32(i32), // days since epoch
}

/// Evaluate a predicate against a RecordBatch, producing a boolean selection mask.
pub fn evaluate_predicate(batch: &RecordBatch, predicate: &Predicate) -> Result<BooleanArray> {
    match predicate {
        Predicate::Eq(col, val) => compare_column(batch, *col, val, CompareOp::Eq),
        Predicate::Ne(col, val) => compare_column(batch, *col, val, CompareOp::Ne),
        Predicate::Lt(col, val) => compare_column(batch, *col, val, CompareOp::Lt),
        Predicate::Le(col, val) => compare_column(batch, *col, val, CompareOp::Le),
        Predicate::Gt(col, val) => compare_column(batch, *col, val, CompareOp::Gt),
        Predicate::Ge(col, val) => compare_column(batch, *col, val, CompareOp::Ge),
        Predicate::IsNull(col) => {
            let array = batch.column(*col);
            Ok(compute::is_null(array)?)
        }
        Predicate::IsNotNull(col) => {
            let array = batch.column(*col);
            Ok(compute::is_not_null(array)?)
        }
        Predicate::And(left, right) => {
            let l = evaluate_predicate(batch, left)?;
            let r = evaluate_predicate(batch, right)?;
            Ok(compute::and(&l, &r)?)
        }
        Predicate::Or(left, right) => {
            let l = evaluate_predicate(batch, left)?;
            let r = evaluate_predicate(batch, right)?;
            Ok(compute::or(&l, &r)?)
        }
        Predicate::Not(inner) => {
            let mask = evaluate_predicate(batch, inner)?;
            Ok(compute::not(&mask)?)
        }
        Predicate::Like(col, pattern) => {
            let array = batch.column(*col);
            match array.data_type() {
                DataType::Utf8 => {
                    let str_arr = array.as_string::<i32>();
                    Ok(crate::string_ops::string_like(str_arr, pattern)?)
                }
                _ => Err(PrismError::InvalidArgument(format!(
                    "LIKE requires Utf8 column, got {:?}", array.data_type()
                ))),
            }
        }
        Predicate::ILike(col, pattern) => {
            let array = batch.column(*col);
            match array.data_type() {
                DataType::Utf8 => {
                    let str_arr = array.as_string::<i32>();
                    Ok(crate::string_ops::string_ilike(str_arr, pattern)?)
                }
                _ => Err(PrismError::InvalidArgument(format!(
                    "ILIKE requires Utf8 column, got {:?}", array.data_type()
                ))),
            }
        }
    }
}

/// Apply a boolean filter mask to a RecordBatch, returning only matching rows.
pub fn filter_batch(batch: &RecordBatch, mask: &BooleanArray) -> Result<RecordBatch> {
    let filter = FilterBuilder::new(mask).optimize().build();
    let columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| filter.filter(col))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// Project specific columns from a RecordBatch by index.
pub fn project_batch(batch: &RecordBatch, column_indices: &[usize]) -> Result<RecordBatch> {
    let schema = batch.schema();
    let projected_fields: Vec<_> = column_indices
        .iter()
        .map(|&i| schema.field(i).clone())
        .collect();
    let projected_schema = SchemaRef::new(arrow_schema::Schema::new(projected_fields));
    let projected_columns: Vec<_> = column_indices
        .iter()
        .map(|&i| batch.column(i).clone())
        .collect();

    Ok(RecordBatch::try_new(projected_schema, projected_columns)?)
}

/// Combined filter + project in a single pass. Applies predicate, then projects columns.
pub fn filter_and_project(
    batch: &RecordBatch,
    predicate: Option<&Predicate>,
    column_indices: &[usize],
) -> Result<RecordBatch> {
    let filtered = if let Some(pred) = predicate {
        let mask = evaluate_predicate(batch, pred)?;
        filter_batch(batch, &mask)?
    } else {
        batch.clone()
    };

    if column_indices.is_empty() {
        Ok(filtered)
    } else {
        project_batch(&filtered, column_indices)
    }
}

// ===== Scalar Expression Evaluation =====

/// Arithmetic operation for scalar expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
}

/// A scalar expression that produces an Arrow array from a RecordBatch.
#[derive(Debug, Clone)]
pub enum ScalarExpr {
    /// Reference to an input column by index.
    ColumnRef(usize),
    /// A literal value broadcast to the batch length.
    Literal(ScalarValue),
    /// Binary arithmetic operation.
    BinaryOp {
        op: ArithmeticOp,
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
    },
    /// Unary negation.
    Negate(Box<ScalarExpr>),
}

/// Evaluate a scalar expression against a RecordBatch, returning a Float64 array.
pub fn evaluate_scalar_expr(batch: &RecordBatch, expr: &ScalarExpr) -> Result<ArrayRef> {
    match expr {
        ScalarExpr::ColumnRef(idx) => Ok(batch.column(*idx).clone()),
        ScalarExpr::Literal(val) => {
            let len = batch.num_rows();
            let arr: ArrayRef = match val {
                ScalarValue::Float64(v) => Arc::new(Float64Array::from(vec![*v; len])),
                ScalarValue::Int64(v) => Arc::new(arrow_array::Int64Array::from(vec![*v; len])),
                ScalarValue::Int32(v) => Arc::new(arrow_array::Int32Array::from(vec![*v; len])),
                ScalarValue::Date32(v) => Arc::new(arrow_array::Date32Array::from(vec![*v; len])),
                _ => return Err(PrismError::InvalidArgument(
                    format!("unsupported literal type in scalar expr: {:?}", val),
                )),
            };
            Ok(arr)
        }
        ScalarExpr::BinaryOp { op, left, right } => {
            let l = evaluate_scalar_expr(batch, left)?;
            let r = evaluate_scalar_expr(batch, right)?;
            // Cast both operands to Float64 for arithmetic
            let l_f64 = cast(&l, &DataType::Float64)?;
            let r_f64 = cast(&r, &DataType::Float64)?;
            let la = l_f64.as_primitive::<Float64Type>();
            let ra = r_f64.as_primitive::<Float64Type>();
            let result: ArrayRef = match op {
                ArithmeticOp::Add => Arc::new(arrow::compute::kernels::numeric::add(la, ra)?),
                ArithmeticOp::Subtract => Arc::new(arrow::compute::kernels::numeric::sub(la, ra)?),
                ArithmeticOp::Multiply => Arc::new(arrow::compute::kernels::numeric::mul(la, ra)?),
                ArithmeticOp::Divide => Arc::new(arrow::compute::kernels::numeric::div(la, ra)?),
            };
            Ok(result)
        }
        ScalarExpr::Negate(inner) => {
            let arr = evaluate_scalar_expr(batch, inner)?;
            let f64_arr = cast(&arr, &DataType::Float64)?;
            let fa = f64_arr.as_primitive::<Float64Type>();
            let negated: Float64Array = fa.iter().map(|v| v.map(|x| -x)).collect();
            Ok(Arc::new(negated))
        }
    }
}

/// Project columns by index plus evaluate computed expressions, producing a new RecordBatch.
pub fn project_batch_with_exprs(
    batch: &RecordBatch,
    column_indices: &[usize],
    expressions: &[ScalarExpr],
) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Passthrough columns
    for &i in column_indices {
        fields.push(schema.field(i).clone());
        columns.push(batch.column(i).clone());
    }

    // Computed expression columns
    for (expr_idx, expr) in expressions.iter().enumerate() {
        let array = evaluate_scalar_expr(batch, expr)?;
        fields.push(Field::new(
            format!("expr_{}", expr_idx),
            array.data_type().clone(),
            true,
        ));
        columns.push(array);
    }

    let out_schema = SchemaRef::new(Schema::new(fields));
    Ok(RecordBatch::try_new(out_schema, columns)?)
}

// --- Internal helpers ---

enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

fn compare_column(
    batch: &RecordBatch,
    col: usize,
    val: &ScalarValue,
    op: CompareOp,
) -> Result<BooleanArray> {
    let array = batch.column(col);

    match (array.data_type(), val) {
        (DataType::Int32, ScalarValue::Int32(v)) => {
            let arr = array.as_primitive::<Int32Type>();
            let scalar_arr = arrow_array::Int32Array::new_scalar(*v);
            apply_cmp(arr as &dyn Datum, &scalar_arr as &dyn Datum, op)
        }
        (DataType::Int64, ScalarValue::Int64(v)) => {
            let arr = array.as_primitive::<Int64Type>();
            let scalar_arr = arrow_array::Int64Array::new_scalar(*v);
            apply_cmp(arr as &dyn Datum, &scalar_arr as &dyn Datum, op)
        }
        (DataType::Float64, ScalarValue::Float64(v)) => {
            let arr = array.as_primitive::<Float64Type>();
            let scalar_arr = arrow_array::Float64Array::new_scalar(*v);
            apply_cmp(arr as &dyn Datum, &scalar_arr as &dyn Datum, op)
        }
        (DataType::Utf8, ScalarValue::Utf8(v)) => {
            let arr = array.as_string::<i32>();
            let scalar_arr = arrow_array::StringArray::new_scalar(v);
            apply_cmp(arr as &dyn Datum, &scalar_arr as &dyn Datum, op)
        }
        (DataType::Date32, ScalarValue::Date32(v)) => {
            let arr = array.as_primitive::<arrow_array::types::Date32Type>();
            let scalar_arr = arrow_array::Date32Array::new_scalar(*v);
            apply_cmp(arr as &dyn Datum, &scalar_arr as &dyn Datum, op)
        }
        // Allow Int32 scalar to compare against Date32 column (Trino sends dates as ints)
        (DataType::Date32, ScalarValue::Int32(v)) => {
            let arr = array.as_primitive::<arrow_array::types::Date32Type>();
            let scalar_arr = arrow_array::Date32Array::new_scalar(*v);
            apply_cmp(arr as &dyn Datum, &scalar_arr as &dyn Datum, op)
        }
        _ => Err(PrismError::InvalidArgument(format!(
            "Cannot compare column type {:?} with scalar {:?}",
            array.data_type(),
            val
        ))),
    }
}

fn apply_cmp(
    left: &dyn Datum,
    right: &dyn Datum,
    op: CompareOp,
) -> Result<BooleanArray> {
    let result = match op {
        CompareOp::Eq => cmp::eq(left, right)?,
        CompareOp::Ne => cmp::neq(left, right)?,
        CompareOp::Lt => cmp::lt(left, right)?,
        CompareOp::Le => cmp::lt_eq(left, right)?,
        CompareOp::Gt => cmp::gt(left, right)?,
        CompareOp::Ge => cmp::gt_eq(left, right)?,
    };
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, Float64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol", "dave", "eve"])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_filter_gt() {
        let batch = test_batch();
        let pred = Predicate::Gt(0, ScalarValue::Int64(3));
        let mask = evaluate_predicate(&batch, &pred).unwrap();
        let result = filter_batch(&batch, &mask).unwrap();
        assert_eq!(result.num_rows(), 2);
        let ids = result.column(0).as_primitive::<Int64Type>();
        assert_eq!(ids.value(0), 4);
        assert_eq!(ids.value(1), 5);
    }

    #[test]
    fn test_filter_and_project() {
        let batch = test_batch();
        let pred = Predicate::Le(2, ScalarValue::Float64(30.0));
        let result = filter_and_project(&batch, Some(&pred), &[1, 2]).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 2);
        let names = result.column(0).as_string::<i32>();
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(2), "carol");
    }

    #[test]
    fn test_compound_predicate() {
        let batch = test_batch();
        let pred = Predicate::And(
            Box::new(Predicate::Ge(0, ScalarValue::Int64(2))),
            Box::new(Predicate::Le(0, ScalarValue::Int64(4))),
        );
        let mask = evaluate_predicate(&batch, &pred).unwrap();
        let result = filter_batch(&batch, &mask).unwrap();
        assert_eq!(result.num_rows(), 3);
    }
}
