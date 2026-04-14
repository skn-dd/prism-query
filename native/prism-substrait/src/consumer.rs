//! Substrait plan consumer — converts Substrait protobuf plans into Prism PlanNodes.
//!
//! This is the core translation layer. The Trino coordinator serializes its
//! optimized plan into Substrait format (via prism-bridge Java module), then
//! this consumer deserializes it into our native execution plan tree.

use std::sync::Arc;

use prost::Message;
use substrait::proto::{
    rel::RelType,
    Plan as SubstraitPlan, Rel,
    read_rel::ReadType,
    expression::{
        literal::LiteralType,
        field_reference::ReferenceType,
        RexType,
        ScalarFunction,
    },
    sort_field::{SortDirection as SubstraitSortDirection, SortKind},
    AggregateRel, Expression, FetchRel, FilterRel, JoinRel, ProjectRel, ReadRel, SortRel,
};

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use prism_executor::filter_project::{ArithmeticOp, Predicate, ScalarExpr, ScalarValue};
use prism_executor::hash_aggregate::{AggExpr, AggFunc};
use prism_executor::hash_join::JoinType;
use prism_executor::sort::{NullOrdering, SortDirection, SortKey};

use crate::plan::{ExecutionPlan, PlanNode};
use crate::{SubstraitError, Result};

/// Consume a Substrait plan (serialized protobuf bytes) and produce an ExecutionPlan.
pub fn consume_plan(substrait_bytes: &[u8]) -> Result<ExecutionPlan> {
    let plan = SubstraitPlan::decode(substrait_bytes)?;

    let root_rel = plan
        .relations
        .first()
        .ok_or_else(|| SubstraitError::MissingField("plan.relations".into()))?;

    let rel = match &root_rel.rel_type {
        Some(substrait::proto::plan_rel::RelType::Root(root)) => root
            .input
            .as_ref()
            .ok_or_else(|| SubstraitError::MissingField("root.input".into()))?,
        Some(substrait::proto::plan_rel::RelType::Rel(rel)) => rel,
        None => return Err(SubstraitError::MissingField("plan_rel.rel_type".into())),
    };

    let root_node = consume_rel(rel)?;

    // Derive output schema from the plan node
    let output_schema = derive_schema(&root_node)?;

    Ok(ExecutionPlan {
        root: root_node,
        output_schema,
    })
}

/// Recursively consume a Substrait Rel into a PlanNode.
fn consume_rel(rel: &Rel) -> Result<PlanNode> {
    match &rel.rel_type {
        Some(RelType::Read(read)) => consume_read(read),
        Some(RelType::Filter(filter)) => consume_filter(filter),
        Some(RelType::Project(project)) => consume_project(project),
        Some(RelType::Aggregate(aggregate)) => consume_aggregate(aggregate),
        Some(RelType::Join(join)) => consume_join(join),
        Some(RelType::Sort(sort)) => consume_sort(sort),
        Some(RelType::Fetch(fetch)) => consume_fetch(fetch),
        Some(other) => Err(SubstraitError::UnsupportedRelation(format!("{:?}", other))),
        None => Err(SubstraitError::MissingField("rel.rel_type".into())),
    }
}

fn consume_read(read: &ReadRel) -> Result<PlanNode> {
    let schema = convert_substrait_schema(&read.base_schema)?;
    let table_name = match &read.read_type {
        Some(ReadType::NamedTable(nt)) => nt.names.join("."),
        _ => "unknown".to_string(),
    };

    // Extract projection if present
    let projection = read.projection.as_ref().map(|mask| {
        mask.select
            .as_ref()
            .map(|s| extract_field_indices(s))
            .unwrap_or_default()
    });

    Ok(PlanNode::Scan {
        table_name,
        schema,
        projection,
    })
}

fn consume_filter(filter: &FilterRel) -> Result<PlanNode> {
    let input = filter
        .input
        .as_ref()
        .ok_or_else(|| SubstraitError::MissingField("filter.input".into()))?;
    let input_node = consume_rel(input)?;

    let condition = filter
        .condition
        .as_ref()
        .ok_or_else(|| SubstraitError::MissingField("filter.condition".into()))?;
    let predicate = convert_expression_to_predicate(condition)?;

    Ok(PlanNode::Filter {
        input: Box::new(input_node),
        predicate,
    })
}

fn consume_project(project: &ProjectRel) -> Result<PlanNode> {
    let input = project
        .input
        .as_ref()
        .ok_or_else(|| SubstraitError::MissingField("project.input".into()))?;
    let input_node = consume_rel(input)?;

    let mut columns: Vec<usize> = Vec::new();
    let mut expressions: Vec<ScalarExpr> = Vec::new();

    for expr in &project.expressions {
        if let Some(col_idx) = extract_column_ref(expr) {
            columns.push(col_idx);
        } else if let Some(scalar_expr) = convert_expression_to_scalar_expr(expr) {
            expressions.push(scalar_expr);
        }
        // Silently skip unrecognized expressions (backward compat)
    }

    Ok(PlanNode::Project {
        input: Box::new(input_node),
        columns,
        expressions,
    })
}

fn consume_aggregate(agg: &AggregateRel) -> Result<PlanNode> {
    let input = agg
        .input
        .as_ref()
        .ok_or_else(|| SubstraitError::MissingField("aggregate.input".into()))?;
    let input_node = consume_rel(input)?;

    // Extract group-by column indices
    let group_by: Vec<usize> = agg
        .groupings
        .iter()
        .flat_map(|g| {
            g.grouping_expressions
                .iter()
                .filter_map(|e| extract_column_ref(e))
        })
        .collect();

    // Extract aggregate expressions
    let aggregates: Vec<AggExpr> = agg
        .measures
        .iter()
        .enumerate()
        .filter_map(|(i, m)| {
            let measure = m.measure.as_ref()?;
            let func = convert_agg_function(measure.function_reference)?;
            let column = measure
                .arguments
                .first()
                .and_then(|arg| {
                    arg.arg_type.as_ref().and_then(|at| match at {
                        substrait::proto::function_argument::ArgType::Value(expr) => {
                            extract_column_ref(expr)
                        }
                        _ => None,
                    })
                })
                .unwrap_or(0);

            Some(AggExpr {
                column,
                func,
                output_name: format!("agg_{}", i),
            })
        })
        .collect();

    Ok(PlanNode::Aggregate {
        input: Box::new(input_node),
        group_by,
        aggregates,
    })
}

fn consume_join(join: &JoinRel) -> Result<PlanNode> {
    let left = join
        .left
        .as_ref()
        .ok_or_else(|| SubstraitError::MissingField("join.left".into()))?;
    let right = join
        .right
        .as_ref()
        .ok_or_else(|| SubstraitError::MissingField("join.right".into()))?;

    let left_node = consume_rel(left)?;
    let right_node = consume_rel(right)?;

    let join_type = match join.r#type() {
        substrait::proto::join_rel::JoinType::Inner => JoinType::Inner,
        substrait::proto::join_rel::JoinType::Left => JoinType::Left,
        substrait::proto::join_rel::JoinType::Right => JoinType::Right,
        substrait::proto::join_rel::JoinType::Outer => JoinType::Full,
        substrait::proto::join_rel::JoinType::LeftSemi => JoinType::LeftSemi,
        substrait::proto::join_rel::JoinType::LeftAnti => JoinType::LeftAnti,
        other => {
            return Err(SubstraitError::UnsupportedJoinType(format!("{:?}", other)));
        }
    };

    // Extract join keys from the expression (assumes equi-join: left.col = right.col)
    let (left_keys, right_keys) = extract_join_keys(join.expression.as_deref())?;

    Ok(PlanNode::Join {
        left: Box::new(left_node),
        right: Box::new(right_node),
        join_type,
        left_keys,
        right_keys,
    })
}

fn consume_sort(sort: &SortRel) -> Result<PlanNode> {
    let input = sort
        .input
        .as_ref()
        .ok_or_else(|| SubstraitError::MissingField("sort.input".into()))?;
    let input_node = consume_rel(input)?;

    let sort_keys: Vec<SortKey> = sort
        .sorts
        .iter()
        .filter_map(|sf| {
            let column = sf.expr.as_ref().and_then(|e| extract_column_ref(e))?;
            let dir_i32 = match &sf.sort_kind {
                Some(SortKind::Direction(d)) => *d,
                _ => SubstraitSortDirection::AscNullsLast as i32,
            };
            let (direction, nulls) = match SubstraitSortDirection::try_from(dir_i32)
                .unwrap_or(SubstraitSortDirection::AscNullsLast)
            {
                SubstraitSortDirection::AscNullsFirst => {
                    (SortDirection::Asc, NullOrdering::NullsFirst)
                }
                SubstraitSortDirection::AscNullsLast => {
                    (SortDirection::Asc, NullOrdering::NullsLast)
                }
                SubstraitSortDirection::DescNullsFirst => {
                    (SortDirection::Desc, NullOrdering::NullsFirst)
                }
                SubstraitSortDirection::DescNullsLast => {
                    (SortDirection::Desc, NullOrdering::NullsLast)
                }
                _ => (SortDirection::Asc, NullOrdering::NullsLast),
            };
            Some(SortKey {
                column,
                direction,
                nulls,
            })
        })
        .collect();

    Ok(PlanNode::Sort {
        input: Box::new(input_node),
        sort_keys,
        limit: None,
    })
}

fn consume_fetch(fetch: &FetchRel) -> Result<PlanNode> {
    let input = fetch
        .input
        .as_ref()
        .ok_or_else(|| SubstraitError::MissingField("fetch.input".into()))?;
    let input_node = consume_rel(input)?;

    // Extract the count (limit) from the oneof CountMode
    let limit = match &fetch.count_mode {
        Some(substrait::proto::fetch_rel::CountMode::Count(c)) if *c >= 0 => {
            Some(*c as usize)
        }
        _ => None, // unset or -1 means ALL records
    };

    // offset_mode is recognized but not yet supported in execution
    let _offset = match &fetch.offset_mode {
        Some(substrait::proto::fetch_rel::OffsetMode::Offset(o)) => *o,
        _ => 0,
    };

    // If the input is a Sort node, fold the limit into it (TopN optimization)
    match input_node {
        PlanNode::Sort { input, sort_keys, limit: _ } => {
            Ok(PlanNode::Sort {
                input,
                sort_keys,
                limit,
            })
        }
        // For non-sort inputs, wrap in a Sort with empty keys (acts as pure LIMIT)
        other => {
            Ok(PlanNode::Sort {
                input: Box::new(other),
                sort_keys: vec![],
                limit,
            })
        }
    }
}

// --- Helper functions ---

fn convert_substrait_schema(
    base_schema: &Option<substrait::proto::NamedStruct>,
) -> Result<SchemaRef> {
    let named_struct = base_schema
        .as_ref()
        .ok_or_else(|| SubstraitError::MissingField("read.base_schema".into()))?;

    // Try to extract type info from the struct definition
    let types: Vec<DataType> = named_struct
        .r#struct
        .as_ref()
        .map(|s| {
            s.types
                .iter()
                .map(|t| convert_substrait_type(t))
                .collect()
        })
        .unwrap_or_default();

    let fields: Vec<Field> = named_struct
        .names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let dt = types.get(i).cloned().unwrap_or(DataType::Utf8);
            Field::new(name, dt, true)
        })
        .collect();

    Ok(Arc::new(Schema::new(fields)))
}

fn convert_substrait_type(substrait_type: &substrait::proto::Type) -> DataType {
    use substrait::proto::r#type::Kind;
    match &substrait_type.kind {
        Some(Kind::Bool(_)) => DataType::Boolean,
        Some(Kind::I8(_)) => DataType::Int8,
        Some(Kind::I16(_)) => DataType::Int16,
        Some(Kind::I32(_)) => DataType::Int32,
        Some(Kind::I64(_)) => DataType::Int64,
        Some(Kind::Fp32(_)) => DataType::Float32,
        Some(Kind::Fp64(_)) => DataType::Float64,
        Some(Kind::String(_)) => DataType::Utf8,
        Some(Kind::Date(_)) => DataType::Date32,
        Some(Kind::Timestamp(_)) => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        Some(Kind::Decimal(d)) => DataType::Decimal128(d.precision as u8, d.scale as i8),
        _ => DataType::Utf8, // fallback
    }
}

fn extract_column_ref(expr: &Expression) -> Option<usize> {
    match &expr.rex_type {
        Some(RexType::Selection(field_ref)) => {
            match &field_ref.reference_type {
                Some(ReferenceType::DirectReference(seg)) => {
                    seg.reference_type.as_ref().and_then(|rt| match rt {
                        substrait::proto::expression::reference_segment::ReferenceType::StructField(sf) => {
                            Some(sf.field as usize)
                        }
                        _ => None,
                    })
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn convert_expression_to_predicate(expr: &Expression) -> Result<Predicate> {
    match &expr.rex_type {
        Some(RexType::ScalarFunction(func)) => convert_scalar_function_to_predicate(func),
        Some(RexType::Literal(lit)) => {
            // A literal true/false used as a predicate
            match &lit.literal_type {
                Some(LiteralType::Boolean(b)) => {
                    if *b {
                        // TRUE predicate — always passes, represent as IsNotNull(0) hack
                        // In practice Trino would not emit this
                        Ok(Predicate::IsNotNull(0))
                    } else {
                        Ok(Predicate::Not(Box::new(Predicate::IsNotNull(0))))
                    }
                }
                _ => Err(SubstraitError::UnsupportedExpression(
                    "non-boolean literal as predicate".into(),
                )),
            }
        }
        _ => Err(SubstraitError::UnsupportedExpression(format!(
            "unsupported predicate expression: {:?}",
            expr.rex_type
        ))),
    }
}

fn convert_scalar_function_to_predicate(func: &ScalarFunction) -> Result<Predicate> {
    // Function references map to comparison operators
    // Substrait uses function_reference IDs; in practice these come from
    // the extension URI definitions. We handle the common ones by convention.
    let func_ref = func.function_reference;

    let args: Vec<&Expression> = func
        .arguments
        .iter()
        .filter_map(|a| match &a.arg_type {
            Some(substrait::proto::function_argument::ArgType::Value(e)) => Some(e),
            _ => None,
        })
        .collect();

    if args.len() == 2 {
        let col = extract_column_ref(args[0]);
        let scalar = extract_scalar_value(args[1]);

        if let (Some(col_idx), Some(val)) = (col, scalar) {
            // Map function references to comparison operators
            // These IDs follow Substrait extension conventions
            return match func_ref {
                1 => Ok(Predicate::Eq(col_idx, val)),    // equal
                2 => Ok(Predicate::Ne(col_idx, val)),    // not_equal
                3 => Ok(Predicate::Lt(col_idx, val)),    // less_than
                4 => Ok(Predicate::Le(col_idx, val)),    // less_than_or_equal
                5 => Ok(Predicate::Gt(col_idx, val)),    // greater_than
                6 => Ok(Predicate::Ge(col_idx, val)),    // greater_than_or_equal
                _ => Err(SubstraitError::UnsupportedExpression(format!(
                    "unknown comparison function ref: {}",
                    func_ref
                ))),
            };
        }

        // AND / OR with two predicate children
        if func_ref == 7 {
            // and
            let left = convert_expression_to_predicate(args[0])?;
            let right = convert_expression_to_predicate(args[1])?;
            return Ok(Predicate::And(Box::new(left), Box::new(right)));
        }
        if func_ref == 8 {
            // or
            let left = convert_expression_to_predicate(args[0])?;
            let right = convert_expression_to_predicate(args[1])?;
            return Ok(Predicate::Or(Box::new(left), Box::new(right)));
        }
    }

    if args.len() == 1 && func_ref == 9 {
        // not
        let inner = convert_expression_to_predicate(args[0])?;
        return Ok(Predicate::Not(Box::new(inner)));
    }

    // LIKE / ILIKE predicates
    if args.len() == 2 && (func_ref == 20 || func_ref == 21) {
        let col = extract_column_ref(args[0]);
        let pattern = extract_scalar_value(args[1]);
        if let (Some(col_idx), Some(ScalarValue::Utf8(pat))) = (col, pattern) {
            return if func_ref == 20 {
                Ok(Predicate::Like(col_idx, pat))
            } else {
                Ok(Predicate::ILike(col_idx, pat))
            };
        }
    }

    Err(SubstraitError::UnsupportedExpression(format!(
        "scalar function ref={} with {} args",
        func_ref,
        args.len()
    )))
}

/// Convert a Substrait Expression into a ScalarExpr for arithmetic evaluation.
fn convert_expression_to_scalar_expr(expr: &Expression) -> Option<ScalarExpr> {
    match &expr.rex_type {
        Some(RexType::Selection(_)) => {
            extract_column_ref(expr).map(ScalarExpr::ColumnRef)
        }
        Some(RexType::Literal(_)) => {
            extract_scalar_value(expr).map(ScalarExpr::Literal)
        }
        Some(RexType::ScalarFunction(func)) => {
            let func_ref = func.function_reference;
            let args: Vec<&Expression> = func.arguments.iter()
                .filter_map(|a| match &a.arg_type {
                    Some(substrait::proto::function_argument::ArgType::Value(e)) => Some(e),
                    _ => None,
                })
                .collect();

            match func_ref {
                10 => binary_scalar_expr(ArithmeticOp::Add, &args),
                11 => binary_scalar_expr(ArithmeticOp::Subtract, &args),
                12 => binary_scalar_expr(ArithmeticOp::Multiply, &args),
                13 => binary_scalar_expr(ArithmeticOp::Divide, &args),
                14 if args.len() == 1 => {
                    convert_expression_to_scalar_expr(args[0])
                        .map(|inner| ScalarExpr::Negate(Box::new(inner)))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn binary_scalar_expr(op: ArithmeticOp, args: &[&Expression]) -> Option<ScalarExpr> {
    if args.len() != 2 { return None; }
    let left = convert_expression_to_scalar_expr(args[0])?;
    let right = convert_expression_to_scalar_expr(args[1])?;
    Some(ScalarExpr::BinaryOp {
        op,
        left: Box::new(left),
        right: Box::new(right),
    })
}

fn extract_scalar_value(expr: &Expression) -> Option<ScalarValue> {
    match &expr.rex_type {
        Some(RexType::Literal(lit)) => match &lit.literal_type {
            Some(LiteralType::I32(v)) => Some(ScalarValue::Int32(*v)),
            Some(LiteralType::I64(v)) => Some(ScalarValue::Int64(*v)),
            Some(LiteralType::Fp64(v)) => Some(ScalarValue::Float64(*v)),
            Some(LiteralType::String(v)) => Some(ScalarValue::Utf8(v.clone())),
            Some(LiteralType::Boolean(v)) => Some(ScalarValue::Boolean(*v)),
            Some(LiteralType::Date(v)) => Some(ScalarValue::Date32(*v)),
            _ => None,
        },
        _ => None,
    }
}

fn convert_agg_function(function_reference: u32) -> Option<AggFunc> {
    // Standard Substrait aggregate function references
    match function_reference {
        0 => Some(AggFunc::Count),
        1 => Some(AggFunc::Sum),
        2 => Some(AggFunc::Min),
        3 => Some(AggFunc::Max),
        4 => Some(AggFunc::Avg),
        5 => Some(AggFunc::CountDistinct),
        _ => None,
    }
}

fn extract_join_keys(
    expr: Option<&Expression>,
) -> Result<(Vec<usize>, Vec<usize>)> {
    // For simplicity, extract equi-join keys from the join expression
    // In practice this would handle AND chains of col_a = col_b
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();

    if let Some(expr) = expr {
        extract_equi_keys(expr, &mut left_keys, &mut right_keys)?;
    }

    Ok((left_keys, right_keys))
}

fn extract_equi_keys(
    expr: &Expression,
    left_keys: &mut Vec<usize>,
    right_keys: &mut Vec<usize>,
) -> Result<()> {
    match &expr.rex_type {
        Some(RexType::ScalarFunction(func)) => {
            let args: Vec<&Expression> = func
                .arguments
                .iter()
                .filter_map(|a| match &a.arg_type {
                    Some(substrait::proto::function_argument::ArgType::Value(e)) => Some(e),
                    _ => None,
                })
                .collect();

            if func.function_reference == 1 && args.len() == 2 {
                // equality
                if let (Some(l), Some(r)) = (extract_column_ref(args[0]), extract_column_ref(args[1])) {
                    left_keys.push(l);
                    right_keys.push(r);
                }
            } else if func.function_reference == 7 && args.len() == 2 {
                // AND — recurse
                extract_equi_keys(args[0], left_keys, right_keys)?;
                extract_equi_keys(args[1], left_keys, right_keys)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn extract_field_indices(
    segment: &substrait::proto::expression::mask_expression::StructSelect,
) -> Vec<usize> {
    segment
        .struct_items
        .iter()
        .map(|item| item.field as usize)
        .collect()
}

fn derive_schema(node: &PlanNode) -> Result<SchemaRef> {
    match node {
        PlanNode::Scan { schema, .. } => Ok(schema.clone()),
        PlanNode::Filter { input, .. } => derive_schema(input),
        PlanNode::Project { input, columns, expressions } => {
            let input_schema = derive_schema(input)?;
            let mut fields: Vec<Field> = columns
                .iter()
                .map(|&i| input_schema.field(i).clone())
                .collect();
            for (i, _expr) in expressions.iter().enumerate() {
                fields.push(Field::new(format!("expr_{}", i), DataType::Float64, true));
            }
            Ok(Arc::new(Schema::new(fields)))
        }
        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let input_schema = derive_schema(input)?;
            let mut fields: Vec<Field> = group_by
                .iter()
                .map(|&i| input_schema.field(i).clone())
                .collect();
            for agg in aggregates {
                let dt = match agg.func {
                    AggFunc::Count | AggFunc::CountDistinct => DataType::Int64,
                    _ => DataType::Float64,
                };
                fields.push(Field::new(&agg.output_name, dt, true));
            }
            Ok(Arc::new(Schema::new(fields)))
        }
        PlanNode::Join { left, right, .. } => {
            let left_schema = derive_schema(left)?;
            let right_schema = derive_schema(right)?;
            let mut fields: Vec<Field> = left_schema.fields().iter().map(|f| f.as_ref().clone()).collect();
            for f in right_schema.fields() {
                let mut field = f.as_ref().clone();
                if left_schema.field_with_name(field.name()).is_ok() {
                    field = Field::new(
                        format!("{}_right", field.name()),
                        field.data_type().clone(),
                        field.is_nullable(),
                    );
                }
                fields.push(field);
            }
            Ok(Arc::new(Schema::new(fields)))
        }
        PlanNode::Sort { input, .. } => derive_schema(input),
        PlanNode::Exchange { input, .. } => derive_schema(input),
    }
}
