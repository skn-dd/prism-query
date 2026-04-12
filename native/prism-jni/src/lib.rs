//! Prism JNI — Java Native Interface bridge.
//!
//! Exposes the Prism native executor to Trino's Java runtime via JNI.
//! The Java side (`PrismNativeExecutor.java`) loads `libprism_jni.so` and
//! calls these functions to execute Substrait plans on Arrow RecordBatches.

use std::sync::Arc;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;
use tokio::runtime::Runtime;

use prism_substrait::consumer::consume_plan;

/// Initialize the native runtime. Called once when the Trino worker starts.
/// Returns a handle (pointer) to the Tokio runtime.
///
/// Java signature: `static native long init()`
#[no_mangle]
pub extern "system" fn Java_io_prism_bridge_PrismNativeExecutor_init(
    mut env: JNIEnv,
    _class: JClass,
) -> jlong {
    // Initialize tracing
    let _ = tracing_subscriber::fmt::try_init();

    // Create a multi-threaded Tokio runtime
    let runtime = Runtime::new().expect("Failed to create Tokio runtime");
    let boxed = Box::new(runtime);
    Box::into_raw(boxed) as jlong
}

/// Shut down the native runtime and free resources.
///
/// Java signature: `static native void shutdown(long runtimeHandle)`
#[no_mangle]
pub extern "system" fn Java_io_prism_bridge_PrismNativeExecutor_shutdown(
    _env: JNIEnv,
    _class: JClass,
    runtime_handle: jlong,
) {
    if runtime_handle != 0 {
        unsafe {
            let _ = Box::from_raw(runtime_handle as *mut Runtime);
        }
    }
}

/// Execute a Substrait plan on Arrow IPC data.
///
/// Takes:
/// - `runtimeHandle`: pointer to the Tokio runtime
/// - `substraitPlan`: Substrait plan as protobuf bytes
/// - `inputData`: input RecordBatches as Arrow IPC stream bytes
///
/// Returns: output RecordBatches as Arrow IPC stream bytes.
///
/// Java signature: `static native byte[] executePlan(long runtimeHandle, byte[] substraitPlan, byte[] inputData)`
#[no_mangle]
pub extern "system" fn Java_io_prism_bridge_PrismNativeExecutor_executePlan<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    runtime_handle: jlong,
    substrait_plan: JByteArray<'local>,
    input_data: JByteArray<'local>,
) -> jbyteArray {
    let result = execute_plan_inner(&mut env, runtime_handle, substrait_plan, input_data);

    match result {
        Ok(bytes) => {
            let output = env.byte_array_from_slice(&bytes).expect("Failed to create byte array");
            output.into_raw()
        }
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", format!("Prism execution error: {}", e));
            std::ptr::null_mut()
        }
    }
}

fn execute_plan_inner(
    env: &mut JNIEnv,
    runtime_handle: jlong,
    substrait_plan: JByteArray,
    input_data: JByteArray,
) -> anyhow::Result<Vec<u8>> {
    // Read Substrait plan bytes from JNI
    let plan_bytes = env.convert_byte_array(substrait_plan)?;

    // Parse the Substrait plan
    let plan = consume_plan(&plan_bytes)?;

    // Read input Arrow IPC data
    let input_bytes = env.convert_byte_array(input_data)?;
    let cursor = std::io::Cursor::new(input_bytes);
    let reader = StreamReader::try_new(cursor, None)?;

    let input_batches: Vec<RecordBatch> = reader
        .collect::<std::result::Result<Vec<_>, _>>()?;

    // Execute the plan
    // For now, pass through the input data (actual execution will be wired in Phase 2)
    let output_batches = execute_plan_node(&plan.root, &input_batches)?;

    // Serialize output to Arrow IPC
    let mut buf = Vec::new();
    if let Some(first) = output_batches.first() {
        let schema = first.schema();
        let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())?;
        for batch in &output_batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }

    Ok(buf)
}

/// Execute a plan node against input batches.
fn execute_plan_node(
    node: &prism_substrait::PlanNode,
    input_batches: &[RecordBatch],
) -> anyhow::Result<Vec<RecordBatch>> {
    use prism_executor::filter_project;
    use prism_executor::hash_aggregate;
    use prism_executor::hash_join;
    use prism_executor::sort;
    use prism_substrait::PlanNode;

    match node {
        PlanNode::Scan { .. } => {
            // Scan nodes receive data from the Java connector via IPC
            Ok(input_batches.to_vec())
        }

        PlanNode::Filter { input, predicate } => {
            let child_batches = execute_plan_node(input, input_batches)?;
            let mut output = Vec::new();
            for batch in &child_batches {
                let mask = filter_project::evaluate_predicate(batch, predicate)?;
                output.push(filter_project::filter_batch(batch, &mask)?);
            }
            Ok(output)
        }

        PlanNode::Project { input, columns } => {
            let child_batches = execute_plan_node(input, input_batches)?;
            let mut output = Vec::new();
            for batch in &child_batches {
                output.push(filter_project::project_batch(batch, columns)?);
            }
            Ok(output)
        }

        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let child_batches = execute_plan_node(input, input_batches)?;
            // Concatenate all input batches for aggregation
            if child_batches.is_empty() {
                return Ok(vec![]);
            }
            let merged = arrow::compute::concat_batches(
                &child_batches[0].schema(),
                &child_batches,
            )?;

            let config = hash_aggregate::HashAggConfig {
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
            };
            let result = hash_aggregate::hash_aggregate(&merged, &config)?;
            Ok(vec![result])
        }

        PlanNode::Join {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
        } => {
            let left_batches = execute_plan_node(left, input_batches)?;
            let right_batches = execute_plan_node(right, input_batches)?;

            // Merge each side
            let left_merged = if left_batches.is_empty() {
                return Ok(vec![]);
            } else {
                arrow::compute::concat_batches(&left_batches[0].schema(), &left_batches)?
            };

            let right_merged = if right_batches.is_empty() {
                return Ok(vec![]);
            } else {
                arrow::compute::concat_batches(&right_batches[0].schema(), &right_batches)?
            };

            let config = hash_join::HashJoinConfig {
                join_type: *join_type,
                probe_keys: left_keys.clone(),
                build_keys: right_keys.clone(),
            };
            let result = hash_join::hash_join(&left_merged, &right_merged, &config)?;
            Ok(vec![result])
        }

        PlanNode::Sort {
            input,
            sort_keys,
            limit,
        } => {
            let child_batches = execute_plan_node(input, input_batches)?;
            if child_batches.is_empty() {
                return Ok(vec![]);
            }
            let merged = arrow::compute::concat_batches(
                &child_batches[0].schema(),
                &child_batches,
            )?;

            let result = if let Some(lim) = limit {
                sort::sort_batch_limit(&merged, sort_keys, *lim)?
            } else {
                sort::sort_batch(&merged, sort_keys)?
            };
            Ok(vec![result])
        }

        PlanNode::Exchange { input, .. } => {
            // Exchange is handled at the Flight layer, not in-process execution
            execute_plan_node(input, input_batches)
        }
    }
}

/// Get the version string of the native executor.
///
/// Java signature: `static native String version()`
#[no_mangle]
pub extern "system" fn Java_io_prism_bridge_PrismNativeExecutor_version<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    let version = format!("prism-native {}", env!("CARGO_PKG_VERSION"));
    let output = env.new_string(version).expect("Failed to create string");
    output.into_raw()
}
