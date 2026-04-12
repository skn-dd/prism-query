package io.prism.bridge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates Trino's internal plan representation (PlanNode tree) into
 * Substrait protobuf format for execution by the native Prism engine.
 *
 * <p>This is the key integration point between Trino's optimizer output
 * and the native execution path. The translator walks the Trino PlanNode
 * tree and produces an equivalent Substrait Plan.</p>
 *
 * <h3>Supported Trino operators → Substrait relations:</h3>
 * <ul>
 *   <li>TableScanNode → ReadRel (NamedTable)</li>
 *   <li>FilterNode → FilterRel</li>
 *   <li>ProjectNode → ProjectRel</li>
 *   <li>AggregationNode → AggregateRel</li>
 *   <li>JoinNode → JoinRel</li>
 *   <li>SortNode / TopNNode → SortRel</li>
 *   <li>ExchangeNode → ExchangeRel (Arrow Flight)</li>
 * </ul>
 *
 * <p>Operators not yet supported by the native engine fall back to
 * Trino's standard Java execution path.</p>
 */
public class SubstraitSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(SubstraitSerializer.class);

    // Function reference IDs matching what prism-substrait/consumer.rs expects
    public static final int FUNC_EQUAL = 1;
    public static final int FUNC_NOT_EQUAL = 2;
    public static final int FUNC_LESS_THAN = 3;
    public static final int FUNC_LESS_EQUAL = 4;
    public static final int FUNC_GREATER_THAN = 5;
    public static final int FUNC_GREATER_EQUAL = 6;
    public static final int FUNC_AND = 7;
    public static final int FUNC_OR = 8;
    public static final int FUNC_NOT = 9;

    // Aggregate function reference IDs
    public static final int AGG_COUNT = 0;
    public static final int AGG_SUM = 1;
    public static final int AGG_MIN = 2;
    public static final int AGG_MAX = 3;
    public static final int AGG_AVG = 4;
    public static final int AGG_COUNT_DISTINCT = 5;

    /**
     * Check if a Trino plan node subtree can be executed natively.
     *
     * <p>Returns true if all operators in the subtree have native implementations.
     * Used by the Prism execution strategy to decide whether to route through
     * the native path or fall back to standard Trino execution.</p>
     *
     * @param operatorType The Trino operator type name
     * @return true if this operator can be executed natively
     */
    public static boolean isNativeSupported(String operatorType) {
        return switch (operatorType) {
            case "TableScanNode",
                 "FilterNode",
                 "ProjectNode",
                 "AggregationNode",
                 "JoinNode",
                 "SortNode",
                 "TopNNode",
                 "ExchangeNode" -> true;
            default -> false;
        };
    }

    /**
     * Placeholder: In the full implementation, this method walks a Trino PlanNode
     * tree and produces Substrait protobuf bytes. The actual implementation will use:
     *
     * - io.substrait.proto.Plan.Builder to construct the Substrait plan
     * - Trino's PlanNode visitor pattern to traverse the tree
     * - Arrow schema mapping from Trino Type to Substrait Type
     *
     * For Phase 2 development.
     */
    // public byte[] serialize(PlanNode trinoRoot) { ... }
}
