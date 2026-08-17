package com.analyticsplatform.transform.dq;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import com.analyticsplatform.transform.dq.DqRule.NullPolicy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Evaluates declarative data-quality rules against a dataset.
 *
 * <h2>One pass, not one per rule</h2>
 *
 * <p>Every column-wise rule compiles into a pair of conditional-count expressions and they are all
 * aggregated together. Nineteen rules therefore cost one scan, not nineteen. On the real dataset
 * that is the difference between DQ being something you run every time and something you switch
 * off when it gets slow — and a check that gets switched off is not a check.
 *
 * <p>Rules needing a groupBy ({@code unique}), a join ({@code referential}) or history
 * ({@code row_count_delta}) cannot fold into that pass and are evaluated separately.
 *
 * <h2>Null handling is explicit</h2>
 *
 * <p>Spark SQL is three-valued: {@code NULL > 0} is neither true nor false, so a naive range check
 * silently passes every null. A column that became entirely null would satisfy all of its range
 * rules — the exact opposite of the intent. Each rule therefore declares a {@link NullPolicy} and
 * the generated expressions honour it.
 */
public final class DqEngine {

    /** The outcome for one rule. */
    public record RuleResult(
            DqRule rule,
            long rowsEvaluated,
            long rowsViolated,
            double violationRate,
            boolean passed) {

        public boolean blocking() {
            return !passed && rule.severity() == DqRule.Severity.FAIL;
        }

        public String describe() {
            return rule.ruleName() + ": " + rowsViolated + "/" + rowsEvaluated
                    + String.format(" (%.4f%%)", violationRate * 100)
                    + " " + rule.severity() + (passed ? " PASS" : " BREACH");
        }
    }

    /** All rule outcomes for one evaluation. */
    public record Report(List<RuleResult> results) {

        public Report {
            results = List.copyOf(results);
        }

        /** True when any FAIL-severity rule breached its threshold. Publication must not proceed. */
        public boolean blocked() {
            return results.stream().anyMatch(RuleResult::blocking);
        }

        public List<RuleResult> breaches() {
            return results.stream().filter(r -> !r.passed()).toList();
        }

        public String summary() {
            long breached = breaches().size();
            return results.size() + " rules evaluated, " + breached + " breached"
                    + (blocked() ? " (BLOCKING)" : "");
        }
    }

    /** External inputs some rule types need. */
    public record Context(
            Map<String, Dataset<Row>> referenceDatasets,
            Optional<Long> previousRowCount) {

        public static Context empty() {
            return new Context(Map.of(), Optional.empty());
        }
    }

    /**
     * Evaluates every enabled rule.
     *
     * <p>Deliberately returns a report rather than throwing on breach. Deciding what a breach means
     * belongs to the caller — silver blocks publication, but a reporting job might only want to
     * record it.
     */
    public Report evaluate(Dataset<Row> data, List<DqRule> rules, Context context) {
        List<RuleResult> results = new ArrayList<>(rules.size());

        List<DqRule> columnWise = rules.stream().filter(DqRule::isColumnWise).toList();
        if (!columnWise.isEmpty()) {
            results.addAll(evaluateColumnWise(data, columnWise));
        }

        for (DqRule rule : rules) {
            if (rule.isColumnWise()) {
                continue;
            }
            results.add(switch (rule.ruleType()) {
                case UNIQUE -> evaluateUnique(data, rule);
                case REFERENTIAL -> evaluateReferential(data, rule, context);
                case ROW_COUNT_DELTA -> evaluateRowCountDelta(data, rule, context);
                default -> throw new IllegalStateException(
                        "unhandled non-column-wise rule type: " + rule.ruleType());
            });
        }

        return new Report(results);
    }

    // ------------------------------------------------------------ single pass

    private List<RuleResult> evaluateColumnWise(Dataset<Row> data, List<DqRule> rules) {
        List<Column> aggregates = new ArrayList<>(rules.size() * 2);
        for (DqRule rule : rules) {
            aggregates.add(countWhere(violates(rule)).alias("v_" + rule.ruleId()));
            aggregates.add(countWhere(evaluated(rule)).alias("e_" + rule.ruleId()));
        }

        Column head = aggregates.get(0);
        Column[] tail = aggregates.subList(1, aggregates.size()).toArray(new Column[0]);
        Row row = data.agg(head, tail).first();

        List<RuleResult> results = new ArrayList<>(rules.size());
        for (DqRule rule : rules) {
            long violated = longAt(row, "v_" + rule.ruleId());
            long total = longAt(row, "e_" + rule.ruleId());
            results.add(result(rule, total, violated));
        }
        return results;
    }

    /** {@code sum(case when cond then 1 else 0 end)} — null-safe, unlike {@code count(when(...))}. */
    private static Column countWhere(Column condition) {
        return sum(when(condition, lit(1L)).otherwise(lit(0L)));
    }

    /**
     * True for rows this rule counts as violating, with the null policy already folded in.
     */
    private static Column violates(DqRule rule) {
        Column raw = rawViolation(rule);
        Column isNull = nullness(rule);

        // NOT_NULL is a rule *about* nullness, so the null policy does not apply to it: a null is
        // the violation by definition. Honouring null_policy here would let a rule named
        // "not null" be configured to permit nulls, which is nonsense.
        if (rule.ruleType() == DqRule.RuleType.NOT_NULL) {
            return isNull;
        }

        return switch (rule.nullPolicy()) {
            // NULL is itself a violation, plus any genuine violation among non-null rows.
            case VIOLATION -> isNull.or(raw.and(isNull.unary_$bang()));
            // NULL is acceptable; only non-null rows can violate.
            case PASS, IGNORE -> raw.and(isNull.unary_$bang());
        };
    }

    /** True for rows that count toward this rule's denominator. */
    private static Column evaluated(DqRule rule) {
        // IGNORE removes nulls from the denominator entirely, so a mostly-null column is judged on
        // the values it does have rather than being diluted toward passing.
        return rule.nullPolicy() == NullPolicy.IGNORE
                ? nullness(rule).unary_$bang()
                : lit(true);
    }

    /**
     * What "null" means for this rule.
     *
     * <p>For a column rule it is the column being null. For an expression rule it is the expression
     * evaluating to NULL — three-valued logic surfacing as a third outcome that is neither pass
     * nor fail, and which must be classified rather than silently dropped.
     */
    private static Column nullness(DqRule rule) {
        if (rule.ruleType() == DqRule.RuleType.EXPRESSION) {
            return expr(requiredParam(rule, "expression")).isNull();
        }
        return col(requireColumn(rule)).isNull();
    }

    /** True when a non-null row breaks the rule, before null policy is considered. */
    private static Column rawViolation(DqRule rule) {
        return switch (rule.ruleType()) {
            case NOT_NULL -> col(requireColumn(rule)).isNull();

            case RANGE -> {
                Double min = rule.paramDouble("min");
                Double max = rule.paramDouble("max");
                Column target = col(requireColumn(rule));
                Column condition = lit(false);
                if (min != null) {
                    condition = condition.or(target.lt(lit(min)));
                }
                if (max != null) {
                    condition = condition.or(target.gt(lit(max)));
                }
                yield condition;
            }

            case ACCEPTED_VALUES -> {
                Object raw = rule.params().get("values");
                if (!(raw instanceof List<?> values) || values.isEmpty()) {
                    throw new IllegalArgumentException(
                            "rule " + rule.ruleName() + " needs a non-empty 'values' list");
                }
                yield col(requireColumn(rule)).isin(values.toArray()).unary_$bang();
            }

            // The expression states what must be TRUE, so violation is its negation.
            case EXPRESSION -> expr(requiredParam(rule, "expression")).unary_$bang();

            case FRESHNESS -> {
                Double maxAgeDays = rule.paramDouble("max_age_days");
                if (maxAgeDays == null) {
                    throw new IllegalArgumentException(
                            "rule " + rule.ruleName() + " needs 'max_age_days'");
                }
                yield col(requireColumn(rule))
                        .lt(expr("current_timestamp() - interval " + maxAgeDays.longValue() + " days"));
            }

            default -> throw new IllegalStateException(
                    rule.ruleType() + " is not a column-wise rule");
        };
    }

    // ------------------------------------------------------- non-column-wise

    /**
     * Counts excess rows rather than duplicated keys: three rows sharing a key are two violations,
     * not one. That makes the violation count comparable with row counts from other rules.
     */
    private RuleResult evaluateUnique(Dataset<Row> data, DqRule rule) {
        String column = requireColumn(rule);
        Row counts = data.agg(
                sum(when(col(column).isNotNull(), lit(1L)).otherwise(lit(0L))).alias("total"),
                countDistinct(col(column)).alias("distinct")).first();

        long total = longAt(counts, "total");
        long distinct = longAt(counts, "distinct");
        return result(rule, total, Math.max(0, total - distinct));
    }

    /** Rows whose key is absent from the reference dataset. */
    private RuleResult evaluateReferential(Dataset<Row> data, DqRule rule, Context context) {
        String refName = requiredParam(rule, "ref_dataset");
        String refColumn = requiredParam(rule, "ref_column");
        Dataset<Row> reference = context.referenceDatasets().get(refName);
        if (reference == null) {
            throw new IllegalArgumentException(
                    "rule " + rule.ruleName() + " needs reference dataset '" + refName + "'");
        }

        String column = requireColumn(rule);
        Dataset<Row> present = data.filter(col(column).isNotNull());
        long total = present.count();
        long orphaned = present.join(
                reference.select(col(refColumn).alias("__ref_key")).distinct(),
                col(column).equalTo(col("__ref_key")), "left_anti").count();

        return result(rule, total, orphaned);
    }

    /**
     * Guards against a silently truncated upstream: a large drop in row count relative to the
     * previous run. Growth is never a violation.
     */
    private RuleResult evaluateRowCountDelta(Dataset<Row> data, DqRule rule, Context context) {
        long current = data.count();
        Optional<Long> previous = context.previousRowCount();
        if (previous.isEmpty() || previous.get() == 0) {
            // Nothing to compare against on a first run. Reporting a violation here would make
            // every new dataset fail its own first load.
            return result(rule, current, 0);
        }

        double maxDrop = Optional.ofNullable(rule.paramDouble("max_drop_pct")).orElse(0.25);
        long before = previous.get();
        double drop = (double) (before - current) / before;

        // Expressed as whole rows so the number is comparable with other rules' counts.
        long violations = drop > maxDrop ? before - current : 0;
        return result(rule, before, violations);
    }

    // ------------------------------------------------------------------ util

    private static RuleResult result(DqRule rule, long evaluated, long violated) {
        double rate = evaluated == 0 ? 0.0 : (double) violated / evaluated;
        return new RuleResult(rule, evaluated, violated, rate, !rule.breaches(evaluated, violated));
    }

    private static String requireColumn(DqRule rule) {
        if (rule.targetColumn() == null || rule.targetColumn().isBlank()) {
            throw new IllegalArgumentException(
                    "rule " + rule.ruleName() + " (" + rule.ruleType() + ") needs a target column");
        }
        return rule.targetColumn();
    }

    private static String requiredParam(DqRule rule, String key) {
        String value = rule.paramString(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "rule " + rule.ruleName() + " needs parameter '" + key + "'");
        }
        return value;
    }

    private static long longAt(Row row, String name) {
        int index = row.fieldIndex(name);
        return row.isNullAt(index) ? 0L : ((Number) row.get(index)).longValue();
    }

    /** Column names referenced by a set of rules, for validating a dataset up front. */
    public static List<String> requiredColumns(List<DqRule> rules) {
        return rules.stream()
                .map(DqRule::targetColumn)
                .filter(java.util.Objects::nonNull)
                .distinct()
                .toList();
    }

    /** Rules whose target column is absent from the dataset — a misconfiguration, not a breach. */
    public static List<DqRule> inapplicable(Dataset<Row> data, List<DqRule> rules) {
        List<String> columns = Arrays.asList(data.columns());
        return rules.stream()
                .filter(r -> r.targetColumn() != null && !columns.contains(r.targetColumn()))
                .toList();
    }
}
