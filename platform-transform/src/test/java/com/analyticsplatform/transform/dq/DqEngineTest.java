package com.analyticsplatform.transform.dq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.analyticsplatform.common.testing.SparkTestSupport;
import com.analyticsplatform.transform.dq.DqEngine.Report;
import com.analyticsplatform.transform.dq.DqEngine.RuleResult;
import com.analyticsplatform.transform.dq.DqRule.NullPolicy;
import com.analyticsplatform.transform.dq.DqRule.RuleType;
import com.analyticsplatform.transform.dq.DqRule.Severity;
import com.analyticsplatform.transform.dq.DqRule.ThresholdType;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The DQ engine against real Spark.
 *
 * <p>The interesting cases are combinations, not individual rules: a rule type behaves differently
 * under each null policy, and both interact with dataset cardinality. §31 asks for the
 * cross-product rather than each dimension in isolation, because that is where the surprises live.
 */
class DqEngineTest {

    private final DqEngine engine = new DqEngine();

    private static final StructType SCHEMA = new StructType(new StructField[] {
        new StructField("v", DataTypes.IntegerType, true, Metadata.empty()),
        new StructField("k", DataTypes.StringType, true, Metadata.empty()),
    });

    /** A one-column dataset; nulls are written as Java nulls. */
    private static Dataset<Row> rows(Integer... values) {
        List<Row> data = new ArrayList<>();
        int index = 0;
        for (Integer value : values) {
            data.add(RowFactory.create(value, "k" + index++));
        }
        return SparkTestSupport.spark().createDataFrame(data, SCHEMA);
    }

    private static DqRule rule(RuleType type, NullPolicy nulls, Map<String, Object> params) {
        return new DqRule(1, "r", "ds", type, "v", params, Severity.FAIL,
                ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.valueOf(1_000_000), nulls);
    }

    private RuleResult evaluateOne(Dataset<Row> data, DqRule rule) {
        Report report = engine.evaluate(data, List.of(rule), DqEngine.Context.empty());
        assertThat(report.results()).hasSize(1);
        return report.results().get(0);
    }

    private static Map<String, Object> range(int min, int max) {
        return Map.of("min", min, "max", max);
    }

    @Nested
    @DisplayName("null policy changes the answer")
    class NullPolicies {

        /**
         * The heart of it. Data is 5, null, 50 with a valid range of 0..10, so exactly one non-null
         * value is out of range and one value is null. Each policy must classify the null
         * differently — and all three are defensible, which is why it has to be declared.
         */
        @ParameterizedTest(name = "{0}: {1} violations out of {2} evaluated")
        @CsvSource({
            "VIOLATION, 2, 3",   // the null counts as a violation
            "PASS,      1, 3",   // the null is acceptable, still in the denominator
            "IGNORE,    1, 2",   // the null leaves the denominator entirely
        })
        void nullPolicyDecidesTheOutcome(NullPolicy policy, long violations, long evaluated) {
            RuleResult result = evaluateOne(
                    rows(5, null, 50), rule(RuleType.RANGE, policy, range(0, 10)));

            assertThat(result.rowsViolated()).isEqualTo(violations);
            assertThat(result.rowsEvaluated()).isEqualTo(evaluated);
        }

        /**
         * A column that becomes entirely null is the failure this exists to catch. Under naive
         * three-valued logic every range check would pass, reporting perfect quality on a column
         * holding no data at all.
         */
        @ParameterizedTest(name = "all-null under {0} → {1} violations of {2}")
        @CsvSource({
            "VIOLATION, 3, 3",   // every row is a violation - the alarm fires
            "PASS,      0, 3",   // explicitly tolerated
            "IGNORE,    0, 0",   // nothing to judge; row_count/freshness rules cover this
        })
        void anEntirelyNullColumn(NullPolicy policy, long violations, long evaluated) {
            RuleResult result = evaluateOne(
                    rows(null, null, null), rule(RuleType.RANGE, policy, range(0, 10)));

            assertThat(result.rowsViolated()).isEqualTo(violations);
            assertThat(result.rowsEvaluated()).isEqualTo(evaluated);
        }

        /** NOT_NULL is a rule about nullness, so the policy must not be able to disarm it. */
        @ParameterizedTest
        @CsvSource({"VIOLATION", "PASS", "IGNORE"})
        @DisplayName("not_null ignores the null policy")
        void notNullIsNotConfigurable(NullPolicy policy) {
            RuleResult result = evaluateOne(
                    rows(1, null, 3), rule(RuleType.NOT_NULL, policy, Map.of()));

            assertThat(result.rowsViolated())
                    .as("a null is the violation by definition, whatever the policy says")
                    .isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("rule types")
    class RuleTypes {

        @Test
        void notNullCountsNulls() {
            assertThat(evaluateOne(rows(1, null, null, 4),
                    rule(RuleType.NOT_NULL, NullPolicy.VIOLATION, Map.of())).rowsViolated())
                    .isEqualTo(2);
        }

        @Test
        void rangeChecksBothBounds() {
            assertThat(evaluateOne(rows(-1, 0, 5, 10, 11),
                    rule(RuleType.RANGE, NullPolicy.PASS, range(0, 10))).rowsViolated())
                    .as("-1 below and 11 above; the bounds themselves are inclusive")
                    .isEqualTo(2);
        }

        @Test
        @DisplayName("a range with only a minimum leaves the top open")
        void rangeWithOnlyMinimum() {
            assertThat(evaluateOne(rows(-5, 0, 1_000_000),
                    rule(RuleType.RANGE, NullPolicy.PASS, Map.of("min", 0))).rowsViolated())
                    .isEqualTo(1);
        }

        @Test
        void acceptedValuesRejectsAnythingElse() {
            assertThat(evaluateOne(rows(1, 2, 3, 99),
                    rule(RuleType.ACCEPTED_VALUES, NullPolicy.PASS,
                            Map.of("values", List.of(1, 2, 3)))).rowsViolated())
                    .isEqualTo(1);
        }

        @Test
        void expressionViolationIsTheNegation() {
            DqRule r = new DqRule(1, "r", "ds", RuleType.EXPRESSION, null,
                    Map.of("expression", "v >= 0"), Severity.FAIL,
                    ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.valueOf(999),
                    NullPolicy.PASS);

            assertThat(evaluateOne(rows(-1, 0, 5, -3), r).rowsViolated()).isEqualTo(2);
        }

        /**
         * An expression can evaluate to NULL, which is neither pass nor fail. That third outcome
         * has to be classified rather than silently dropped.
         */
        @ParameterizedTest(name = "expression returning NULL under {0} → {1} violations")
        @CsvSource({"VIOLATION, 2", "PASS, 1", "IGNORE, 1"})
        void expressionNullResultFollowsPolicy(NullPolicy policy, long violations) {
            DqRule r = new DqRule(1, "r", "ds", RuleType.EXPRESSION, null,
                    Map.of("expression", "v >= 0"), Severity.FAIL,
                    ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.valueOf(999), policy);

            assertThat(evaluateOne(rows(-1, null, 5), r).rowsViolated()).isEqualTo(violations);
        }

        /** Three rows sharing a key are two excess rows, not one duplicated key. */
        @Test
        @DisplayName("unique counts excess rows")
        void uniqueCountsExcessRows() {
            List<Row> data = List.of(
                    RowFactory.create(1, "a"), RowFactory.create(2, "a"),
                    RowFactory.create(3, "a"), RowFactory.create(4, "b"));
            Dataset<Row> frame = SparkTestSupport.spark().createDataFrame(data, SCHEMA);

            DqRule r = new DqRule(1, "r", "ds", RuleType.UNIQUE, "k", Map.of(), Severity.FAIL,
                    ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.valueOf(999),
                    NullPolicy.VIOLATION);

            assertThat(evaluateOne(frame, r).rowsViolated()).isEqualTo(2);
        }

        @Test
        void referentialFindsOrphans() {
            Dataset<Row> reference = SparkTestSupport.spark().createDataFrame(
                    List.of(RowFactory.create(1, "x"), RowFactory.create(2, "x")), SCHEMA);

            DqRule r = new DqRule(1, "r", "ds", RuleType.REFERENTIAL, "v",
                    Map.of("ref_dataset", "zones", "ref_column", "v"), Severity.FAIL,
                    ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.valueOf(999),
                    NullPolicy.PASS);

            Report report = engine.evaluate(rows(1, 2, 99, null), List.of(r),
                    new DqEngine.Context(Map.of("zones", reference), Optional.empty()));

            assertThat(report.results().get(0).rowsViolated()).as("99 is an orphan").isEqualTo(1);
            assertThat(report.results().get(0).rowsEvaluated())
                    .as("the null is not evaluated").isEqualTo(3);
        }

        @Test
        @DisplayName("row_count_delta only fires on a drop, never on growth")
        void rowCountDeltaOnlyFiresOnDrops() {
            DqRule r = new DqRule(1, "r", "ds", RuleType.ROW_COUNT_DELTA, null,
                    Map.of("max_drop_pct", 0.25), Severity.WARN,
                    ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO, NullPolicy.PASS);

            // 100 -> 50 is a 50% drop.
            assertThat(engine.evaluate(rows(1, 2, 3), List.of(r),
                    new DqEngine.Context(Map.of(), Optional.of(100L)))
                    .results().get(0).passed()).isFalse();

            // Growth is never a violation.
            assertThat(engine.evaluate(rows(1, 2, 3), List.of(r),
                    new DqEngine.Context(Map.of(), Optional.of(2L)))
                    .results().get(0).passed()).isTrue();
        }

        /** A first load has nothing to compare against and must not fail itself. */
        @Test
        @DisplayName("row_count_delta passes on a first run")
        void rowCountDeltaPassesWithoutHistory() {
            DqRule r = new DqRule(1, "r", "ds", RuleType.ROW_COUNT_DELTA, null,
                    Map.of("max_drop_pct", 0.25), Severity.FAIL,
                    ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO, NullPolicy.PASS);

            assertThat(engine.evaluate(rows(1), List.of(r), DqEngine.Context.empty())
                    .results().get(0).passed()).isTrue();
        }
    }

    @Nested
    @DisplayName("dataset cardinality")
    class Cardinality {

        static Stream<Arguments> cardinalities() {
            return Stream.of(
                    Arguments.of("empty", new Integer[] {}, 0L, 0L),
                    Arguments.of("one valid", new Integer[] {5}, 1L, 0L),
                    Arguments.of("one invalid", new Integer[] {50}, 1L, 1L),
                    Arguments.of("two rows", new Integer[] {5, 50}, 2L, 1L),
                    Arguments.of("all invalid", new Integer[] {50, 60, 70}, 3L, 3L),
                    Arguments.of("one invalid among many",
                            new Integer[] {1, 2, 3, 4, 50}, 5L, 1L),
                    Arguments.of("one valid among many",
                            new Integer[] {50, 60, 70, 80, 5}, 5L, 4L));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("cardinalities")
        void rangeAcrossCardinalities(
                String name, Integer[] values, long evaluated, long violated) {

            RuleResult result = evaluateOne(
                    rows(values), rule(RuleType.RANGE, NullPolicy.PASS, range(0, 10)));

            assertThat(result.rowsEvaluated()).isEqualTo(evaluated);
            assertThat(result.rowsViolated()).isEqualTo(violated);
        }

        /** An empty dataset must produce zeroes, not nulls, and must not breach. */
        @Test
        @DisplayName("an empty dataset yields zeroes and passes")
        void emptyDatasetIsSafe() {
            RuleResult result = evaluateOne(rows(),
                    rule(RuleType.NOT_NULL, NullPolicy.VIOLATION, Map.of()));

            assertThat(result.rowsEvaluated()).isZero();
            assertThat(result.rowsViolated()).isZero();
            assertThat(result.violationRate()).isZero();
            assertThat(result.passed()).isTrue();
        }
    }

    @Nested
    @DisplayName("the single pass agrees with evaluating rules one at a time")
    class SinglePass {

        /**
         * The optimization must not change the answer. Folding nineteen rules into one aggregate is
         * only worth doing if it produces identical results to the obvious implementation.
         */
        @Test
        @DisplayName("batched and individual evaluation agree")
        void batchedMatchesIndividual() {
            Dataset<Row> data = rows(1, null, 50, 3, -2);
            List<DqRule> rules = List.of(
                    new DqRule(1, "not_null", "ds", RuleType.NOT_NULL, "v", Map.of(),
                            Severity.FAIL, ThresholdType.MAX_VIOLATION_COUNT,
                            BigDecimal.valueOf(999), NullPolicy.VIOLATION),
                    new DqRule(2, "range", "ds", RuleType.RANGE, "v", range(0, 10),
                            Severity.WARN, ThresholdType.MAX_VIOLATION_COUNT,
                            BigDecimal.valueOf(999), NullPolicy.IGNORE),
                    new DqRule(3, "expr", "ds", RuleType.EXPRESSION, null,
                            Map.of("expression", "v > 0"), Severity.FAIL,
                            ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.valueOf(999),
                            NullPolicy.PASS));

            Report batched = engine.evaluate(data, rules, DqEngine.Context.empty());

            for (DqRule rule : rules) {
                RuleResult alone = evaluateOne(data, rule);
                RuleResult inBatch = batched.results().stream()
                        .filter(r -> r.rule().ruleId() == rule.ruleId()).findFirst().orElseThrow();

                assertThat(inBatch.rowsViolated())
                        .as("%s violations", rule.ruleName()).isEqualTo(alone.rowsViolated());
                assertThat(inBatch.rowsEvaluated())
                        .as("%s evaluated", rule.ruleName()).isEqualTo(alone.rowsEvaluated());
            }
        }
    }

    @Nested
    @DisplayName("report semantics")
    class Reporting {

        @Test
        @DisplayName("only a breached FAIL rule blocks")
        void onlyFailSeverityBlocks() {
            DqRule warn = new DqRule(1, "warn", "ds", RuleType.NOT_NULL, "v", Map.of(),
                    Severity.WARN, ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO,
                    NullPolicy.VIOLATION);
            DqRule fail = new DqRule(2, "fail", "ds", RuleType.NOT_NULL, "v", Map.of(),
                    Severity.FAIL, ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO,
                    NullPolicy.VIOLATION);

            assertThat(engine.evaluate(rows(1, null), List.of(warn), DqEngine.Context.empty())
                    .blocked()).as("a breached WARN records but does not block").isFalse();
            assertThat(engine.evaluate(rows(1, null), List.of(fail), DqEngine.Context.empty())
                    .blocked()).as("a breached FAIL blocks").isTrue();
            assertThat(engine.evaluate(rows(1, 2), List.of(fail), DqEngine.Context.empty())
                    .blocked()).as("an unbreached FAIL does not block").isFalse();
        }

        @Test
        @DisplayName("breaches lists every rule over threshold, both severities")
        void breachesListsAll() {
            DqRule warn = new DqRule(1, "warn", "ds", RuleType.NOT_NULL, "v", Map.of(),
                    Severity.WARN, ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO,
                    NullPolicy.VIOLATION);
            DqRule fail = new DqRule(2, "fail", "ds", RuleType.RANGE, "v", range(0, 10),
                    Severity.FAIL, ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO,
                    NullPolicy.VIOLATION);

            Report report = engine.evaluate(rows(null, 50), List.of(warn, fail),
                    DqEngine.Context.empty());

            assertThat(report.breaches()).hasSize(2);
            assertThat(report.blocked()).isTrue();
            assertThat(report.summary()).contains("2 rules evaluated", "2 breached", "BLOCKING");
        }
    }

    @Nested
    @DisplayName("misconfiguration fails loudly")
    class Misconfiguration {

        @Test
        void missingTargetColumnIsRefused() {
            DqRule r = new DqRule(1, "r", "ds", RuleType.RANGE, null, range(0, 10),
                    Severity.FAIL, ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO,
                    NullPolicy.VIOLATION);

            assertThatThrownBy(() -> evaluateOne(rows(1), r))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("needs a target column");
        }

        @Test
        void missingExpressionParamIsRefused() {
            DqRule r = new DqRule(1, "r", "ds", RuleType.EXPRESSION, null, Map.of(),
                    Severity.FAIL, ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO,
                    NullPolicy.VIOLATION);

            assertThatThrownBy(() -> evaluateOne(rows(1), r))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'expression'");
        }

        @Test
        void emptyAcceptedValuesIsRefused() {
            DqRule r = rule(RuleType.ACCEPTED_VALUES, NullPolicy.PASS,
                    Map.of("values", List.of()));

            assertThatThrownBy(() -> evaluateOne(rows(1), r))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("non-empty");
        }

        @Test
        void missingReferenceDatasetIsRefused() {
            DqRule r = new DqRule(1, "r", "ds", RuleType.REFERENTIAL, "v",
                    Map.of("ref_dataset", "absent", "ref_column", "v"), Severity.FAIL,
                    ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO, NullPolicy.PASS);

            assertThatThrownBy(() -> evaluateOne(rows(1), r))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("reference dataset");
        }

        /** A rule pointing at a column the dataset lacks is a config error, not a data breach. */
        @Test
        @DisplayName("rules targeting absent columns are reported separately")
        void inapplicableRulesAreIdentified() {
            DqRule present = rule(RuleType.NOT_NULL, NullPolicy.VIOLATION, Map.of());
            DqRule absent = new DqRule(2, "absent", "ds", RuleType.NOT_NULL, "nope", Map.of(),
                    Severity.FAIL, ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO,
                    NullPolicy.VIOLATION);

            assertThat(DqEngine.inapplicable(rows(1), List.of(present, absent)))
                    .containsExactly(absent);
            assertThat(DqEngine.requiredColumns(List.of(present, absent)))
                    .containsExactlyInAnyOrder("v", "nope");
        }
    }

    @Test
    @DisplayName("describe is readable in a log line")
    void describeIsReadable() {
        RuleResult result = evaluateOne(rows(1, null),
                rule(RuleType.NOT_NULL, NullPolicy.VIOLATION, Map.of()));

        assertThat(result.describe()).contains("r:", "1/2", "FAIL");
        assertThat(Arrays.asList(result.describe().split(" "))).isNotEmpty();
    }
}
