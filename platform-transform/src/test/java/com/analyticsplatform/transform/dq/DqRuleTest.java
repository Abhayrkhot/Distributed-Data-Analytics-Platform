package com.analyticsplatform.transform.dq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.analyticsplatform.transform.dq.DqRule.NullPolicy;
import com.analyticsplatform.transform.dq.DqRule.RuleType;
import com.analyticsplatform.transform.dq.DqRule.Severity;
import com.analyticsplatform.transform.dq.DqRule.ThresholdType;
import java.math.BigDecimal;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Threshold semantics and rule validation.
 *
 * <p>The boundary cases carry real weight: {@code >} versus {@code >=} decides whether a run at
 * exactly the threshold aborts or proceeds, and getting it wrong is invisible until the day a run
 * lands precisely on the line.
 */
class DqRuleTest {

    private static DqRule rule(ThresholdType type, String threshold) {
        return new DqRule(1, "r", "ds", RuleType.RANGE, "c", Map.of(), Severity.FAIL,
                type, new BigDecimal(threshold), NullPolicy.VIOLATION);
    }

    @Nested
    @DisplayName("count thresholds")
    class CountThresholds {

        @ParameterizedTest(name = "{0} violations against a limit of 5 → breaches={1}")
        @CsvSource({"0, false", "4, false", "5, false", "6, true", "100, true"})
        void countBoundary(long violations, boolean expected) {
            assertThat(rule(ThresholdType.MAX_VIOLATION_COUNT, "5").breaches(1000, violations))
                    .isEqualTo(expected);
        }

        /** "At most N" means N passes. Stated as its own test because it is the ambiguous case. */
        @Test
        @DisplayName("exactly at the limit passes")
        void exactlyAtLimitPasses() {
            assertThat(rule(ThresholdType.MAX_VIOLATION_COUNT, "5").breaches(1000, 5)).isFalse();
            assertThat(rule(ThresholdType.MAX_VIOLATION_COUNT, "5").breaches(1000, 6)).isTrue();
        }

        /** Zero tolerance: a single violation breaches. */
        @Test
        @DisplayName("a zero threshold breaches on one violation")
        void zeroToleranceBreachesImmediately() {
            assertThat(rule(ThresholdType.MAX_VIOLATION_COUNT, "0").breaches(1000, 0)).isFalse();
            assertThat(rule(ThresholdType.MAX_VIOLATION_COUNT, "0").breaches(1000, 1)).isTrue();
        }

        /** A count threshold does not scale with dataset size — that is the point of choosing it. */
        @Test
        @DisplayName("count thresholds ignore the denominator")
        void countIgnoresDenominator() {
            DqRule r = rule(ThresholdType.MAX_VIOLATION_COUNT, "5");

            assertThat(r.breaches(10, 6)).isTrue();
            assertThat(r.breaches(10_000_000, 6)).isTrue();
        }
    }

    @Nested
    @DisplayName("fraction thresholds")
    class FractionThresholds {

        @ParameterizedTest(name = "{0}/{1} against 0.05 → breaches={2}")
        @CsvSource({
            "0, 100, false",     // none
            "4, 100, false",     // under
            "5, 100, false",     // exactly at the limit
            "6, 100, true",      // over
            "1, 20,  false",     // exactly 0.05 again, different denominator
            "2, 20,  true",
        })
        void fractionBoundary(long violated, long evaluated, boolean expected) {
            assertThat(rule(ThresholdType.MAX_VIOLATION_FRACTION, "0.05")
                    .breaches(evaluated, violated)).isEqualTo(expected);
        }

        @Test
        @DisplayName("zero tolerance breaches on a single row in a million")
        void zeroToleranceIsStrict() {
            DqRule r = rule(ThresholdType.MAX_VIOLATION_FRACTION, "0.0");

            assertThat(r.breaches(1_000_000, 0)).isFalse();
            assertThat(r.breaches(1_000_000, 1)).isTrue();
        }

        /**
         * An empty dataset violates nothing. Whether emptiness is itself a problem is a freshness
         * or row-count question — answering it here would make every rule fire on an empty load.
         */
        @Test
        @DisplayName("an empty dataset does not breach")
        void emptyDatasetDoesNotBreach() {
            assertThat(rule(ThresholdType.MAX_VIOLATION_FRACTION, "0.0").breaches(0, 0)).isFalse();
        }

        @Test
        @DisplayName("a threshold of 1.0 permits everything")
        void fullToleranceNeverBreaches() {
            assertThat(rule(ThresholdType.MAX_VIOLATION_FRACTION, "1.0").breaches(100, 100))
                    .isFalse();
        }
    }

    @Nested
    @DisplayName("validation")
    class Validation {

        @Test
        @DisplayName("a fraction above 1.0 is refused")
        void fractionAboveOneIsRefused() {
            assertThatThrownBy(() -> rule(ThresholdType.MAX_VIOLATION_FRACTION, "1.5"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("fraction threshold above 1.0");
        }

        @Test
        @DisplayName("a negative threshold is refused")
        void negativeThresholdIsRefused() {
            assertThatThrownBy(() -> rule(ThresholdType.MAX_VIOLATION_COUNT, "-1"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("non-negative");
        }

        @Test
        @DisplayName("a count above 1.0 is fine — it is rows, not a ratio")
        void countAboveOneIsAllowed() {
            assertThatCode(() -> rule(ThresholdType.MAX_VIOLATION_COUNT, "5000"))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("missing required fields are refused")
        void missingFieldsAreRefused() {
            assertThatThrownBy(() -> new DqRule(1, "r", "ds", null, "c", Map.of(),
                    Severity.FAIL, ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO,
                    NullPolicy.VIOLATION)).isInstanceOf(IllegalArgumentException.class);

            assertThatThrownBy(() -> new DqRule(1, "  ", "ds", RuleType.RANGE, "c", Map.of(),
                    Severity.FAIL, ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO,
                    NullPolicy.VIOLATION)).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("ruleName");
        }

        @Test
        @DisplayName("params default to empty rather than null")
        void paramsDefaultToEmpty() {
            DqRule r = new DqRule(1, "r", "ds", RuleType.RANGE, "c", null, Severity.FAIL,
                    ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO, NullPolicy.VIOLATION);

            assertThat(r.params()).isEmpty();
            assertThat(r.paramString("absent")).isNull();
            assertThat(r.paramDouble("absent")).isNull();
        }

        @Test
        @DisplayName("numeric params parse from both numbers and strings")
        void paramsParseLeniently() {
            DqRule r = new DqRule(1, "r", "ds", RuleType.RANGE, "c",
                    Map.of("min", 0, "max", "500.5"), Severity.FAIL,
                    ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO, NullPolicy.VIOLATION);

            assertThat(r.paramDouble("min")).isZero();
            assertThat(r.paramDouble("max")).isEqualTo(500.5);
        }
    }

    @Nested
    @DisplayName("evaluation strategy")
    class Strategy {

        /**
         * Which rules fold into the single pass. Getting this wrong does not break correctness but
         * silently costs a full scan per rule, which is how DQ ends up switched off.
         */
        @ParameterizedTest
        @EnumSource(RuleType.class)
        void columnWiseClassification(RuleType type) {
            DqRule r = new DqRule(1, "r", "ds", type, "c", Map.of(), Severity.FAIL,
                    ThresholdType.MAX_VIOLATION_COUNT, BigDecimal.ZERO, NullPolicy.VIOLATION);

            boolean expected = switch (type) {
                case NOT_NULL, RANGE, ACCEPTED_VALUES, EXPRESSION, FRESHNESS -> true;
                case UNIQUE, REFERENTIAL, ROW_COUNT_DELTA -> false;
            };
            assertThat(r.isColumnWise()).isEqualTo(expected);
        }
    }

    @Nested
    @DisplayName("enum parsing mirrors the database vocabulary")
    class Parsing {

        @Test
        void parsesLowercaseDatabaseValues() {
            assertThat(RuleType.parse("accepted_values")).isEqualTo(RuleType.ACCEPTED_VALUES);
            assertThat(Severity.parse("warn")).isEqualTo(Severity.WARN);
            assertThat(ThresholdType.parse("max_violation_fraction"))
                    .isEqualTo(ThresholdType.MAX_VIOLATION_FRACTION);
            assertThat(NullPolicy.parse("ignore")).isEqualTo(NullPolicy.IGNORE);
        }

        @Test
        @DisplayName("an unknown value is refused rather than defaulted")
        void unknownValuesAreRefused() {
            assertThatThrownBy(() -> RuleType.parse("nonsense"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> NullPolicy.parse("maybe"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
