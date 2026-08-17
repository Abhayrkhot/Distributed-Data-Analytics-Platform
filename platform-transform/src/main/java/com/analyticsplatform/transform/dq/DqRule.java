package com.analyticsplatform.transform.dq;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Map;

/**
 * One declarative data-quality rule, as stored in {@code control.dq_rule}.
 *
 * <p>Rules are data, not code, so tightening a threshold is a SQL change rather than a rebuild and
 * redeploy. That matters because thresholds get tuned in response to real data, usually at an
 * inconvenient moment.
 */
public record DqRule(
        long ruleId,
        String ruleName,
        String datasetName,
        RuleType ruleType,
        String targetColumn,
        Map<String, Object> params,
        Severity severity,
        ThresholdType thresholdType,
        BigDecimal thresholdValue,
        NullPolicy nullPolicy) {

    public enum RuleType {
        NOT_NULL, RANGE, UNIQUE, REFERENTIAL, ROW_COUNT_DELTA, FRESHNESS, ACCEPTED_VALUES,
        EXPRESSION;

        public static RuleType parse(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }
    }

    /** WARN records and continues; FAIL blocks publication. */
    public enum Severity {
        WARN, FAIL;

        public static Severity parse(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }
    }

    /**
     * What {@code thresholdValue} means. Stored explicitly because a bare {@code 0.05} is
     * ambiguous — five percent of rows allowed to fail, or ninety-five percent required to pass?
     * Those differ, and the difference is invisible at the call site.
     */
    public enum ThresholdType {
        /** Fraction of evaluated rows allowed to violate, 0.0 to 1.0. */
        MAX_VIOLATION_FRACTION,
        /** Absolute number of rows allowed to violate. */
        MAX_VIOLATION_COUNT;

        public static ThresholdType parse(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }
    }

    /**
     * How NULL in the target column is treated.
     *
     * <p>Spark SQL is three-valued: {@code NULL > 0} is neither true nor false, so a range check
     * silently ignores nulls unless told otherwise. Leaving that implicit means a column that
     * becomes entirely null passes every range rule it has — which is the opposite of what anyone
     * intends.
     */
    public enum NullPolicy {
        /** NULL counts as a violation. */
        VIOLATION,
        /** NULL is acceptable; only non-null values are checked. */
        PASS,
        /** NULL rows are excluded from the denominator entirely. */
        IGNORE;

        public static NullPolicy parse(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }
    }

    public DqRule {
        if (ruleName == null || ruleName.isBlank()) {
            throw new IllegalArgumentException("ruleName is required");
        }
        if (ruleType == null || severity == null || thresholdType == null || nullPolicy == null) {
            throw new IllegalArgumentException("rule " + ruleName + " is missing required fields");
        }
        if (thresholdValue == null || thresholdValue.signum() < 0) {
            throw new IllegalArgumentException(
                    "rule " + ruleName + " needs a non-negative threshold");
        }
        if (thresholdType == ThresholdType.MAX_VIOLATION_FRACTION
                && thresholdValue.compareTo(BigDecimal.ONE) > 0) {
            throw new IllegalArgumentException(
                    "rule " + ruleName + " has a fraction threshold above 1.0");
        }
        params = params == null ? Map.of() : Map.copyOf(params);
    }

    /** Whether this rule can be evaluated in the shared single-pass aggregate. */
    public boolean isColumnWise() {
        return switch (ruleType) {
            case NOT_NULL, RANGE, ACCEPTED_VALUES, EXPRESSION, FRESHNESS -> true;
            // These need a groupBy, a join, or a previous run's count, so none of them fit a
            // single row-wise pass.
            case UNIQUE, REFERENTIAL, ROW_COUNT_DELTA -> false;
        };
    }

    /**
     * Whether the observed violations breach this rule's threshold.
     *
     * <p>Strictly greater-than: a threshold of "at most 5 bad rows" passes on exactly 5. The
     * boundary is tested in both directions, because {@code >} versus {@code >=} here silently
     * changes which runs abort.
     */
    public boolean breaches(long rowsEvaluated, long rowsViolated) {
        return switch (thresholdType) {
            case MAX_VIOLATION_COUNT -> rowsViolated > thresholdValue.longValue();
            case MAX_VIOLATION_FRACTION -> {
                if (rowsEvaluated == 0) {
                    // No rows to judge. An empty dataset violates nothing; whether it *should* be
                    // an error is a freshness or row-count question, not this rule's.
                    yield false;
                }
                BigDecimal rate = BigDecimal.valueOf(rowsViolated)
                        .divide(BigDecimal.valueOf(rowsEvaluated), java.math.MathContext.DECIMAL64);
                yield rate.compareTo(thresholdValue) > 0;
            }
        };
    }

    public String paramString(String key) {
        Object value = params.get(key);
        return value == null ? null : String.valueOf(value);
    }

    public Double paramDouble(String key) {
        Object value = params.get(key);
        if (value == null) {
            return null;
        }
        return value instanceof Number number ? number.doubleValue()
                : Double.parseDouble(String.valueOf(value));
    }
}
