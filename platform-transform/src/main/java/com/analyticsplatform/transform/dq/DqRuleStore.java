package com.analyticsplatform.transform.dq;

import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.transform.dq.DqEngine.RuleResult;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Loads DQ rules from {@code control.dq_rule} and records outcomes to {@code control.dq_result}.
 *
 * <p>Rules live in the database so that tightening a threshold is a SQL change rather than a
 * rebuild and redeploy. That matters in practice: thresholds get tuned in response to real data,
 * usually at an inconvenient moment.
 */
public final class DqRuleStore {

    private static final String SELECT_RULES = """
            SELECT rule_id, rule_name, dataset_name, rule_type, target_column,
                   rule_params::text, severity, threshold_type, threshold_value, null_policy
              FROM control.dq_rule
             WHERE dataset_name = ? AND enabled
             ORDER BY rule_id
            """;

    private static final String INSERT_RESULT = """
            INSERT INTO control.dq_result
                (run_id, rule_id, dataset_name, rows_evaluated, rows_violated,
                 violation_rate, passed, severity, sample_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
            ON CONFLICT (run_id, rule_id) DO UPDATE
               SET rows_evaluated = EXCLUDED.rows_evaluated,
                   rows_violated  = EXCLUDED.rows_violated,
                   violation_rate = EXCLUDED.violation_rate,
                   passed         = EXCLUDED.passed,
                   evaluated_at   = now()
            """;

    /** Row count from the most recent successful run, for {@code row_count_delta}. */
    private static final String SELECT_PREVIOUS_COUNT = """
            SELECT m.row_count
              FROM control.unit_manifest m
             WHERE m.dataset_name = ? AND m.pipeline_stage = ?
             ORDER BY m.published_at DESC
             LIMIT 1
            """;

    private final ConnectionSource connections;

    public DqRuleStore(ConnectionSource connections) {
        this.connections = connections;
    }

    /** Enabled rules for a dataset. */
    public List<DqRule> rulesFor(String datasetName) {
        List<DqRule> rules = new ArrayList<>();
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(SELECT_RULES)) {

            statement.setString(1, datasetName);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    rules.add(new DqRule(
                            rows.getLong("rule_id"),
                            rows.getString("rule_name"),
                            rows.getString("dataset_name"),
                            DqRule.RuleType.parse(rows.getString("rule_type")),
                            rows.getString("target_column"),
                            parseParams(rows.getString("rule_params")),
                            DqRule.Severity.parse(rows.getString("severity")),
                            DqRule.ThresholdType.parse(rows.getString("threshold_type")),
                            rows.getBigDecimal("threshold_value"),
                            DqRule.NullPolicy.parse(rows.getString("null_policy"))));
                }
            }
        } catch (SQLException e) {
            throw new DqException("failed to load DQ rules for " + datasetName, e);
        }
        return rules;
    }

    /** Records every rule outcome for a run. */
    public void recordResults(long runId, String datasetName, List<RuleResult> results) {
        try (Connection connection = connections.open()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(INSERT_RESULT)) {
                for (RuleResult result : results) {
                    statement.setLong(1, runId);
                    statement.setLong(2, result.rule().ruleId());
                    statement.setString(3, datasetName);
                    statement.setLong(4, result.rowsEvaluated());
                    statement.setLong(5, result.rowsViolated());
                    // Clamped because the column is CHECK (0..1) and a row_count_delta rule can
                    // report violations exceeding its denominator when volume collapses.
                    statement.setBigDecimal(6, BigDecimal.valueOf(
                            Math.min(1.0, Math.max(0.0, result.violationRate()))));
                    statement.setBoolean(7, result.passed());
                    statement.setString(8, result.rule().severity().name());
                    statement.setString(9, sampleJson(result));
                    statement.addBatch();
                }
                statement.executeBatch();
            }
            connection.commit();
        } catch (SQLException e) {
            throw new DqException("failed to record DQ results for run " + runId, e);
        }
    }

    /** The previously published row count for a stage, if any. */
    public Optional<Long> previousRowCount(String datasetName, String pipelineStage) {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(SELECT_PREVIOUS_COUNT)) {

            statement.setString(1, datasetName);
            statement.setString(2, pipelineStage);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(rows.getLong(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new DqException("failed to read previous row count for " + datasetName, e);
        }
    }

    /**
     * Minimal JSON for the result sample.
     *
     * <p>Hand-rolled for the same reason as the control plane's: Spark ships its own Jackson, and a
     * version clash surfaces as {@code NoSuchMethodError} on the cluster — a miserable failure to
     * diagnose in exchange for serializing four fields.
     */
    private static String sampleJson(RuleResult result) {
        return "{\"rule_type\":\"" + result.rule().ruleType().name().toLowerCase(java.util.Locale.ROOT)
                + "\",\"target_column\":" + quoteOrNull(result.rule().targetColumn())
                + ",\"null_policy\":\"" + result.rule().nullPolicy().name().toLowerCase(java.util.Locale.ROOT)
                + "\",\"threshold_type\":\"" + result.rule().thresholdType().name().toLowerCase(java.util.Locale.ROOT)
                + "\",\"threshold_value\":" + result.rule().thresholdValue().toPlainString() + "}";
    }

    private static String quoteOrNull(String value) {
        return value == null ? "null" : "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    /**
     * Parses the {@code rule_params} JSON object.
     *
     * <p>Deliberately narrow: it handles the flat objects the seed actually stores — strings,
     * numbers, and arrays of either. A general JSON parser here would mean depending on Jackson,
     * which is the clash described above. Anything more complex than this belongs in its own column,
     * not smuggled through a params blob.
     */
    static Map<String, Object> parseParams(String json) {
        Map<String, Object> params = new HashMap<>();
        if (json == null || json.isBlank()) {
            return params;
        }
        String body = json.trim();
        if (body.startsWith("{")) {
            body = body.substring(1, body.length() - 1);
        }

        int index = 0;
        while (index < body.length()) {
            int keyStart = body.indexOf('"', index);
            if (keyStart < 0) {
                break;
            }
            int keyEnd = body.indexOf('"', keyStart + 1);
            String key = body.substring(keyStart + 1, keyEnd);

            int colon = body.indexOf(':', keyEnd);
            int valueStart = colon + 1;
            while (valueStart < body.length() && Character.isWhitespace(body.charAt(valueStart))) {
                valueStart++;
            }

            int valueEnd;
            Object value;
            if (body.charAt(valueStart) == '[') {
                valueEnd = body.indexOf(']', valueStart) + 1;
                value = parseArray(body.substring(valueStart + 1, valueEnd - 1));
            } else if (body.charAt(valueStart) == '"') {
                valueEnd = body.indexOf('"', valueStart + 1) + 1;
                value = body.substring(valueStart + 1, valueEnd - 1);
            } else {
                valueEnd = valueStart;
                while (valueEnd < body.length() && body.charAt(valueEnd) != ','
                        && body.charAt(valueEnd) != '}') {
                    valueEnd++;
                }
                value = parseScalar(body.substring(valueStart, valueEnd).trim());
            }

            params.put(key, value);
            index = valueEnd;
        }
        return params;
    }

    private static List<Object> parseArray(String contents) {
        List<Object> values = new ArrayList<>();
        for (String element : contents.split(",")) {
            String trimmed = element.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            values.add(trimmed.startsWith("\"")
                    ? trimmed.substring(1, trimmed.length() - 1)
                    : parseScalar(trimmed));
        }
        return values;
    }

    private static Object parseScalar(String raw) {
        if ("true".equals(raw) || "false".equals(raw)) {
            return Boolean.parseBoolean(raw);
        }
        if ("null".equals(raw)) {
            return null;
        }
        try {
            // Deliberately NOT `raw.contains(".") ? Double.valueOf(raw) : Long.valueOf(raw)`.
            // A conditional expression with Double and Long branches triggers binary numeric
            // promotion, so its type is `double` and every integer would come back as a Double.
            // That silently widens an integer threshold and is invisible at the call site.
            if (raw.contains(".") || raw.contains("e") || raw.contains("E")) {
                return Double.valueOf(raw);
            }
            return Long.valueOf(raw);
        } catch (NumberFormatException e) {
            return raw;
        }
    }

    /** Wraps SQL failures from the DQ path. */
    public static final class DqException extends RuntimeException {
        public DqException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
