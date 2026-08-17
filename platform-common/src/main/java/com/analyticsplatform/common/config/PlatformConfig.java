package com.analyticsplatform.common.config;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Validated platform configuration (§42).
 *
 * <p>Every value is checked at construction, before a job touches data. The failure this prevents
 * is the expensive one: a job that reads several million rows, spends minutes shuffling, and only
 * then discovers its ClickHouse endpoint was misspelled. Validation is cheap and the feedback is
 * immediate, so all of it happens up front.
 *
 * <p>Errors are collected rather than thrown one at a time. Fixing six typos across six failed
 * runs is a bad way to spend an afternoon.
 *
 * <p>Built from a {@code Map} rather than reading {@code System.getenv()} directly so the
 * validation rules are testable without mutating the process environment.
 */
public final class PlatformConfig {

    // Required
    public static final String PG_JDBC_URL = "PG_JDBC_URL";
    public static final String POSTGRES_USER = "POSTGRES_USER";
    public static final String POSTGRES_PASSWORD = "POSTGRES_PASSWORD";
    public static final String CH_JDBC_URL = "CH_JDBC_URL";
    public static final String CLICKHOUSE_USER = "CLICKHOUSE_USER";
    public static final String CLICKHOUSE_PASSWORD = "CLICKHOUSE_PASSWORD";
    public static final String KAFKA_BOOTSTRAP = "KAFKA_BOOTSTRAP";
    public static final String DATA_ROOT = "DATA_ROOT";
    public static final String STAGING_ROOT = "STAGING_ROOT";

    // Optional, with defaults
    public static final String SHUFFLE_PARTITIONS = "SHUFFLE_PARTITIONS";
    public static final String PARQUET_COMPRESSION = "PARQUET_COMPRESSION";
    public static final String SCHEMA_POLICY = "SCHEMA_POLICY";
    public static final String LEASE_SECONDS = "LEASE_SECONDS";
    public static final String JDBC_TIMEOUT_SECONDS = "JDBC_TIMEOUT_SECONDS";

    private static final List<String> REQUIRED = List.of(
            PG_JDBC_URL, POSTGRES_USER, POSTGRES_PASSWORD,
            CH_JDBC_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD,
            KAFKA_BOOTSTRAP, DATA_ROOT, STAGING_ROOT);

    private static final Set<String> SUPPORTED_COMPRESSION =
            Set.of("zstd", "snappy", "gzip", "lz4", "uncompressed");

    /** strict rejects any change; allow_widening permits additive and widening transitions. */
    private static final Set<String> SUPPORTED_SCHEMA_POLICIES =
            Set.of("strict", "allow_widening");

    private static final Map<String, String> DEFAULTS = Map.of(
            SHUFFLE_PARTITIONS, "12",
            PARQUET_COMPRESSION, "zstd",
            SCHEMA_POLICY, "allow_widening",
            LEASE_SECONDS, "900",
            JDBC_TIMEOUT_SECONDS, "30");

    private final Map<String, String> values;

    private PlatformConfig(Map<String, String> values) {
        this.values = Map.copyOf(values);
    }

    /** Thrown when configuration is unusable. Carries every problem found, not just the first. */
    public static final class ConfigurationException extends RuntimeException {
        private final transient List<String> problems;

        ConfigurationException(List<String> problems) {
            super("invalid platform configuration:\n  - " + String.join("\n  - ", problems));
            this.problems = List.copyOf(problems);
        }

        public List<String> problems() {
            return problems;
        }
    }

    public static PlatformConfig fromEnvironment() {
        return from(System.getenv());
    }

    /**
     * Validates and builds a configuration.
     *
     * @throws ConfigurationException listing every problem found
     */
    public static PlatformConfig from(Map<String, String> source) {
        Map<String, String> merged = new LinkedHashMap<>(DEFAULTS);
        source.forEach((key, value) -> {
            if (value != null && !value.isBlank()) {
                merged.put(key, value.trim());
            }
        });

        List<String> problems = new java.util.ArrayList<>();

        for (String key : REQUIRED) {
            if (!merged.containsKey(key) || merged.get(key).isBlank()) {
                problems.add(key + " is required but missing or blank");
            }
        }

        requirePrefix(merged, problems, PG_JDBC_URL, "jdbc:postgresql:");
        requirePrefix(merged, problems, CH_JDBC_URL, "jdbc:clickhouse:");
        validateBootstrap(merged, problems);
        validatePositiveInt(merged, problems, SHUFFLE_PARTITIONS);
        validatePositiveInt(merged, problems, LEASE_SECONDS);
        validatePositiveInt(merged, problems, JDBC_TIMEOUT_SECONDS);
        validateMembership(merged, problems, PARQUET_COMPRESSION, SUPPORTED_COMPRESSION);
        validateMembership(merged, problems, SCHEMA_POLICY, SUPPORTED_SCHEMA_POLICIES);
        validateRoots(merged, problems);

        if (!problems.isEmpty()) {
            throw new ConfigurationException(problems);
        }
        return new PlatformConfig(merged);
    }

    private static void requirePrefix(
            Map<String, String> values, List<String> problems, String key, String prefix) {
        String value = values.get(key);
        if (value != null && !value.startsWith(prefix)) {
            problems.add(key + " must start with '" + prefix + "' but was '" + value + "'");
        }
    }

    /** Accepts {@code host:port}, comma-separated. A bare hostname is the usual mistake. */
    private static void validateBootstrap(Map<String, String> values, List<String> problems) {
        String value = values.get(KAFKA_BOOTSTRAP);
        if (value == null) {
            return;
        }
        for (String broker : value.split(",")) {
            String trimmed = broker.trim();
            int colon = trimmed.lastIndexOf(':');
            if (colon <= 0 || colon == trimmed.length() - 1) {
                problems.add(KAFKA_BOOTSTRAP + " entry '" + trimmed
                        + "' must be host:port");
                continue;
            }
            try {
                int port = Integer.parseInt(trimmed.substring(colon + 1));
                if (port < 1 || port > 65535) {
                    problems.add(KAFKA_BOOTSTRAP + " port out of range in '" + trimmed + "'");
                }
            } catch (NumberFormatException e) {
                problems.add(KAFKA_BOOTSTRAP + " port is not a number in '" + trimmed + "'");
            }
        }
    }

    private static void validatePositiveInt(
            Map<String, String> values, List<String> problems, String key) {
        String value = values.get(key);
        if (value == null) {
            return;
        }
        try {
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                problems.add(key + " must be positive but was " + parsed);
            }
        } catch (NumberFormatException e) {
            problems.add(key + " must be an integer but was '" + value + "'");
        }
    }

    private static void validateMembership(
            Map<String, String> values, List<String> problems, String key, Set<String> allowed) {
        String value = values.get(key);
        if (value != null && !allowed.contains(value.toLowerCase(java.util.Locale.ROOT))) {
            problems.add(key + " '" + value + "' is not supported; expected one of "
                    + new TreeMap<>(allowed.stream()
                            .collect(java.util.stream.Collectors.toMap(s -> s, s -> ""))).keySet());
        }
    }

    /**
     * Staging and target must be genuinely separate trees (§37).
     *
     * <p>If staging sits inside the data root, staging cleanup can delete published output; if the
     * data root sits inside staging, the same cleanup takes the whole warehouse with it. Both are
     * unrecoverable and both are easy to configure by accident.
     */
    private static void validateRoots(Map<String, String> values, List<String> problems) {
        String dataRoot = values.get(DATA_ROOT);
        String stagingRoot = values.get(STAGING_ROOT);
        if (dataRoot == null || stagingRoot == null) {
            return;
        }

        Path data = Path.of(dataRoot).normalize().toAbsolutePath();
        Path staging = Path.of(stagingRoot).normalize().toAbsolutePath();

        if (data.equals(staging)) {
            problems.add(STAGING_ROOT + " must not equal " + DATA_ROOT + " (" + data + ")");
            return;
        }
        if (staging.startsWith(data)) {
            problems.add(STAGING_ROOT + " (" + staging + ") must not sit inside "
                    + DATA_ROOT + " (" + data + "): staging cleanup could delete published output");
        }
        if (data.startsWith(staging)) {
            problems.add(DATA_ROOT + " (" + data + ") must not sit inside "
                    + STAGING_ROOT + " (" + staging + "): staging cleanup would delete the warehouse");
        }
    }

    // ------------------------------------------------------------------- accessors

    public String get(String key) {
        String value = values.get(key);
        if (value == null) {
            throw new IllegalArgumentException("no configuration value for " + key);
        }
        return value;
    }

    public int getInt(String key) {
        return Integer.parseInt(get(key));
    }

    public String postgresUrl() {
        return get(PG_JDBC_URL);
    }

    public String postgresUser() {
        return get(POSTGRES_USER);
    }

    public String postgresPassword() {
        return get(POSTGRES_PASSWORD);
    }

    public String clickhouseUrl() {
        return get(CH_JDBC_URL);
    }

    public String clickhouseUser() {
        return get(CLICKHOUSE_USER);
    }

    public String clickhousePassword() {
        return get(CLICKHOUSE_PASSWORD);
    }

    public String kafkaBootstrap() {
        return get(KAFKA_BOOTSTRAP);
    }

    public Path dataRoot() {
        return Path.of(get(DATA_ROOT)).normalize().toAbsolutePath();
    }

    public Path stagingRoot() {
        return Path.of(get(STAGING_ROOT)).normalize().toAbsolutePath();
    }

    public int shufflePartitions() {
        return getInt(SHUFFLE_PARTITIONS);
    }

    public String parquetCompression() {
        return get(PARQUET_COMPRESSION);
    }

    public String schemaPolicy() {
        return get(SCHEMA_POLICY);
    }

    public int leaseSeconds() {
        return getInt(LEASE_SECONDS);
    }

    public int jdbcTimeoutSeconds() {
        return getInt(JDBC_TIMEOUT_SECONDS);
    }

    /**
     * Configuration with secrets masked, safe to log (§50).
     *
     * <p>The whole point of keeping passwords out of tracked files is undone by a startup banner
     * that prints them, so the redacting view is the only rendering this class offers.
     */
    public Map<String, String> redacted() {
        Map<String, String> out = new TreeMap<>();
        values.forEach((key, value) -> out.put(key, isSecret(key) ? "***" : value));
        return out;
    }

    @Override
    public String toString() {
        return "PlatformConfig" + redacted();
    }

    private static boolean isSecret(String key) {
        String upper = key.toUpperCase(java.util.Locale.ROOT);
        return upper.contains("PASSWORD") || upper.contains("SECRET") || upper.contains("TOKEN");
    }
}
