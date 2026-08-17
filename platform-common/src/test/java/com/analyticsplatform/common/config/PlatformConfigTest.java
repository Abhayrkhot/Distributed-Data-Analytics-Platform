package com.analyticsplatform.common.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import com.analyticsplatform.common.config.PlatformConfig.ConfigurationException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class PlatformConfigTest {

    private static Map<String, String> valid() {
        Map<String, String> config = new HashMap<>();
        config.put(PlatformConfig.PG_JDBC_URL, "jdbc:postgresql://postgres:5432/platform");
        config.put(PlatformConfig.POSTGRES_USER, "platform");
        config.put(PlatformConfig.POSTGRES_PASSWORD, "s3cret");
        config.put(PlatformConfig.CH_JDBC_URL, "jdbc:clickhouse://clickhouse:8123/analytics");
        config.put(PlatformConfig.CLICKHOUSE_USER, "platform");
        config.put(PlatformConfig.CLICKHOUSE_PASSWORD, "s3cret");
        config.put(PlatformConfig.KAFKA_BOOTSTRAP, "kafka:9092");
        config.put(PlatformConfig.DATA_ROOT, "/data/warehouse");
        config.put(PlatformConfig.STAGING_ROOT, "/data/staging");
        return config;
    }

    private static Map<String, String> validWith(String key, String value) {
        Map<String, String> config = valid();
        if (value == null) {
            config.remove(key);
        } else {
            config.put(key, value);
        }
        return config;
    }

    @Nested
    @DisplayName("valid configuration")
    class Valid {

        @Test
        void acceptsACompleteConfiguration() {
            PlatformConfig config = PlatformConfig.from(valid());

            assertThat(config.postgresUrl()).isEqualTo("jdbc:postgresql://postgres:5432/platform");
            assertThat(config.kafkaBootstrap()).isEqualTo("kafka:9092");
            assertThat(config.dataRoot().toString()).endsWith("/data/warehouse");
        }

        @Test
        @DisplayName("optional values fall back to defaults")
        void defaultsAreApplied() {
            PlatformConfig config = PlatformConfig.from(valid());

            assertThat(config.shufflePartitions()).isEqualTo(12);
            assertThat(config.parquetCompression()).isEqualTo("zstd");
            assertThat(config.schemaPolicy()).isEqualTo("allow_widening");
            assertThat(config.leaseSeconds()).isEqualTo(900);
        }

        @Test
        @DisplayName("blank values are treated as absent, not as empty overrides")
        void blankValuesFallBackToDefaults() {
            PlatformConfig config = PlatformConfig.from(
                    validWith(PlatformConfig.SHUFFLE_PARTITIONS, "   "));

            assertThat(config.shufflePartitions()).isEqualTo(12);
        }

        @Test
        void acceptsMultipleKafkaBrokers() {
            assertThat(PlatformConfig.from(validWith(
                    PlatformConfig.KAFKA_BOOTSTRAP, "a:9092,b:9093, c:9094"))
                    .kafkaBootstrap()).contains("c:9094");
        }
    }

    @Nested
    @DisplayName("required values")
    class Required {

        @ParameterizedTest(name = "{0} is required")
        @ValueSource(strings = {
            "PG_JDBC_URL", "POSTGRES_USER", "POSTGRES_PASSWORD",
            "CH_JDBC_URL", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD",
            "KAFKA_BOOTSTRAP", "DATA_ROOT", "STAGING_ROOT",
        })
        void missingRequiredValueIsRejected(String key) {
            assertThatThrownBy(() -> PlatformConfig.from(validWith(key, null)))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining(key);
        }

        /** Fixing six typos across six failed runs is a bad afternoon; report them together. */
        @Test
        @DisplayName("every problem is reported at once, not just the first")
        void allProblemsAreCollected() {
            Map<String, String> broken = valid();
            broken.remove(PlatformConfig.POSTGRES_USER);
            broken.remove(PlatformConfig.KAFKA_BOOTSTRAP);
            broken.put(PlatformConfig.SHUFFLE_PARTITIONS, "-4");
            broken.put(PlatformConfig.PARQUET_COMPRESSION, "brotli");

            ConfigurationException thrown = catchThrowableOfType(
                    ConfigurationException.class, () -> PlatformConfig.from(broken));

            assertThat(thrown.problems()).hasSize(4);
            assertThat(thrown.problems())
                    .anyMatch(p -> p.contains("POSTGRES_USER"))
                    .anyMatch(p -> p.contains("KAFKA_BOOTSTRAP"))
                    .anyMatch(p -> p.contains("SHUFFLE_PARTITIONS"))
                    .anyMatch(p -> p.contains("PARQUET_COMPRESSION"));
        }
    }

    @Nested
    @DisplayName("endpoint validation")
    class Endpoints {

        @ParameterizedTest(name = "PG url {0} is rejected")
        @ValueSource(strings = {
            "postgresql://postgres:5432/platform",     // missing jdbc: prefix
            "jdbc:mysql://postgres:5432/platform",     // wrong driver
            "jdbc:clickhouse://postgres:8123/x",       // the two swapped
        })
        void invalidPostgresUrlIsRejected(String url) {
            assertThatThrownBy(() -> PlatformConfig.from(validWith(PlatformConfig.PG_JDBC_URL, url)))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining("jdbc:postgresql:");
        }

        @Test
        void invalidClickHouseUrlIsRejected() {
            assertThatThrownBy(() -> PlatformConfig.from(
                    validWith(PlatformConfig.CH_JDBC_URL, "http://clickhouse:8123")))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining("jdbc:clickhouse:");
        }

        /** A bare hostname is the usual Kafka misconfiguration. */
        @ParameterizedTest(name = "bootstrap ''{0}'' is rejected")
        @ValueSource(strings = {"kafka", "kafka:", ":9092", "kafka:notaport", "kafka:70000", "kafka:0"})
        void invalidKafkaBootstrapIsRejected(String bootstrap) {
            assertThatThrownBy(() -> PlatformConfig.from(
                    validWith(PlatformConfig.KAFKA_BOOTSTRAP, bootstrap)))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining("KAFKA_BOOTSTRAP");
        }
    }

    @Nested
    @DisplayName("numeric and enumerated values")
    class Values {

        @ParameterizedTest(name = "{0}={1} is rejected")
        @CsvSource({
            "SHUFFLE_PARTITIONS, 0",
            "SHUFFLE_PARTITIONS, -1",
            "SHUFFLE_PARTITIONS, twelve",
            "LEASE_SECONDS, 0",
            "LEASE_SECONDS, -60",
            "JDBC_TIMEOUT_SECONDS, -1",
        })
        void nonPositiveOrNonNumericIsRejected(String key, String value) {
            assertThatThrownBy(() -> PlatformConfig.from(validWith(key, value)))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining(key);
        }

        @Test
        void unsupportedCompressionIsRejected() {
            assertThatThrownBy(() -> PlatformConfig.from(
                    validWith(PlatformConfig.PARQUET_COMPRESSION, "brotli")))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining("PARQUET_COMPRESSION");
        }

        @Test
        void unsupportedSchemaPolicyIsRejected() {
            assertThatThrownBy(() -> PlatformConfig.from(
                    validWith(PlatformConfig.SCHEMA_POLICY, "yolo")))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining("SCHEMA_POLICY");
        }
    }

    @Nested
    @DisplayName("filesystem safety (§37)")
    class Roots {

        @Test
        @DisplayName("staging must not equal the data root")
        void identicalRootsAreRejected() {
            Map<String, String> config = validWith(PlatformConfig.STAGING_ROOT, "/data/warehouse");

            assertThatThrownBy(() -> PlatformConfig.from(config))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining("must not equal");
        }

        /** Staging cleanup inside the warehouse would delete published output. */
        @Test
        @DisplayName("staging inside the data root is rejected")
        void stagingInsideDataRootIsRejected() {
            Map<String, String> config = validWith(
                    PlatformConfig.STAGING_ROOT, "/data/warehouse/staging");

            assertThatThrownBy(() -> PlatformConfig.from(config))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining("could delete published output");
        }

        /** The inverse is worse: cleanup would take the whole warehouse. */
        @Test
        @DisplayName("the data root inside staging is rejected")
        void dataRootInsideStagingIsRejected() {
            Map<String, String> config = valid();
            config.put(PlatformConfig.STAGING_ROOT, "/data");
            config.put(PlatformConfig.DATA_ROOT, "/data/warehouse");

            assertThatThrownBy(() -> PlatformConfig.from(config))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining("would delete the warehouse");
        }

        /** Traversal must be resolved before comparison, or the check is trivially bypassed. */
        @Test
        @DisplayName("path traversal cannot disguise nesting")
        void traversalIsNormalizedBeforeComparison() {
            Map<String, String> config = validWith(
                    PlatformConfig.STAGING_ROOT, "/data/warehouse/../warehouse/staging");

            assertThatThrownBy(() -> PlatformConfig.from(config))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining("could delete published output");
        }

        @Test
        void siblingRootsAreAccepted() {
            assertThat(PlatformConfig.from(valid()).stagingRoot().toString())
                    .endsWith("/data/staging");
        }
    }

    @Nested
    @DisplayName("secret handling (§50)")
    class Secrets {

        @Test
        @DisplayName("redacted output masks every secret-shaped key")
        void secretsAreMasked() {
            Map<String, String> redacted = PlatformConfig.from(valid()).redacted();

            assertThat(redacted.get(PlatformConfig.POSTGRES_PASSWORD)).isEqualTo("***");
            assertThat(redacted.get(PlatformConfig.CLICKHOUSE_PASSWORD)).isEqualTo("***");
            assertThat(redacted.get(PlatformConfig.POSTGRES_USER)).isEqualTo("platform");
        }

        /** toString is what ends up in a log line by accident, so it must be safe by default. */
        @Test
        @DisplayName("toString never exposes a password")
        void toStringIsSafe() {
            assertThat(PlatformConfig.from(valid()).toString())
                    .doesNotContain("s3cret")
                    .contains("***");
        }

        @Test
        @DisplayName("accessors still return the real value")
        void accessorsReturnRealValues() {
            assertThat(PlatformConfig.from(valid()).postgresPassword()).isEqualTo("s3cret");
        }
    }
}
