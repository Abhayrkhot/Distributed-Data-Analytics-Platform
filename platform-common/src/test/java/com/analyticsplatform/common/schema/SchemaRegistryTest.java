package com.analyticsplatform.common.schema;

import static com.analyticsplatform.common.schema.Schemas.f;
import static com.analyticsplatform.common.schema.Schemas.of;
import static com.analyticsplatform.common.schema.Schemas.type;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.schema.SchemaCompatibility.ChangeType;
import com.analyticsplatform.common.schema.SchemaRegistry.Policy;
import com.analyticsplatform.common.schema.SchemaRegistry.SchemaEvolutionException;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The parts of {@link SchemaRegistry} that need no database: policy parsing, the failure path when
 * the control plane is unreachable, and the exception's diff accessor.
 *
 * <p>Behaviour that genuinely requires Postgres lives in {@code SchemaRegistryIT}.
 */
class SchemaRegistryTest {

    @Nested
    @DisplayName("policy parsing")
    class PolicyParsing {

        @ParameterizedTest(name = "''{0}'' parses to {1}")
        @CsvSource({
            "strict, STRICT",
            "STRICT, STRICT",
            "Strict, STRICT",
            "allow_widening, ALLOW_WIDENING",
            "ALLOW_WIDENING, ALLOW_WIDENING",
        })
        void knownPoliciesParse(String value, Policy expected) {
            assertThat(Policy.fromConfig(value)).isEqualTo(expected);
        }

        /**
         * An unrecognized policy must fail loudly. Defaulting to the permissive option would mean
         * a typo silently disables schema enforcement.
         */
        @ParameterizedTest(name = "''{0}'' is rejected")
        @ValueSource(strings = {"lenient", "none", "allow", "", "allow-widening"})
        void unknownPolicyIsRejected(String value) {
            assertThatThrownBy(() -> Policy.fromConfig(value))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unknown schema policy");
        }
    }

    @Nested
    @DisplayName("control-plane failures")
    class ControlPlaneFailures {

        /** Connections that always fail, standing in for an unreachable database. */
        private final ConnectionSource unreachable = () -> {
            throw new SQLException("connection refused");
        };

        @Test
        @DisplayName("register surfaces a connection failure rather than silently succeeding")
        void registerFailsWhenUnreachable() {
            SchemaRegistry registry = new SchemaRegistry(unreachable, Policy.ALLOW_WIDENING);

            assertThatThrownBy(() -> registry.register("ds", of(f("a", type("long")))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("failed to register schema for ds")
                    .hasRootCauseMessage("connection refused");
        }

        @Test
        @DisplayName("latest surfaces a connection failure")
        void latestFailsWhenUnreachable() {
            SchemaRegistry registry = new SchemaRegistry(unreachable, Policy.ALLOW_WIDENING);

            assertThatThrownBy(() -> registry.latest("ds"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("failed to read schema for ds");
        }
    }

    @Nested
    @DisplayName("evolution exception")
    class EvolutionException {

        /** The diff travels with the exception so the message can name what actually broke. */
        @Test
        @DisplayName("carries the diff that caused the rejection")
        void exceptionCarriesTheDiff() {
            SchemaCompatibility.SchemaDiff diff = SchemaCompatibility.classify(
                    of(f("a", type("long"))), of(f("a", type("int"))));

            SchemaEvolutionException thrown = new SchemaEvolutionException("refused", diff);

            assertThat(thrown.diff()).isSameAs(diff);
            assertThat(thrown.diff().changeType()).isEqualTo(ChangeType.BREAKING);
            assertThat(thrown.diff().breakingReasons()).isNotEmpty();
        }

        @Test
        @DisplayName("diff collections are defensively copied")
        void diffIsImmutable() {
            SchemaCompatibility.SchemaDiff diff = new SchemaCompatibility.SchemaDiff(
                    ChangeType.ADDITIVE, List.of("x"), List.of(), List.of(), List.of());

            assertThatThrownBy(() -> diff.addedColumns().add("y"))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
