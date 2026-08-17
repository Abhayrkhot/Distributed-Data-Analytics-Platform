package com.analyticsplatform.common.dao;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Unit coverage for the parts of {@link JdbcControlPlane} that need no database.
 *
 * <p>The JSON encoder is hand-rolled to avoid a Jackson version clash with Spark's own copy, which
 * makes it exactly the kind of code that needs escaping tests — a missed case would corrupt the
 * {@code config_json} column or fail the {@code ::jsonb} cast at insert time.
 */
class JdbcControlPlaneTest {

    @Test
    @DisplayName("an empty or null map encodes as an empty object")
    void emptyMapIsEmptyObject() {
        assertThat(JdbcControlPlane.toJson(Map.of())).isEqualTo("{}");
        assertThat(JdbcControlPlane.toJson(null)).isEqualTo("{}");
    }

    @Test
    @DisplayName("entries encode in iteration order")
    void entriesAreEncoded() {
        Map<String, String> config = new LinkedHashMap<>();
        config.put("shuffle_partitions", "12");
        config.put("compression", "zstd");

        assertThat(JdbcControlPlane.toJson(config))
                .isEqualTo("{\"shuffle_partitions\":\"12\",\"compression\":\"zstd\"}");
    }

    /** Any of these left unescaped produces invalid JSON and a failed insert. */
    @ParameterizedTest(name = "{0} is escaped")
    @CsvSource(delimiter = '|', value = {
        "quote        | a\"b   | {\"k\":\"a\\\"b\"}",
        "backslash    | a\\b   | {\"k\":\"a\\\\b\"}",
        "tab          | a\tb   | {\"k\":\"a\\tb\"}",
    })
    void specialCharactersAreEscaped(String label, String value, String expected) {
        assertThat(JdbcControlPlane.toJson(Map.of("k", value))).isEqualTo(expected);
    }

    @Test
    @DisplayName("newlines and carriage returns are escaped")
    void newlinesAreEscaped() {
        assertThat(JdbcControlPlane.toJson(Map.of("k", "a\nb")))
                .isEqualTo("{\"k\":\"a\\nb\"}");
        assertThat(JdbcControlPlane.toJson(Map.of("k", "a\rb")))
                .isEqualTo("{\"k\":\"a\\rb\"}");
    }

    /** Raw control characters are not legal in JSON strings and must be unicode-escaped. */
    @Test
    @DisplayName("control characters become unicode escapes")
    void controlCharactersAreEscaped() {
        assertThat(JdbcControlPlane.toJson(Map.of("k", "ab")))
                .isEqualTo("{\"k\":\"a\\u0001b\"}");
    }

    @Test
    @DisplayName("a key containing a quote is escaped too, not just the value")
    void keysAreEscapedAsWell() {
        assertThat(JdbcControlPlane.toJson(Map.of("a\"b", "v")))
                .isEqualTo("{\"a\\\"b\":\"v\"}");
    }

    @Test
    @DisplayName("non-ASCII text passes through unchanged")
    void unicodePassesThrough() {
        assertThat(JdbcControlPlane.toJson(Map.of("zone", "Åland")))
                .isEqualTo("{\"zone\":\"Åland\"}");
    }
}
