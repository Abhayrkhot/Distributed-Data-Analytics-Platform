package com.analyticsplatform.transform.dq;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The narrow params parser.
 *
 * <p>Tested directly because it is hand-rolled to avoid a Jackson dependency that clashes with the
 * copy Spark ships. Hand-rolled parsing earns its own tests — the failure mode is a silently
 * mis-parsed threshold, which would disable a rule while appearing to run it.
 */
class DqRuleStoreTest {

    /** Concrete element type so AssertJ's varargs resolve; List<?> yields capture-of-?. */
    @SuppressWarnings("unchecked")
    private static List<Object> listAt(Map<String, Object> params, String key) {
        return (List<Object>) params.get(key);
    }

    @Test
    @DisplayName("an empty or absent object yields no params")
    void emptyInput() {
        assertThat(DqRuleStore.parseParams(null)).isEmpty();
        assertThat(DqRuleStore.parseParams("")).isEmpty();
        assertThat(DqRuleStore.parseParams("  ")).isEmpty();
        assertThat(DqRuleStore.parseParams("{}")).isEmpty();
    }

    @Test
    @DisplayName("integers parse as longs")
    void integers() {
        Map<String, Object> params = DqRuleStore.parseParams("{\"min\": 0, \"max\": 500}");

        assertThat(params).containsEntry("min", 0L).containsEntry("max", 500L);
    }

    @Test
    @DisplayName("decimals parse as doubles")
    void decimals() {
        Map<String, Object> params = DqRuleStore.parseParams("{\"max_drop_pct\": 0.25}");

        assertThat(params).containsEntry("max_drop_pct", 0.25);
    }

    @Test
    @DisplayName("strings parse without their quotes")
    void strings() {
        Map<String, Object> params = DqRuleStore.parseParams(
                "{\"expression\": \"dropoff_ts > pickup_ts\"}");

        assertThat(params).containsEntry("expression", "dropoff_ts > pickup_ts");
    }

    /** The accepted_values rule depends on this. */
    @Test
    @DisplayName("string arrays parse as lists")
    void stringArrays() {
        Map<String, Object> params = DqRuleStore.parseParams(
                "{\"values\": [\"yellow\", \"green\"]}");

        assertThat(params.get("values")).isInstanceOf(List.class);
        assertThat(listAt(params, "values")).containsExactly("yellow", "green");
    }

    @Test
    @DisplayName("numeric arrays parse as lists")
    void numericArrays() {
        assertThat(listAt(DqRuleStore.parseParams("{\"values\": [1, 2, 3]}"), "values"))
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    @DisplayName("several keys of mixed type parse together")
    void mixedTypes() {
        Map<String, Object> params = DqRuleStore.parseParams(
                "{\"min\": 0, \"max\": 500.5, \"label\": \"fare\", \"values\": [1, 2]}");

        assertThat(params).hasSize(4)
                .containsEntry("min", 0L)
                .containsEntry("max", 500.5)
                .containsEntry("label", "fare");
        assertThat(listAt(params, "values")).containsExactly(1L, 2L);
    }

    /**
     * An expression containing a colon, brace or comma must not be mis-split. These are exactly the
     * characters the parser uses as structure, so this is where a hand-rolled parser breaks.
     */
    @Test
    @DisplayName("punctuation inside a string value does not break parsing")
    void punctuationInsideStrings() {
        Map<String, Object> params = DqRuleStore.parseParams(
                "{\"expression\": \"total >= fare - 0.01\", \"note\": \"a, b: c\"}");

        assertThat(params).containsEntry("expression", "total >= fare - 0.01")
                .containsEntry("note", "a, b: c");
    }

    @Test
    @DisplayName("booleans and nulls parse")
    void booleansAndNulls() {
        Map<String, Object> params = DqRuleStore.parseParams(
                "{\"strict\": true, \"loose\": false, \"absent\": null}");

        assertThat(params).containsEntry("strict", true).containsEntry("loose", false);
        assertThat(params.get("absent")).isNull();
    }

    @Test
    @DisplayName("negative numbers parse")
    void negativeNumbers() {
        assertThat(DqRuleStore.parseParams("{\"min\": -10, \"max\": -0.5}"))
                .containsEntry("min", -10L).containsEntry("max", -0.5);
    }

    /** Every params shape the seed actually stores must round-trip. */
    @Test
    @DisplayName("all seeded params shapes parse")
    void seededShapes() {
        assertThat(DqRuleStore.parseParams("{\"min\": 0, \"max\": 10000}")).hasSize(2);
        assertThat(DqRuleStore.parseParams("{\"values\": [\"yellow\", \"green\"]}")).hasSize(1);
        assertThat(DqRuleStore.parseParams(
                "{\"expression\": \"dropoff_ts > pickup_ts\"}")).hasSize(1);
        assertThat(DqRuleStore.parseParams(
                "{\"ref_dataset\": \"raw.taxi_zone_lookup\", \"ref_column\": \"LocationID\"}"))
                .hasSize(2);
        assertThat(DqRuleStore.parseParams("{\"max_drop_pct\": 0.25}")).hasSize(1);
        assertThat(DqRuleStore.parseParams("{\"max_age_days\": 3650}")).hasSize(1);
    }
}
