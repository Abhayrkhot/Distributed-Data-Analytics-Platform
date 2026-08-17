package com.analyticsplatform.common.key;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Locale;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Assume;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Label;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/**
 * Generated coverage for {@link TripKey} (§17).
 *
 * <p>Generators lean deliberately on the characters the serializer uses as structure — {@code ;},
 * {@code =}, {@code :}, digits — because those are the inputs that could shift a field boundary,
 * and uniform random text would essentially never produce them.
 */
@Label("TripKey")
class TripKeyProperties {

    /** Text containing the serializer's own delimiters, plus ordinary values and nulls. */
    @Provide
    Arbitrary<String> hostileText() {
        Arbitrary<String> delimiterish = Arbitraries.strings()
                .withChars(';', '=', ':', '-', '1', 'a', ' ')
                .ofMinLength(1).ofMaxLength(8);
        Arbitrary<String> ordinary = Arbitraries.strings()
                .withCharRange('a', 'z').ofMinLength(1).ofMaxLength(6);
        Arbitrary<String> sentinelLookalikes = Arbitraries.of(
                "-1:", "<null>", "null", "-1", "0:", "=", ";");
        return Arbitraries.oneOf(delimiterish, ordinary, sentinelLookalikes);
    }

    @Provide
    Arbitrary<String> nullableText() {
        return Arbitraries.oneOf(hostileText(), Arbitraries.just(null));
    }

    @Provide
    Arbitrary<BigDecimal> amounts() {
        return Combinators.combine(
                        Arbitraries.longs().between(-100_000, 1_000_000),
                        Arbitraries.integers().between(0, 4))
                .as((unscaled, scale) -> BigDecimal.valueOf(unscaled, scale));
    }

    @Provide
    Arbitrary<Instant> instants() {
        return Arbitraries.longs()
                .between(1_500_000_000_000L, 1_800_000_000_000L)
                .map(Instant::ofEpochMilli);
    }

    @Provide
    Arbitrary<Integer> locationIds() {
        return Arbitraries.integers().between(1, 265);
    }

    // ------------------------------------------------------------------ determinism

    @Property
    void sameInputAlwaysProducesSameKey(
            @ForAll("nullableText") String source,
            @ForAll("nullableText") String vendor,
            @ForAll("instants") Instant pickup,
            @ForAll("instants") Instant dropoff,
            @ForAll("locationIds") Integer pickupLoc,
            @ForAll("locationIds") Integer dropoffLoc,
            @ForAll("amounts") BigDecimal distance,
            @ForAll("amounts") BigDecimal total) {

        String first = TripKey.of(source, vendor, pickup, dropoff, pickupLoc, dropoffLoc, distance, total);
        String second = TripKey.of(source, vendor, pickup, dropoff, pickupLoc, dropoffLoc, distance, total);

        assertThat(second).isEqualTo(first);
        assertThat(first).matches("[0-9a-f]{64}");
    }

    // -------------------------------------------------------------- canonicalization

    /**
     * Trailing zeros must not split one trip into several keys. This is the property that makes
     * dedup work at all: the same trip written as 12.5 and 12.50 has to collapse.
     */
    @Property
    void trailingZerosNeverChangeTheKey(
            @ForAll("amounts") BigDecimal distance,
            @ForAll("amounts") BigDecimal total,
            @ForAll("locationIds") Integer loc) {

        Instant pickup = Instant.ofEpochMilli(1_700_000_000_000L);
        Instant dropoff = pickup.plusSeconds(600);

        String plain = TripKey.of("yellow", "1", pickup, dropoff, loc, loc, distance, total);
        String padded = TripKey.of("yellow", "1", pickup, dropoff, loc, loc,
                distance.setScale(distance.scale() + 3, java.math.RoundingMode.UNNECESSARY),
                total.setScale(total.scale() + 3, java.math.RoundingMode.UNNECESSARY));

        assertThat(padded).isEqualTo(plain);
    }

    /** Case and surrounding whitespace are not identity. */
    @Property
    void caseAndWhitespaceAreNormalized(@ForAll("hostileText") String source) {
        Assume.that(!source.isBlank());

        Instant pickup = Instant.ofEpochMilli(1_700_000_000_000L);

        assertThat(TripKey.of("  " + source.toUpperCase(Locale.ROOT) + " ", "1",
                        pickup, pickup, 1, 2, BigDecimal.ONE, BigDecimal.TEN))
                .isEqualTo(TripKey.of(source.toLowerCase(Locale.ROOT), "1",
                        pickup, pickup, 1, 2, BigDecimal.ONE, BigDecimal.TEN));
    }

    // ------------------------------------------------------------------ identity

    /**
     * The aliasing property length-prefixing exists for. Two field pairs that differ after
     * canonicalization must never serialize to the same material, however the delimiters fall.
     */
    @Property
    void distinctFieldsNeverAliasAcrossBoundaries(
            @ForAll("hostileText") String sourceA, @ForAll("hostileText") String vendorA,
            @ForAll("hostileText") String sourceB, @ForAll("hostileText") String vendorB) {

        Instant pickup = Instant.ofEpochMilli(1_700_000_000_000L);

        // Predict canonicalization independently of the implementation: blank collapses to null,
        // otherwise trim and lowercase.
        Assume.that(!canonical(sourceA).equals(canonical(sourceB))
                || !canonical(vendorA).equals(canonical(vendorB)));

        String left = TripKey.canonicalKeyMaterial(sourceA, vendorA, pickup, pickup,
                1, 2, BigDecimal.ONE, BigDecimal.TEN);
        String right = TripKey.canonicalKeyMaterial(sourceB, vendorB, pickup, pickup,
                1, 2, BigDecimal.ONE, BigDecimal.TEN);

        assertThat(left).isNotEqualTo(right);
    }

    /** Equal keys must imply equal material: digests alone cannot distinguish match from collision. */
    @Property
    void equalKeysImplyEqualMaterial(
            @ForAll("hostileText") String sourceA, @ForAll("hostileText") String sourceB,
            @ForAll("amounts") BigDecimal amount) {

        Instant pickup = Instant.ofEpochMilli(1_700_000_000_000L);

        String keyA = TripKey.of(sourceA, "1", pickup, pickup, 1, 2, BigDecimal.ONE, amount);
        String keyB = TripKey.of(sourceB, "1", pickup, pickup, 1, 2, BigDecimal.ONE, amount);

        if (keyA.equals(keyB)) {
            assertThat(TripKey.canonicalKeyMaterial(sourceA, "1", pickup, pickup, 1, 2,
                    BigDecimal.ONE, amount))
                    .isEqualTo(TripKey.canonicalKeyMaterial(sourceB, "1", pickup, pickup, 1, 2,
                            BigDecimal.ONE, amount));
        }
    }

    @Property
    void changingPickupLocationChangesTheKey(
            @ForAll("locationIds") Integer a, @ForAll("locationIds") Integer b) {

        Assume.that(!a.equals(b));
        Instant pickup = Instant.ofEpochMilli(1_700_000_000_000L);

        assertThat(TripKey.of("yellow", "1", pickup, pickup, a, 99, BigDecimal.ONE, BigDecimal.TEN))
                .isNotEqualTo(
                TripKey.of("yellow", "1", pickup, pickup, b, 99, BigDecimal.ONE, BigDecimal.TEN));
    }

    @Property
    void changingPickupTimeChangesTheKey(
            @ForAll("instants") Instant a, @ForAll("instants") Instant b) {

        // Equal to the millisecond is the same trip; the format keeps millisecond precision.
        Assume.that(a.toEpochMilli() != b.toEpochMilli());

        assertThat(TripKey.of("yellow", "1", a, a, 1, 2, BigDecimal.ONE, BigDecimal.TEN))
                .isNotEqualTo(
                TripKey.of("yellow", "1", b, b, 1, 2, BigDecimal.ONE, BigDecimal.TEN));
    }

    // ------------------------------------------------------------------ nulls

    /**
     * A null field must stay distinct from any text, including text that mimics the {@code -1:}
     * null encoding.
     */
    @Property
    void nullIsNeverForgeableByText(@ForAll("hostileText") String vendor) {
        Assume.that(!vendor.isBlank());
        Instant pickup = Instant.ofEpochMilli(1_700_000_000_000L);

        String withNull = TripKey.of("yellow", null, pickup, pickup, 1, 2,
                BigDecimal.ONE, BigDecimal.TEN);
        String withText = TripKey.of("yellow", vendor, pickup, pickup, 1, 2,
                BigDecimal.ONE, BigDecimal.TEN);

        assertThat(withText).isNotEqualTo(withNull);
    }

    /** Every field may be null at once without the key computation failing. */
    @Property
    void anyCombinationOfNullsStillProducesAKey(
            @ForAll("nullableText") String source, @ForAll("nullableText") String vendor) {

        assertThat(TripKey.of(source, vendor, null, null, null, null, null, null))
                .matches("[0-9a-f]{64}");
    }

    private static String canonical(String raw) {
        if (raw == null) {
            return " NULL";
        }
        String trimmed = raw.trim();
        return trimmed.isEmpty() ? " NULL" : trimmed.toLowerCase(Locale.ROOT);
    }
}
