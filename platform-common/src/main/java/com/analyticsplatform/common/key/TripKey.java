package com.analyticsplatform.common.key;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Derived deduplication key for a trip.
 *
 * <p><strong>This is not a source primary key.</strong> NYC TLC trip records carry no globally
 * unique trip identifier, so the key is computed from the fields that together identify a trip in
 * practice. Two source records producing the same key are <em>source duplicates</em> — the same
 * trip reported twice — which is exactly what silver dedup is meant to collapse.
 *
 * <p>The measurable quantity is therefore the <strong>duplicate-key rate</strong>, not a "collision
 * rate": a SHA-256 collision (distinct canonical inputs, identical digest) is not observable at
 * this scale and is not claimed anywhere.
 *
 * <h2>Canonicalize, then hash</h2>
 *
 * <p>Equivalent representations of the same value must produce one key. {@code 12.5}, {@code 12.50}
 * and {@code 12.500000} are the same amount, and hashing their raw text would split one trip into
 * three. So every field is normalized to a fixed representation first:
 *
 * <ul>
 *   <li>source — lowercased and trimmed
 *   <li>vendor and location ids — canonical integers, no padding
 *   <li>timestamps — UTC at fixed millisecond precision
 *   <li>distance — fixed scale 3; amounts — fixed scale 2
 *   <li>null — an explicit sentinel, never an empty string
 * </ul>
 *
 * <h2>Unambiguous serialization</h2>
 *
 * <p>Fields are length-prefixed rather than joined by a delimiter. Naive concatenation lets
 * distinct inputs alias: {@code ("a", "b|c")} and {@code ("a|b", "c")} both render as
 * {@code a|b|c}. A length prefix makes every boundary explicit, and a length of {@code -1} encodes
 * null — a value no real string can produce, so a literal {@code "<null>"} stays distinct from an
 * actual null.
 */
public final class TripKey {

    /** Distance carries three decimal places in TLC data. */
    private static final int DISTANCE_SCALE = 3;

    /** Monetary amounts carry two. */
    private static final int AMOUNT_SCALE = 2;

    /** Fixed millisecond precision in UTC, so equivalent instants render identically. */
    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private TripKey() {
    }

    /**
     * Computes the trip key: SHA-256 over the canonical key material, lowercase hex.
     */
    public static String of(
            String source,
            String vendorId,
            Instant pickupTs,
            Instant dropoffTs,
            Integer pickupLocationId,
            Integer dropoffLocationId,
            BigDecimal tripDistance,
            BigDecimal totalAmount) {

        return sha256Hex(canonicalKeyMaterial(
                source, vendorId, pickupTs, dropoffTs,
                pickupLocationId, dropoffLocationId, tripDistance, totalAmount));
    }

    /**
     * The exact bytes that get hashed.
     *
     * <p>Exposed so tests can assert the stronger property that equal keys imply equal key
     * material — comparing digests alone could not distinguish a genuine match from a collision.
     */
    public static String canonicalKeyMaterial(
            String source,
            String vendorId,
            Instant pickupTs,
            Instant dropoffTs,
            Integer pickupLocationId,
            Integer dropoffLocationId,
            BigDecimal tripDistance,
            BigDecimal totalAmount) {

        StringBuilder material = new StringBuilder(160);
        appendField(material, "source", canonicalText(source));
        appendField(material, "vendor_id", canonicalText(vendorId));
        appendField(material, "pickup_ts", canonicalTimestamp(pickupTs));
        appendField(material, "dropoff_ts", canonicalTimestamp(dropoffTs));
        appendField(material, "pickup_location_id", canonicalInteger(pickupLocationId));
        appendField(material, "dropoff_location_id", canonicalInteger(dropoffLocationId));
        appendField(material, "trip_distance", canonicalDecimal(tripDistance, DISTANCE_SCALE));
        appendField(material, "total_amount", canonicalDecimal(totalAmount, AMOUNT_SCALE));
        return material.toString();
    }

    /**
     * Appends one length-prefixed field. A {@code null} value is encoded as length {@code -1},
     * which no real string can produce.
     */
    private static void appendField(StringBuilder out, String name, String value) {
        out.append(name).append('=');
        if (value == null) {
            out.append("-1:");
        } else {
            out.append(value.length()).append(':').append(value);
        }
        out.append(';');
    }

    private static String canonicalText(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        // An all-whitespace field is missing data, not a distinct value; treating it as null
        // keeps "" and "   " from producing two keys for one trip.
        return trimmed.isEmpty() ? null : trimmed.toLowerCase(Locale.ROOT);
    }

    private static String canonicalTimestamp(Instant instant) {
        return instant == null ? null : TIMESTAMP_FORMAT.format(instant);
    }

    private static String canonicalInteger(Integer value) {
        return value == null ? null : Integer.toString(value);
    }

    /**
     * Normalizes to a fixed scale so trailing-zero differences cannot split one trip into several.
     *
     * <p>{@link RoundingMode#HALF_UP} is specified explicitly: an unspecified rounding mode throws
     * on values needing rounding, which would turn a data-quality issue into a crash.
     */
    private static String canonicalDecimal(BigDecimal value, int scale) {
        if (value == null) {
            return null;
        }
        BigDecimal scaled = value.setScale(scale, RoundingMode.HALF_UP);
        // stripTrailingZeros would undo the fixed scale; negate -0 so it renders as 0.00.
        if (scaled.signum() == 0) {
            scaled = scaled.abs();
        }
        return scaled.toPlainString();
    }

    private static String sha256Hex(String input) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
