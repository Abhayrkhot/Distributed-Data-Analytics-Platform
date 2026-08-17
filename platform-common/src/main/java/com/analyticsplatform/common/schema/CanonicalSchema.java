package com.analyticsplatform.common.schema;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Deterministic canonical form for Spark schemas, and the SHA-256 taken over it.
 *
 * <p>The registry compares schemas by hash, so two schemas that are <em>logically</em> identical
 * must produce identical bytes. Hashing Spark's own representation does not achieve that: field
 * order is preserved in {@link StructType}, type spellings vary, and — the trap —
 * {@link DataType#simpleString()} on a nested struct embeds its child field names in
 * <em>declaration order</em>. Hashing that string would make {@code struct<a,b>} and
 * {@code struct<b,a>} hash differently despite being the same type. Nested types are therefore
 * canonicalized recursively rather than delegated to {@code simpleString()}.
 *
 * <p>Canonical form, applied recursively:
 * <ol>
 *   <li>field name lowercased ({@link Locale#ROOT}, so a Turkish locale cannot change the result)
 *   <li>canonical type spelling — {@code long}, not Spark's {@code bigint}
 *   <li>nullable flag
 *   <li>fields sorted by canonical name
 *   <li>joined as {@code name:type:nullable}, one field per line
 * </ol>
 *
 * <pre>
 * airport_fee:double:true
 * cbd_congestion_fee:decimal(10,2):true
 * fare_amount:double:true
 * passenger_count:long:true
 * </pre>
 */
public final class CanonicalSchema {

    private static final String FIELD_SEPARATOR = ":";
    private static final String LINE_SEPARATOR = "\n";

    private CanonicalSchema() {
    }

    /**
     * Renders a schema in canonical form.
     *
     * @throws IllegalArgumentException if two fields collide once lowercased. Spark permits
     *         {@code Fare} and {@code fare} side by side; canonicalization would silently merge
     *         them, so this fails closed rather than producing a hash that hides a real ambiguity.
     */
    public static String canonicalize(StructType schema) {
        return canonicalFields(schema).stream().collect(Collectors.joining(LINE_SEPARATOR));
    }

    /** SHA-256 over the canonical form, lowercase hex. */
    public static String hash(StructType schema) {
        return sha256Hex(canonicalize(schema));
    }

    /** Canonical names of a schema's top-level fields, sorted. Used for schema diffing. */
    public static List<String> fieldNames(StructType schema) {
        List<String> names = new ArrayList<>();
        for (StructField field : schema.fields()) {
            names.add(canonicalName(field.name()));
        }
        names.sort(Comparator.naturalOrder());
        return names;
    }

    /** Lowercases a field name deterministically. */
    public static String canonicalName(String rawName) {
        return rawName.toLowerCase(Locale.ROOT);
    }

    /**
     * Canonical spelling of a type. Normalizes Spark's aliases (it renders {@code LongType} as
     * {@code bigint} and {@code IntegerType} as {@code int}) onto one consistent vocabulary, so
     * the widening rules in {@link SchemaCompatibility} can match on a single name per type.
     */
    public static String canonicalType(DataType type) {
        if (type instanceof DecimalType decimal) {
            // Precision and scale are part of the type's identity: decimal(10,2) and
            // decimal(12,2) are genuinely different and must not collapse together.
            return "decimal(" + decimal.precision() + "," + decimal.scale() + ")";
        }
        if (type instanceof StructType struct) {
            return "struct<" + String.join(",", canonicalFields(struct)) + ">";
        }
        if (type instanceof ArrayType array) {
            return "array<" + canonicalType(array.elementType()) + ","
                    + array.containsNull() + ">";
        }
        if (type instanceof MapType map) {
            return "map<" + canonicalType(map.keyType()) + ","
                    + canonicalType(map.valueType()) + ","
                    + map.valueContainsNull() + ">";
        }
        return switch (type.typeName()) {
            case "byte", "tinyint" -> "byte";
            case "short", "smallint" -> "short";
            case "integer", "int" -> "int";
            case "long", "bigint" -> "long";
            case "float", "real" -> "float";
            case "double" -> "double";
            case "boolean" -> "boolean";
            case "string" -> "string";
            case "binary" -> "binary";
            case "date" -> "date";
            case "timestamp" -> "timestamp";
            case "timestamp_ntz" -> "timestamp_ntz";
            default -> type.typeName();
        };
    }

    /** Sorted {@code name:type:nullable} entries for a struct's fields. */
    private static List<String> canonicalFields(StructType schema) {
        Set<String> seen = new HashSet<>();
        List<String> entries = new ArrayList<>(schema.fields().length);

        for (StructField field : schema.fields()) {
            String name = canonicalName(field.name());
            if (!seen.add(name)) {
                throw new IllegalArgumentException(
                        "duplicate field name after canonicalization: '" + name
                                + "'. Spark allows case-distinct siblings, but they cannot be "
                                + "represented unambiguously in canonical form.");
            }
            entries.add(name + FIELD_SEPARATOR
                    + canonicalType(field.dataType()) + FIELD_SEPARATOR
                    + field.nullable());
        }

        entries.sort(Comparator.naturalOrder());
        return entries;
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
            // SHA-256 is required of every JVM; absence means a broken platform, not a
            // condition callers could sensibly recover from.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
