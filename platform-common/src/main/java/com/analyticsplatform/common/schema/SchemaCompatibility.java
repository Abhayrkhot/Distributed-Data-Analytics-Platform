package com.analyticsplatform.common.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Classifies a schema transition as initial, additive, widening, or breaking.
 *
 * <p>The policy this feeds: {@code ADDITIVE} and {@code WIDENING} register a new schema version
 * and let the run proceed; {@code BREAKING} aborts before anything is published.
 *
 * <p><strong>Fail-closed by construction.</strong> Only the transitions explicitly listed as
 * widening are treated as safe; everything else that is not an exact match is breaking. So
 * {@code int -> double} is classified breaking even though it feels like a widening — it is lossy
 * above 2^53, and the cost of wrongly allowing a lossy change (silent data corruption) is far
 * worse than the cost of wrongly rejecting a safe one (an explicit failure a human resolves).
 *
 * <p><strong>Any breaking component makes the whole transition breaking.</strong> A schema that
 * adds a column and narrows another is breaking, not additive — severity combines by taking the
 * maximum, never by taking the most recent or most common change.
 */
public final class SchemaCompatibility {

    /** Ordered by severity: a later constant dominates an earlier one when combining. */
    public enum ChangeType {
        INITIAL,
        ADDITIVE,
        WIDENING,
        BREAKING;

        ChangeType max(ChangeType other) {
            return this.ordinal() >= other.ordinal() ? this : other;
        }
    }

    /** The outcome of comparing two schemas. */
    public record SchemaDiff(
            ChangeType changeType,
            List<String> addedColumns,
            List<String> removedColumns,
            List<String> retypedColumns,
            List<String> breakingReasons) {

        public SchemaDiff {
            addedColumns = List.copyOf(addedColumns);
            removedColumns = List.copyOf(removedColumns);
            retypedColumns = List.copyOf(retypedColumns);
            breakingReasons = List.copyOf(breakingReasons);
        }

        /** True when the transition may proceed to publication. */
        public boolean isCompatible() {
            return changeType != ChangeType.BREAKING;
        }
    }

    private SchemaCompatibility() {
    }

    /**
     * Classifies the move from {@code previous} to {@code current}.
     *
     * @param previous the last registered schema, or {@code null} when nothing is registered yet
     */
    public static SchemaDiff classify(StructType previous, StructType current) {
        if (current == null) {
            throw new IllegalArgumentException("current schema must not be null");
        }
        if (previous == null) {
            return new SchemaDiff(ChangeType.INITIAL, List.of(), List.of(), List.of(), List.of());
        }

        Map<String, StructField> before = byCanonicalName(previous);
        Map<String, StructField> after = byCanonicalName(current);

        List<String> added = new ArrayList<>();
        List<String> removed = new ArrayList<>();
        List<String> retyped = new ArrayList<>();
        List<String> reasons = new ArrayList<>();
        ChangeType result = ChangeType.ADDITIVE;

        for (Map.Entry<String, StructField> entry : before.entrySet()) {
            String name = entry.getKey();
            StructField previousField = entry.getValue();
            StructField currentField = after.get(name);

            if (currentField == null) {
                // Dropping a column breaks every consumer that reads it.
                removed.add(name);
                reasons.add("column removed: " + name);
                result = result.max(ChangeType.BREAKING);
                continue;
            }

            ChangeType typeChange = compareTypes(
                    previousField.dataType(), currentField.dataType(), name, reasons);
            if (typeChange != ChangeType.ADDITIVE) {
                retyped.add(name + " (" + CanonicalSchema.canonicalType(previousField.dataType())
                        + " -> " + CanonicalSchema.canonicalType(currentField.dataType()) + ")");
            }
            result = result.max(typeChange);

            // Tightening nullability invalidates rows already written as null.
            if (previousField.nullable() && !currentField.nullable()) {
                reasons.add("column became required: " + name);
                result = result.max(ChangeType.BREAKING);
            } else if (!previousField.nullable() && currentField.nullable()) {
                result = result.max(ChangeType.WIDENING);
            }
        }

        for (Map.Entry<String, StructField> entry : after.entrySet()) {
            if (before.containsKey(entry.getKey())) {
                continue;
            }
            added.add(entry.getKey());
            // A new nullable column is safe: existing rows read it as null. A new required one
            // is not, because there is no value those rows could possibly have.
            if (!entry.getValue().nullable()) {
                reasons.add("new column is not nullable: " + entry.getKey());
                result = result.max(ChangeType.BREAKING);
            }
        }

        Collections.sort(added);
        Collections.sort(removed);
        Collections.sort(retyped);

        return new SchemaDiff(result, added, removed, retyped, reasons);
    }

    /**
     * Compares two types.
     *
     * @return {@code ADDITIVE} when identical (i.e. contributes no severity), {@code WIDENING}
     *         when safely widened, {@code BREAKING} otherwise
     */
    private static ChangeType compareTypes(
            DataType from, DataType to, String path, List<String> reasons) {

        if (CanonicalSchema.canonicalType(from).equals(CanonicalSchema.canonicalType(to))) {
            return ChangeType.ADDITIVE;
        }

        if (from instanceof StructType a && to instanceof StructType b) {
            // Nested structs recurse through the full rule set, so a breaking change buried
            // inside a struct surfaces as breaking at the top level.
            SchemaDiff nested = classify(a, b);
            nested.breakingReasons().forEach(r -> reasons.add(path + "." + r));
            return nested.changeType() == ChangeType.INITIAL ? ChangeType.ADDITIVE : nested.changeType();
        }

        if (from instanceof ArrayType a && to instanceof ArrayType b) {
            ChangeType element = compareTypes(a.elementType(), b.elementType(), path + "[]", reasons);
            ChangeType nullability = compareContainsNull(
                    a.containsNull(), b.containsNull(), path + "[] elements", reasons);
            return element.max(nullability);
        }

        if (from instanceof MapType a && to instanceof MapType b) {
            // Key types must match exactly: widening a key changes which entries collide.
            if (!CanonicalSchema.canonicalType(a.keyType())
                    .equals(CanonicalSchema.canonicalType(b.keyType()))) {
                reasons.add("map key type changed at " + path);
                return ChangeType.BREAKING;
            }
            ChangeType value = compareTypes(a.valueType(), b.valueType(), path + "{}", reasons);
            ChangeType nullability = compareContainsNull(
                    a.valueContainsNull(), b.valueContainsNull(), path + "{} values", reasons);
            return value.max(nullability);
        }

        if (from instanceof DecimalType a && to instanceof DecimalType b) {
            // Safe only if neither the fractional digits nor the integral digits shrink.
            boolean scaleKept = b.scale() >= a.scale();
            boolean integralDigitsKept = (b.precision() - b.scale()) >= (a.precision() - a.scale());
            if (scaleKept && integralDigitsKept) {
                return ChangeType.WIDENING;
            }
            reasons.add("decimal narrowed at " + path + ": "
                    + CanonicalSchema.canonicalType(from) + " -> "
                    + CanonicalSchema.canonicalType(to));
            return ChangeType.BREAKING;
        }

        if (isNumericWidening(CanonicalSchema.canonicalType(from), CanonicalSchema.canonicalType(to))) {
            return ChangeType.WIDENING;
        }

        reasons.add("incompatible type change at " + path + ": "
                + CanonicalSchema.canonicalType(from) + " -> "
                + CanonicalSchema.canonicalType(to));
        return ChangeType.BREAKING;
    }

    private static ChangeType compareContainsNull(
            boolean from, boolean to, String path, List<String> reasons) {
        if (from == to) {
            return ChangeType.ADDITIVE;
        }
        if (!from) {
            return ChangeType.WIDENING;   // false -> true, now permits nulls
        }
        reasons.add("no longer nullable at " + path);
        return ChangeType.BREAKING;       // true -> false, existing nulls become invalid
    }

    /**
     * The explicit widening ladder. Integral promotions only: {@code int -> double} is absent
     * deliberately, because doubles cannot represent every long exactly.
     */
    private static boolean isNumericWidening(String from, String to) {
        List<String> integralLadder = List.of("byte", "short", "int", "long");
        int fromRank = integralLadder.indexOf(from);
        int toRank = integralLadder.indexOf(to);
        if (fromRank >= 0 && toRank >= 0) {
            return toRank > fromRank;
        }
        return "float".equals(from) && "double".equals(to);
    }

    /** Indexes fields by canonical name, preserving declaration order for stable messages. */
    private static Map<String, StructField> byCanonicalName(StructType schema) {
        Map<String, StructField> map = new LinkedHashMap<>();
        for (StructField field : schema.fields()) {
            String name = CanonicalSchema.canonicalName(field.name());
            if (map.put(name, field) != null) {
                throw new IllegalArgumentException(
                        "duplicate field name after canonicalization: '" + name + "'");
            }
        }
        return map;
    }
}
