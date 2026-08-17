package com.analyticsplatform.common.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.analyticsplatform.common.schema.SchemaCompatibility.ChangeType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Assume;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Label;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Generated coverage for canonicalization and the compatibility matrix (§17).
 *
 * <p>The generator deliberately produces nested structs, arrays, maps, decimals and mixed
 * nullability rather than flat primitive schemas — the enumerated tests already cover the flat
 * cases, and the interesting failures live in recursion.
 */
@Label("Schema canonicalization and compatibility")
class SchemaProperties {

    private static final int MAX_DEPTH = 2;

    // ---------------------------------------------------------------- generators

    @Provide
    Arbitrary<StructType> schemas() {
        return structs(MAX_DEPTH);
    }

    /** Numeric-only schemas, so widening and narrowing always have somewhere to go. */
    @Provide
    Arbitrary<StructType> numericSchemas() {
        return fields(numericTypes()).map(SchemaProperties::toStruct);
    }

    private Arbitrary<StructType> structs(int depth) {
        return fields(types(depth)).map(SchemaProperties::toStruct);
    }

    /**
     * Field names are drawn from a lowercase alphabet and forced unique. Case-colliding siblings
     * are a separate, deliberately-rejected case covered by {@code CanonicalSchemaTest}; letting
     * the generator produce them here would just exercise that rejection over and over.
     */
    private Arbitrary<List<StructField>> fields(Arbitrary<DataType> typeArbitrary) {
        Arbitrary<List<String>> names = Arbitraries.strings()
                .withCharRange('a', 'h').ofMinLength(1).ofMaxLength(3)
                .list().ofMinSize(1).ofMaxSize(5).uniqueElements();

        return names.flatMap(nameList ->
                typeArbitrary.list().ofSize(nameList.size()).flatMap(typeList ->
                        Arbitraries.of(true, false).list().ofSize(nameList.size()).map(nullables -> {
                            List<StructField> out = new ArrayList<>(nameList.size());
                            for (int i = 0; i < nameList.size(); i++) {
                                out.add(new StructField(
                                        nameList.get(i), typeList.get(i),
                                        nullables.get(i), Metadata.empty()));
                            }
                            return out;
                        })));
    }

    private Arbitrary<DataType> numericTypes() {
        return Arbitraries.of(
                DataTypes.ByteType, DataTypes.ShortType, DataTypes.IntegerType,
                DataTypes.FloatType, DataTypes.createDecimalType(10, 2));
    }

    private Arbitrary<DataType> types(int depth) {
        Arbitrary<DataType> primitives = Arbitraries.of(
                DataTypes.ByteType, DataTypes.ShortType, DataTypes.IntegerType, DataTypes.LongType,
                DataTypes.FloatType, DataTypes.DoubleType, DataTypes.StringType,
                DataTypes.BooleanType, DataTypes.DateType, DataTypes.TimestampType,
                DataTypes.BinaryType);

        // precision = scale + extra keeps precision >= scale, which Spark requires.
        Arbitrary<DataType> decimals = Combinators.combine(
                        Arbitraries.integers().between(0, 6),
                        Arbitraries.integers().between(1, 12))
                .as((scale, extra) -> DataTypes.createDecimalType(scale + extra, scale));

        if (depth <= 0) {
            return Arbitraries.oneOf(primitives, decimals);
        }

        Arbitrary<DataType> arrays = Combinators.combine(
                        types(depth - 1), Arbitraries.of(true, false))
                .as(DataTypes::createArrayType);

        Arbitrary<DataType> maps = Combinators.combine(
                        Arbitraries.of(DataTypes.StringType, DataTypes.IntegerType),
                        types(depth - 1), Arbitraries.of(true, false))
                .as(DataTypes::createMapType);

        Arbitrary<DataType> nested = structs(depth - 1).map(s -> (DataType) s);

        return Arbitraries.oneOf(primitives, decimals, arrays, maps, nested);
    }

    private static StructType toStruct(List<StructField> fields) {
        return new StructType(fields.toArray(new StructField[0]));
    }

    // ------------------------------------------------------------- canonicalization

    @Property
    void reorderingFieldsNeverChangesTheCanonicalForm(@ForAll("schemas") StructType schema) {
        StructType reversed = reorder(schema);

        assertThat(CanonicalSchema.canonicalize(reversed))
                .isEqualTo(CanonicalSchema.canonicalize(schema));
    }

    @Property
    void reorderingFieldsNeverChangesTheHash(@ForAll("schemas") StructType schema) {
        assertThat(CanonicalSchema.hash(reorder(schema)))
                .isEqualTo(CanonicalSchema.hash(schema));
    }

    /** Canonicalizing an already-canonical schema must be a no-op. */
    @Property
    void canonicalizationIsIdempotent(@ForAll("schemas") StructType schema) {
        String once = CanonicalSchema.canonicalize(schema);

        assertThat(CanonicalSchema.canonicalize(schema)).isEqualTo(once);
        assertThat(CanonicalSchema.hash(schema)).isEqualTo(CanonicalSchema.hash(schema));
    }

    /** Distinct canonical forms must not share a hash. */
    @Property
    void distinctSchemasHashDifferently(
            @ForAll("schemas") StructType a, @ForAll("schemas") StructType b) {

        Assume.that(!CanonicalSchema.canonicalize(a).equals(CanonicalSchema.canonicalize(b)));

        assertThat(CanonicalSchema.hash(a)).isNotEqualTo(CanonicalSchema.hash(b));
    }

    @Property
    void identicalSchemasAreNeverBreaking(@ForAll("schemas") StructType schema) {
        assertThat(SchemaCompatibility.classify(schema, reorder(schema)).changeType())
                .isNotEqualTo(ChangeType.BREAKING);
    }

    // ------------------------------------------------------------- compatibility

    @Property
    void wideningIsNeverBreaking(@ForAll("numericSchemas") StructType schema) {
        StructType widened = mapTypes(schema, SchemaProperties::widen);

        assertThat(SchemaCompatibility.classify(schema, widened).changeType())
                .isNotEqualTo(ChangeType.BREAKING);
    }

    @Property
    void narrowingIsAlwaysBreaking(@ForAll("numericSchemas") StructType schema) {
        StructType narrowed = mapTypes(schema, SchemaProperties::narrow);

        // Some types have nowhere narrower to go; only assert where something moved.
        Assume.that(!CanonicalSchema.canonicalize(narrowed)
                .equals(CanonicalSchema.canonicalize(schema)));

        assertThat(SchemaCompatibility.classify(schema, narrowed).changeType())
                .isEqualTo(ChangeType.BREAKING);
    }

    /**
     * The severity-combining invariant under generation: adding a safe column alongside a
     * narrowed one must still resolve to breaking.
     */
    @Property
    void oneNarrowedFieldPoisonsAnyNumberOfSafeChanges(
            @ForAll("numericSchemas") StructType schema) {

        StructType narrowed = mapTypes(schema, SchemaProperties::narrow);
        Assume.that(!CanonicalSchema.canonicalize(narrowed)
                .equals(CanonicalSchema.canonicalize(schema)));

        List<StructField> withExtra = new ArrayList<>(List.of(narrowed.fields()));
        withExtra.add(new StructField("zzz_added", DataTypes.StringType, true, Metadata.empty()));

        assertThat(SchemaCompatibility.classify(schema, toStruct(withExtra)).changeType())
                .isEqualTo(ChangeType.BREAKING);
    }

    /** Dropping any field is breaking, whatever else the transition does. */
    @Property
    void removingAnyFieldIsBreaking(@ForAll("schemas") StructType schema) {
        Assume.that(schema.fields().length > 1);

        List<StructField> remaining = new ArrayList<>(List.of(schema.fields()));
        remaining.remove(0);

        assertThat(SchemaCompatibility.classify(schema, toStruct(remaining)).changeType())
                .isEqualTo(ChangeType.BREAKING);
    }

    /** Adding a nullable column is additive; adding a required one is breaking. */
    @Property
    void addedColumnNullabilityDecidesSeverity(
            @ForAll("schemas") StructType schema, @ForAll boolean nullable) {

        List<StructField> extended = new ArrayList<>(List.of(schema.fields()));
        extended.add(new StructField("zzz_added", DataTypes.StringType, nullable, Metadata.empty()));

        ChangeType result = SchemaCompatibility.classify(schema, toStruct(extended)).changeType();

        if (nullable) {
            assertThat(result).isNotEqualTo(ChangeType.BREAKING);
        } else {
            assertThat(result).isEqualTo(ChangeType.BREAKING);
        }
    }

    // ------------------------------------------------------------------ helpers

    private static StructType reorder(StructType schema) {
        List<StructField> fields = new ArrayList<>(List.of(schema.fields()));
        Collections.reverse(fields);
        return toStruct(fields);
    }

    private static StructType mapTypes(StructType schema, java.util.function.UnaryOperator<DataType> op) {
        List<StructField> out = new ArrayList<>(schema.fields().length);
        for (StructField field : schema.fields()) {
            out.add(new StructField(
                    field.name(), op.apply(field.dataType()), field.nullable(), Metadata.empty()));
        }
        return toStruct(out);
    }

    private static DataType widen(DataType type) {
        if (type instanceof DecimalType d) {
            return DataTypes.createDecimalType(Math.min(d.precision() + 1, 38), d.scale());
        }
        if (type instanceof ArrayType a) {
            return DataTypes.createArrayType(widen(a.elementType()), true);
        }
        if (type instanceof MapType m) {
            return DataTypes.createMapType(m.keyType(), widen(m.valueType()), true);
        }
        if (type instanceof StructType s) {
            return mapTypes(s, SchemaProperties::widen);
        }
        if (DataTypes.ByteType.equals(type)) return DataTypes.ShortType;
        if (DataTypes.ShortType.equals(type)) return DataTypes.IntegerType;
        if (DataTypes.IntegerType.equals(type)) return DataTypes.LongType;
        if (DataTypes.FloatType.equals(type)) return DataTypes.DoubleType;
        return type;
    }

    private static DataType narrow(DataType type) {
        if (type instanceof DecimalType d) {
            int precision = d.precision() - 1;
            return precision >= Math.max(d.scale(), 1)
                    ? DataTypes.createDecimalType(precision, d.scale())
                    : type;
        }
        if (type instanceof ArrayType a) {
            return DataTypes.createArrayType(narrow(a.elementType()), a.containsNull());
        }
        if (type instanceof MapType m) {
            return DataTypes.createMapType(m.keyType(), narrow(m.valueType()), m.valueContainsNull());
        }
        if (type instanceof StructType s) {
            return mapTypes(s, SchemaProperties::narrow);
        }
        if (DataTypes.ShortType.equals(type)) return DataTypes.ByteType;
        if (DataTypes.IntegerType.equals(type)) return DataTypes.ShortType;
        if (DataTypes.LongType.equals(type)) return DataTypes.IntegerType;
        if (DataTypes.DoubleType.equals(type)) return DataTypes.FloatType;
        return type;
    }
}
