package com.analyticsplatform.common.schema;

import static com.analyticsplatform.common.schema.Schemas.f;
import static com.analyticsplatform.common.schema.Schemas.of;
import static com.analyticsplatform.common.schema.Schemas.oneColumn;
import static com.analyticsplatform.common.schema.Schemas.type;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.analyticsplatform.common.schema.SchemaCompatibility.ChangeType;
import com.analyticsplatform.common.schema.SchemaCompatibility.SchemaDiff;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The compatibility matrix, exercised in both directions: every widening pair is also asserted to
 * be breaking when reversed. Testing only the safe direction would pass against an implementation
 * that classified everything as widening.
 */
class SchemaCompatibilityTest {

    @Test
    @DisplayName("no previous schema is the initial version")
    void nullPreviousIsInitial() {
        SchemaDiff diff = SchemaCompatibility.classify(null, oneColumn("long"));

        assertThat(diff.changeType()).isEqualTo(ChangeType.INITIAL);
        assertThat(diff.isCompatible()).isTrue();
    }

    @Nested
    @DisplayName("widening ladder")
    class Widening {

        // Pipe-delimited: decimal(10,2) contains a comma that CsvSource would split on.
        @ParameterizedTest(name = "{0} -> {1} is widening")
        @CsvSource(delimiter = '|', value = {
            "byte | short", "short | int", "int | long",
            "byte | int", "byte | long", "short | long",
            "float | double",
            "decimal(10,2) | decimal(12,2)",     // more integral digits
            "decimal(10,2) | decimal(12,4)",     // more integral digits and more scale
            "decimal(10,2) | decimal(10,2)",     // identical is trivially fine
        })
        void wideningIsAllowed(String from, String to) {
            SchemaDiff diff = SchemaCompatibility.classify(oneColumn(from), oneColumn(to));

            assertThat(diff.changeType())
                    .isIn(ChangeType.WIDENING, ChangeType.ADDITIVE);
            assertThat(diff.isCompatible()).isTrue();
        }

        /** The same pairs reversed must all be rejected. */
        @ParameterizedTest(name = "{1} -> {0} is breaking")
        @CsvSource(delimiter = '|', value = {
            "byte | short", "short | int", "int | long",
            "byte | int", "byte | long", "short | long",
            "float | double",
            "decimal(10,2) | decimal(12,2)",
            "decimal(10,2) | decimal(12,4)",
        })
        void narrowingIsRejected(String from, String to) {
            SchemaDiff diff = SchemaCompatibility.classify(oneColumn(to), oneColumn(from));

            assertThat(diff.changeType()).isEqualTo(ChangeType.BREAKING);
            assertThat(diff.isCompatible()).isFalse();
            assertThat(diff.breakingReasons()).isNotEmpty();
        }

        @Test
        @DisplayName("required -> nullable is widening")
        void requiredToNullableWidens() {
            SchemaDiff diff = SchemaCompatibility.classify(
                    oneColumn("long", false), oneColumn("long", true));

            assertThat(diff.changeType()).isEqualTo(ChangeType.WIDENING);
        }

        @Test
        @DisplayName("nullable -> required is breaking")
        void nullableToRequiredBreaks() {
            SchemaDiff diff = SchemaCompatibility.classify(
                    oneColumn("long", true), oneColumn("long", false));

            assertThat(diff.changeType()).isEqualTo(ChangeType.BREAKING);
            assertThat(diff.breakingReasons()).anyMatch(r -> r.contains("became required"));
        }
    }

    @Nested
    @DisplayName("breaking changes")
    class Breaking {

        @ParameterizedTest(name = "{0} -> {1}")
        @CsvSource(delimiter = '|', value = {
            "long | int", "double | int", "double | float", "long | short",
            "string | long", "string | double", "string | int",
            "timestamp | date", "date | timestamp",
            "long | string", "boolean | int", "binary | string",
            "decimal(12,4) | decimal(10,4)",   // fewer integral digits
            "decimal(10,4) | decimal(10,2)",   // scale shrinks, fraction lost
        })
        void incompatibleTypeChangesAreBreaking(String from, String to) {
            SchemaDiff diff = SchemaCompatibility.classify(oneColumn(from), oneColumn(to));

            assertThat(diff.changeType()).isEqualTo(ChangeType.BREAKING);
        }

        /**
         * Deliberately breaking despite feeling like a widening: a double cannot represent every
         * long exactly, and the policy is fail-closed for anything not explicitly proven safe.
         */
        @ParameterizedTest(name = "{0} -> {1} is breaking (lossy despite looking wider)")
        @CsvSource({"int, double", "long, double", "int, float", "long, float"})
        void integralToFloatingIsBreaking(String from, String to) {
            assertThat(SchemaCompatibility.classify(oneColumn(from), oneColumn(to)).changeType())
                    .isEqualTo(ChangeType.BREAKING);
        }

        @Test
        @DisplayName("dropping a column is breaking")
        void removingColumnBreaks() {
            StructType before = of(f("a", type("long")), f("b", type("string")));
            StructType after = of(f("a", type("long")));

            SchemaDiff diff = SchemaCompatibility.classify(before, after);

            assertThat(diff.changeType()).isEqualTo(ChangeType.BREAKING);
            assertThat(diff.removedColumns()).containsExactly("b");
        }

        @Test
        @DisplayName("a new non-nullable column is breaking")
        void newRequiredColumnBreaks() {
            StructType before = of(f("a", type("long")));
            StructType after = of(f("a", type("long")), f("b", type("string"), false));

            SchemaDiff diff = SchemaCompatibility.classify(before, after);

            assertThat(diff.changeType()).isEqualTo(ChangeType.BREAKING);
            assertThat(diff.breakingReasons()).anyMatch(r -> r.contains("not nullable"));
        }
    }

    @Nested
    @DisplayName("additive")
    class Additive {

        /** The real 2024 -> 2025 TLC change: NYC added cbd_congestion_fee. */
        @Test
        @DisplayName("a new nullable column is additive")
        void newNullableColumnIsAdditive() {
            StructType before = of(f("fare_amount", type("double")));
            StructType after = of(
                    f("fare_amount", type("double")),
                    f("cbd_congestion_fee", type("decimal(10,2)")));

            SchemaDiff diff = SchemaCompatibility.classify(before, after);

            assertThat(diff.changeType()).isEqualTo(ChangeType.ADDITIVE);
            assertThat(diff.addedColumns()).containsExactly("cbd_congestion_fee");
            assertThat(diff.isCompatible()).isTrue();
        }
    }

    @Nested
    @DisplayName("multiple simultaneous changes")
    class MultiChange {

        @Test
        @DisplayName("additive + additive stays additive")
        void twoAdditionsStayAdditive() {
            StructType before = of(f("a", type("long")));
            StructType after = of(f("a", type("long")), f("b", type("string")), f("c", type("int")));

            assertThat(SchemaCompatibility.classify(before, after).changeType())
                    .isEqualTo(ChangeType.ADDITIVE);
        }

        @Test
        @DisplayName("additive + widening resolves to widening")
        void additivePlusWideningIsWidening() {
            StructType before = of(f("a", type("int")));
            StructType after = of(f("a", type("long")), f("b", type("string")));

            assertThat(SchemaCompatibility.classify(before, after).changeType())
                    .isEqualTo(ChangeType.WIDENING);
        }

        /**
         * The severity-combining invariant: one breaking component poisons the whole transition,
         * however many safe changes accompany it.
         */
        @Test
        @DisplayName("additive + widening + breaking resolves to breaking")
        void anyBreakingComponentDominates() {
            StructType before = of(f("a", type("int")), f("b", type("long")));
            StructType after = of(
                    f("a", type("long")),        // widening
                    f("b", type("int")),         // breaking
                    f("c", type("string")));     // additive

            SchemaDiff diff = SchemaCompatibility.classify(before, after);

            assertThat(diff.changeType()).isEqualTo(ChangeType.BREAKING);
            assertThat(diff.addedColumns()).containsExactly("c");
        }

        @Test
        @DisplayName("many breaking changes are still just breaking")
        void multipleBreakingChanges() {
            StructType before = of(f("a", type("long")), f("b", type("double")), f("c", type("string")));
            StructType after = of(f("a", type("int")), f("b", type("int")));

            SchemaDiff diff = SchemaCompatibility.classify(before, after);

            assertThat(diff.changeType()).isEqualTo(ChangeType.BREAKING);
            assertThat(diff.breakingReasons()).hasSizeGreaterThanOrEqualTo(3);
        }
    }

    @Nested
    @DisplayName("type kind changes")
    class KindChanges {

        /**
         * Swapping one kind of type for another entirely. Each case exercises the "both sides are
         * the same kind" guard from the failing side, which is where a careless instanceof pair
         * would let an incompatible pair fall through to the widening ladder.
         */
        @Test
        @DisplayName("a struct replaced by a primitive is breaking")
        void structToPrimitive() {
            StructType asStruct = of(f("v", of(f("inner", type("int")))));
            StructType asString = of(f("v", type("string")));

            assertThat(SchemaCompatibility.classify(asStruct, asString).changeType())
                    .isEqualTo(ChangeType.BREAKING);
            assertThat(SchemaCompatibility.classify(asString, asStruct).changeType())
                    .isEqualTo(ChangeType.BREAKING);
        }

        @Test
        @DisplayName("an array replaced by a primitive is breaking")
        void arrayToPrimitive() {
            StructType asArray = of(f("v", DataTypes.createArrayType(type("int"), true)));
            StructType asInt = of(f("v", type("int")));

            assertThat(SchemaCompatibility.classify(asArray, asInt).changeType())
                    .isEqualTo(ChangeType.BREAKING);
            assertThat(SchemaCompatibility.classify(asInt, asArray).changeType())
                    .isEqualTo(ChangeType.BREAKING);
        }

        @Test
        @DisplayName("a map replaced by an array is breaking")
        void mapToArray() {
            StructType asMap = of(f("v",
                    DataTypes.createMapType(type("string"), type("int"), true)));
            StructType asArray = of(f("v", DataTypes.createArrayType(type("int"), true)));

            assertThat(SchemaCompatibility.classify(asMap, asArray).changeType())
                    .isEqualTo(ChangeType.BREAKING);
            assertThat(SchemaCompatibility.classify(asArray, asMap).changeType())
                    .isEqualTo(ChangeType.BREAKING);
        }

        @Test
        @DisplayName("a decimal replaced by a double is breaking")
        void decimalToDouble() {
            StructType asDecimal = of(f("v", type("decimal(10,2)")));
            StructType asDouble = of(f("v", type("double")));

            assertThat(SchemaCompatibility.classify(asDecimal, asDouble).changeType())
                    .isEqualTo(ChangeType.BREAKING);
            assertThat(SchemaCompatibility.classify(asDouble, asDecimal).changeType())
                    .isEqualTo(ChangeType.BREAKING);
        }

        /** A nested struct whose inner shape is unchanged contributes no severity. */
        @Test
        @DisplayName("an unchanged nested struct is not a change")
        void identicalNestedStructIsNoChange() {
            StructType schema = of(f("outer", of(f("inner", type("int")))));

            assertThat(SchemaCompatibility.classify(schema, schema).changeType())
                    .isEqualTo(ChangeType.ADDITIVE);
        }
    }

    @Nested
    @DisplayName("input validation")
    class InputValidation {

        @Test
        @DisplayName("a null current schema is rejected")
        void nullCurrentSchemaIsRejected() {
            assertThatThrownBy(() -> SchemaCompatibility.classify(oneColumn("long"), null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("current schema must not be null");
        }

        /**
         * Case-colliding siblings must be refused during classification too, not only during
         * canonicalization — otherwise a diff would silently compare only one of them.
         */
        @Test
        @DisplayName("case-colliding field names are rejected during classification")
        void collidingNamesAreRejectedWhenClassifying() {
            StructType colliding = of(f("Fare", type("double")), f("fare", type("double")));

            assertThatThrownBy(() -> SchemaCompatibility.classify(oneColumn("long"), colliding))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("duplicate field name");
        }
    }

    @Nested
    @DisplayName("nested structures")
    class Nested_ {

        @Test
        @DisplayName("widening inside a struct is widening")
        void nestedWidening() {
            StructType before = of(f("outer", of(f("inner", type("int")))));
            StructType after = of(f("outer", of(f("inner", type("long")))));

            assertThat(SchemaCompatibility.classify(before, after).changeType())
                    .isEqualTo(ChangeType.WIDENING);
        }

        /** A breaking change buried in a nested struct must surface at the top level. */
        @Test
        @DisplayName("narrowing inside a struct is breaking")
        void nestedNarrowing() {
            StructType before = of(f("outer", of(f("inner", type("long")))));
            StructType after = of(f("outer", of(f("inner", type("int")))));

            SchemaDiff diff = SchemaCompatibility.classify(before, after);

            assertThat(diff.changeType()).isEqualTo(ChangeType.BREAKING);
            assertThat(diff.breakingReasons()).anyMatch(r -> r.contains("outer."));
        }

        @Test
        @DisplayName("dropping a nested field is breaking")
        void nestedRemoval() {
            StructType before = of(f("outer", of(f("x", type("int")), f("y", type("int")))));
            StructType after = of(f("outer", of(f("x", type("int")))));

            assertThat(SchemaCompatibility.classify(before, after).changeType())
                    .isEqualTo(ChangeType.BREAKING);
        }

        @Test
        @DisplayName("array element widening is widening; narrowing is breaking")
        void arrayElementChanges() {
            StructType intArray = of(f("xs", DataTypes.createArrayType(type("int"), true)));
            StructType longArray = of(f("xs", DataTypes.createArrayType(type("long"), true)));

            assertThat(SchemaCompatibility.classify(intArray, longArray).changeType())
                    .isEqualTo(ChangeType.WIDENING);
            assertThat(SchemaCompatibility.classify(longArray, intArray).changeType())
                    .isEqualTo(ChangeType.BREAKING);
        }

        @Test
        @DisplayName("array elements becoming non-nullable is breaking")
        void arrayNullabilityTightening() {
            StructType nullable = of(f("xs", DataTypes.createArrayType(type("int"), true)));
            StructType notNull = of(f("xs", DataTypes.createArrayType(type("int"), false)));

            assertThat(SchemaCompatibility.classify(nullable, notNull).changeType())
                    .isEqualTo(ChangeType.BREAKING);
            assertThat(SchemaCompatibility.classify(notNull, nullable).changeType())
                    .isEqualTo(ChangeType.WIDENING);
        }

        @Test
        @DisplayName("map value widening is allowed but key changes are breaking")
        void mapChanges() {
            StructType intValues = of(f("m",
                    DataTypes.createMapType(type("string"), type("int"), true)));
            StructType longValues = of(f("m",
                    DataTypes.createMapType(type("string"), type("long"), true)));
            StructType longKeys = of(f("m",
                    DataTypes.createMapType(type("long"), type("int"), true)));

            assertThat(SchemaCompatibility.classify(intValues, longValues).changeType())
                    .isEqualTo(ChangeType.WIDENING);
            assertThat(SchemaCompatibility.classify(intValues, longKeys).changeType())
                    .isEqualTo(ChangeType.BREAKING);
        }
    }
}
