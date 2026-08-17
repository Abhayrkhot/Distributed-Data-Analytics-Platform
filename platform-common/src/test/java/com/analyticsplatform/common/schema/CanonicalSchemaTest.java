package com.analyticsplatform.common.schema;

import static com.analyticsplatform.common.schema.Schemas.f;
import static com.analyticsplatform.common.schema.Schemas.of;
import static com.analyticsplatform.common.schema.Schemas.type;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class CanonicalSchemaTest {

    @Nested
    @DisplayName("order independence")
    class OrderIndependence {

        @Test
        @DisplayName("declaration order does not affect the canonical form")
        void topLevelFieldOrderIsIrrelevant() {
            StructType ab = of(f("alpha", type("long")), f("beta", type("string")));
            StructType ba = of(f("beta", type("string")), f("alpha", type("long")));

            assertThat(CanonicalSchema.canonicalize(ba))
                    .isEqualTo(CanonicalSchema.canonicalize(ab));
            assertThat(CanonicalSchema.hash(ba)).isEqualTo(CanonicalSchema.hash(ab));
        }

        /**
         * The specific trap this class exists for: {@code DataType.simpleString()} renders a
         * nested struct with its child fields in declaration order, so hashing Spark's own
         * rendering would make these two differ despite being the same type.
         */
        @Test
        @DisplayName("nested struct field order does not affect the hash")
        void nestedFieldOrderIsIrrelevant() {
            StructType xy = of(f("outer", of(f("x", type("int")), f("y", type("string")))));
            StructType yx = of(f("outer", of(f("y", type("string")), f("x", type("int")))));

            assertThat(CanonicalSchema.hash(yx)).isEqualTo(CanonicalSchema.hash(xy));
        }

        @Test
        @DisplayName("order independence holds two levels deep")
        void deeplyNestedFieldOrderIsIrrelevant() {
            StructType left = of(f("a", of(f("b", of(f("c", type("int")), f("d", type("long")))))));
            StructType right = of(f("a", of(f("b", of(f("d", type("long")), f("c", type("int")))))));

            assertThat(CanonicalSchema.hash(right)).isEqualTo(CanonicalSchema.hash(left));
        }

        @Test
        @DisplayName("canonicalization is idempotent")
        void canonicalizationIsIdempotent() {
            StructType schema = of(f("b", type("long")), f("a", type("string")));
            String once = CanonicalSchema.canonicalize(schema);

            // Re-canonicalizing already-sorted input must not reorder or reformat it.
            assertThat(CanonicalSchema.canonicalize(schema)).isEqualTo(once);
        }
    }

    @Nested
    @DisplayName("type spelling")
    class TypeSpelling {

        /** Spark renders LongType as "bigint" and IntegerType as "int"; we normalize both. */
        @ParameterizedTest(name = "{0} canonicalizes to {1}")
        @CsvSource({
            "byte, byte", "short, short", "int, int", "long, long",
            "float, float", "double, double", "boolean, boolean",
            "string, string", "binary, binary", "date, date", "timestamp, timestamp",
        })
        void primitiveTypesUseOneVocabulary(String input, String expected) {
            assertThat(CanonicalSchema.canonicalType(type(input))).isEqualTo(expected);
        }

        @Test
        @DisplayName("Spark's bigint alias resolves to long")
        void sparkAliasIsNormalized() {
            // LongType.typeName() is "long" but simpleString() is "bigint"; the canonical
            // vocabulary must not depend on which one Spark happens to hand back.
            assertThat(CanonicalSchema.canonicalType(DataTypes.LongType)).isEqualTo("long");
            assertThat(DataTypes.LongType.simpleString()).isEqualTo("bigint");
        }

        /** Precision and scale are part of a decimal's identity and must not collapse. */
        @Test
        @DisplayName("decimals of different precision or scale stay distinct")
        void decimalsCarryPrecisionAndScale() {
            assertThat(CanonicalSchema.canonicalType(type("decimal(10,2)")))
                    .isEqualTo("decimal(10,2)");
            assertThat(CanonicalSchema.hash(of(f("v", type("decimal(10,2)")))))
                    .isNotEqualTo(CanonicalSchema.hash(of(f("v", type("decimal(12,2)")))));
            assertThat(CanonicalSchema.hash(of(f("v", type("decimal(10,2)")))))
                    .isNotEqualTo(CanonicalSchema.hash(of(f("v", type("decimal(10,4)")))));
        }

        @Test
        @DisplayName("array and map carry their nullability flags")
        void collectionTypesIncludeNullability() {
            assertThat(CanonicalSchema.canonicalType(
                    DataTypes.createArrayType(type("int"), true))).isEqualTo("array<int,true>");
            assertThat(CanonicalSchema.canonicalType(
                    DataTypes.createArrayType(type("int"), false))).isEqualTo("array<int,false>");
            assertThat(CanonicalSchema.canonicalType(
                    DataTypes.createMapType(type("string"), type("long"), true)))
                    .isEqualTo("map<string,long,true>");
        }
    }

    @Nested
    @DisplayName("field names")
    class FieldNames {

        @Test
        @DisplayName("field-name casing is normalized")
        void namesAreLowercased() {
            assertThat(CanonicalSchema.hash(of(f("Fare_Amount", type("double")))))
                    .isEqualTo(CanonicalSchema.hash(of(f("fare_amount", type("double")))));
        }

        /**
         * Spark permits case-distinct siblings. Canonicalization would silently merge them, so
         * this fails closed rather than emitting a hash that hides a real ambiguity.
         */
        @Test
        @DisplayName("case-colliding sibling fields are rejected, not merged")
        void collidingNamesAreRejected() {
            StructType colliding = of(f("Fare", type("double")), f("fare", type("double")));

            assertThatThrownBy(() -> CanonicalSchema.canonicalize(colliding))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("duplicate field name");
        }

        @Test
        @DisplayName("a collision nested inside a struct is also rejected")
        void collidingNestedNamesAreRejected() {
            StructType colliding = of(f("outer", of(f("X", type("int")), f("x", type("int")))));

            assertThatThrownBy(() -> CanonicalSchema.hash(colliding))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("fieldNames returns canonical names, sorted")
        void fieldNamesAreCanonicalAndSorted() {
            assertThat(CanonicalSchema.fieldNames(
                    of(f("Zulu", type("int")), f("alpha", type("int")), f("Mike", type("int")))))
                    .containsExactly("alpha", "mike", "zulu");
        }
    }

    @Nested
    @DisplayName("canonical form and hash")
    class Form {

        @Test
        @DisplayName("renders one sorted name:type:nullable line per field")
        void formIsLineOrientedAndSorted() {
            StructType schema = of(
                    f("passenger_count", type("long")),
                    f("airport_fee", type("double")),
                    f("fare_amount", type("double"), false));

            assertThat(CanonicalSchema.canonicalize(schema)).isEqualTo(
                    "airport_fee:double:true\n"
                  + "fare_amount:double:false\n"
                  + "passenger_count:long:true");
        }

        @Test
        @DisplayName("nullability is part of the identity")
        void nullabilityChangesTheHash() {
            assertThat(CanonicalSchema.hash(of(f("v", type("long"), true))))
                    .isNotEqualTo(CanonicalSchema.hash(of(f("v", type("long"), false))));
        }

        @Test
        @DisplayName("the hash is 64 lowercase hex characters and stable across calls")
        void hashFormatAndStability() {
            StructType schema = of(f("a", type("long")));

            assertThat(CanonicalSchema.hash(schema)).matches("[0-9a-f]{64}");
            assertThat(CanonicalSchema.hash(schema)).isEqualTo(CanonicalSchema.hash(schema));
        }

        @Test
        @DisplayName("an empty schema canonicalizes rather than failing")
        void emptySchemaIsHandled() {
            assertThat(CanonicalSchema.canonicalize(of())).isEmpty();
            assertThat(CanonicalSchema.hash(of())).matches("[0-9a-f]{64}");
        }
    }
}
