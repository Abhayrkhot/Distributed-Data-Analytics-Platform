package com.analyticsplatform.common.schema;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Schema-building helpers shared by the schema tests and, via the test-jar, by other modules'
 * component tests.
 */
public final class Schemas {

    private Schemas() {
    }

    public static StructType of(StructField... fields) {
        return new StructType(fields);
    }

    /** Nullable field. */
    public static StructField f(String name, DataType type) {
        return new StructField(name, type, true, Metadata.empty());
    }

    /** Field with explicit nullability. */
    public static StructField f(String name, DataType type, boolean nullable) {
        return new StructField(name, type, nullable, Metadata.empty());
    }

    /**
     * Resolves a canonical type name to a Spark {@link DataType}, so the compatibility matrix can
     * be expressed as readable CSV rows rather than as Java type literals.
     */
    public static DataType type(String canonicalName) {
        return switch (canonicalName) {
            case "byte" -> DataTypes.ByteType;
            case "short" -> DataTypes.ShortType;
            case "int" -> DataTypes.IntegerType;
            case "long" -> DataTypes.LongType;
            case "float" -> DataTypes.FloatType;
            case "double" -> DataTypes.DoubleType;
            case "boolean" -> DataTypes.BooleanType;
            case "string" -> DataTypes.StringType;
            case "binary" -> DataTypes.BinaryType;
            case "date" -> DataTypes.DateType;
            case "timestamp" -> DataTypes.TimestampType;
            case "timestamp_ntz" -> DataTypes.TimestampNTZType;
            default -> {
                if (canonicalName.startsWith("decimal(")) {
                    String args = canonicalName.substring(8, canonicalName.length() - 1);
                    String[] parts = args.split(",");
                    yield DataTypes.createDecimalType(
                            Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim()));
                }
                throw new IllegalArgumentException("unknown type name in fixture: " + canonicalName);
            }
        };
    }

    /** A single-column schema, for exercising one type change at a time. */
    public static StructType oneColumn(String typeName) {
        return of(f("value", type(typeName)));
    }

    public static StructType oneColumn(String typeName, boolean nullable) {
        return of(f("value", type(typeName), nullable));
    }
}
