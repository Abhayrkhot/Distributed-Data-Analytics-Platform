package com.analyticsplatform.ingest.source;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

/**
 * Conforms heterogeneous TLC sources onto one bronze schema.
 *
 * <p>The divergence is real, not contrived: yellow uses {@code tpep_*} timestamps and carries
 * {@code Airport_fee}; green uses {@code lpep_*} and carries {@code ehail_fee} and
 * {@code trip_type}; 2025 files add {@code cbd_congestion_fee} that 2024 files do not have. Column
 * order differs too.
 *
 * <p>Bronze rejects nothing. It is a faithful, conformed record of what arrived — filtering belongs
 * to silver, where the rules are declared and their outcomes recorded. Mixing the two would mean a
 * row could vanish between the file and the warehouse with nothing to say why.
 *
 * <p>Columns a source genuinely lacks are materialized as typed nulls rather than omitted, so every
 * source produces a schema that hashes identically and the union needs no schema merging.
 */
public final class SourceNormalizer {

    /** Source names as they appear in {@code source}. */
    public static final String YELLOW = "yellow";
    public static final String GREEN = "green";

    /**
     * Bronze columns in canonical order, mapped to the type each must have.
     *
     * <p>Ordered so the produced DataFrame is stable regardless of the input's column order —
     * otherwise the schema hash would depend on which file happened to be read.
     */
    private static final Map<String, org.apache.spark.sql.types.DataType> BRONZE_COLUMNS =
            new LinkedHashMap<>();

    static {
        BRONZE_COLUMNS.put("source", DataTypes.StringType);
        BRONZE_COLUMNS.put("vendor_id", DataTypes.IntegerType);
        BRONZE_COLUMNS.put("pickup_ts", DataTypes.TimestampType);
        BRONZE_COLUMNS.put("dropoff_ts", DataTypes.TimestampType);
        BRONZE_COLUMNS.put("passenger_count", DataTypes.LongType);
        BRONZE_COLUMNS.put("trip_distance_mi", DataTypes.DoubleType);
        BRONZE_COLUMNS.put("rate_code", DataTypes.LongType);
        BRONZE_COLUMNS.put("store_and_fwd_flag", DataTypes.StringType);
        BRONZE_COLUMNS.put("pickup_location_id", DataTypes.IntegerType);
        BRONZE_COLUMNS.put("dropoff_location_id", DataTypes.IntegerType);
        BRONZE_COLUMNS.put("payment_type_code", DataTypes.LongType);
        BRONZE_COLUMNS.put("fare_amount", DataTypes.DoubleType);
        BRONZE_COLUMNS.put("extra", DataTypes.DoubleType);
        BRONZE_COLUMNS.put("mta_tax", DataTypes.DoubleType);
        BRONZE_COLUMNS.put("tip_amount", DataTypes.DoubleType);
        BRONZE_COLUMNS.put("tolls_amount", DataTypes.DoubleType);
        BRONZE_COLUMNS.put("improvement_surcharge", DataTypes.DoubleType);
        BRONZE_COLUMNS.put("total_amount", DataTypes.DoubleType);
        BRONZE_COLUMNS.put("congestion_surcharge", DataTypes.DoubleType);
        BRONZE_COLUMNS.put("airport_fee", DataTypes.DoubleType);
        BRONZE_COLUMNS.put("cbd_congestion_fee", DataTypes.DoubleType);
        BRONZE_COLUMNS.put("ehail_fee", DataTypes.DoubleType);
        BRONZE_COLUMNS.put("trip_type", DataTypes.LongType);
    }

    /** Yellow's names for the shared bronze columns. */
    private static final Map<String, String> YELLOW_MAPPING = Map.ofEntries(
            Map.entry("vendor_id", "VendorID"),
            Map.entry("pickup_ts", "tpep_pickup_datetime"),
            Map.entry("dropoff_ts", "tpep_dropoff_datetime"),
            Map.entry("passenger_count", "passenger_count"),
            Map.entry("trip_distance_mi", "trip_distance"),
            Map.entry("rate_code", "RatecodeID"),
            Map.entry("store_and_fwd_flag", "store_and_fwd_flag"),
            Map.entry("pickup_location_id", "PULocationID"),
            Map.entry("dropoff_location_id", "DOLocationID"),
            Map.entry("payment_type_code", "payment_type"),
            Map.entry("fare_amount", "fare_amount"),
            Map.entry("extra", "extra"),
            Map.entry("mta_tax", "mta_tax"),
            Map.entry("tip_amount", "tip_amount"),
            Map.entry("tolls_amount", "tolls_amount"),
            Map.entry("improvement_surcharge", "improvement_surcharge"),
            Map.entry("total_amount", "total_amount"),
            Map.entry("congestion_surcharge", "congestion_surcharge"),
            Map.entry("airport_fee", "Airport_fee"),
            Map.entry("cbd_congestion_fee", "cbd_congestion_fee"));

    /** Green's names. Note lpep_*, and that it has no airport fee or CBD fee at all. */
    private static final Map<String, String> GREEN_MAPPING = Map.ofEntries(
            Map.entry("vendor_id", "VendorID"),
            Map.entry("pickup_ts", "lpep_pickup_datetime"),
            Map.entry("dropoff_ts", "lpep_dropoff_datetime"),
            Map.entry("passenger_count", "passenger_count"),
            Map.entry("trip_distance_mi", "trip_distance"),
            Map.entry("rate_code", "RatecodeID"),
            Map.entry("store_and_fwd_flag", "store_and_fwd_flag"),
            Map.entry("pickup_location_id", "PULocationID"),
            Map.entry("dropoff_location_id", "DOLocationID"),
            Map.entry("payment_type_code", "payment_type"),
            Map.entry("fare_amount", "fare_amount"),
            Map.entry("extra", "extra"),
            Map.entry("mta_tax", "mta_tax"),
            Map.entry("tip_amount", "tip_amount"),
            Map.entry("tolls_amount", "tolls_amount"),
            Map.entry("improvement_surcharge", "improvement_surcharge"),
            Map.entry("total_amount", "total_amount"),
            Map.entry("congestion_surcharge", "congestion_surcharge"),
            Map.entry("ehail_fee", "ehail_fee"),
            Map.entry("trip_type", "trip_type"));

    private SourceNormalizer() {
    }

    /** Bronze column names, in canonical order. */
    public static List<String> bronzeColumns() {
        return List.copyOf(BRONZE_COLUMNS.keySet());
    }

    /** Normalizes a yellow taxi file. Handles both the 2024 and 2025 shapes. */
    public static Dataset<Row> normalizeYellow(Dataset<Row> raw) {
        return normalize(raw, YELLOW, YELLOW_MAPPING);
    }

    /** Normalizes a green taxi file. */
    public static Dataset<Row> normalizeGreen(Dataset<Row> raw) {
        return normalize(raw, GREEN, GREEN_MAPPING);
    }

    /** Dispatches on source name. */
    public static Dataset<Row> normalize(Dataset<Row> raw, String source) {
        return switch (source.toLowerCase(Locale.ROOT)) {
            case YELLOW -> normalizeYellow(raw);
            case GREEN -> normalizeGreen(raw);
            default -> throw new IllegalArgumentException("unknown source: " + source);
        };
    }

    private static Dataset<Row> normalize(
            Dataset<Row> raw, String source, Map<String, String> mapping) {

        List<String> available = Arrays.asList(raw.columns());
        Column[] projection = new Column[BRONZE_COLUMNS.size()];
        int index = 0;

        for (Map.Entry<String, org.apache.spark.sql.types.DataType> bronze
                : BRONZE_COLUMNS.entrySet()) {
            String name = bronze.getKey();
            if ("source".equals(name)) {
                projection[index++] = lit(source).cast(DataTypes.StringType).alias(name);
                continue;
            }

            String sourceColumn = mapping.get(name);
            boolean present = sourceColumn != null && available.contains(sourceColumn);

            // A column the source lacks becomes a typed null rather than being omitted, so every
            // source yields an identically-hashing schema and the union needs no merging.
            projection[index++] = present
                    ? col(sourceColumn).cast(bronze.getValue()).alias(name)
                    : lit(null).cast(bronze.getValue()).alias(name);
        }

        return raw.select(projection);
    }
}
