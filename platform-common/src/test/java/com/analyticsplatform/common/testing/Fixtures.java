package com.analyticsplatform.common.testing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Loads the CSV fixtures and golden expectations in {@code tests/}.
 *
 * <p>Schemas are declared explicitly rather than inferred. Inference reads a sample and guesses,
 * so a column that happens to hold only integers in a 12-row fixture infers as {@code int} while
 * the real file infers as {@code double} — the fixture would then test a shape the pipeline never
 * sees. Declaring them also means the fixture schemas are readable evidence of what TLC actually
 * ships.
 */
public final class Fixtures {

    private Fixtures() {
    }

    /** Real TLC yellow columns for 2024. */
    public static final StructType YELLOW_2024_SCHEMA = new StructType(new StructField[] {
        nullable("VendorID", DataTypes.IntegerType),
        nullable("tpep_pickup_datetime", DataTypes.TimestampType),
        nullable("tpep_dropoff_datetime", DataTypes.TimestampType),
        nullable("passenger_count", DataTypes.LongType),
        nullable("trip_distance", DataTypes.DoubleType),
        nullable("RatecodeID", DataTypes.LongType),
        nullable("store_and_fwd_flag", DataTypes.StringType),
        nullable("PULocationID", DataTypes.IntegerType),
        nullable("DOLocationID", DataTypes.IntegerType),
        nullable("payment_type", DataTypes.LongType),
        nullable("fare_amount", DataTypes.DoubleType),
        nullable("extra", DataTypes.DoubleType),
        nullable("mta_tax", DataTypes.DoubleType),
        nullable("tip_amount", DataTypes.DoubleType),
        nullable("tolls_amount", DataTypes.DoubleType),
        nullable("improvement_surcharge", DataTypes.DoubleType),
        nullable("total_amount", DataTypes.DoubleType),
        nullable("congestion_surcharge", DataTypes.DoubleType),
        nullable("Airport_fee", DataTypes.DoubleType),
    });

    /** 2025 is 2024 plus {@code cbd_congestion_fee} — the real additive change. */
    public static final StructType YELLOW_2025_SCHEMA = appendField(
            YELLOW_2024_SCHEMA, nullable("cbd_congestion_fee", DataTypes.DoubleType));

    /** Green differs structurally: lpep_* timestamps, ehail_fee, trip_type, different order. */
    public static final StructType GREEN_2024_SCHEMA = new StructType(new StructField[] {
        nullable("VendorID", DataTypes.IntegerType),
        nullable("lpep_pickup_datetime", DataTypes.TimestampType),
        nullable("lpep_dropoff_datetime", DataTypes.TimestampType),
        nullable("store_and_fwd_flag", DataTypes.StringType),
        nullable("RatecodeID", DataTypes.LongType),
        nullable("PULocationID", DataTypes.IntegerType),
        nullable("DOLocationID", DataTypes.IntegerType),
        nullable("passenger_count", DataTypes.LongType),
        nullable("trip_distance", DataTypes.DoubleType),
        nullable("fare_amount", DataTypes.DoubleType),
        nullable("extra", DataTypes.DoubleType),
        nullable("mta_tax", DataTypes.DoubleType),
        nullable("tip_amount", DataTypes.DoubleType),
        nullable("tolls_amount", DataTypes.DoubleType),
        nullable("ehail_fee", DataTypes.DoubleType),
        nullable("improvement_surcharge", DataTypes.DoubleType),
        nullable("total_amount", DataTypes.DoubleType),
        nullable("payment_type", DataTypes.LongType),
        nullable("trip_type", DataTypes.LongType),
        nullable("congestion_surcharge", DataTypes.DoubleType),
    });

    public static final StructType ZONE_LOOKUP_SCHEMA = new StructType(new StructField[] {
        nullable("LocationID", DataTypes.IntegerType),
        nullable("Borough", DataTypes.StringType),
        nullable("Zone", DataTypes.StringType),
        nullable("service_zone", DataTypes.StringType),
    });

    /**
     * The repository root, found by walking up from the working directory.
     *
     * <p>Maven runs tests with the working directory set to the module, not the repo, and a
     * relative {@code ../tests} silently breaks when a test is run from an IDE with a different
     * working directory. Walking up until the marker is found works from either.
     */
    public static Path repoRoot() {
        Path candidate = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (candidate != null) {
            if (Files.isDirectory(candidate.resolve("tests/fixtures"))
                    && Files.isRegularFile(candidate.resolve("pom.xml"))) {
                return candidate;
            }
            candidate = candidate.getParent();
        }
        throw new IllegalStateException(
                "could not locate the repository root from " + System.getProperty("user.dir"));
    }

    public static Path fixturesDir() {
        return repoRoot().resolve("tests/fixtures");
    }

    public static Path goldenDir() {
        return repoRoot().resolve("tests/golden");
    }

    // ---------------------------------------------------------------- loading

    public static Dataset<Row> yellow2024() {
        return csv("yellow_tripdata_2024-01.csv", YELLOW_2024_SCHEMA);
    }

    public static Dataset<Row> yellow2025() {
        return csv("yellow_tripdata_2025-01.csv", YELLOW_2025_SCHEMA);
    }

    public static Dataset<Row> green2024() {
        return csv("green_tripdata_2024-01.csv", GREEN_2024_SCHEMA);
    }

    public static Dataset<Row> taxiZones() {
        return csv("taxi_zone_lookup.csv", ZONE_LOOKUP_SCHEMA);
    }

    /** A golden expectation file, with types inferred — these are assertions, not pipeline input. */
    public static Dataset<Row> golden(String name) {
        return SparkTestSupport.spark().read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("nullValue", "")
                .csv(goldenDir().resolve(name).toString());
    }

    private static Dataset<Row> csv(String name, StructType schema) {
        return SparkTestSupport.spark().read()
                .option("header", "true")
                .option("nullValue", "")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .schema(schema)
                .csv(fixturesDir().resolve(name).toString());
    }

    /**
     * Writes a fixture out as Parquet and returns the directory.
     *
     * <p>The pipeline reads Parquet, so tests that exercise the real reader need Parquet input;
     * the fixtures stay CSV so a reviewer can read them.
     */
    public static Path asParquet(Dataset<Row> data, String name) {
        try {
            Path directory = Files.createTempDirectory("fixture-" + name + "-");
            Path target = directory.resolve(name);
            data.write().mode("overwrite").parquet(target.toString());
            target.toFile().deleteOnExit();
            return target;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to materialize fixture " + name, e);
        }
    }

    private static StructField nullable(String name, org.apache.spark.sql.types.DataType type) {
        return new StructField(name, type, true, Metadata.empty());
    }

    private static StructType appendField(StructType base, StructField extra) {
        StructField[] fields = new StructField[base.fields().length + 1];
        System.arraycopy(base.fields(), 0, fields, 0, base.fields().length);
        fields[base.fields().length] = extra;
        return new StructType(fields);
    }
}
