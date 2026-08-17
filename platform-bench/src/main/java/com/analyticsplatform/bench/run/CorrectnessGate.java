package com.analyticsplatform.bench.run;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.md5;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Fingerprints inputs and outputs so a benchmark comparison can be validated.
 *
 * <h2>Timing without this is worthless</h2>
 *
 * <p>A configuration that reads fewer rows, or produces different aggregates, is not faster at the
 * same work — it is doing different work. Every such result <em>looks</em> like a win, which is
 * precisely why the check has to run before any timing is accepted rather than as a footnote after.
 *
 * <h2>Order-independent by construction</h2>
 *
 * <p>Output fingerprints are computed by summing per-row hashes rather than hashing a collected,
 * ordered list. Spark makes no ordering guarantee without an explicit sort, so an order-sensitive
 * fingerprint would report a difference whenever partitioning changed — which is exactly what these
 * configurations change. It would fail constantly and for the wrong reason.
 */
public final class CorrectnessGate {

    private CorrectnessGate() {
    }

    /** What a workload consumed. */
    public record InputProfile(String fingerprint, long rowCount, long bytes, int fileCount) {
    }

    /**
     * Describes the input.
     *
     * <p>The fingerprint covers row count and the sum of per-row hashes, so two runs over the same
     * logical data agree regardless of how it was partitioned or which files it came from.
     */
    public static InputProfile profileInput(Dataset<Row> input, long bytes, int fileCount) {
        long rowCount = input.count();
        return new InputProfile(contentHash(input), rowCount, bytes, fileCount);
    }

    /**
     * A hash of the dataset's contents, independent of row order and partitioning.
     *
     * <p>Summing per-row hashes is commutative, so any permutation yields the same value. Two
     * different datasets could in principle sum alike; combined with the row count that is
     * vanishingly unlikely, and the alternative — a global sort on every check — would cost more
     * than the benchmark it guards.
     */
    public static String contentHash(Dataset<Row> data) {
        List<Column> columns = new ArrayList<>();
        for (String name : sortedColumns(data)) {
            // Cast to string so a Decimal(10,2) and a Decimal(12,4) holding the same value hash
            // alike: a config that changes a cast must not read as a correctness failure.
            columns.add(col("`" + name + "`").cast("string"));
        }
        if (columns.isEmpty()) {
            return "empty-schema";
        }

        Column rowHash = md5(concat_ws("", columns.toArray(new Column[0])));
        // conv(...) turns the leading 8 hex digits into a bigint so the sum stays exact rather
        // than accumulating floating-point error across millions of rows.
        Column asLong = org.apache.spark.sql.functions.conv(
                org.apache.spark.sql.functions.substring(rowHash, 1, 8), 16, 10)
                .cast("long");

        Row result = data.agg(
                sum(asLong).alias("hash_sum"),
                org.apache.spark.sql.functions.count(lit(1)).alias("row_count"),
                min(asLong).alias("hash_min")).first();

        // Explicit type arguments are load-bearing. String.valueOf(result.getAs("x")) makes the
        // compiler infer T = char[], because String.valueOf(char[]) is more specific than
        // String.valueOf(Object) - producing a ClassCastException at runtime with a message
        // (Long cannot be cast to [C) that gives no hint the cause is overload resolution.
        Long hashSum = result.<Long>getAs("hash_sum");
        Long hashMin = result.<Long>getAs("hash_min");
        Long rowCount = result.<Long>getAs("row_count");

        return "rows=" + rowCount + ",sum=" + hashSum + ",min=" + hashMin;
    }

    private static List<String> sortedColumns(Dataset<Row> data) {
        List<String> names = new ArrayList<>(Arrays.asList(data.columns()));
        // Sorted so a config that merely reorders a projection is not mistaken for a difference.
        names.sort(String::compareTo);
        return names;
    }

    /**
     * Whether two outputs are logically identical.
     *
     * @return empty when they match, or a description of the difference
     */
    public static java.util.Optional<String> compare(
            String baselineLabel, Dataset<Row> baseline,
            String candidateLabel, Dataset<Row> candidate) {

        List<String> baselineColumns = sortedColumns(baseline);
        List<String> candidateColumns = sortedColumns(candidate);
        if (!baselineColumns.equals(candidateColumns)) {
            return java.util.Optional.of(
                    candidateLabel + " produced different columns than " + baselineLabel
                            + ": " + baselineColumns + " vs " + candidateColumns);
        }

        String baselineHash = contentHash(baseline);
        String candidateHash = contentHash(candidate);
        if (!baselineHash.equals(candidateHash)) {
            return java.util.Optional.of(
                    candidateLabel + " produced different output than " + baselineLabel
                            + "\n  " + baselineLabel + ": " + baselineHash
                            + "\n  " + candidateLabel + ": " + candidateHash);
        }
        return java.util.Optional.empty();
    }
}
