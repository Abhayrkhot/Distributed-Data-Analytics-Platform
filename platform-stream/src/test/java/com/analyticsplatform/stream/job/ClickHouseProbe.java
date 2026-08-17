package com.analyticsplatform.stream.job;

import com.analyticsplatform.common.config.PlatformConfig;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Runs ClickHouse SQL over ClickHouse's HTTP interface.
 *
 * <h2>Why HTTP rather than JDBC</h2>
 *
 * <p>Because {@code clickhouse-jdbc} and Spark cannot share a JVM. The driver's own SQL parser
 * ({@code com.clickhouse.jdbc.internal.ClickHouseLexer}) is ANTLR-4.13-generated, while Spark 3.5's
 * parsers are generated with 4.9.3 — and an ANTLR runtime cannot deserialize an ATN produced by the
 * other major version. Whichever copy the classloader reaches first breaks the other:
 *
 * <ul>
 *   <li>4.13 on the classpath → every {@code spark.sql()} dies with
 *       {@code Could not deserialize ATN with version 3 (expected 4)}
 *   <li>4.9.3 only → {@code NoClassDefFoundError: ClickHouseLexer}
 * </ul>
 *
 * <p>The {@code -all} fat jar makes this worse rather than better: it bundles 224 ANTLR classes
 * un-relocated, so a Maven exclusion resolves 4.9.3 correctly while the shaded copies still shadow
 * it. That is a conflict no dependency declaration can express its way out of.
 *
 * <p>The HTTP interface is a first-class ClickHouse API, needs no driver, and has no ANTLR anywhere
 * near it. Since gold and streaming both write through the native Spark connector, dropping the JDBC
 * driver removed the conflict from the project entirely rather than working around it.
 *
 * <h2>Why not just use Spark for reads</h2>
 *
 * <p>Spark cannot express {@code FINAL}. Its parser has no such modifier, so
 * {@code SELECT ... FROM t FINAL} parses {@code FINAL} as a <em>table alias</em>: the query is
 * accepted, runs against the raw table, and silently returns un-deduplicated rows with no error.
 * The same applies to {@code argMax} and {@code OPTIMIZE}. Anything needing ClickHouse's own
 * semantics has to reach ClickHouse.
 */
final class ClickHouseProbe {

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private final URI endpoint;
    private final String user;
    private final String password;
    private final String database;

    ClickHouseProbe(PlatformConfig config) {
        this.database = config.clickhouseDatabase();
        this.user = config.clickhouseUser();
        this.password = config.clickhousePassword();
        this.endpoint = URI.create("http://" + config.clickhouseHost() + ":"
                + config.clickhouseHttpPort() + "/?database=" + database);
    }

    /**
     * Sends SQL and returns the raw response body.
     *
     * <p>Credentials travel as headers rather than query parameters, so they cannot end up in a
     * ClickHouse query log or an access log line.
     */
    private String send(String sql) {
        HttpRequest request = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofSeconds(60))
                .header("X-ClickHouse-User", user)
                .header("X-ClickHouse-Key", password)
                .POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8))
                .build();

        try {
            HttpResponse<String> response =
                    http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "ClickHouse returned " + response.statusCode() + " for: " + sql
                                + "\n  " + response.body());
            }
            return response.body().trim();
        } catch (IOException e) {
            throw new IllegalStateException("HTTP request failed for: " + sql, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while querying ClickHouse", e);
        }
    }

    /** A single long result. */
    long queryLong(String sql) {
        String body = send(sql);
        if (body.isEmpty()) {
            throw new IllegalStateException("query returned no rows: " + sql);
        }
        return Long.parseLong(body.lines().findFirst().orElseThrow().trim());
    }

    void execute(String sql) {
        send(sql);
    }

    String table() {
        return database + "." + StreamIngestJob.SINK_TABLE;
    }

    /** Rows physically present, including duplicates awaiting a merge. */
    long physicalRows(String borough) {
        return queryLong("SELECT count() FROM " + table()
                + " WHERE pickup_borough = '" + borough + "'");
    }

    /** Rows a deduplicated read returns. */
    long finalRows(String borough) {
        return queryLong("SELECT count() FROM " + table() + " FINAL"
                + " WHERE pickup_borough = '" + borough + "'");
    }

    /** The surviving trip count under FINAL. */
    long finalTripCount(String borough) {
        return queryLong("SELECT trip_count FROM " + table() + " FINAL"
                + " WHERE pickup_borough = '" + borough + "'");
    }

    /** Version-aware aggregation, independent of merge state. */
    long argMaxTripCount(String borough) {
        return queryLong("SELECT argMax(trip_count, version) FROM " + table()
                + " WHERE pickup_borough = '" + borough + "'");
    }

    /** Forces a merge so physical state converges to logical. */
    void optimize() {
        execute("OPTIMIZE TABLE " + table() + " FINAL");
    }

    /**
     * Synchronous delete, so cleanup completes before the next test runs.
     *
     * <p>{@code mutations_sync = 1} matters here: an asynchronous mutation would let one test's rows
     * survive into the next, and the resulting cross-contamination reads as a flaky test.
     */
    void deleteBorough(String borough) {
        execute("DELETE FROM " + table() + " WHERE pickup_borough = '" + borough
                + "' SETTINGS mutations_sync = 1");
    }
}
