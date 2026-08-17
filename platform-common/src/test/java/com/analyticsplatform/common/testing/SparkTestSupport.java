package com.analyticsplatform.common.testing;

import org.apache.spark.sql.SparkSession;

/**
 * A shared local Spark session for component tests.
 *
 * <p>One session per JVM, created lazily and never stopped. Starting a session costs seconds;
 * doing it per test class would dominate the runtime of a suite that is supposed to stay fast
 * enough to run on every save.
 *
 * <p>Configured for determinism rather than speed: UTC everywhere so timestamps do not depend on
 * the developer's machine, and a small fixed shuffle-partition count so output ordering does not
 * drift between runs. Adaptive execution is off here — it coalesces partitions in ways that vary
 * with data size, which is exactly the nondeterminism §47 hunts for.
 */
public final class SparkTestSupport {

    private static volatile SparkSession session;

    private SparkTestSupport() {
    }

    public static SparkSession spark() {
        SparkSession local = session;
        if (local == null) {
            synchronized (SparkTestSupport.class) {
                local = session;
                if (local == null) {
                    local = SparkSession.builder()
                            .appName("platform-component-tests")
                            .master("local[2]")
                            .config("spark.sql.session.timeZone", "UTC")
                            .config("spark.sql.shuffle.partitions", "2")
                            .config("spark.sql.adaptive.enabled", "false")
                            .config("spark.ui.enabled", "false")
                            // Keeps Derby metastore noise out of the module directory.
                            .config("spark.sql.catalogImplementation", "in-memory")
                            .getOrCreate();
                    local.sparkContext().setLogLevel("WARN");
                    session = local;
                }
            }
        }
        return local;
    }
}
