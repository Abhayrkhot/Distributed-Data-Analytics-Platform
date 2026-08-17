package com.analyticsplatform.common.dao;

import com.analyticsplatform.common.config.PlatformConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Supplies JDBC connections.
 *
 * <p>A functional interface rather than a concrete pool because the control plane is used from
 * Spark drivers, from integration tests, and from the invariant checker, and each wants a
 * different lifetime. It also keeps the DAOs testable against a connection that a test controls.
 *
 * <p>No pooling here on purpose: control-plane writes happen a handful of times per run, from the
 * driver only. A pool would add a lifecycle to manage and a shutdown to get wrong for no
 * measurable gain.
 */
@FunctionalInterface
public interface ConnectionSource {

    /** Opens a new connection. Callers own it and must close it. */
    Connection open() throws SQLException;

    /**
     * Connections to the platform's Postgres control plane.
     *
     * <p>The password is passed through JDBC {@link Properties} rather than embedded in the URL:
     * a URL carrying credentials tends to end up in exception messages and log lines.
     */
    static ConnectionSource postgres(PlatformConfig config) {
        String url = config.postgresUrl();
        Properties properties = new Properties();
        properties.setProperty("user", config.postgresUser());
        properties.setProperty("password", config.postgresPassword());
        properties.setProperty("ApplicationName", "analytics-platform");
        // Bounded, per §52: no production path may wait forever.
        properties.setProperty("connectTimeout", String.valueOf(config.jdbcTimeoutSeconds()));
        properties.setProperty("socketTimeout", String.valueOf(config.jdbcTimeoutSeconds()));

        return () -> DriverManager.getConnection(url, properties);
    }
}
