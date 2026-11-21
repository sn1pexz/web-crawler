package com.webcrawler.persistence;

import com.webcrawler.config.AppPaths;
import org.h2.tools.RunScript;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseManager implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(DatabaseManager.class.getName());
    private static final String SCHEMA_RESOURCE = "/db/schema.sql";

    private final String jdbcUrl;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @FunctionalInterface
    public interface SqlTransactionWork<T> {
        T execute(Connection connection) throws SQLException;
    }

    public DatabaseManager(AppPaths appPaths) {
        Objects.requireNonNull(appPaths, "appPaths must not be null");
        appPaths.ensureDirectoriesExist();
        this.jdbcUrl = appPaths.getJdbcUrl();
    }

    public DatabaseManager(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("jdbcUrl must not be blank");
        }
        this.jdbcUrl = jdbcUrl.trim();
    }

    public synchronized void initializeSchema() {
        ensureOpen();
        try (InputStream schemaStream = DatabaseManager.class.getResourceAsStream(SCHEMA_RESOURCE)) {
            if (schemaStream == null) {
                throw new IllegalStateException("Database schema resource not found: " + SCHEMA_RESOURCE);
            }
            try (Connection conn = getConnection();
                 InputStreamReader reader = new InputStreamReader(schemaStream, StandardCharsets.UTF_8)) {
                RunScript.execute(conn, reader);
            }
            LOGGER.fine("Database schema initialized successfully");
        } catch (IOException | SQLException ex) {
            throw new IllegalStateException("Failed to initialize database schema", ex);
        }
    }

    public Connection getConnection() throws SQLException {
        ensureOpen();
        return DriverManager.getConnection(jdbcUrl);
    }

    //Atomic transaction with rollback on any failure
    public <T> T executeInTransaction(SqlTransactionWork<T> work) {
        Objects.requireNonNull(work, "work must not be null");
        ensureOpen();
        try (Connection conn = getConnection()) {
            boolean previousAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                T result = work.execute(conn);
                conn.commit();
                return result;
            } catch (Throwable ex) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackEx) {
                    ex.addSuppressed(rollbackEx);
                }
                if (ex instanceof RuntimeException runtimeEx) {
                    throw runtimeEx;
                }
                if (ex instanceof Error error) {
                    throw error;
                }
                throw new IllegalStateException("Database transaction failed", ex);
            } finally {
                try {
                    conn.setAutoCommit(previousAutoCommit);
                } catch (SQLException resetEx) {
                    LOGGER.log(Level.FINE, "Failed to reset autoCommit on connection", resetEx);
                }
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to acquire or close database connection", ex);
        }
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement()) {
            stmt.execute("SHUTDOWN");
        } catch (SQLException ex) {
            LOGGER.log(Level.FINE, "Database shutdown completed with notice", ex);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("DatabaseManager is already closed");
        }
    }
}
