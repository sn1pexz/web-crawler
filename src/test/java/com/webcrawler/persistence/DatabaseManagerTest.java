package com.webcrawler.persistence;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseManagerTest {

    @Test
    void initializesAllFiveTablesIdempotentlyInMemory() throws SQLException {
        String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
        try (DatabaseManager db = new DatabaseManager(url)) {
            db.initializeSchema();
            db.initializeSchema();

            Set<String> tables = new HashSet<>();
            try (Connection conn = db.getConnection()) {
                DatabaseMetaData meta = conn.getMetaData();
                try (ResultSet rs = meta.getTables(null, "PUBLIC", "%", new String[]{"TABLE"})) {
                    while (rs.next()) {
                        tables.add(rs.getString("TABLE_NAME"));
                    }
                }
            }

            assertTrue(tables.containsAll(Set.of(
                    "CRAWL_SESSIONS",
                    "SITES",
                    "PAGES",
                    "STATE_SNAPSHOTS",
                    "CACHE_ENTRIES"
            )));
        }
    }

    @Test
    void executeInTransactionCommitsAndRollsBackAtomically() {
        String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
        try (DatabaseManager db = new DatabaseManager(url)) {
            db.initializeSchema();

            Long insertedId = db.executeInTransaction(conn -> {
                try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO sites (host) VALUES ('committed.example')")) {
                    stmt.executeUpdate();
                }
                try (PreparedStatement query = conn.prepareStatement("SELECT id FROM sites WHERE host = 'committed.example'");
                     ResultSet rs = query.executeQuery()) {
                    assertTrue(rs.next());
                    return rs.getLong(1);
                }
            });
            assertTrue(insertedId > 0);

            assertThrows(IllegalStateException.class, () -> db.executeInTransaction(conn -> {
                try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO sites (host) VALUES ('rolledback.example')")) {
                    stmt.executeUpdate();
                }
                throw new SQLException("Simulated DB failure");
            }));

            int count = db.executeInTransaction(conn -> {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) FROM sites");
                     ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    return rs.getInt(1);
                }
            });
            assertEquals(1, count);
        }
    }
}
