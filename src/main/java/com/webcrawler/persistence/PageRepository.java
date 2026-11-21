package com.webcrawler.persistence;

import com.webcrawler.model.Page;
import com.webcrawler.model.Site;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class PageRepository {

    private static final String SQL_STATE_UNIQUE_VIOLATION = "23505";
    private static final int MAX_TITLE_LENGTH = 1024;
    private static final int MAX_ERROR_LENGTH = 2048;
    private static final int MAX_FILE_PATH_LENGTH = 1024;

    private final DatabaseManager databaseManager;

    public PageRepository(DatabaseManager databaseManager) {
        this.databaseManager = Objects.requireNonNull(databaseManager, "databaseManager must not be null");
    }

    public long getOrCreateSiteId(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        String normalizedHost = host.trim().toLowerCase(Locale.ROOT);

        return databaseManager.executeInTransaction(conn -> {
            Optional<Long> existing = findSiteId(conn, normalizedHost);
            if (existing.isPresent()) {
                return existing.get();
            }
            try {
                return insertSite(conn, normalizedHost);
            } catch (SQLException ex) {
                if (isUniqueViolation(ex)) {
                    conn.rollback();
                    return findSiteId(conn, normalizedHost)
                            .orElseThrow(() -> new IllegalStateException(
                                    "Failed to locate site after concurrent insert for host=" + normalizedHost, ex));
                }
                throw ex;
            }
        });
    }

    public void save(Page page, String host) {
        Objects.requireNonNull(page, "page must not be null");
        if (page.getSessionId() <= 0) {
            throw new IllegalArgumentException("page.sessionId must be positive, got " + page.getSessionId());
        }
        if (page.getUrl() == null || page.getUrl().isBlank()) {
            throw new IllegalArgumentException("page.url must not be blank");
        }
        if (page.getDepth() < 0) {
            throw new IllegalArgumentException("page.depth must be >= 0, got " + page.getDepth());
        }

        long siteId = getOrCreateSiteId(host);
        page.setSiteId(siteId);
        LocalDateTime downloadedAt = page.getDownloadedAt() != null ? page.getDownloadedAt() : LocalDateTime.now();
        page.setDownloadedAt(downloadedAt);

        String sql = """
                MERGE INTO pages (
                    session_id, site_id, url, title, downloaded_at,
                    size_bytes, depth, http_status, error_text, has_term, file_path
                ) KEY (session_id, url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        long pageId = databaseManager.executeInTransaction(conn -> {
            SQLException lastUniqueViolation = null;
            for (int attempt = 0; attempt < 2; attempt++) {
                try (PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                    stmt.setLong(1, page.getSessionId());
                    stmt.setLong(2, siteId);
                    stmt.setString(3, page.getUrl());
                    stmt.setString(4, truncate(page.getTitle(), MAX_TITLE_LENGTH));
                    stmt.setTimestamp(5, Timestamp.valueOf(downloadedAt));
                    stmt.setLong(6, page.getSize());
                    stmt.setInt(7, page.getDepth());
                    stmt.setInt(8, page.getHttpStatus());
                    stmt.setString(9, truncate(page.getError(), MAX_ERROR_LENGTH));
                    stmt.setBoolean(10, page.isHasTerm());
                    stmt.setString(11, truncate(page.getFilePath(), MAX_FILE_PATH_LENGTH));

                    stmt.executeUpdate();
                    try (ResultSet keys = stmt.getGeneratedKeys()) {
                        if (keys.next()) {
                            long generated = keys.getLong(1);
                            if (generated > 0) {
                                return generated;
                            }
                        }
                    }
                    return findPageId(conn, page.getSessionId(), page.getUrl());
                } catch (SQLException ex) {
                    if (isUniqueViolation(ex)) {
                        lastUniqueViolation = ex;
                        conn.rollback();
                        continue;
                    }
                    throw ex;
                }
            }
            throw new IllegalStateException("Concurrent conflict saving page url=" + page.getUrl(), lastUniqueViolation);
        });
        page.setId(pageId);
    }

    public List<Page> findBySessionId(long sessionId) {
        String sql = """
                SELECT id, session_id, site_id, url, title, downloaded_at,
                       size_bytes, depth, http_status, error_text, has_term, file_path
                FROM pages
                WHERE session_id = ?
                ORDER BY id ASC
                """;
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, sessionId);
            try (ResultSet rs = stmt.executeQuery()) {
                List<Page> pages = new ArrayList<>();
                while (rs.next()) {
                    pages.add(mapPage(rs));
                }
                return Collections.unmodifiableList(pages);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to load pages for sessionId=" + sessionId, ex);
        }
    }

    public Set<String> findVisitedUrlsBySessionId(long sessionId) {
        String sql = "SELECT url FROM pages WHERE session_id = ? ORDER BY id ASC";
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, sessionId);
            try (ResultSet rs = stmt.executeQuery()) {
                Set<String> urls = new LinkedHashSet<>();
                while (rs.next()) {
                    urls.add(rs.getString("url"));
                }
                return Collections.unmodifiableSet(urls);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to load visited URLs for sessionId=" + sessionId, ex);
        }
    }

    public List<Site> findAllSites() {
        String sql = "SELECT id, host FROM sites ORDER BY host ASC";
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            List<Site> sites = new ArrayList<>();
            while (rs.next()) {
                sites.add(new Site(rs.getLong("id"), rs.getString("host")));
            }
            return Collections.unmodifiableList(sites);
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to load sites", ex);
        }
    }

    private static Optional<Long> findSiteId(Connection conn, String normalizedHost) throws SQLException {
        String selectSql = "SELECT id FROM sites WHERE host = ?";
        try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
            stmt.setString(1, normalizedHost);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(rs.getLong("id"));
                }
                return Optional.empty();
            }
        }
    }

    private static long insertSite(Connection conn, String normalizedHost) throws SQLException {
        String insertSql = "INSERT INTO sites (host) VALUES (?)";
        try (PreparedStatement stmt = conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
            stmt.setString(1, normalizedHost);
            stmt.executeUpdate();
            try (ResultSet keys = stmt.getGeneratedKeys()) {
                if (!keys.next()) {
                    throw new IllegalStateException("Failed to retrieve generated id for sites host=" + normalizedHost);
                }
                return keys.getLong(1);
            }
        }
    }

    private static long findPageId(Connection conn, long sessionId, String url) throws SQLException {
        String sql = "SELECT id FROM pages WHERE session_id = ? AND url = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, sessionId);
            stmt.setString(2, url);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("Saved page row not found for sessionId=" + sessionId + ", url=" + url);
                }
                return rs.getLong("id");
            }
        }
    }

    private static Page mapPage(ResultSet rs) throws SQLException {
        Timestamp ts = rs.getTimestamp("downloaded_at");
        LocalDateTime downloadedAt = ts != null ? ts.toLocalDateTime() : null;
        return new Page(
                rs.getLong("id"),
                rs.getLong("session_id"),
                rs.getLong("site_id"),
                rs.getString("url"),
                rs.getString("title"),
                downloadedAt,
                rs.getLong("size_bytes"),
                rs.getInt("depth"),
                rs.getInt("http_status"),
                rs.getString("error_text"),
                rs.getBoolean("has_term"),
                rs.getString("file_path")
        );
    }

    private static boolean isUniqueViolation(SQLException ex) {
        return SQL_STATE_UNIQUE_VIOLATION.equals(ex.getSQLState());
    }

    private static String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }
}
