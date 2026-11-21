package com.webcrawler.core;

import com.webcrawler.model.HostStatistics;
import com.webcrawler.persistence.DatabaseManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StatisticsService {

    public static final String KEY_VISITED = "visited";
    public static final String KEY_MATCHED = "matched";
    public static final String KEY_ERRORS = "errors";
    public static final String KEY_TOTAL_BYTES = "totalBytes";

    private static final String ERROR_PREDICATE =
            "(p.error_text IS NOT NULL AND TRIM(p.error_text) <> '') OR p.http_status >= 400";

    private final DatabaseManager databaseManager;

    public StatisticsService(DatabaseManager databaseManager) {
        this.databaseManager = Objects.requireNonNull(databaseManager, "databaseManager must not be null");
    }

    public Map<String, Long> getSessionSummary(long sessionId) {
        return fetchSummary(sessionId);
    }

    public Map<String, Long> getOverallSummary() {
        return fetchSummary(null);
    }

    public Map<String, Long> getHostDistribution() {
        return fetchHostDistribution(null);
    }

    public Map<String, Long> getHostDistribution(long sessionId) {
        return fetchHostDistribution(sessionId);
    }

    public List<HostStatistics> getHostMetrics(Long sessionId) {
        StringBuilder sql = new StringBuilder("""
                SELECT s.host,
                       COUNT(p.id) AS visited_count,
                       COALESCE(SUM(CASE WHEN p.has_term THEN 1 ELSE 0 END), 0) AS matched_count,
                       COALESCE(SUM(CASE WHEN\s""").append(ERROR_PREDICATE).append("""
                 THEN 1 ELSE 0 END), 0) AS error_count,
                       COALESCE(SUM(p.size_bytes), 0) AS total_bytes
                FROM sites s
                INNER JOIN pages p ON p.site_id = s.id
                """);
        if (sessionId != null) {
            sql.append(" WHERE p.session_id = ?");
        }
        sql.append(" GROUP BY s.id, s.host ORDER BY visited_count DESC, s.host ASC");

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            if (sessionId != null) {
                stmt.setLong(1, sessionId);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                List<HostStatistics> metrics = new ArrayList<>();
                while (rs.next()) {
                    metrics.add(new HostStatistics(
                            rs.getString("host"),
                            rs.getLong("visited_count"),
                            rs.getLong("matched_count"),
                            rs.getLong("error_count"),
                            rs.getLong("total_bytes")
                    ));
                }
                return Collections.unmodifiableList(metrics);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to compute host metrics for sessionId=" + sessionId, ex);
        }
    }

    public boolean hasAnySessions() {
        String sql = "SELECT 1 FROM crawl_sessions LIMIT 1";
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            return rs.next();
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to check session existence", ex);
        }
    }

    private Map<String, Long> fetchSummary(Long sessionId) {
        StringBuilder sql = new StringBuilder("""
                SELECT COUNT(*) AS visited_count,
                       COALESCE(SUM(CASE WHEN p.has_term THEN 1 ELSE 0 END), 0) AS matched_count,
                       COALESCE(SUM(CASE WHEN\s""").append(ERROR_PREDICATE).append("""
                 THEN 1 ELSE 0 END), 0) AS error_count,
                       COALESCE(SUM(p.size_bytes), 0) AS total_bytes
                FROM pages p
                """);
        if (sessionId != null) {
            sql.append(" WHERE p.session_id = ?");
        }

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            if (sessionId != null) {
                stmt.setLong(1, sessionId);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                Map<String, Long> summary = new LinkedHashMap<>();
                if (rs.next()) {
                    summary.put(KEY_VISITED, rs.getLong("visited_count"));
                    summary.put(KEY_MATCHED, rs.getLong("matched_count"));
                    summary.put(KEY_ERRORS, rs.getLong("error_count"));
                    summary.put(KEY_TOTAL_BYTES, rs.getLong("total_bytes"));
                } else {
                    summary.put(KEY_VISITED, 0L);
                    summary.put(KEY_MATCHED, 0L);
                    summary.put(KEY_ERRORS, 0L);
                    summary.put(KEY_TOTAL_BYTES, 0L);
                }
                return Collections.unmodifiableMap(summary);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to compute summary statistics for sessionId=" + sessionId, ex);
        }
    }

    private Map<String, Long> fetchHostDistribution(Long sessionId) {
        StringBuilder sql = new StringBuilder("""
                SELECT s.host, COUNT(p.id) AS visited_count
                FROM sites s
                INNER JOIN pages p ON p.site_id = s.id
                """);
        if (sessionId != null) {
            sql.append(" WHERE p.session_id = ?");
        }
        sql.append(" GROUP BY s.id, s.host ORDER BY visited_count DESC, s.host ASC");

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            if (sessionId != null) {
                stmt.setLong(1, sessionId);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                Map<String, Long> distribution = new LinkedHashMap<>();
                while (rs.next()) {
                    distribution.put(rs.getString("host"), rs.getLong("visited_count"));
                }
                return Collections.unmodifiableMap(distribution);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to compute host distribution for sessionId=" + sessionId, ex);
        }
    }
}
