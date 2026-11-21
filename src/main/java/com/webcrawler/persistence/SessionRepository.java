package com.webcrawler.persistence;

import com.webcrawler.filter.FilterType;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.SessionStatus;
import com.webcrawler.model.StrategyType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SessionRepository {

    private static final String SELECT_COLUMNS = """
            SELECT id, status, completion_reason, visited_count, matched_count, error_count,
                   start_url, search_term, strategy, max_depth, page_limit,
                   whole_word, include_subdomains, enabled_filters, is_distributed, server_port
            FROM crawl_sessions
            """;

    private static final int MAX_COMPLETION_REASON_LENGTH = 255;

    private final DatabaseManager databaseManager;

    public SessionRepository(DatabaseManager databaseManager) {
        this.databaseManager = Objects.requireNonNull(databaseManager, "databaseManager must not be null");
    }

    public CrawlSession create(CrawlSession session) {
        Objects.requireNonNull(session, "session must not be null");
        CrawlSettings settings = Objects.requireNonNull(session.getSettings(), "session.settings must not be null");
        SessionStatus status = Objects.requireNonNull(session.getStatus(), "session.status must not be null");
        StrategyType strategy = Objects.requireNonNull(settings.getStrategy(), "settings.strategy must not be null");
        if (settings.getStartUrl() == null || settings.getStartUrl().isBlank()) {
            throw new IllegalArgumentException("settings.startUrl must not be blank");
        }
        if (settings.getSearchTerm() == null) {
            throw new IllegalArgumentException("settings.searchTerm must not be null");
        }

        String sql = """
                INSERT INTO crawl_sessions (
                    status, completion_reason, visited_count, matched_count, error_count,
                    start_url, search_term, strategy, max_depth, page_limit,
                    whole_word, include_subdomains, enabled_filters, is_distributed, server_port
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        return databaseManager.executeInTransaction(conn -> {
            try (PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                stmt.setString(1, status.name());
                stmt.setString(2, truncate(session.getCompletionReason(), MAX_COMPLETION_REASON_LENGTH));
                stmt.setInt(3, session.getVisitedCount());
                stmt.setInt(4, session.getMatchedCount());
                stmt.setInt(5, session.getErrorCount());
                stmt.setString(6, settings.getStartUrl());
                stmt.setString(7, settings.getSearchTerm());
                stmt.setString(8, strategy.name());
                stmt.setInt(9, settings.getMaxDepth());
                stmt.setInt(10, settings.getPageLimit());
                stmt.setBoolean(11, settings.isWholeWord());
                stmt.setBoolean(12, settings.isIncludeSubdomains());
                stmt.setString(13, serializeFilters(settings.getEnabledFilters()));
                stmt.setBoolean(14, session.isDistributed());
                setNullableInt(stmt, 15, session.getServerPort());

                stmt.executeUpdate();
                try (ResultSet keys = stmt.getGeneratedKeys()) {
                    if (!keys.next()) {
                        throw new IllegalStateException("Failed to retrieve generated id for crawl_sessions");
                    }
                    session.setId(keys.getLong(1));
                }
                return session;
            }
        });
    }

    public void update(CrawlSession session) {
        Objects.requireNonNull(session, "session must not be null");
        if (session.getId() <= 0) {
            throw new IllegalArgumentException("session.id must be positive, got " + session.getId());
        }
        SessionStatus status = Objects.requireNonNull(session.getStatus(), "session.status must not be null");

        String sql = """
                UPDATE crawl_sessions
                SET status = ?,
                    completion_reason = ?,
                    visited_count = ?,
                    matched_count = ?,
                    error_count = ?,
                    is_distributed = ?,
                    server_port = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """;

        databaseManager.executeInTransaction(conn -> {
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, status.name());
                stmt.setString(2, truncate(session.getCompletionReason(), MAX_COMPLETION_REASON_LENGTH));
                stmt.setInt(3, session.getVisitedCount());
                stmt.setInt(4, session.getMatchedCount());
                stmt.setInt(5, session.getErrorCount());
                stmt.setBoolean(6, session.isDistributed());
                setNullableInt(stmt, 7, session.getServerPort());
                stmt.setLong(8, session.getId());

                int updated = stmt.executeUpdate();
                if (updated == 0) {
                    throw new IllegalStateException("Crawl session not found for update: id=" + session.getId());
                }
                return null;
            }
        });
    }

    public Optional<CrawlSession> findById(long sessionId) {
        String sql = SELECT_COLUMNS + " WHERE id = ?";
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, sessionId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to find crawl session by id=" + sessionId, ex);
        }
    }

    public List<CrawlSession> findAllOrderByCreatedDesc() {
        String sql = SELECT_COLUMNS + " ORDER BY created_at DESC, id DESC";
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            List<CrawlSession> sessions = new ArrayList<>();
            while (rs.next()) {
                sessions.add(mapRow(rs));
            }
            return Collections.unmodifiableList(sessions);
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to list crawl sessions", ex);
        }
    }

    public List<Long> findPausedSessionIds() {
        String sql = "SELECT id FROM crawl_sessions WHERE status = ? ORDER BY updated_at DESC, id DESC";
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, SessionStatus.PAUSED.name());
            try (ResultSet rs = stmt.executeQuery()) {
                List<Long> ids = new ArrayList<>();
                while (rs.next()) {
                    ids.add(rs.getLong("id"));
                }
                return Collections.unmodifiableList(ids);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to list paused session IDs", ex);
        }
    }

    public boolean hasAnySessions() {
        String sql = "SELECT 1 FROM crawl_sessions LIMIT 1";
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            return rs.next();
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to check whether any sessions exist", ex);
        }
    }

    private static CrawlSession mapRow(ResultSet rs) throws SQLException {
        long id = rs.getLong("id");
        CrawlSettings settings = new CrawlSettings(
                rs.getString("start_url"),
                rs.getString("search_term"),
                StrategyType.valueOf(rs.getString("strategy")),
                rs.getInt("max_depth"),
                rs.getInt("page_limit"),
                rs.getBoolean("whole_word"),
                rs.getBoolean("include_subdomains"),
                parseFilters(rs.getString("enabled_filters"))
        );

        CrawlSession session = new CrawlSession(id, settings);
        session.setStatus(SessionStatus.valueOf(rs.getString("status")));
        session.setCompletionReason(rs.getString("completion_reason"));
        session.setVisitedCount(rs.getInt("visited_count"));
        session.setMatchedCount(rs.getInt("matched_count"));
        session.setErrorCount(rs.getInt("error_count"));
        session.setDistributed(rs.getBoolean("is_distributed"));

        int rawPort = rs.getInt("server_port");
        session.setServerPort(rs.wasNull() ? null : rawPort);
        return session;
    }

    private static String serializeFilters(Set<FilterType> filters) {
        if (filters == null || filters.isEmpty()) {
            return "";
        }
        return filters.stream()
                .map(FilterType::name)
                .collect(Collectors.joining(","));
    }

    private static Set<FilterType> parseFilters(String serialized) {
        if (serialized == null || serialized.isBlank()) {
            return EnumSet.noneOf(FilterType.class);
        }
        EnumSet<FilterType> result = EnumSet.noneOf(FilterType.class);
        for (String token : serialized.split(",")) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                result.add(FilterType.valueOf(trimmed));
            }
        }
        return result;
    }

    private static void setNullableInt(PreparedStatement stmt, int index, Integer value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, Types.INTEGER);
        } else {
            stmt.setInt(index, value);
        }
    }

    private static String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }
}
