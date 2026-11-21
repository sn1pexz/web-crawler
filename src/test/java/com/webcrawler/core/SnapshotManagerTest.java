package com.webcrawler.core;

import com.webcrawler.filter.FilterType;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.QueueEntry;
import com.webcrawler.model.SessionStatus;
import com.webcrawler.model.StateSnapshot;
import com.webcrawler.model.StrategyType;
import com.webcrawler.persistence.DatabaseManager;
import com.webcrawler.persistence.SessionRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotManagerTest {

    private DatabaseManager db;
    private SessionRepository sessionRepository;
    private SnapshotManager snapshotManager;

    @BeforeEach
    void setUp() {
        db = new DatabaseManager("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        db.initializeSchema();
        sessionRepository = new SessionRepository(db);
        snapshotManager = new SnapshotManager(db);
    }

    @AfterEach
    void tearDown() {
        db.close();
    }

    @Test
    void saveAndLoadRoundTripsSnapshotAndMarksSessionPaused() {
        CrawlSettings settings = new CrawlSettings(
                "https://example.com/start",
                "алгоритм",
                StrategyType.DFS,
                4,
                150,
                true,
                true,
                EnumSet.of(FilterType.SCRIPT, FilterType.AD, FilterType.NAVIGATION)
        );
        CrawlSession session = new CrawlSession(settings);
        session.start();
        session.setVisitedCount(14);
        session.setMatchedCount(3);
        session.setErrorCount(2);
        sessionRepository.create(session);

        List<QueueEntry> queue = List.of(
                new QueueEntry("https://example.com/a", 1),
                new QueueEntry("https://sub.example.com/b?x=1", 2)
        );
        Map<String, Integer> counters = Map.of(
                StateSnapshot.COUNTER_VISITED, 14,
                StateSnapshot.COUNTER_MATCHED, 3,
                StateSnapshot.COUNTER_ERRORS, 2
        );
        StateSnapshot snapshot = new StateSnapshot(session.getId(), settings, queue, counters);

        snapshotManager.save(snapshot);

        CrawlSession persisted = sessionRepository.findById(session.getId()).orElseThrow();
        assertEquals(SessionStatus.PAUSED, persisted.getStatus());
        assertEquals(14, persisted.getVisitedCount());
        assertEquals(3, persisted.getMatchedCount());
        assertEquals(2, persisted.getErrorCount());

        assertEquals(List.of(session.getId()), snapshotManager.getPausedSessionIds());

        StateSnapshot loaded = snapshotManager.load(session.getId());
        assertEquals(snapshot, loaded);
    }

    @Test
    void saveRollsBackSessionUpdateWhenSnapshotWriteFails() throws Exception {
        CrawlSettings settings = new CrawlSettings("https://example.com", "java");
        CrawlSession session = new CrawlSession(settings);
        session.start();
        session.setVisitedCount(5);
        session.setMatchedCount(1);
        session.setErrorCount(0);
        sessionRepository.create(session);

        try (Connection conn = db.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TRIGGER trg_fail_snapshot
                    BEFORE INSERT ON state_snapshots
                    FOR EACH ROW
                    CALL "com.webcrawler.core.SnapshotManagerTest$FailingTrigger"
                    """);
        }

        StateSnapshot snapshot = new StateSnapshot(
                session.getId(),
                settings,
                List.of(new QueueEntry("https://example.com/next", 1)),
                Map.of(StateSnapshot.COUNTER_VISITED, 99, StateSnapshot.COUNTER_MATCHED, 88, StateSnapshot.COUNTER_ERRORS, 77)
        );

        assertThrows(IllegalStateException.class, () -> snapshotManager.save(snapshot));

        CrawlSession afterRollback = sessionRepository.findById(session.getId()).orElseThrow();
        assertEquals(SessionStatus.RUNNING, afterRollback.getStatus());
        assertEquals(5, afterRollback.getVisitedCount());
        assertEquals(1, afterRollback.getMatchedCount());
        assertEquals(0, afterRollback.getErrorCount());
        assertTrue(snapshotManager.getPausedSessionIds().isEmpty());
    }

    @Test
    void loadThrowsUkrainianExceptionWhenSnapshotMissingOrCorrupted() throws Exception {
        IllegalStateException missingEx = assertThrows(IllegalStateException.class, () -> snapshotManager.load(12345L));
        assertTrue(missingEx.getMessage().contains("Знімок стану"));

        CrawlSession session = sessionRepository.create(new CrawlSession(new CrawlSettings("https://example.com", "java")));
        insertRawSnapshot(session.getId(), "{not-valid-json", "[]", "{}");
        assertThrows(IllegalStateException.class, () -> snapshotManager.load(session.getId()));

        insertRawSnapshot(session.getId(),
                "{\"startUrl\":\"not-a-url\",\"searchTerm\":\"java\",\"strategy\":\"BFS\",\"maxDepth\":2,\"pageLimit\":10,\"enabledFilters\":[]}",
                "[]",
                "{}");
        assertThrows(IllegalStateException.class, () -> snapshotManager.load(session.getId()));

        insertRawSnapshot(session.getId(),
                "{\"startUrl\":\"https://example.com\",\"searchTerm\":\"java\",\"strategy\":\"BFS\",\"maxDepth\":2,\"pageLimit\":10,\"enabledFilters\":[\"BOGUS\"]}",
                "[]",
                "{}");
        assertThrows(IllegalStateException.class, () -> snapshotManager.load(session.getId()));

        insertRawSnapshot(session.getId(),
                "{\"startUrl\":\"https://example.com\",\"searchTerm\":\"java\",\"strategy\":\"BFS\",\"maxDepth\":2,\"pageLimit\":10,\"enabledFilters\":[\"SCRIPT\"]}",
                "[{\"url\":\"https://example.com\",\"depth\":-1}]",
                "{}");
        assertThrows(IllegalStateException.class, () -> snapshotManager.load(session.getId()));

        insertRawSnapshot(session.getId(),
                "{\"startUrl\":\"https://example.com\",\"searchTerm\":\"java\",\"strategy\":\"BFS\",\"maxDepth\":2,\"pageLimit\":10,\"enabledFilters\":[\"SCRIPT\"]}",
                "[]",
                "{\"visited\":-3}");
        assertThrows(IllegalStateException.class, () -> snapshotManager.load(session.getId()));
    }

    private void insertRawSnapshot(long sessionId, String settingsJson, String queueJson, String countersJson)
            throws SQLException {
        String sql = """
                MERGE INTO state_snapshots (session_id, settings_json, queue_json, counters_json, saved_at)
                KEY (session_id)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """;
        try (Connection conn = db.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, sessionId);
            stmt.setString(2, settingsJson);
            stmt.setString(3, queueJson);
            stmt.setString(4, countersJson);
            stmt.executeUpdate();
        }
    }

    public static class FailingTrigger implements org.h2.api.Trigger {
        @Override
        public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
            throw new SQLException("Simulated snapshot write failure");
        }
    }
}
