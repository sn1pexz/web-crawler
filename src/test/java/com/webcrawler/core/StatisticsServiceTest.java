package com.webcrawler.core;

import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.HostStatistics;
import com.webcrawler.model.Page;
import com.webcrawler.persistence.DatabaseManager;
import com.webcrawler.persistence.PageRepository;
import com.webcrawler.persistence.SessionRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatisticsServiceTest {

    private DatabaseManager db;
    private SessionRepository sessionRepository;
    private PageRepository pageRepository;
    private StatisticsService statisticsService;

    @BeforeEach
    void setUp() {
        db = new DatabaseManager("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        db.initializeSchema();
        sessionRepository = new SessionRepository(db);
        pageRepository = new PageRepository(db);
        statisticsService = new StatisticsService(db);
    }

    @AfterEach
    void tearDown() {
        db.close();
    }

    @Test
    void emptyDatabaseReturnsZeroSummariesAndNoSessions() {
        assertFalse(statisticsService.hasAnySessions());

        Map<String, Long> overall = statisticsService.getOverallSummary();
        assertEquals(Map.of(
                StatisticsService.KEY_VISITED, 0L,
                StatisticsService.KEY_MATCHED, 0L,
                StatisticsService.KEY_ERRORS, 0L,
                StatisticsService.KEY_TOTAL_BYTES, 0L
        ), overall);

        assertEquals(overall, statisticsService.getSessionSummary(1L));
        assertTrue(statisticsService.getHostDistribution().isEmpty());
        assertTrue(statisticsService.getHostDistribution(1L).isEmpty());
        assertTrue(statisticsService.getHostMetrics(null).isEmpty());
        assertTrue(statisticsService.getHostMetrics(1L).isEmpty());
    }

    @Test
    void computesSessionCumulativeHostDistributionAndMetricsViaSql() {
        CrawlSession s1 = sessionRepository.create(new CrawlSession(new CrawlSettings("https://alpha.example", "java")));
        CrawlSession s2 = sessionRepository.create(new CrawlSession(new CrawlSettings("https://beta.example", "spring")));
        assertTrue(statisticsService.hasAnySessions());

        savePage(s1.getId(), "alpha.example", "https://alpha.example/1", 1000L, 200, null, true);
        savePage(s1.getId(), "alpha.example", "https://alpha.example/2", 500L, 200, null, false);
        savePage(s1.getId(), "beta.example", "https://beta.example/err", 250L, 500, "HTTP 500", false);

        savePage(s2.getId(), "beta.example", "https://beta.example/1", 2000L, 200, null, true);
        savePage(s2.getId(), "gamma.example", "https://gamma.example/timeout", 0L, 0, "Connection timeout", false);

        assertEquals(Map.of(
                StatisticsService.KEY_VISITED, 3L,
                StatisticsService.KEY_MATCHED, 1L,
                StatisticsService.KEY_ERRORS, 1L,
                StatisticsService.KEY_TOTAL_BYTES, 1750L
        ), statisticsService.getSessionSummary(s1.getId()));

        assertEquals(Map.of(
                StatisticsService.KEY_VISITED, 2L,
                StatisticsService.KEY_MATCHED, 1L,
                StatisticsService.KEY_ERRORS, 1L,
                StatisticsService.KEY_TOTAL_BYTES, 2000L
        ), statisticsService.getSessionSummary(s2.getId()));

        assertEquals(Map.of(
                StatisticsService.KEY_VISITED, 5L,
                StatisticsService.KEY_MATCHED, 2L,
                StatisticsService.KEY_ERRORS, 2L,
                StatisticsService.KEY_TOTAL_BYTES, 3750L
        ), statisticsService.getOverallSummary());

        assertEquals(Map.of(
                "alpha.example", 2L,
                "beta.example", 1L
        ), statisticsService.getHostDistribution(s1.getId()));

        assertEquals(Map.of(
                "alpha.example", 2L,
                "beta.example", 2L,
                "gamma.example", 1L
        ), statisticsService.getHostDistribution());

        List<HostStatistics> overallMetrics = statisticsService.getHostMetrics(null);
        assertEquals(List.of(
                new HostStatistics("alpha.example", 2L, 1L, 0L, 1500L),
                new HostStatistics("beta.example", 2L, 1L, 1L, 2250L),
                new HostStatistics("gamma.example", 1L, 0L, 1L, 0L)
        ), overallMetrics);

        List<HostStatistics> s1Metrics = statisticsService.getHostMetrics(s1.getId());
        assertEquals(List.of(
                new HostStatistics("alpha.example", 2L, 1L, 0L, 1500L),
                new HostStatistics("beta.example", 1L, 0L, 1L, 250L)
        ), s1Metrics);
    }

    private void savePage(
            long sessionId,
            String host,
            String url,
            long sizeBytes,
            int httpStatus,
            String errorText,
            boolean hasTerm) {
        Page page = new Page(
                0L,
                sessionId,
                0L,
                url,
                "Title",
                LocalDateTime.now(),
                sizeBytes,
                0,
                httpStatus,
                errorText,
                hasTerm,
                hasTerm ? host + "/index.html" : null
        );
        pageRepository.save(page, host);
    }
}
