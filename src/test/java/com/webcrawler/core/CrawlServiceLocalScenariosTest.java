package com.webcrawler.core;

import com.webcrawler.config.AppPaths;
import com.webcrawler.filter.FilterType;
import com.webcrawler.loader.DomainRateLimiter;
import com.webcrawler.loader.LoadResult;
import com.webcrawler.loader.PageLoadException;
import com.webcrawler.loader.PageLoader;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.HostStatistics;
import com.webcrawler.model.Page;
import com.webcrawler.model.SessionStatus;
import com.webcrawler.model.Site;
import com.webcrawler.model.StrategyType;
import com.webcrawler.persistence.DatabaseManager;
import com.webcrawler.processing.LinkExtractor;
import com.webcrawler.processing.TermSearcher;
import com.webcrawler.storage.PathSanitizer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrawlServiceLocalScenariosTest {

    private static final String LONG_TEXT =
            "Це змістовний український текст наукової статті про веб-сканування, індексування документів та надійний пошук інформації. ";

    @TempDir
    Path tempDir;

    private final List<CrawlService> servicesToShutdown = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (CrawlService service : servicesToShutdown) {
            try {
                service.shutdown();
            } catch (Exception ignored) {
            }
        }
        servicesToShutdown.clear();
    }

    private CrawlService createFileBackedService(Path dataDir, PageLoader loader, int poolSize) {
        AppPaths paths = new AppPaths(dataDir);
        DatabaseManager db = new DatabaseManager(paths);
        CrawlService service = new CrawlService(
                paths,
                db,
                loader,
                new DomainRateLimiter(0L),
                null,
                new LinkExtractor(),
                new TermSearcher(),
                poolSize,
                10,
                50L,
                500L,
                1000L,
                40L,
                50L
        );
        servicesToShutdown.add(service);
        return service;
    }

    @Test
    void scenario1RejectsInvalidSettingsWithUkrainianErrorsAndStartsValidSessionAfterwards() throws Exception {
        StubPageLoader loader = new StubPageLoader(Map.of(
                "https://example.com/", StubPageLoader.ok(html("OK", "<p>Знайдений термін програмування: " + LONG_TEXT + "</p>"))
        ));
        CrawlService service = createFileBackedService(tempDir.resolve("s1-invalid"), loader, 2);
        List<String> statusMessages = new CopyOnWriteArrayList<>();
        service.addEventListener(new CrawlEventListener() {
            @Override
            public void onStatusMessage(String message) {
                statusMessages.add(message);
            }
        });

        CrawlSettings badUrl = new CrawlSettings("ftp://not-http.example", "програмування");
        IllegalArgumentException urlEx = assertThrows(IllegalArgumentException.class, () -> service.start(badUrl));
        assertTrue(urlEx.getMessage().contains("Некоректна стартова адреса"), urlEx.getMessage());

        CrawlSettings blankTerm = new CrawlSettings("https://example.com", "   ");
        IllegalArgumentException termEx = assertThrows(IllegalArgumentException.class, () -> service.start(blankTerm));
        assertTrue(termEx.getMessage().contains("терміну пошуку"), termEx.getMessage());

        CrawlSettings badDepth = new CrawlSettings("https://example.com", "java", StrategyType.BFS, -1, 10, false, false, FilterType.defaultEnabled());
        assertThrows(IllegalArgumentException.class, () -> service.start(badDepth));

        CrawlSettings badLimit = new CrawlSettings("https://example.com", "java", StrategyType.BFS, 1, 0, false, false, FilterType.defaultEnabled());
        assertThrows(IllegalArgumentException.class, () -> service.start(badLimit));

        assertFalse(service.hasAnySessions());
        assertFalse(statusMessages.isEmpty());

        CountDownLatch finished = new CountDownLatch(1);
        service.addEventListener(finishedListener(finished));
        CrawlSession valid = service.start(new CrawlSettings("https://example.com", "програмування"));
        assertTrue(finished.await(5, TimeUnit.SECONDS));
        assertEquals(SessionStatus.FINISHED, valid.getStatus());
        assertEquals(1, valid.getVisitedCount());
        assertEquals(1, valid.getMatchedCount());
    }

    @Test
    void scenario1ExecutesBfsAndDfsStrategiesFiltersErrorsRobotsAndSavesCompositeTree() throws Exception {
        Map<String, StubPageLoader.Response> graph = Map.of(
                "https://graph.test/", StubPageLoader.ok(html("Root", "<p>" + LONG_TEXT + "</p><a href='/a'>A</a><a href='/b'>B</a>")),
                "https://graph.test/a", StubPageLoader.ok(html("A", "<p>" + LONG_TEXT + "</p><a href='/a/1'>A1</a>")),
                "https://graph.test/a/1", StubPageLoader.ok(html("A1", "<p>ціль " + LONG_TEXT + "</p>")),
                "https://graph.test/b", StubPageLoader.ok(html("B", "<p>ціль " + LONG_TEXT + "</p><a href='/b/1'>B1</a>")),
                "https://graph.test/b/1", StubPageLoader.ok(html("B1", "<p>" + LONG_TEXT + "</p>"))
        );

        StubPageLoader bfsLoader = new StubPageLoader(graph);
        CrawlService bfsService = createFileBackedService(tempDir.resolve("bfs"), bfsLoader, 1);
        CountDownLatch bfsDone = new CountDownLatch(1);
        bfsService.addEventListener(finishedListener(bfsDone));
        CrawlSettings bfsSettings = new CrawlSettings("https://graph.test/", "ціль", StrategyType.BFS, 2, 3, false, false, FilterType.defaultEnabled());
        CrawlSession bfsSession = bfsService.start(bfsSettings);
        assertTrue(bfsDone.await(5, TimeUnit.SECONDS));

        Set<String> bfsUrls = bfsService.findPagesBySessionId(bfsSession.getId()).stream()
                .map(Page::getUrl).collect(Collectors.toSet());
        assertEquals(Set.of("https://graph.test/", "https://graph.test/a", "https://graph.test/b"), bfsUrls);
        assertEquals(1, bfsSession.getMatchedCount());

        StubPageLoader dfsLoader = new StubPageLoader(graph);
        CrawlService dfsService = createFileBackedService(tempDir.resolve("dfs"), dfsLoader, 1);
        CountDownLatch dfsDone = new CountDownLatch(1);
        dfsService.addEventListener(finishedListener(dfsDone));
        CrawlSettings dfsSettings = new CrawlSettings("https://graph.test/", "ціль", StrategyType.DFS, 2, 3, false, false, FilterType.defaultEnabled());
        CrawlSession dfsSession = dfsService.start(dfsSettings);
        assertTrue(dfsDone.await(5, TimeUnit.SECONDS));

        Set<String> dfsUrls = dfsService.findPagesBySessionId(dfsSession.getId()).stream()
                .map(Page::getUrl).collect(Collectors.toSet());
        assertEquals(Set.of("https://graph.test/", "https://graph.test/a", "https://graph.test/a/1"), dfsUrls);
        assertEquals(1, dfsSession.getMatchedCount());

        String rootHtml = """
                <html><head><title>Головна</title></head><body>
                <p>Вступна сторінка про інформаційний пошук. %s</p>
                <a href="/article">Стаття</a>
                <a href="/short">Коротка</a>
                <a href="/private/hidden">Прихована</a>
                <a href="/broken">Помилка</a>
                <a href="https://sub.site.test/subpage">Піддомен</a>
                <a href="https://external.other.org/">Зовнішній</a>
                </body></html>
                """.formatted(LONG_TEXT);
        String articleHtml = """
                <html><head>
                  <title>Аналіз кирилиці</title>
                  <script>alert('bad');</script>
                  <style>p { color: red; }</style>
                </head><body>
                  <nav>Меню сайту</nav>
                  <div class="ad-banner">Рекламний блок</div>
                  <iframe src="/frame.html"></iframe>
                  <article>
                    <p>Система виконує пошук слова кирилиця у документах і зберігає очищений результат. %s</p>
                    <a id="rel" href="/next?z=2&a=1">Далі</a>
                    <img id="pic" src="/img/logo.png">
                  </article>
                </body></html>
                """.formatted(LONG_TEXT);
        String shortHtml = """
                <html><head><title>Коротка</title></head><body>
                  <nav>%s</nav>
                  <p>Кирилиця тут але мало</p>
                  <a href="/after-short">Після короткої</a>
                </body></html>
                """.formatted("Навігаційний блок ".repeat(15));
        String afterShortHtml = """
                <html><head><title>Після короткої</title></head><body>
                  <p>Кирилиця знайдена на сторінці після короткої сторінки з достатнім обсягом тексту. %s</p>
                </body></html>
                """.formatted(LONG_TEXT);
        String subHtml = """
                <html><head><title>Піддомен</title></head><body>
                  <p>Піддомен також містить слово кирилиця і достатню кількість тексту для збереження. %s</p>
                </body></html>
                """.formatted(LONG_TEXT);

        Map<String, StubPageLoader.Response> siteGraph = new ConcurrentHashMap<>();
        siteGraph.put("https://site.test/", StubPageLoader.ok(rootHtml));
        siteGraph.put("https://site.test/article", StubPageLoader.ok(articleHtml));
        siteGraph.put("https://site.test/short", StubPageLoader.ok(shortHtml));
        siteGraph.put("https://site.test/after-short", StubPageLoader.ok(afterShortHtml));
        siteGraph.put("https://site.test/next?a=1&z=2", StubPageLoader.ok(html("Next", "<p>Тільки текст без цільового слова. " + LONG_TEXT + "</p>")));
        siteGraph.put("https://site.test/private/hidden", StubPageLoader.ok(html("Hidden", "<p>Кирилиця " + LONG_TEXT + "</p>")));
        siteGraph.put("https://site.test/broken", StubPageLoader.error(500, "HTTP 500"));
        siteGraph.put("https://sub.site.test/subpage", StubPageLoader.ok(subHtml));
        siteGraph.put("https://external.other.org/", StubPageLoader.ok(html("Ext", "<p>Кирилиця " + LONG_TEXT + "</p>")));

        StubPageLoader siteLoader = new StubPageLoader(siteGraph, "User-agent: *\nDisallow: /private/\n");
        Path siteDataDir = tempDir.resolve("scenario1-full");
        CrawlService siteService = createFileBackedService(siteDataDir, siteLoader, 2);

        CountDownLatch siteDone = new CountDownLatch(1);
        List<String> errorUrls = new CopyOnWriteArrayList<>();
        List<String> siteMessages = new CopyOnWriteArrayList<>();
        siteService.addEventListener(new CrawlEventListener() {
            @Override
            public void onError(String url, String errorMessage) {
                errorUrls.add(url + " -> " + errorMessage);
            }

            @Override
            public void onStatusMessage(String message) {
                siteMessages.add(message);
            }

            @Override
            public void onSessionFinished(CrawlSession s) {
                siteDone.countDown();
            }
        });

        CrawlSettings fullSettings = new CrawlSettings(
                "https://site.test/",
                "кирилиця",
                StrategyType.BFS,
                2,
                50,
                true,
                true,
                EnumSet.allOf(FilterType.class)
        );
        CrawlSession fullSession = siteService.start(fullSettings);
        assertTrue(siteDone.await(15, TimeUnit.SECONDS),
                "Timed out! status=" + fullSession.getStatus()
                        + ", reason=" + fullSession.getCompletionReason()
                        + ", visited=" + fullSession.getVisitedCount()
                        + ", errors=" + errorUrls
                        + ", messages=" + siteMessages);

        assertEquals(SessionStatus.FINISHED, fullSession.getStatus());
        assertFalse(siteService.isRunning());
        assertEquals(0, siteLoader.requestCount("https://site.test/private/hidden"));
        assertEquals(0, siteLoader.requestCount("https://external.other.org/"));
        assertEquals(1, errorUrls.size());
        assertTrue(errorUrls.get(0).startsWith("https://site.test/broken -> "));

        assertEquals(7, fullSession.getVisitedCount());
        assertEquals(3, fullSession.getMatchedCount());
        assertEquals(1, fullSession.getErrorCount());

        Map<String, Page> byUrl = siteService.findPagesBySessionId(fullSession.getId()).stream()
                .collect(Collectors.toMap(Page::getUrl, p -> p));
        assertFalse(byUrl.get("https://site.test/short").isHasTerm());
        assertNull(byUrl.get("https://site.test/short").getFilePath());
        assertTrue(byUrl.get("https://site.test/after-short").isHasTerm());

        Path articleFile = Path.of(byUrl.get("https://site.test/article").getFilePath());
        assertTrue(Files.exists(articleFile));
        Path expectedArticleFile = new AppPaths(siteDataDir).getResultsDir()
                .resolve(PathSanitizer.sanitize("https://site.test/article").relativePath())
                .toAbsolutePath().normalize();
        assertEquals(expectedArticleFile, articleFile.toAbsolutePath().normalize());

        Document cleanedDoc = Jsoup.parse(Files.readString(articleFile, StandardCharsets.UTF_8));
        assertTrue(cleanedDoc.select("script, style, nav, iframe, .ad-banner").isEmpty());
        assertEquals("https://site.test/next?z=2&a=1", cleanedDoc.selectFirst("#rel").attr("href"));
        assertEquals("https://site.test/img/logo.png", cleanedDoc.selectFirst("#pic").attr("src"));

        Map<String, Site> sites = siteService.getSitesSnapshot();
        assertEquals(Set.of("site.test", "sub.site.test"), sites.keySet());
        assertNotNull(sites.get("site.test").getRootDirectory());
    }

    @Test
    void scenario2PausePersistsSnapshotAndRestartResumesWithoutRefetchingVisitedUrls() throws Exception {
        CountDownLatch p1Started = new CountDownLatch(1);
        CountDownLatch allowP1Finish = new CountDownLatch(1);

        Map<String, StubPageLoader.Response> pages = Map.of(
                "https://resume.test/p1", StubPageLoader.blocking(
                        html("P1", "<p>Сторінка один містить термін пошуку. " + LONG_TEXT + "</p><a href='/p2'>P2</a><a href='/p3'>P3</a>"),
                        p1Started,
                        allowP1Finish),
                "https://resume.test/p2", StubPageLoader.ok(
                        html("P2", "<p>Сторінка два посилається назад на <a href='/p1'>P1</a> без шуканого слова. " + LONG_TEXT + "</p>")),
                "https://resume.test/p3", StubPageLoader.ok(
                        html("P3", "<p>Сторінка три також містить термін пошуку. " + LONG_TEXT + "</p>"))
        );

        StubPageLoader loader1 = new StubPageLoader(pages);
        Path dataDir = tempDir.resolve("scenario2-restart");
        CrawlService service1 = createFileBackedService(dataDir, loader1, 1);

        CountDownLatch pausedLatch = new CountDownLatch(1);
        service1.addEventListener(new CrawlEventListener() {
            @Override
            public void onSessionPaused(CrawlSession session) {
                pausedLatch.countDown();
            }
        });

        CrawlSettings settings = new CrawlSettings("https://resume.test/p1", "термін", StrategyType.BFS, 2, 10, false, false, FilterType.defaultEnabled());
        CrawlSession session = service1.start(settings);
        long sessionId = session.getId();

        assertTrue(p1Started.await(5, TimeUnit.SECONDS));
        Thread pauseCaller = new Thread(service1::pause);
        pauseCaller.start();
        Thread.sleep(60L);
        allowP1Finish.countDown();
        pauseCaller.join(5000L);

        assertTrue(pausedLatch.await(5, TimeUnit.SECONDS));
        assertTrue(service1.isPaused());
        assertFalse(service1.isRunning());
        assertEquals(SessionStatus.PAUSED, session.getStatus());
        assertEquals(List.of(sessionId), service1.getPausedSessions());
        assertEquals(1, loader1.requestCount("https://resume.test/p1"));
        assertEquals(0, loader1.requestCount("https://resume.test/p2"));

        service1.shutdown();
        servicesToShutdown.remove(service1);

        StubPageLoader loader2 = new StubPageLoader(pages);
        CrawlService service2 = createFileBackedService(dataDir, loader2, 2);
        service2.setCacheTtlHours(0);

        assertEquals(List.of(sessionId), service2.getPausedSessions());
        CountDownLatch finishedLatch = new CountDownLatch(1);
        service2.addEventListener(finishedListener(finishedLatch));

        service2.resume(sessionId);
        assertTrue(finishedLatch.await(5, TimeUnit.SECONDS));

        CrawlSession resumed = service2.findSessionById(sessionId).orElseThrow();
        assertEquals(SessionStatus.FINISHED, resumed.getStatus());
        assertEquals(3, resumed.getVisitedCount());
        assertEquals(2, resumed.getMatchedCount());
        assertTrue(service2.getPausedSessions().isEmpty());

        assertEquals(0, loader2.requestCount("https://resume.test/p1"));
        assertEquals(1, loader2.requestCount("https://resume.test/p2"));
        assertEquals(1, loader2.requestCount("https://resume.test/p3"));
    }

    @Test
    void scenario2PauseOnExitSavesSnapshotAndCorruptOrSaveFailureSnapshotsHandleExceptionsCleanly() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Map<String, StubPageLoader.Response> pages = Map.of(
                "https://exit.test/", StubPageLoader.blocking(
                        html("Root", "<p>Термін " + LONG_TEXT + "</p><a href='/child'>Child</a>"), entered, release),
                "https://exit.test/child", StubPageLoader.ok(html("Child", "<p>Термін " + LONG_TEXT + "</p>"))
        );
        Path exitDir = tempDir.resolve("scenario2-exit");
        CrawlService exitService = createFileBackedService(exitDir, new StubPageLoader(pages), 1);
        CrawlSession exitSession = exitService.start(new CrawlSettings("https://exit.test/", "термін"));
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        Thread exitCaller = new Thread(exitService::pauseOnExit);
        exitCaller.start();
        Thread.sleep(60L);
        release.countDown();
        exitCaller.join(5000L);

        assertEquals(List.of(exitSession.getId()), exitService.getPausedSessions());
        assertEquals(SessionStatus.PAUSED, exitService.findSessionById(exitSession.getId()).orElseThrow().getStatus());

        Path corruptDir = tempDir.resolve("scenario2-corrupt");
        StubPageLoader simpleLoader = new StubPageLoader(Map.of(
                "https://corrupt.test/", StubPageLoader.ok(html("OK", "<p>Слово тест " + LONG_TEXT + "</p>"))
        ));
        CrawlService corruptService = createFileBackedService(corruptDir, simpleLoader, 1);

        CountDownLatch e2 = new CountDownLatch(1);
        CountDownLatch r2 = new CountDownLatch(1);
        StubPageLoader slowLoader = new StubPageLoader(Map.of(
                "https://corrupt.test/", StubPageLoader.blocking(
                        html("OK", "<p>Слово тест " + LONG_TEXT + "</p><a href='/p2'>P2</a>"), e2, r2),
                "https://corrupt.test/p2", StubPageLoader.ok(html("P2", "<p>Слово тест " + LONG_TEXT + "</p>"))
        ));
        CrawlService badSnapService = createFileBackedService(tempDir.resolve("scenario2-badsnap"), slowLoader, 1);
        CrawlSession pausedSession = badSnapService.start(new CrawlSettings("https://corrupt.test/", "тест"));
        assertTrue(e2.await(5, TimeUnit.SECONDS));
        Thread pauseThread = new Thread(badSnapService::pause);
        pauseThread.start();
        Thread.sleep(50L);
        r2.countDown();
        pauseThread.join(5000L);

        try (Connection conn = badSnapService.getDatabaseManager().getConnection();
             PreparedStatement stmt = conn.prepareStatement("UPDATE state_snapshots SET queue_json = ? WHERE session_id = ?")) {
            stmt.setString(1, "{corrupted-json");
            stmt.setLong(2, pausedSession.getId());
            stmt.executeUpdate();
        }

        List<String> errorNotifications = new CopyOnWriteArrayList<>();
        badSnapService.addEventListener(new CrawlEventListener() {
            @Override
            public void onError(String url, String errorMessage) {
                errorNotifications.add(errorMessage);
            }
        });

        IllegalStateException corruptEx = assertThrows(IllegalStateException.class,
                () -> badSnapService.resume(pausedSession.getId()));
        assertTrue(corruptEx.getMessage().contains("пошкоджено"), corruptEx.getMessage());
        assertFalse(errorNotifications.isEmpty());

        CountDownLatch newDone = new CountDownLatch(1);
        badSnapService.addEventListener(finishedListener(newDone));
        CrawlSession fresh = badSnapService.start(new CrawlSettings("https://corrupt.test/p2", "тест"));
        assertTrue(newDone.await(5, TimeUnit.SECONDS));
        assertEquals(SessionStatus.FINISHED, fresh.getStatus());

        assertThrows(IllegalStateException.class, () -> corruptService.resume(9999L));

        CountDownLatch e3 = new CountDownLatch(1);
        CountDownLatch r3 = new CountDownLatch(1);
        StubPageLoader failLoader = new StubPageLoader(Map.of(
                "https://failpause.test/", StubPageLoader.blocking(
                        html("FP", "<p>Слово тест " + LONG_TEXT + "</p><a href='/p2'>P2</a>"), e3, r3),
                "https://failpause.test/p2", StubPageLoader.ok(html("P2", "<p>Слово тест " + LONG_TEXT + "</p>"))
        ));
        CrawlService failService = createFileBackedService(tempDir.resolve("scenario2-failsnap"), failLoader, 1);
        CrawlSession failTarget = failService.start(new CrawlSettings("https://failpause.test/", "тест"));
        assertTrue(e3.await(5, TimeUnit.SECONDS));

        try (Connection conn = failService.getDatabaseManager().getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE state_snapshots");
        }

        AtomicBoolean pauseFailed = new AtomicBoolean(false);
        Thread failPauseThread = new Thread(() -> {
            try {
                failService.pause();
            } catch (RuntimeException ex) {
                pauseFailed.set(true);
            }
        });
        failPauseThread.start();
        Thread.sleep(50L);
        r3.countDown();
        failPauseThread.join(5000L);

        assertTrue(pauseFailed.get());
        CrawlSession failedInDb = failService.findSessionById(failTarget.getId()).orElseThrow();
        assertEquals(SessionStatus.FAILED, failedInDb.getStatus());
        assertEquals(1, failService.findPagesBySessionId(failTarget.getId()).size());
    }

    @Test
    void scenario3StopIsIrreversibleAndCacheTtlAndClearCacheControlNetworkFetches() throws Exception {
        CountDownLatch p1Entered = new CountDownLatch(1);
        CountDownLatch allowP1 = new CountDownLatch(1);
        Map<String, StubPageLoader.Response> pages = Map.of(
                "https://stop.test/", StubPageLoader.blocking(
                        html("Root", "<p>Шукане слово є тут. " + LONG_TEXT + "</p><a href='/p2'>P2</a>"), p1Entered, allowP1),
                "https://stop.test/p2", StubPageLoader.ok(html("P2", "<p>Шукане слово є тут. " + LONG_TEXT + "</p>"))
        );
        StubPageLoader stopLoader = new StubPageLoader(pages);
        CrawlService service = createFileBackedService(tempDir.resolve("scenario3-stop"), stopLoader, 1);

        CrawlSession session = service.start(new CrawlSettings("https://stop.test/", "шукане"));
        assertTrue(p1Entered.await(5, TimeUnit.SECONDS));

        CountDownLatch finishedLatch = new CountDownLatch(1);
        service.addEventListener(finishedListener(finishedLatch));

        Thread stopThread = new Thread(service::stop);
        stopThread.start();
        Thread.sleep(60L);
        allowP1.countDown();
        stopThread.join(5000L);

        assertTrue(finishedLatch.await(5, TimeUnit.SECONDS));
        CrawlSession stoppedInDb = service.findSessionById(session.getId()).orElseThrow();
        assertEquals(SessionStatus.FINISHED, stoppedInDb.getStatus());
        assertEquals(CrawlSession.REASON_STOPPED_BY_USER, stoppedInDb.getCompletionReason());
        assertEquals(1, stoppedInDb.getVisitedCount());
        assertEquals(1, stoppedInDb.getMatchedCount());
        assertEquals(0, stopLoader.requestCount("https://stop.test/p2"));
        assertTrue(service.getPausedSessions().isEmpty());
        assertThrows(IllegalStateException.class, () -> service.resume(session.getId()));

        StubPageLoader cacheTestLoader = new StubPageLoader(Map.of(
                "https://cache.test/", StubPageLoader.ok(html("Cached", "<p>Ключ кешування. " + LONG_TEXT + "</p>"))
        ));
        CrawlService cacheService = createFileBackedService(tempDir.resolve("scenario3-cache"), cacheTestLoader, 1);
        assertEquals(24, cacheService.getCacheTtlHours());
        assertThrows(IllegalArgumentException.class, () -> cacheService.setCacheTtlHours(-1));

        CountDownLatch c1 = new CountDownLatch(1);
        cacheService.setEventListener(finishedListener(c1));
        cacheService.start(new CrawlSettings("https://cache.test/", "кешування"));
        assertTrue(c1.await(5, TimeUnit.SECONDS));
        assertEquals(1, cacheTestLoader.requestCount("https://cache.test/"));

        CountDownLatch c2 = new CountDownLatch(1);
        cacheService.setEventListener(finishedListener(c2));
        cacheService.start(new CrawlSettings("https://cache.test/", "кешування"));
        assertTrue(c2.await(5, TimeUnit.SECONDS));
        assertEquals(1, cacheTestLoader.requestCount("https://cache.test/"));

        cacheService.clearCache();
        CountDownLatch c3 = new CountDownLatch(1);
        cacheService.setEventListener(finishedListener(c3));
        cacheService.start(new CrawlSettings("https://cache.test/", "кешування"));
        assertTrue(c3.await(5, TimeUnit.SECONDS));
        assertEquals(2, cacheTestLoader.requestCount("https://cache.test/"));

        cacheService.setCacheTtlHours(0);
        CountDownLatch c4 = new CountDownLatch(1);
        cacheService.setEventListener(finishedListener(c4));
        cacheService.start(new CrawlSettings("https://cache.test/", "кешування"));
        assertTrue(c4.await(5, TimeUnit.SECONDS));
        assertEquals(3, cacheTestLoader.requestCount("https://cache.test/"));
    }

    @Test
    void scenario4ComputesSessionOverallAndHostStatisticsFromDatabase() throws Exception {
        Map<String, StubPageLoader.Response> siteMap = Map.of(
                "https://alpha.test/", StubPageLoader.ok(html("Alpha", "<p>Дані аналіз " + LONG_TEXT + "</p><a href='https://sub.alpha.test/p'>Sub</a><a href='/err'>Err</a>")),
                "https://sub.alpha.test/p", StubPageLoader.ok(html("Sub", "<p>Сторінка без слова " + LONG_TEXT + "</p>")),
                "https://alpha.test/err", StubPageLoader.error(503, "Service Unavailable"),
                "https://beta.test/", StubPageLoader.ok(html("Beta", "<p>Дані бета " + LONG_TEXT + "</p>"))
        );

        CrawlService service = createFileBackedService(tempDir.resolve("scenario4-stats"), new StubPageLoader(siteMap), 2);
        assertFalse(service.hasAnySessions());

        CountDownLatch s1Done = new CountDownLatch(1);
        service.setEventListener(finishedListener(s1Done));
        CrawlSettings s1Settings = new CrawlSettings("https://alpha.test/", "аналіз", StrategyType.BFS, 1, 10, false, true, FilterType.defaultEnabled());
        CrawlSession s1 = service.start(s1Settings);
        assertTrue(s1Done.await(5, TimeUnit.SECONDS));

        assertTrue(service.hasAnySessions());
        Map<String, Long> s1Summary = service.getSessionSummary(s1.getId());
        assertEquals(3L, s1Summary.get(StatisticsService.KEY_VISITED));
        assertEquals(1L, s1Summary.get(StatisticsService.KEY_MATCHED));
        assertEquals(1L, s1Summary.get(StatisticsService.KEY_ERRORS));
        assertTrue(s1Summary.get(StatisticsService.KEY_TOTAL_BYTES) > 0L);

        CountDownLatch s2Done = new CountDownLatch(1);
        service.setEventListener(finishedListener(s2Done));
        CrawlSettings s2Settings = new CrawlSettings("https://beta.test/", "бета", StrategyType.BFS, 0, 10, false, false, FilterType.defaultEnabled());
        CrawlSession s2 = service.start(s2Settings);
        assertTrue(s2Done.await(5, TimeUnit.SECONDS));

        Map<String, Long> overall = service.getOverallSummary();
        assertEquals(4L, overall.get(StatisticsService.KEY_VISITED));
        assertEquals(2L, overall.get(StatisticsService.KEY_MATCHED));
        assertEquals(1L, overall.get(StatisticsService.KEY_ERRORS));

        Map<String, Long> hostDist = service.getHostDistribution();
        assertEquals(2L, hostDist.get("alpha.test"));
        assertEquals(1L, hostDist.get("sub.alpha.test"));
        assertEquals(1L, hostDist.get("beta.test"));

        List<HostStatistics> s1Metrics = service.getHostMetrics(s1.getId());
        assertEquals(2, s1Metrics.size());
        assertEquals("alpha.test", s1Metrics.get(0).host());
        assertEquals(2L, s1Metrics.get(0).visitedCount());
        assertEquals(1L, s1Metrics.get(0).matchedCount());
        assertEquals(1L, s1Metrics.get(0).errorCount());

        assertEquals(2, service.findAllSessions().size());
        assertEquals(3, service.findPagesBySessionId(s1.getId()).size());
    }

    private static CrawlEventListener finishedListener(CountDownLatch latch) {
        return new CrawlEventListener() {
            @Override
            public void onSessionFinished(CrawlSession session) {
                latch.countDown();
            }
        };
    }

    private static String html(String title, String bodyContent) {
        return "<html><head><title>" + title + "</title></head><body>" + bodyContent + "</body></html>";
    }

    private static final class StubPageLoader implements PageLoader {

        record Response(String html, int status, String error, CountDownLatch entered, CountDownLatch release) {
        }

        static Response ok(String html) {
            return new Response(html, 200, null, null, null);
        }

        static Response error(int status, String message) {
            return new Response(null, status, message, null, null);
        }

        static Response blocking(String html, CountDownLatch entered, CountDownLatch release) {
            return new Response(html, 200, null, entered, release);
        }

        private final Map<String, Response> responses;
        private final String robotsTxt;
        private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();

        StubPageLoader(Map<String, Response> responses) {
            this(responses, "");
        }

        StubPageLoader(Map<String, Response> responses, String robotsTxt) {
            this.responses = responses;
            this.robotsTxt = robotsTxt;
        }

        int requestCount(String url) {
            AtomicInteger counter = counts.get(url);
            return counter != null ? counter.get() : 0;
        }

        @Override
        public LoadResult loadWithMetadata(String url) throws PageLoadException {
            counts.computeIfAbsent(url, ignored -> new AtomicInteger()).incrementAndGet();
            Response response = responses.get(url);
            if (response == null) {
                throw new PageLoadException("HTTP 404 for " + url, 404, false);
            }
            if (response.entered() != null) {
                response.entered().countDown();
            }
            if (response.release() != null) {
                try {
                    response.release().await(5, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new PageLoadException("Interrupted", 0, false, ex);
                }
            }
            if (response.error() != null) {
                throw new PageLoadException(response.error(), response.status(), false);
            }
            byte[] bytes = response.html().getBytes(StandardCharsets.UTF_8);
            return new LoadResult(response.html(), response.status(), bytes.length, LocalDateTime.now(), url, false);
        }

        @Override
        public LoadResult loadRobotsTxt(String hostRootUrl) {
            if (robotsTxt == null || robotsTxt.isBlank()) {
                return new LoadResult("", 404, 0L, LocalDateTime.now(), hostRootUrl + "/robots.txt", false);
            }
            return new LoadResult(robotsTxt, 200, robotsTxt.length(), LocalDateTime.now(), hostRootUrl + "/robots.txt", false);
        }
    }
}
