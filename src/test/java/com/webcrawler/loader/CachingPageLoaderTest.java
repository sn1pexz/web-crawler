package com.webcrawler.loader;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.webcrawler.config.AppPaths;
import com.webcrawler.core.BreadthFirstCrawl;
import com.webcrawler.filter.FilterType;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.persistence.CacheRepository;
import com.webcrawler.persistence.DatabaseManager;
import com.webcrawler.persistence.PageRepository;
import com.webcrawler.persistence.SessionRepository;
import com.webcrawler.processing.LinkExtractor;
import com.webcrawler.processing.TermSearcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CachingPageLoaderTest {

    @TempDir
    Path tempDir;

    private DatabaseManager db;
    private AppPaths appPaths;
    private CacheRepository cacheRepository;
    private HttpServer server;
    private String baseUrl;
    private final Map<String, AtomicInteger> hitCounts = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() throws IOException {
        appPaths = new AppPaths(tempDir);
        appPaths.ensureDirectoriesExist();
        db = new DatabaseManager("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        db.initializeSchema();
        cacheRepository = new CacheRepository(db, appPaths);

        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
        db.close();
    }

    private void startServerWithRoutes(Map<String, ResponseSpec> routes) {
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getRawPath();
            String query = exchange.getRequestURI().getRawQuery();
            String fullKey = query != null ? path + "?" + query : path;
            hitCounts.computeIfAbsent(path, ignored -> new AtomicInteger()).incrementAndGet();

            ResponseSpec spec = routes.getOrDefault(fullKey, routes.get(path));
            if (spec == null) {
                sendResponse(exchange, 404, "text/plain; charset=UTF-8", new byte[0]);
                return;
            }
            sendResponse(exchange, spec.status, spec.contentType, spec.body.getBytes(StandardCharsets.UTF_8));
        });
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static void sendResponse(HttpExchange exchange, int status, String contentType, byte[] body)
            throws IOException {
        if (contentType != null) {
            exchange.getResponseHeaders().set("Content-Type", contentType);
        }
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        } else {
            exchange.close();
        }
    }

    private static HttpPageLoader fastHttpLoader() {
        return new HttpPageLoader(
                Duration.ofSeconds(2),
                Duration.ofSeconds(2),
                new long[]{10L},
                0,
                HttpPageLoader.MAX_RESPONSE_BYTES
        );
    }

    @Test
    void recrawlWithDifferentSearchTermAndFiltersUsesCacheWithoutHittingServer() {
        String html = """
                <html>
                  <head><title>Кешована сторінка</title></head>
                  <body>
                    <nav><a href="/second">Друга сторінка</a></nav>
                    <article>
                      <p>Це достатньо довгий текст українською мовою про архітектуру програмного забезпечення, кешування та алгоритми обходу сайтів.</p>
                    </article>
                  </body>
                </html>
                """;
        String secondHtml = """
                <html>
                  <head><title>Друга сторінка</title></head>
                  <body>
                    <article>
                      <p>Додатковий матеріал про мережеві запити, HTTP-проксі, фільтрацію несемантичних блоків та збереження результатів у дерево каталогів.</p>
                    </article>
                  </body>
                </html>
                """;
        startServerWithRoutes(Map.of(
                "/robots.txt", ResponseSpec.text(200, "User-agent: *\nAllow: /\n"),
                "/index", ResponseSpec.html(200, html),
                "/second", ResponseSpec.html(200, secondHtml)
        ));

        CachingPageLoader cachingLoader = new CachingPageLoader(fastHttpLoader(), cacheRepository);
        assertEquals(CachingPageLoader.DEFAULT_TTL_HOURS, cachingLoader.getTtlHours());

        SessionRepository sessionRepo = new SessionRepository(db);
        PageRepository pageRepo = new PageRepository(db);

        CrawlSettings firstSettings = new CrawlSettings(
                baseUrl + "/index",
                "архітектуру",
                com.webcrawler.model.StrategyType.BFS,
                1,
                10,
                false,
                false,
                FilterType.defaultEnabled()
        );
        CrawlSession firstSession = sessionRepo.create(new CrawlSession(firstSettings));
        BreadthFirstCrawl firstCrawl = new BreadthFirstCrawl(
                cachingLoader,
                new LinkExtractor(),
                new TermSearcher(),
                new RobotsTxtChecker(cachingLoader),
                new DomainRateLimiter(0L),
                pageRepo,
                appPaths.getResultsDir().resolve("run1"),
                null
        );
        firstCrawl.crawl(firstSession);

        assertEquals(2, firstSession.getVisitedCount());
        assertEquals(1, firstSession.getMatchedCount());
        assertEquals(1, hitCounts.get("/index").get());
        assertEquals(1, hitCounts.get("/second").get());
        assertEquals(1, hitCounts.get("/robots.txt").get());

        CrawlSettings secondSettings = new CrawlSettings(
                baseUrl + "/index",
                "фільтрацію",
                com.webcrawler.model.StrategyType.BFS,
                1,
                10,
                false,
                false,
                EnumSet.allOf(FilterType.class)
        );
        CrawlSession secondSession = sessionRepo.create(new CrawlSession(secondSettings));
        BreadthFirstCrawl secondCrawl = new BreadthFirstCrawl(
                cachingLoader,
                new LinkExtractor(),
                new TermSearcher(),
                new RobotsTxtChecker(cachingLoader),
                new DomainRateLimiter(0L),
                pageRepo,
                appPaths.getResultsDir().resolve("run2"),
                null
        );
        secondCrawl.crawl(secondSession);

        assertEquals(2, secondSession.getVisitedCount());
        assertEquals(1, secondSession.getMatchedCount());
        assertEquals(1, hitCounts.get("/index").get());
        assertEquals(1, hitCounts.get("/second").get());
        assertEquals(1, hitCounts.get("/robots.txt").get());
    }

    @Test
    void normalizesKeysAndRespectsTtlExpirationAndZeroTtlBypass() throws Exception {
        startServerWithRoutes(Map.of(
                "/page", ResponseSpec.html(200, "<html><body>Версія 1</body></html>")
        ));
        CachingPageLoader loader = new CachingPageLoader(fastHttpLoader(), cacheRepository, 24);

        LoadResult miss = loader.loadWithMetadata(baseUrl + "/page?b=2&a=1#frag");
        assertFalse(miss.fromCache());
        assertEquals(1, hitCounts.get("/page").get());

        LoadResult hit = loader.loadWithMetadata(baseUrl + "/page?a=1&b=2");
        assertTrue(hit.fromCache());
        assertEquals(miss.html(), hit.html());
        assertEquals(1, hitCounts.get("/page").get());
        assertEquals(miss.html(), loader.load(baseUrl + "/page?a=1&b=2"));
        assertEquals(1, hitCounts.get("/page").get());

        loader.setTtlHours(0);
        assertEquals(0, loader.getTtlHours());
        LoadResult bypassed = loader.loadWithMetadata(baseUrl + "/page?a=1&b=2");
        assertFalse(bypassed.fromCache());
        assertEquals(2, hitCounts.get("/page").get());

        loader.setTtlHours(24);
        String normalizedKey = baseUrl + "/page?a=1&b=2";
        try (Connection conn = db.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "UPDATE cache_entries SET cached_at = ? WHERE cache_key = ? AND entry_type = ?")) {
            stmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now().minusHours(25)));
            stmt.setString(2, normalizedKey);
            stmt.setString(3, CacheRepository.TYPE_PAGE);
            stmt.executeUpdate();
        }

        LoadResult expiredReload = loader.loadWithMetadata(normalizedKey);
        assertFalse(expiredReload.fromCache());
        assertEquals(3, hitCounts.get("/page").get());

        LoadResult freshHit = loader.loadWithMetadata(normalizedKey);
        assertTrue(freshHit.fromCache());
        assertEquals(3, hitCounts.get("/page").get());
    }

    @Test
    void negativeCaches404And410RobotsTxtAndSkipsCachingNon2xxPages() throws Exception {
        startServerWithRoutes(Map.of(
                "/error500", ResponseSpec.html(500, "<html><body>Server error</body></html>"),
                "/missing", ResponseSpec.html(404, "<html><body>Not found</body></html>")
        ));
        CachingPageLoader loader = new CachingPageLoader(fastHttpLoader(), cacheRepository);

        LoadResult robotsMiss = loader.loadRobotsTxt(baseUrl);
        assertEquals(404, robotsMiss.httpStatus());
        assertFalse(robotsMiss.fromCache());
        assertEquals(1, hitCounts.get("/robots.txt").get());

        LoadResult robotsHit = loader.loadRobotsTxt(baseUrl + "/sub/page");
        assertEquals(404, robotsHit.httpStatus());
        assertTrue(robotsHit.fromCache());
        assertEquals(1, hitCounts.get("/robots.txt").get());

        assertThrows(PageLoadException.class, () -> loader.loadWithMetadata(baseUrl + "/missing"));
        assertThrows(PageLoadException.class, () -> loader.loadWithMetadata(baseUrl + "/missing"));
        assertEquals(2, hitCounts.get("/missing").get());

        assertThrows(PageLoadException.class, () -> loader.loadWithMetadata(baseUrl + "/error500"));
        assertThrows(PageLoadException.class, () -> loader.loadWithMetadata(baseUrl + "/error500"));
        assertEquals(4, hitCounts.get("/error500").get());

        assertTrue(cacheRepository.findValidEntry(baseUrl + "/missing", CacheRepository.TYPE_PAGE, 24).isEmpty());
        assertTrue(cacheRepository.findValidEntry(baseUrl + "/error500", CacheRepository.TYPE_PAGE, 24).isEmpty());
    }

    @Test
    void caches410RobotsTxtAndDoesNotCacheNon2xxFromStubDelegate() throws Exception {
        AtomicInteger stubCalls = new AtomicInteger();
        PageLoader stubDelegate = new PageLoader() {
            @Override
            public LoadResult loadWithMetadata(String url) {
                stubCalls.incrementAndGet();
                return new LoadResult("Not found", 404, 9L, LocalDateTime.now(), url, false);
            }

            @Override
            public LoadResult loadRobotsTxt(String hostRootUrl) {
                stubCalls.incrementAndGet();
                return new LoadResult("", 410, 0L, LocalDateTime.now(), hostRootUrl + "/robots.txt", false);
            }
        };

        CachingPageLoader loader = new CachingPageLoader(stubDelegate, cacheRepository);

        LoadResult firstRobots = loader.loadRobotsTxt("https://example.org");
        assertEquals(410, firstRobots.httpStatus());
        assertFalse(firstRobots.fromCache());
        assertEquals(1, stubCalls.get());

        LoadResult secondRobots = loader.loadRobotsTxt("https://example.org/page");
        assertEquals(410, secondRobots.httpStatus());
        assertTrue(secondRobots.fromCache());
        assertEquals(1, stubCalls.get());

        LoadResult non2xxPage1 = loader.loadWithMetadata("https://example.org/page");
        LoadResult non2xxPage2 = loader.loadWithMetadata("https://example.org/page");
        assertEquals(404, non2xxPage1.httpStatus());
        assertFalse(non2xxPage2.fromCache());
        assertEquals(3, stubCalls.get());
    }

    private record ResponseSpec(int status, String contentType, String body) {
        static ResponseSpec html(int status, String body) {
            return new ResponseSpec(status, "text/html; charset=UTF-8", body);
        }

        static ResponseSpec text(int status, String body) {
            return new ResponseSpec(status, "text/plain; charset=UTF-8", body);
        }
    }
}
