package com.webcrawler.core;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.webcrawler.filter.FilterType;
import com.webcrawler.loader.DomainRateLimiter;
import com.webcrawler.loader.HttpPageLoader;
import com.webcrawler.loader.RobotsTxtChecker;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.QueueEntry;
import com.webcrawler.model.StrategyType;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CrawlStrategyOrderTest {

    private static final String PADDING =
            "Достатньо довгий текст сторінки українською мовою для подолання мінімального порогу фільтрації вмісту.";

    @TempDir
    Path tempDir;

    private HttpServer server;
    private String baseUrl;
    private List<String> visitOrder;

    @BeforeEach
    void setUp() throws IOException {
        visitOrder = Collections.synchronizedList(new ArrayList<>());
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());

        Map<String, String> pages = Map.of(
                "/", "<html><body><p>Root " + PADDING + "</p><a href=\"/a\">A</a><a href=\"/b\">B</a></body></html>",
                "/a", "<html><body><p>Page A " + PADDING + "</p><a href=\"/a/1\">A1</a><a href=\"/a/2\">A2</a></body></html>",
                "/b", "<html><body><p>Page B " + PADDING + "</p><a href=\"/b/1\">B1</a></body></html>",
                "/a/1", "<html><body><p>Leaf A1 " + PADDING + "</p></body></html>",
                "/a/2", "<html><body><p>Leaf A2 " + PADDING + "</p></body></html>",
                "/b/1", "<html><body><p>Leaf B1 " + PADDING + "</p></body></html>"
        );

        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getRawPath();
            if ("/robots.txt".equals(path)) {
                sendText(exchange, 404, "text/plain; charset=UTF-8", "");
                return;
            }
            visitOrder.add(path);
            String html = pages.get(path);
            if (html == null) {
                sendText(exchange, 404, "text/plain; charset=UTF-8", "");
                return;
            }
            sendText(exchange, 200, "text/html; charset=UTF-8", html);
        });
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    private static void sendText(HttpExchange exchange, int status, String contentType, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
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
    void breadthFirstCrawlVisitsTreeLevelByLevelInFifoOrder() {
        CrawlSettings settings = new CrawlSettings(
                baseUrl + "/",
                "текст",
                StrategyType.BFS,
                2,
                20,
                false,
                false,
                FilterType.defaultEnabled()
        );
        CrawlSession session = new CrawlSession(settings);
        HttpPageLoader loader = fastHttpLoader();

        BreadthFirstCrawl bfs = new BreadthFirstCrawl(
                loader,
                new LinkExtractor(),
                new TermSearcher(),
                new RobotsTxtChecker(loader),
                new DomainRateLimiter(0L),
                null,
                tempDir.resolve("bfs"),
                null,
                1
        );

        bfs.crawl(session);

        assertEquals(List.of("/", "/a", "/b", "/a/1", "/a/2", "/b/1"), visitOrder);
        assertEquals(6, session.getVisitedCount());
        assertEquals(6, session.getMatchedCount());
    }

    @Test
    void depthFirstCrawlVisitsBranchesDeepBeforeSiblingsInLifoOrder() {
        CrawlSettings settings = new CrawlSettings(
                baseUrl + "/",
                "текст",
                StrategyType.DFS,
                2,
                20,
                false,
                false,
                FilterType.defaultEnabled()
        );
        CrawlSession session = new CrawlSession(settings);
        HttpPageLoader loader = fastHttpLoader();

        DepthFirstCrawl dfs = new DepthFirstCrawl(
                loader,
                new LinkExtractor(),
                new TermSearcher(),
                new RobotsTxtChecker(loader),
                new DomainRateLimiter(0L),
                null,
                tempDir.resolve("dfs"),
                null,
                1
        );

        dfs.crawl(session);

        assertEquals(List.of("/", "/a", "/a/1", "/a/2", "/b", "/b/1"), visitOrder);
        assertEquals(6, session.getVisitedCount());
        assertEquals(6, session.getMatchedCount());
    }

    @Test
    void depthFirstAndBreadthFirstPreserveExactOrderAcrossSnapshotAndRestore() {
        QueueEntry bottom = new QueueEntry("https://example.com/bottom", 1);
        QueueEntry middle = new QueueEntry("https://example.com/middle", 2);
        QueueEntry top = new QueueEntry("https://example.com/top", 3);

        DepthFirstCrawl dfs = new DepthFirstCrawl();
        dfs.addEntry(bottom);
        dfs.addEntry(middle);
        dfs.addEntry(top);

        List<QueueEntry> dfsSnapshot = dfs.getQueueSnapshot();
        assertEquals(List.of(top, middle, bottom), dfsSnapshot);

        DepthFirstCrawl restoredDfs = new DepthFirstCrawl();
        restoredDfs.restoreQueue(dfsSnapshot);
        assertEquals(List.of(top, middle, bottom), restoredDfs.getQueueSnapshot());
        assertEquals(top, restoredDfs.nextEntry());
        assertEquals(middle, restoredDfs.nextEntry());
        assertEquals(bottom, restoredDfs.nextEntry());
        assertNull(restoredDfs.nextEntry());

        DepthFirstCrawl collectionDfs = new DepthFirstCrawl(List.of(top, middle, bottom));
        assertEquals(top, collectionDfs.nextEntry());
        assertEquals(middle, collectionDfs.nextEntry());
        assertEquals(bottom, collectionDfs.nextEntry());

        BreadthFirstCrawl bfs = new BreadthFirstCrawl();
        bfs.addEntry(top);
        bfs.addEntry(middle);
        bfs.addEntry(bottom);

        List<QueueEntry> bfsSnapshot = bfs.getQueueSnapshot();
        assertEquals(List.of(top, middle, bottom), bfsSnapshot);

        BreadthFirstCrawl restoredBfs = new BreadthFirstCrawl();
        restoredBfs.restoreQueue(bfsSnapshot);
        assertEquals(top, restoredBfs.nextEntry());
        assertEquals(middle, restoredBfs.nextEntry());
        assertEquals(bottom, restoredBfs.nextEntry());
        assertNull(restoredBfs.nextEntry());
    }
}
