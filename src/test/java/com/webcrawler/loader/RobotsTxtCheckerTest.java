package com.webcrawler.loader;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RobotsTxtCheckerTest {

    @Test
    void enforcesDisallowAllowLongestMatchAndTieBreaking() {
        String robotsTxt = """
                # Site rules
                User-agent: *
                Disallow: /admin
                Allow: /admin/public
                Disallow: /admin/public/secret
                Allow: /exact
                Disallow: /exact
                Disallow: /search?q=private
                Disallow: /*.pdf$
                """;

        StubPageLoader stub = new StubPageLoader();
        stub.putRobots("https://example.com", new LoadResult(
                robotsTxt, 200, robotsTxt.length(), LocalDateTime.now(), "https://example.com/robots.txt", false));

        RobotsTxtChecker checker = new RobotsTxtChecker(stub);

        assertTrue(checker.isAllowed("https://example.com/"));
        assertTrue(checker.isAllowed("https://example.com/index.html"));
        assertFalse(checker.isAllowed("https://example.com/admin"));
        assertFalse(checker.isAllowed("https://example.com/admin/users"));
        assertTrue(checker.isAllowed("https://example.com/admin/public/info"));
        assertFalse(checker.isAllowed("https://example.com/admin/public/secret/file"));
        assertTrue(checker.isAllowed("https://example.com/exact"));
        assertFalse(checker.isAllowed("https://example.com/search?q=private&page=1"));
        assertFalse(checker.isAllowed("https://example.com/docs/manual.pdf"));
        assertTrue(checker.isAllowed("https://example.com/docs/manual.pdf/view"));
        assertEquals(1, stub.robotsCalls.get());
    }

    @Test
    void prefersSpecificBotGroupOverWildcardAndExtractsCrawlDelay() {
        String robotsTxt = """
                User-agent: *
                Disallow: /
                Crawl-delay: 10

                User-agent: WebCrawlerCoursework
                Disallow: /private/
                Crawl-delay: 1.5
                """;

        StubPageLoader stub = new StubPageLoader();
        stub.putRobots("https://site.org", new LoadResult(
                robotsTxt, 200, robotsTxt.length(), LocalDateTime.now(), "https://site.org/robots.txt", false));

        RobotsTxtChecker checker = new RobotsTxtChecker(stub);

        assertTrue(checker.isAllowed("https://site.org/articles/news"));
        assertFalse(checker.isAllowed("https://site.org/private/key"));
        assertEquals(1500L, checker.getCrawlDelayMs("https://site.org/articles/news"));
        assertEquals(1, stub.robotsCalls.get());
    }

    private static final class StubPageLoader implements PageLoader {
        private final Map<String, LoadResult> responses = new HashMap<>();
        private final AtomicInteger robotsCalls = new AtomicInteger();

        void putRobots(String origin, LoadResult result) {
            responses.put(origin, result);
        }

        @Override
        public LoadResult loadWithMetadata(String url) throws PageLoadException {
            throw new UnsupportedOperationException("Not needed in RobotsTxtCheckerTest");
        }

        @Override
        public LoadResult loadRobotsTxt(String hostRootUrl) throws PageLoadException {
            robotsCalls.incrementAndGet();
            LoadResult result = responses.get(hostRootUrl);
            if (result != null) {
                return result;
            }
            return new LoadResult("", 404, 0L, LocalDateTime.now(), hostRootUrl + "/robots.txt", false);
        }
    }
}
