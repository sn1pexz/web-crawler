package com.webcrawler.loader;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DomainRateLimiterTest {

    @Test
    void enforcesBaseIntervalAndCrawlDelayMaxPerDomain() {
        assertEquals(500L, DomainRateLimiter.DEFAULT_BASE_INTERVAL_MS);

        DomainRateLimiter limiter = new DomainRateLimiter(80L);
        String url1 = "http://site-a.example/page1";
        String url2 = "http://site-a.example/page2";
        String otherDomainUrl = "http://site-b.example/page1";

        long startNanos = System.nanoTime();
        limiter.acquire(url1, 0L);
        limiter.acquire(otherDomainUrl, 0L);
        long afterFirstAcquireMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        assertTrue(afterFirstAcquireMs < 60L, "First acquire per domain should be immediate, took " + afterFirstAcquireMs + " ms");

        limiter.acquire(url2, 20L);
        long afterSecondAcquireMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        assertTrue(afterSecondAcquireMs >= 80L, "Expected >= 80 ms wait, got " + afterSecondAcquireMs + " ms");

        DomainRateLimiter crawlDelayLimiter = new DomainRateLimiter(50L);
        long t0 = System.nanoTime();
        crawlDelayLimiter.acquire("http://slow-site.example/1", 150L);
        crawlDelayLimiter.acquire("http://slow-site.example/2", 150L);
        long crawlDelayElapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        assertTrue(crawlDelayElapsedMs >= 150L, "Expected >= 150 ms crawl delay, got " + crawlDelayElapsedMs + " ms");
    }

    @Test
    void scalesIntervalByActiveWorkersCountSoTotalRateDoesNotGrow() throws Exception {
        long baseIntervalMs = 60L;
        int workers = 3;

        List<DomainRateLimiter> workerLimiters = new ArrayList<>();
        for (int i = 0; i < workers; i++) {
            DomainRateLimiter limiter = new DomainRateLimiter(baseIntervalMs);
            limiter.setActiveWorkersCount(workers);
            assertEquals(workers, limiter.getActiveWorkersCount());
            workerLimiters.add(limiter);
        }

        DomainRateLimiter clampCheck = new DomainRateLimiter(baseIntervalMs);
        clampCheck.setActiveWorkersCount(0);
        assertEquals(1, clampCheck.getActiveWorkersCount());

        DomainRateLimiter single = new DomainRateLimiter(baseIntervalMs);
        single.setActiveWorkersCount(workers);
        long t0 = System.nanoTime();
        single.acquire("https://example.com/1", 0L);
        single.acquire("https://example.com/2", 0L);
        single.acquire("https://example.com/3", 0L);
        long sequentialElapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        assertTrue(sequentialElapsedMs >= 360L,
                "3 requests with activeWorkersCount=3 (180ms each) must take >= 360 ms, took " + sequentialElapsedMs + " ms");

        ExecutorService pool = Executors.newFixedThreadPool(workers);
        try {
            CountDownLatch startGate = new CountDownLatch(1);
            List<Future<Long>> futures = new ArrayList<>();
            for (int w = 0; w < workers; w++) {
                DomainRateLimiter workerLimiter = workerLimiters.get(w);
                futures.add(pool.submit(() -> {
                    startGate.await();
                    long workerStart = System.nanoTime();
                    for (int r = 0; r < 3; r++) {
                        workerLimiter.acquire("https://shared-target.example/p" + r, 0L);
                    }
                    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - workerStart);
                }));
            }
            startGate.countDown();
            for (Future<Long> future : futures) {
                long workerDurationMs = future.get(5, TimeUnit.SECONDS);
                assertTrue(workerDurationMs >= 360L,
                        "Worker with 3x scaled interval must take >= 360 ms for 3 requests, took " + workerDurationMs + " ms");
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
