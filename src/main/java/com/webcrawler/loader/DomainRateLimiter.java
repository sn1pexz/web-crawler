package com.webcrawler.loader;

import com.webcrawler.util.UrlNormalizer;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DomainRateLimiter {

    public static final long DEFAULT_BASE_INTERVAL_MS = 500L;

    private static final long MAX_INTERVAL_MS = 86_400_000L * 365L;

    private final long baseIntervalMs;
    private final AtomicInteger activeWorkersCount = new AtomicInteger(1);
    private final ConcurrentMap<String, DomainState> domainStates = new ConcurrentHashMap<>();

    public DomainRateLimiter() {
        this(DEFAULT_BASE_INTERVAL_MS);
    }

    public DomainRateLimiter(long baseIntervalMs) {
        if (baseIntervalMs < 0L) {
            throw new IllegalArgumentException("baseIntervalMs must be >= 0");
        }
        this.baseIntervalMs = baseIntervalMs;
    }

    public void setActiveWorkersCount(int workers) {
        activeWorkersCount.set(Math.max(1, workers));
    }

    public int getActiveWorkersCount() {
        return activeWorkersCount.get();
    }

    public long getBaseIntervalMs() {
        return baseIntervalMs;
    }

    public void acquire(String url, long crawlDelayMs) {
        String domainKey = extractDomainKey(url);
        if (Thread.currentThread().isInterrupted()) {
            return;
        }

        long effectiveIntervalMs = computeEffectiveIntervalMs(crawlDelayMs);
        long intervalNanos = TimeUnit.MILLISECONDS.toNanos(effectiveIntervalMs);

        DomainState state = domainStates.computeIfAbsent(domainKey, ignored -> new DomainState());
        long nowNanos = System.nanoTime();
        long waitNanos = state.reserveWaitNanos(nowNanos, intervalNanos);

        if (waitNanos <= 0L) {
            return;
        }

        long targetNanos = nowNanos + waitNanos;
        while (true) {
            long remainingNanos = targetNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                return;
            }
            long millis = remainingNanos / 1_000_000L;
            int nanos = (int) (remainingNanos % 1_000_000L);
            try {
                Thread.sleep(millis, nanos);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    //Scale delay by active workers so total domain rate stays bounded
    long computeEffectiveIntervalMs(long crawlDelayMs) {
        long clampedDelay = Math.max(0L, crawlDelayMs);
        long perSiteInterval = Math.max(baseIntervalMs, clampedDelay);
        int workers = Math.max(1, activeWorkersCount.get());
        try {
            return Math.min(Math.multiplyExact(perSiteInterval, workers), MAX_INTERVAL_MS);
        } catch (ArithmeticException overflow) {
            return MAX_INTERVAL_MS;
        }
    }

    private static String extractDomainKey(String url) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("URL must not be null or blank");
        }
        if (UrlNormalizer.isValidHttpUrl(url)) {
            return UrlNormalizer.extractHost(url);
        }
        return url.trim().toLowerCase(Locale.ROOT);
    }

    private static final class DomainState {
        private long nextAllowedNanos;
        private boolean initialized;

        synchronized long reserveWaitNanos(long nowNanos, long intervalNanos) {
            if (!initialized) {
                initialized = true;
                nextAllowedNanos = nowNanos + intervalNanos;
                return 0L;
            }
            long waitNanos = nextAllowedNanos - nowNanos;
            if (waitNanos <= 0L) {
                nextAllowedNanos = nowNanos + intervalNanos;
                return 0L;
            }
            nextAllowedNanos = nextAllowedNanos + intervalNanos;
            return waitNanos;
        }
    }
}
