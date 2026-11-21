package com.webcrawler.loader;

import com.webcrawler.persistence.CacheRepository;
import com.webcrawler.util.UrlNormalizer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

//Proxy for PageLoader, adds caching
public class CachingPageLoader implements PageLoader {

    public static final int DEFAULT_TTL_HOURS = 24;

    private static final int WRITE_STRIPE_COUNT = 64;

    private final PageLoader delegate;
    private final CacheRepository cacheRepository;
    private volatile int ttlHours;
    private final Object[] writeStripes = createWriteStripes();

    public CachingPageLoader(PageLoader delegate, CacheRepository cacheRepository) {
        this(delegate, cacheRepository, DEFAULT_TTL_HOURS);
    }

    public CachingPageLoader(PageLoader delegate, CacheRepository cacheRepository, int ttlHours) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.cacheRepository = Objects.requireNonNull(cacheRepository, "cacheRepository must not be null");
        this.ttlHours = ttlHours;
    }

    @Override
    public LoadResult loadWithMetadata(String url) throws PageLoadException {
        String key = url != null ? UrlNormalizer.tryNormalize(url).orElse(url) : null;
        int currentTtl = this.ttlHours;
        if (currentTtl > 0 && key != null && !key.isBlank()) {
            Optional<CacheRepository.CacheRecord> cached =
                    cacheRepository.findValidEntry(key, CacheRepository.TYPE_PAGE, currentTtl);
            if (cached.isPresent()) {
                return toLoadResult(cached.get());
            }
        }

        LoadResult result = delegate.loadWithMetadata(url);
        if (result != null && isSuccessfulStatus(result.httpStatus())) {
            String effectiveKey = (key != null && !key.isBlank())
                    ? key
                    : normalizeFallback(result.finalUrl());
            if (effectiveKey != null && !effectiveKey.isBlank()) {
                String host = extractHostSafely(effectiveKey, result.finalUrl());
                long contentLength = resolveContentLength(result);
                //Per-key striped lock serializes concurrent cache writes
                synchronized (stripeFor(effectiveKey)) {
                    cacheRepository.saveEntry(
                            effectiveKey,
                            CacheRepository.TYPE_PAGE,
                            host,
                            result.html(),
                            result.httpStatus(),
                            contentLength,
                            result.finalUrl()
                    );
                }
            }
        }
        return result;
    }

    @Override
    public LoadResult loadRobotsTxt(String hostRootUrl) throws PageLoadException {
        String origin;
        try {
            origin = UrlNormalizer.extractOrigin(hostRootUrl);
        } catch (IllegalArgumentException ex) {
            throw new PageLoadException("Invalid host root URL: " + hostRootUrl, 0, false, ex);
        }

        int currentTtl = this.ttlHours;
        if (currentTtl > 0) {
            Optional<CacheRepository.CacheRecord> cached =
                    cacheRepository.findValidEntry(origin, CacheRepository.TYPE_ROBOTS, currentTtl);
            if (cached.isPresent()) {
                return toLoadResult(cached.get());
            }
        }

        LoadResult result = delegate.loadRobotsTxt(hostRootUrl);
        if (result != null && isCacheableRobotsStatus(result.httpStatus())) {
            String host = UrlNormalizer.extractHost(origin);
            long contentLength = resolveContentLength(result);
            String finalUrl = (result.finalUrl() != null && !result.finalUrl().isBlank())
                    ? result.finalUrl()
                    : origin + "/robots.txt";
            synchronized (stripeFor(origin)) {
                cacheRepository.saveEntry(
                        origin,
                        CacheRepository.TYPE_ROBOTS,
                        host,
                        result.html(),
                        result.httpStatus(),
                        contentLength,
                        finalUrl
                );
            }
        }
        return result;
    }

    public void clearCache() {
        cacheRepository.clearAll();
    }

    public int getTtlHours() {
        return ttlHours;
    }

    public void setTtlHours(int ttlHours) {
        this.ttlHours = ttlHours;
    }

    private static LoadResult toLoadResult(CacheRepository.CacheRecord record) {
        return new LoadResult(
                record.content(),
                record.httpStatus(),
                record.contentLength(),
                record.cachedAt(),
                record.finalUrl(),
                true
        );
    }

    private static boolean isSuccessfulStatus(int status) {
        return status >= 200 && status < 300;
    }

    private static boolean isCacheableRobotsStatus(int status) {
        return isSuccessfulStatus(status) || status == 404 || status == 410;
    }

    private static long resolveContentLength(LoadResult result) {
        if (result.contentLength() >= 0) {
            return result.contentLength();
        }
        return result.html() != null ? result.html().getBytes(StandardCharsets.UTF_8).length : 0L;
    }

    private static String normalizeFallback(String url) {
        if (url == null || url.isBlank()) {
            return null;
        }
        return UrlNormalizer.tryNormalize(url).orElse(url);
    }

    private static String extractHostSafely(String primaryUrl, String fallbackUrl) {
        if (UrlNormalizer.isValidHttpUrl(primaryUrl)) {
            return UrlNormalizer.extractHost(primaryUrl);
        }
        if (UrlNormalizer.isValidHttpUrl(fallbackUrl)) {
            return UrlNormalizer.extractHost(fallbackUrl);
        }
        return null;
    }

    private Object stripeFor(String key) {
        return writeStripes[Math.floorMod(key.hashCode(), WRITE_STRIPE_COUNT)];
    }

    private static Object[] createWriteStripes() {
        Object[] stripes = new Object[WRITE_STRIPE_COUNT];
        for (int i = 0; i < WRITE_STRIPE_COUNT; i++) {
            stripes[i] = new Object();
        }
        return stripes;
    }
}
