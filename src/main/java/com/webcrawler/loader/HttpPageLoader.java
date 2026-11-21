package com.webcrawler.loader;

import com.webcrawler.util.UrlNormalizer;

import javax.net.ssl.SSLException;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.UnresolvedAddressException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HttpPageLoader implements PageLoader {

    public static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    public static final Duration READ_TIMEOUT = Duration.ofSeconds(15);
    public static final int MAX_ATTEMPTS = 3;
    public static final long[] RETRY_BACKOFF_MS = {1000L, 3000L};
    public static final int MAX_JITTER_MS = 500;
    public static final int MAX_REDIRECTS = 5;
    public static final int MAX_RESPONSE_BYTES = 5 * 1024 * 1024;
    public static final String USER_AGENT = "WebCrawlerCoursework/1.0";

    private static final int STREAM_BUFFER_BYTES = 8192;
    private static final Set<String> ALLOWED_HTML_MIME_TYPES = Set.of("text/html", "application/xhtml+xml");
    private static final Set<Integer> RETRYABLE_HTTP_STATUSES = Set.of(408, 429, 500, 502, 503, 504);
    private static final Set<Integer> REDIRECT_STATUSES = Set.of(301, 302, 303, 307, 308);
    private static final ScheduledThreadPoolExecutor BODY_TIMEOUT_SCHEDULER = createBodyTimeoutScheduler();

    private final HttpClient httpClient;
    private final Duration readTimeout;
    private final long[] retryDelaysMs;
    private final int maxJitterMs;
    private final int maxResponseBytes;

    public HttpPageLoader() {
        this(CONNECT_TIMEOUT, READ_TIMEOUT, RETRY_BACKOFF_MS, MAX_JITTER_MS, MAX_RESPONSE_BYTES);
    }

    public HttpPageLoader(
            Duration connectTimeout,
            Duration readTimeout,
            long[] retryDelaysMs,
            int maxJitterMs,
            int maxResponseBytes
    ) {
        Objects.requireNonNull(connectTimeout, "connectTimeout must not be null");
        Objects.requireNonNull(readTimeout, "readTimeout must not be null");
        Objects.requireNonNull(retryDelaysMs, "retryDelaysMs must not be null");
        if (connectTimeout.isZero() || connectTimeout.isNegative()) {
            throw new IllegalArgumentException("connectTimeout must be positive");
        }
        if (readTimeout.isZero() || readTimeout.isNegative()) {
            throw new IllegalArgumentException("readTimeout must be positive");
        }
        if (maxJitterMs < 0) {
            throw new IllegalArgumentException("maxJitterMs must be >= 0");
        }
        if (maxResponseBytes <= 0) {
            throw new IllegalArgumentException("maxResponseBytes must be > 0");
        }
        long[] delaysCopy = retryDelaysMs.clone();
        for (long delay : delaysCopy) {
            if (delay < 0) {
                throw new IllegalArgumentException("retryDelaysMs values must be >= 0");
            }
        }

        this.readTimeout = readTimeout;
        this.retryDelaysMs = delaysCopy;
        this.maxJitterMs = maxJitterMs;
        this.maxResponseBytes = maxResponseBytes;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(connectTimeout)
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    @Override
    public LoadResult loadWithMetadata(String url) throws PageLoadException {
        URI initialUri = parseRequestUri(url);
        return loadWithRetries(initialUri, ResourceKind.HTML_PAGE);
    }

    @Override
    public LoadResult loadRobotsTxt(String hostRootUrl) throws PageLoadException {
        String origin;
        try {
            origin = UrlNormalizer.extractOrigin(hostRootUrl);
        } catch (IllegalArgumentException ex) {
            throw new PageLoadException("Invalid host root URL: " + hostRootUrl, 0, false, ex);
        }
        URI robotsUri = parseRequestUri(origin + "/robots.txt");
        return loadWithRetries(robotsUri, ResourceKind.ROBOTS_TXT);
    }

    private LoadResult loadWithRetries(URI initialUri, ResourceKind kind) throws PageLoadException {
        int maxAttempts = 1 + retryDelaysMs.length;
        PageLoadException lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            if (Thread.currentThread().isInterrupted()) {
                throw new PageLoadException("HTTP request interrupted for " + initialUri, 0, false);
            }
            try {
                return executeWithRedirects(initialUri, kind);
            } catch (PageLoadException ex) {
                lastException = ex;
                if (!ex.isRetryable() || attempt == maxAttempts) {
                    throw ex;
                }
                sleepBeforeRetry(attempt - 1, initialUri, ex);
            }
        }
        throw lastException;
    }

    private void sleepBeforeRetry(int delayIndex, URI uri, PageLoadException lastException)
            throws PageLoadException {
        long baseDelayMs = retryDelaysMs[delayIndex];
        long jitterMs = maxJitterMs > 0 ? ThreadLocalRandom.current().nextInt(maxJitterMs + 1) : 0L;
        long totalWaitMs = baseDelayMs + jitterMs;
        if (totalWaitMs <= 0L) {
            return;
        }
        try {
            Thread.sleep(totalWaitMs);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new PageLoadException(
                    "HTTP retry wait interrupted for " + uri,
                    lastException.getHttpStatus(),
                    false,
                    ex
            );
        }
    }

    private LoadResult executeWithRedirects(URI initialUri, ResourceKind kind) throws PageLoadException {
        URI currentUri = initialUri;
        int redirectCount = 0;

        while (true) {
            HttpRequest request = HttpRequest.newBuilder(currentUri)
                    .timeout(readTimeout)
                    .header("User-Agent", USER_AGENT)
                    .header("Accept", kind.acceptHeader)
                    .GET()
                    .build();

            HttpResponse<InputStream> response;
            try {
                response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new PageLoadException("HTTP request interrupted for " + currentUri, 0, false, ex);
            } catch (IOException | RuntimeException ex) {
                throw classifyException(currentUri, ex);
            }

            try (InputStream bodyStream = response.body()) {
                int status = response.statusCode();

                if (REDIRECT_STATUSES.contains(status)) {
                    redirectCount++;
                    if (redirectCount > MAX_REDIRECTS) {
                        throw new PageLoadException(
                                "Too many redirects (exceeded " + MAX_REDIRECTS + ") for " + initialUri,
                                status,
                                false
                        );
                    }
                    String location = response.headers().firstValue("Location").orElse(null);
                    if (location == null || location.isBlank()) {
                        throw new PageLoadException(
                                "Redirect HTTP " + status + " missing Location header for " + currentUri,
                                status,
                                false
                        );
                    }
                    currentUri = resolveRedirectUri(currentUri, location);
                    continue;
                }

                if (kind == ResourceKind.ROBOTS_TXT && (status == 404 || status == 410)) {
                    return new LoadResult(
                            "",
                            status,
                            0L,
                            LocalDateTime.now(),
                            canonicalUrl(currentUri),
                            false
                    );
                }

                if (status < 200 || status >= 300) {
                    boolean retryable = RETRYABLE_HTTP_STATUSES.contains(status);
                    throw new PageLoadException(
                            "HTTP error " + status + " for " + currentUri,
                            status,
                            retryable
                    );
                }

                Charset charset = validateAndExtractCharset(response, currentUri, status, kind);
                validateContentLengthHeader(response, currentUri, status);
                byte[] bodyBytes = readBoundedBodyWithTimeout(bodyStream, currentUri, status);
                String bodyText = new String(bodyBytes, charset);
                return new LoadResult(
                        bodyText,
                        status,
                        bodyBytes.length,
                        LocalDateTime.now(),
                        canonicalUrl(currentUri),
                        false
                );
            } catch (PageLoadException ex) {
                throw ex;
            } catch (IOException | RuntimeException ex) {
                throw classifyException(currentUri, ex);
            }
        }
    }

    private Charset validateAndExtractCharset(
            HttpResponse<?> response,
            URI currentUri,
            int status,
            ResourceKind kind
    ) throws PageLoadException {
        Optional<String> contentTypeOpt = response.headers().firstValue("Content-Type");
        if (contentTypeOpt.isEmpty() || contentTypeOpt.get().isBlank()) {
            if (kind == ResourceKind.ROBOTS_TXT) {
                return StandardCharsets.UTF_8;
            }
            throw new PageLoadException("Missing Content-Type header for " + currentUri, status, false);
        }

        String rawContentType = contentTypeOpt.get();
        ContentTypeInfo parsed = parseContentType(rawContentType);
        if (!kind.isAllowedMimeType(parsed.mimeType)) {
            throw new PageLoadException(
                    "Disallowed Content-Type '" + rawContentType + "' for " + currentUri,
                    status,
                    false
            );
        }
        return parsed.charset;
    }

    private void validateContentLengthHeader(HttpResponse<?> response, URI currentUri, int status)
            throws PageLoadException {
        Optional<String> rawLength = response.headers().firstValue("Content-Length");
        if (rawLength.isEmpty()) {
            return;
        }
        long declaredLength;
        try {
            declaredLength = Long.parseLong(rawLength.get().trim());
        } catch (NumberFormatException ignored) {
            return;
        }
        if (declaredLength > maxResponseBytes) {
            throw new PageLoadException(
                    "Response Content-Length (" + declaredLength + " bytes) exceeds limit of "
                            + maxResponseBytes + " bytes for " + currentUri,
                    status,
                    false
            );
        }
    }

    private byte[] readBoundedBodyWithTimeout(InputStream bodyStream, URI currentUri, int status)
            throws IOException, PageLoadException {
        AtomicBoolean readCompleted = new AtomicBoolean(false);
        AtomicBoolean timedOut = new AtomicBoolean(false);
        ScheduledFuture<?> timeoutTask = BODY_TIMEOUT_SCHEDULER.schedule(() -> {
            if (readCompleted.compareAndSet(false, true)) {
                timedOut.set(true);
                try {
                    bodyStream.close();
                } catch (IOException ignored) {
                }
            }
        }, readTimeout.toNanos(), TimeUnit.NANOSECONDS);

        try {
            byte[] bytes = readBoundedBody(bodyStream, currentUri, status, readCompleted);
            if (timedOut.get()) {
                throw new HttpTimeoutException("Response body read timed out after " + readTimeout.toMillis() + " ms");
            }
            return bytes;
        } catch (IOException ex) {
            if (timedOut.get()) {
                throw new HttpTimeoutException("Response body read timed out after " + readTimeout.toMillis() + " ms");
            }
            throw ex;
        } finally {
            readCompleted.compareAndSet(false, true);
            timeoutTask.cancel(false);
        }
    }

    private byte[] readBoundedBody(
            InputStream bodyStream,
            URI currentUri,
            int status,
            AtomicBoolean readCompleted
    ) throws IOException, PageLoadException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[STREAM_BUFFER_BYTES];
        int totalRead = 0;
        int bytesRead;

        while ((bytesRead = bodyStream.read(buffer)) != -1) {
            if (bytesRead > maxResponseBytes - totalRead) {
                throw new PageLoadException(
                        "Response body exceeds maximum allowed size of "
                                + maxResponseBytes + " bytes for " + currentUri,
                        status,
                        false
                );
            }
            out.write(buffer, 0, bytesRead);
            totalRead += bytesRead;
        }
        readCompleted.compareAndSet(false, true);
        return out.toByteArray();
    }

    private static ScheduledThreadPoolExecutor createBodyTimeoutScheduler() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "http-body-timeout-watchdog");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    private static URI parseRequestUri(String rawUrl) throws PageLoadException {
        if (!UrlNormalizer.isValidHttpUrl(rawUrl)) {
            throw new PageLoadException("Invalid HTTP/HTTPS URL: " + rawUrl, 0, false);
        }
        String trimmed = rawUrl.trim();
        int hashIdx = trimmed.indexOf('#');
        String withoutFragment = hashIdx >= 0 ? trimmed.substring(0, hashIdx) : trimmed;
        try {
            URI uri = new URI(withoutFragment);
            return ensureNonEmptyPath(uri);
        } catch (URISyntaxException ex) {
            throw new PageLoadException("Invalid HTTP/HTTPS URL: " + rawUrl, 0, false, ex);
        }
    }

    private static URI resolveRedirectUri(URI baseUri, String location) throws PageLoadException {
        String trimmedLocation = location.trim();
        URI resolved;
        try {
            resolved = baseUri.resolve(trimmedLocation);
        } catch (IllegalArgumentException ex) {
            throw new PageLoadException("Invalid redirect Location '" + location + "'", 0, false, ex);
        }
        String resolvedStr = resolved.toString();
        int hashIdx = resolvedStr.indexOf('#');
        String withoutFragment = hashIdx >= 0 ? resolvedStr.substring(0, hashIdx) : resolvedStr;
        if (!UrlNormalizer.isValidHttpUrl(withoutFragment)) {
            throw new PageLoadException("Unsupported redirect target URL: " + withoutFragment, 0, false);
        }
        try {
            return ensureNonEmptyPath(new URI(withoutFragment));
        } catch (URISyntaxException ex) {
            throw new PageLoadException("Invalid redirect target URL: " + withoutFragment, 0, false, ex);
        }
    }

    private static URI ensureNonEmptyPath(URI uri) throws URISyntaxException {
        if (uri.getRawPath() != null && !uri.getRawPath().isEmpty()) {
            return uri;
        }
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        String authority = uri.getRawAuthority();
        String query = uri.getRawQuery();
        String rebuilt = scheme + "://" + authority + "/" + (query != null ? "?" + query : "");
        return new URI(rebuilt);
    }

    private static String canonicalUrl(URI uri) {
        String raw = uri.toString();
        return UrlNormalizer.tryNormalize(raw).orElse(raw);
    }

    private static ContentTypeInfo parseContentType(String rawHeader) {
        String[] parts = rawHeader.split(";");
        String mimeType = parts[0].trim().toLowerCase(Locale.ROOT);
        Charset charset = StandardCharsets.UTF_8;

        for (int i = 1; i < parts.length; i++) {
            String param = parts[i].trim();
            int eqIdx = param.indexOf('=');
            if (eqIdx <= 0) {
                continue;
            }
            String key = param.substring(0, eqIdx).trim().toLowerCase(Locale.ROOT);
            if ("charset".equals(key)) {
                String value = stripQuotes(param.substring(eqIdx + 1).trim());
                if (!value.isEmpty()) {
                    try {
                        charset = Charset.forName(value);
                    } catch (IllegalCharsetNameException | UnsupportedCharsetException ignored) {
                        charset = StandardCharsets.UTF_8;
                    }
                }
                break;
            }
        }
        return new ContentTypeInfo(mimeType, charset);
    }

    private static String stripQuotes(String value) {
        if (value.length() >= 2) {
            char first = value.charAt(0);
            char last = value.charAt(value.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return value.substring(1, value.length() - 1).trim();
            }
        }
        return value;
    }

    private static PageLoadException classifyException(URI uri, Throwable error) {
        boolean retryable = isRetryableThrowable(error);
        String message = error.getMessage() != null && !error.getMessage().isBlank()
                ? error.getMessage()
                : error.getClass().getSimpleName();
        return new PageLoadException("HTTP request failed for " + uri + ": " + message, 0, retryable, error);
    }

    private static boolean isRetryableThrowable(Throwable root) {
        //Check full cause chain for non-retryable DNS/SSL/URI errors first
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable t = root; t != null && visited.add(t); t = t.getCause()) {
            if (t instanceof UnknownHostException
                    || t instanceof UnresolvedAddressException
                    || t instanceof SSLException
                    || t instanceof URISyntaxException
                    || t instanceof MalformedURLException
                    || t instanceof IllegalArgumentException
                    || t instanceof SecurityException
                    || t instanceof InterruptedException) {
                return false;
            }
        }

        visited.clear();
        for (Throwable t = root; t != null && visited.add(t); t = t.getCause()) {
            if (t instanceof HttpTimeoutException
                    || t instanceof SocketTimeoutException
                    || t instanceof ConnectException
                    || t instanceof SocketException
                    || t instanceof EOFException
                    || t instanceof ClosedChannelException
                    || t instanceof IOException) {
                return true;
            }
        }
        return false;
    }

    private enum ResourceKind {
        HTML_PAGE("text/html,application/xhtml+xml;q=0.9,*/*;q=0.1") {
            @Override
            boolean isAllowedMimeType(String mimeType) {
                return ALLOWED_HTML_MIME_TYPES.contains(mimeType);
            }
        },
        ROBOTS_TXT("text/plain;q=0.9,*/*;q=0.1") {
            @Override
            boolean isAllowedMimeType(String mimeType) {
                return "text/plain".equals(mimeType);
            }
        };

        private final String acceptHeader;

        ResourceKind(String acceptHeader) {
            this.acceptHeader = acceptHeader;
        }

        abstract boolean isAllowedMimeType(String mimeType);
    }

    private record ContentTypeInfo(String mimeType, Charset charset) {
    }
}
