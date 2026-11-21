package com.webcrawler.loader;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class HttpPageLoaderTest {

    private HttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
    }

    private String startServer(HttpHandler handler) throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", handler);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static HttpPageLoader fastLoader(int maxResponseBytes) {
        return new HttpPageLoader(
                Duration.ofSeconds(2),
                Duration.ofMillis(400),
                new long[]{10L, 20L},
                0,
                maxResponseBytes
        );
    }

    private static HttpPageLoader fastLoaderWithReadTimeout(Duration readTimeout) {
        return new HttpPageLoader(
                Duration.ofSeconds(2),
                readTimeout,
                new long[]{10L, 20L},
                0,
                HttpPageLoader.MAX_RESPONSE_BYTES
        );
    }

    private static void sendBytes(HttpExchange exchange, int status, String contentType, byte[] body)
            throws IOException {
        Headers headers = exchange.getResponseHeaders();
        if (contentType != null) {
            headers.set("Content-Type", contentType);
        }
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            } catch (IOException ignored) {
            }
        } else {
            exchange.close();
        }
    }

    @Test
    void retriesOnTransientHttpStatusesAndSucceedsOnThirdAttempt() throws Exception {
        int[] retryableCodes = {408, 429, 500, 502, 503, 504};
        for (int statusCode : retryableCodes) {
            tearDown();
            AtomicInteger attempts = new AtomicInteger();
            String okHtml = "<html><body>Recovered from " + statusCode + "</body></html>";

            String baseUrl = startServer(exchange -> {
                int current = attempts.incrementAndGet();
                if (current < 3) {
                    sendBytes(exchange, statusCode, "text/html", "Try later".getBytes(StandardCharsets.UTF_8));
                } else {
                    sendBytes(exchange, 200, "text/html; charset=UTF-8", okHtml.getBytes(StandardCharsets.UTF_8));
                }
            });

            LoadResult result = fastLoader(HttpPageLoader.MAX_RESPONSE_BYTES).loadWithMetadata(baseUrl + "/flaky");
            assertEquals(3, attempts.get(), "Expected 3 attempts for HTTP " + statusCode);
            assertEquals(200, result.httpStatus());
            assertEquals(okHtml, result.html());
        }
    }

    @Test
    void followsUpToFiveRedirectsAndRejectsSixthRedirectImmediately() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        String baseUrl = startServer(exchange -> {
            requests.incrementAndGet();
            String path = exchange.getRequestURI().getPath();
            if (path.startsWith("/hop")) {
                int hop = Integer.parseInt(path.substring(4));
                if (hop < 6) {
                    String location = (hop == 5) ? "/hop6?done=1" : "/hop" + (hop + 1);
                    exchange.getResponseHeaders().set("Location", location);
                    exchange.sendResponseHeaders(302, -1);
                    exchange.close();
                    return;
                }
                sendBytes(exchange, 200, "text/html; charset=UTF-8",
                        "<html><body>Final destination</body></html>".getBytes(StandardCharsets.UTF_8));
                return;
            }
            if ("/too-many".equals(path)) {
                exchange.getResponseHeaders().set("Location", "/hop0");
                exchange.sendResponseHeaders(301, -1);
                exchange.close();
                return;
            }
            sendBytes(exchange, 404, "text/html", new byte[0]);
        });

        HttpPageLoader loader = fastLoader(HttpPageLoader.MAX_RESPONSE_BYTES);

        LoadResult ok = loader.loadWithMetadata(baseUrl + "/hop1");
        assertEquals(200, ok.httpStatus());
        assertEquals(baseUrl + "/hop6?done=1", ok.finalUrl());
        assertTrue(ok.html().contains("Final destination"));

        requests.set(0);
        PageLoadException tooManyEx = assertThrows(PageLoadException.class,
                () -> loader.loadWithMetadata(baseUrl + "/too-many"));
        assertFalse(tooManyEx.isRetryable());
        assertEquals(6, requests.get());
    }

    @Test
    void rejectsDisallowedContentTypeBeforeReadingBody() throws Exception {
        CountDownLatch bodyWaitLatch = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();

        String baseUrl = startServer(exchange -> {
            attempts.incrementAndGet();
            String path = exchange.getRequestURI().getPath();
            String mime = path.endsWith(".png") ? "image/png" : "application/pdf";
            exchange.getResponseHeaders().set("Content-Type", mime);
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().flush();
            try {
                bodyWaitLatch.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            exchange.close();
        });

        HttpPageLoader loader = fastLoaderWithReadTimeout(Duration.ofSeconds(2));
        try {
            long startNanos = System.nanoTime();
            PageLoadException pngEx = assertThrows(PageLoadException.class,
                    () -> loader.loadWithMetadata(baseUrl + "/image.png"));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            assertFalse(pngEx.isRetryable());
            assertEquals(1, attempts.get());
            assertTrue(elapsedMs < 1500, "Must reject disallowed Content-Type before waiting for body, took " + elapsedMs + " ms");

            PageLoadException pdfEx = assertThrows(PageLoadException.class,
                    () -> loader.loadWithMetadata(baseUrl + "/file.pdf"));
            assertFalse(pdfEx.isRetryable());
            assertEquals(2, attempts.get());
        } finally {
            bodyWaitLatch.countDown();
        }
    }

    @Test
    void rejectsExceedingMaxResponseBytesViaContentLengthAndChunkedStream() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        String baseUrl = startServer(exchange -> {
            attempts.incrementAndGet();
            String path = exchange.getRequestURI().getPath();
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            if ("/large-header".equals(path)) {
                byte[] payload = "x".repeat(500).getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, payload.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(payload);
                } catch (IOException ignored) {
                }
            } else {
                exchange.sendResponseHeaders(200, 0);
                try (OutputStream os = exchange.getResponseBody()) {
                    byte[] chunk = "a".repeat(120).getBytes(StandardCharsets.UTF_8);
                    os.write(chunk);
                    os.write(chunk);
                } catch (IOException ignored) {
                }
            }
        });

        HttpPageLoader loader = fastLoader(100);

        PageLoadException headerEx = assertThrows(PageLoadException.class,
                () -> loader.loadWithMetadata(baseUrl + "/large-header"));
        assertFalse(headerEx.isRetryable());
        assertEquals(1, attempts.get());

        PageLoadException chunkedEx = assertThrows(PageLoadException.class,
                () -> loader.loadWithMetadata(baseUrl + "/large-chunked"));
        assertFalse(chunkedEx.isRetryable());
        assertEquals(2, attempts.get());
    }

    @Test
    void readTimeoutTriggersRetryAndSucceedsWhenSubsequentAttemptRespondsFast() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        String baseUrl = startServer(exchange -> {
            int attempt = attempts.incrementAndGet();
            if (attempt < 3) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            sendBytes(exchange, 200, "text/html; charset=UTF-8",
                    "<html><body>Finally fast</body></html>".getBytes(StandardCharsets.UTF_8));
        });

        HttpPageLoader loader = fastLoaderWithReadTimeout(Duration.ofMillis(80));
        LoadResult result = loader.loadWithMetadata(baseUrl + "/slow-headers");

        assertEquals(3, attempts.get());
        assertEquals(200, result.httpStatus());
        assertTrue(result.html().contains("Finally fast"));
    }
}
