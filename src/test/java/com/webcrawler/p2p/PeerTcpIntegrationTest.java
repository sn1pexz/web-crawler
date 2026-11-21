package com.webcrawler.p2p;

import com.webcrawler.core.CrawlEventListener;
import com.webcrawler.core.CrawlStrategy;
import com.webcrawler.filter.FilterType;
import com.webcrawler.loader.DomainRateLimiter;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.Page;
import com.webcrawler.model.QueueEntry;
import com.webcrawler.model.SessionStatus;
import com.webcrawler.model.StrategyType;
import com.webcrawler.p2p.protocol.MessageType;
import com.webcrawler.p2p.protocol.PeerMessage;
import com.webcrawler.p2p.protocol.PeerMessageCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerTcpIntegrationTest {

    @TempDir
    Path tempDir;

    private final List<PeerClient> clientsToClose = new ArrayList<>();
    private final List<PeerServer> serversToStop = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (PeerClient client : clientsToClose) {
            try {
                client.disconnectGracefully();
            } catch (Exception ignored) {
            }
        }
        for (PeerServer server : serversToStop) {
            try {
                server.stop();
            } catch (Exception ignored) {
            }
        }
        clientsToClose.clear();
        serversToStop.clear();
    }

    @Test
    void scenario5HelloAndJoinDeliverSettingsAndRejectAfterSessionFinished() throws Exception {
        CrawlSettings settings = new CrawlSettings(
                "https://example.com/root",
                "розподілений",
                StrategyType.DFS,
                4,
                50,
                true,
                true,
                EnumSet.of(FilterType.SCRIPT, FilterType.NAVIGATION)
        );
        CrawlSession session = new CrawlSession(settings);
        session.start();
        SessionCoordinator coordinator = new SessionCoordinator(session, null, null, null, CrawlEventListener.NO_OP);
        PeerServer server = track(new PeerServer(coordinator, 50L, 500L, 1000L));
        server.openPort(findFreePort());

        DomainRateLimiter limiter = new DomainRateLimiter(0L);
        PeerClient client1 = track(new PeerClient(
                "peer-alpha",
                s -> new TestBatchStrategy((batch, st, stop) -> new CrawlStrategy.BatchResult(List.of(), List.of(), List.of())),
                limiter,
                tempDir,
                CrawlEventListener.NO_OP,
                50L,
                50L
        ));
        client1.connect("127.0.0.1", server.getPort());

        assertTrue(client1.isConnected());
        assertEquals(settings, client1.getSessionSettings());
        assertEquals(2, limiter.getActiveWorkersCount());
        assertEquals(2, coordinator.getActiveWorkersCount());

        coordinator.broadcastCommand("STOP");
        assertEquals(SessionStatus.FINISHED, session.getStatus());

        PeerClient lateClient = track(new PeerClient(
                "peer-late",
                s -> new TestBatchStrategy((batch, st, stop) -> new CrawlStrategy.BatchResult(List.of(), List.of(), List.of())),
                new DomainRateLimiter(0L),
                tempDir,
                CrawlEventListener.NO_OP,
                50L,
                50L
        ));
        IOException rejectedEx = assertThrows(IOException.class, () -> lateClient.connect("127.0.0.1", server.getPort()));
        assertTrue(rejectedEx.getMessage().contains("завершена") || rejectedEx.getMessage().contains("відхилив"),
                rejectedEx.getMessage());
        assertFalse(lateClient.isConnected());
    }

    @Test
    void scenario6EndToEndBatchProcessingEmptyRetryWorkerScalingAndCompletionStop() throws Exception {
        CrawlSession session = runningSession("https://example.com", 4);
        CountDownLatch finishedLatch = new CountDownLatch(1);
        AtomicInteger lostPeersCount = new AtomicInteger();
        SessionCoordinator coordinator = new SessionCoordinator(
                session,
                null,
                null,
                null,
                new CrawlEventListener() {
                    @Override
                    public void onPeerLost(PeerNode peer) {
                        lostPeersCount.incrementAndGet();
                    }

                    @Override
                    public void onSessionFinished(CrawlSession s) {
                        finishedLatch.countDown();
                    }
                },
                2
        );
        PeerServer server = track(new PeerServer(coordinator, 50L, 1000L, 1500L));
        server.openPort(findFreePort());

        DomainRateLimiter limiter1 = new DomainRateLimiter(0L);
        DomainRateLimiter limiter2 = new DomainRateLimiter(0L);
        List<String> client1Messages = new CopyOnWriteArrayList<>();

        TestBatchStrategy.Handler sharedHandler = (batch, settings, stopCheck) -> {
            List<Page> pages = new ArrayList<>();
            List<QueueEntry> links = new ArrayList<>();
            for (QueueEntry entry : batch) {
                pages.add(page(entry.getUrl(), entry.getDepth(), true));
                if (entry.getUrl().equals("https://example.com/")) {
                    Thread.sleep(120L);
                    links.add(new QueueEntry("https://example.com/p1", 1));
                    links.add(new QueueEntry("https://example.com/p2", 1));
                    links.add(new QueueEntry("https://example.com/p3", 1));
                    links.add(new QueueEntry("https://example.com/p4", 1));
                }
            }
            return new CrawlStrategy.BatchResult(pages, links, List.of());
        };

        PeerClient peer1 = track(new PeerClient(
                "peer-1",
                s -> new TestBatchStrategy(sharedHandler),
                limiter1,
                tempDir,
                new CrawlEventListener() {
                    @Override
                    public void onStatusMessage(String message) {
                        client1Messages.add(message);
                    }
                },
                50L,
                50L
        ));
        PeerClient peer2 = track(new PeerClient(
                "peer-2",
                s -> new TestBatchStrategy(sharedHandler),
                limiter2,
                tempDir,
                CrawlEventListener.NO_OP,
                50L,
                50L
        ));

        peer1.connect("127.0.0.1", server.getPort());
        peer2.connect("127.0.0.1", server.getPort());

        peer1.startProcessingLoop();
        peer2.startProcessingLoop();

        assertTrue(finishedLatch.await(5, TimeUnit.SECONDS), "Distributed crawl did not finish in time");
        awaitCondition(() -> peer1.isStopped() && peer2.isStopped(), 3000L);

        assertEquals(SessionStatus.FINISHED, session.getStatus());
        assertEquals(SessionCoordinator.REASON_PAGE_LIMIT_REACHED, session.getCompletionReason());
        assertEquals(4, session.getVisitedCount());
        assertEquals(4, session.getMatchedCount());
        assertEquals(3, limiter1.getActiveWorkersCount());
        assertEquals(3, limiter2.getActiveWorkersCount());
        assertEquals(0, lostPeersCount.get());
        assertTrue(client1Messages.stream().anyMatch(m -> m.contains("завершено") && m.contains("диску")));
    }

    @Test
    void scenario8UnresponsivePeerOnCommandIsMarkedLostWhileHealthyPeerExecutes() throws Exception {
        CrawlSession session = runningSession("https://example.com", 20);
        List<PeerNode> lostPeers = new CopyOnWriteArrayList<>();
        SessionCoordinator coordinator = new SessionCoordinator(
                session,
                null,
                null,
                null,
                new CrawlEventListener() {
                    @Override
                    public void onPeerLost(PeerNode peer) {
                        lostPeers.add(peer);
                    }
                },
                1
        );
        PeerServer server = track(new PeerServer(coordinator, 5000L, 250L, 250L));
        server.openPort(findFreePort());

        PeerMessageCodec codec = new PeerMessageCodec();
        try (Socket rawSocket = new Socket("127.0.0.1", server.getPort())) {
            BufferedWriter rawOut = new BufferedWriter(new OutputStreamWriter(rawSocket.getOutputStream(), StandardCharsets.UTF_8));
            BufferedReader rawIn = new BufferedReader(new InputStreamReader(rawSocket.getInputStream(), StandardCharsets.UTF_8));

            rawOut.write(codec.encode(new PeerMessage(MessageType.HELLO, "peer-mute", "r1", null, null)) + "\n");
            rawOut.flush();
            assertEquals("OK", codec.decode(rawIn.readLine()).getStatus());

            rawOut.write(codec.encode(new PeerMessage(MessageType.JOIN, "peer-mute", "r2", null, null)) + "\n");
            rawOut.flush();
            assertEquals("OK", codec.decode(rawIn.readLine()).getStatus());

            rawOut.write(codec.encode(new PeerMessage(MessageType.TASK_REQUEST, "peer-mute", "r3", null, null)) + "\n");
            rawOut.flush();
            PeerMessage batchMsg = codec.decode(rawIn.readLine());
            assertEquals(MessageType.TASK_BATCH, batchMsg.getType());
            assertEquals(1, codec.extractQueueEntries(batchMsg.getPayload()).size());

            PeerClient healthyPeer = track(new PeerClient(
                    "peer-good",
                    s -> new TestBatchStrategy((entries, st, stop) -> new CrawlStrategy.BatchResult(List.of(), List.of(), List.of())),
                    new DomainRateLimiter(0L),
                    tempDir,
                    CrawlEventListener.NO_OP,
                    50L,
                    50L
            ));
            healthyPeer.connect("127.0.0.1", server.getPort());
            healthyPeer.startProcessingLoop();

            coordinator.broadcastCommand("PAUSE");

            assertEquals(SessionStatus.PAUSED, session.getStatus());
            assertTrue(healthyPeer.isPaused());
            assertEquals(List.of("peer-mute"), lostPeers.stream().map(PeerNode::getId).toList());
            assertFalse(coordinator.hasInFlightBatches());
            assertEquals(1, coordinator.getQueueSnapshot().size());
        }
    }

    private static int findFreePort() throws IOException {
        for (int attempt = 0; attempt < 20; attempt++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                int port = socket.getLocalPort();
                if (port >= PeerServer.MIN_PORT && port <= PeerServer.MAX_PORT) {
                    return port;
                }
            }
        }
        throw new IOException("Не вдалося підібрати вільний TCP-порт у діапазоні 1024..65535.");
    }

    private PeerClient track(PeerClient client) {
        clientsToClose.add(client);
        return client;
    }

    private PeerServer track(PeerServer server) {
        serversToStop.add(server);
        return server;
    }

    private static CrawlSession runningSession(String startUrl, int pageLimit) {
        CrawlSettings settings = new CrawlSettings(startUrl, "java");
        settings.setPageLimit(pageLimit);
        settings.setMaxDepth(2);
        CrawlSession session = new CrawlSession(settings);
        session.setDistributed(true);
        session.start();
        return session;
    }

    private static Page page(String url, int depth, boolean hasTerm) {
        Page p = new Page(url, depth);
        p.setTitle("Title " + url);
        p.setDownloadedAt(LocalDateTime.now());
        p.setSize(512L);
        p.setHttpStatus(200);
        p.setHasTerm(hasTerm);
        return p;
    }

    private static void awaitCondition(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() >= deadline) {
                throw new AssertionError("Timed out waiting for condition after " + timeoutMs + " ms");
            }
            Thread.sleep(15L);
        }
    }

    private static final class TestBatchStrategy extends CrawlStrategy {

        @FunctionalInterface
        interface Handler {
            BatchResult handle(List<QueueEntry> batch, CrawlSettings settings, BooleanSupplier stopOrPauseCheck)
                    throws InterruptedException;
        }

        private final Handler handler;

        TestBatchStrategy(Handler handler) {
            this.handler = handler;
        }

        @Override
        public BatchResult processBatch(
                List<QueueEntry> batch,
                CrawlSettings settings,
                Path resultsRoot,
                BooleanSupplier stopOrPauseCheck) {
            try {
                return handler.handle(batch, settings, stopOrPauseCheck);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return new BatchResult(List.of(), List.of(), batch);
            }
        }

        @Override
        protected QueueEntry nextEntry() {
            return null;
        }

        @Override
        protected void addEntry(QueueEntry entry) {
        }
    }
}
