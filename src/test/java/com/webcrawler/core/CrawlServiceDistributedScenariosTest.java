package com.webcrawler.core;

import com.webcrawler.config.AppPaths;
import com.webcrawler.filter.FilterType;
import com.webcrawler.loader.DomainRateLimiter;
import com.webcrawler.loader.LoadResult;
import com.webcrawler.loader.PageLoadException;
import com.webcrawler.loader.PageLoader;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.Page;
import com.webcrawler.model.SessionStatus;
import com.webcrawler.model.StrategyType;
import com.webcrawler.p2p.PeerNode;
import com.webcrawler.persistence.DatabaseManager;
import com.webcrawler.processing.LinkExtractor;
import com.webcrawler.processing.TermSearcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrawlServiceDistributedScenariosTest {

    private static final String LONG_TEXT =
            "Це змістовний український текст про розподілене сканування веб-сайтів у мережі P2P та індексацію сторінок. ";

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

    private CrawlService createService(
            Path dataDir,
            PageLoader loader,
            int poolSize,
            int batchSize,
            long heartbeatCheckMs,
            long commandAckMs,
            long pauseAckMs,
            long emptyRetryMs,
            long peerHeartbeatMs,
            Supplier<LocalDateTime> clock) {
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
                batchSize,
                heartbeatCheckMs,
                commandAckMs,
                pauseAckMs,
                emptyRetryMs,
                peerHeartbeatMs,
                clock
        );
        servicesToShutdown.add(service);
        return service;
    }

    private CrawlService createFastService(Path dataDir, PageLoader loader, int poolSize, int batchSize) {
        return createService(
                dataDir, loader, poolSize, batchSize,
                30L, 1000L, 2000L, 25L, 30L, LocalDateTime::now);
    }

    @Test
    void scenario5BusyPortAndUnreachableHostFailWithUkrainianErrorsWithoutCorruptingState() throws Exception {
        StubPageLoader loader = new StubPageLoader(Map.of(
                "https://dist.test/", StubPageLoader.ok(html("Root", "<p>Кластер працює успішно. " + LONG_TEXT + "</p>"))
        ));
        CrawlService initiator = createFastService(tempDir.resolve("s5-init"), loader, 1, 2);
        CrawlService peer = createFastService(tempDir.resolve("s5-peer"), loader, 1, 2);

        List<String> initiatorMessages = new CopyOnWriteArrayList<>();
        initiator.addEventListener(statusCollector(initiatorMessages));
        List<String> peerErrors = new CopyOnWriteArrayList<>();
        peer.addEventListener(new CrawlEventListener() {
            @Override
            public void onError(String url, String errorMessage) {
                peerErrors.add(errorMessage);
            }
        });

        CrawlSettings settings = new CrawlSettings("https://dist.test/", "кластер");

        try (ServerSocket busy = new ServerSocket(0)) {
            int busyPort = busy.getLocalPort();
            IOException portEx = assertThrows(IOException.class,
                    () -> initiator.createDistributedSession(settings, busyPort));
            assertTrue(portEx.getMessage().contains("Не вдалося відкрити порт"), portEx.getMessage());
            assertFalse(initiator.hasAnySessions());
            assertFalse(initiator.isRunning());
            assertFalse(initiator.isDistributedInitiator());
            assertTrue(initiatorMessages.stream().anyMatch(m -> m.contains("порт")));
        }

        assertThrows(IllegalArgumentException.class, () -> initiator.createDistributedSession(settings, 0));
        assertThrows(IllegalArgumentException.class, () -> initiator.createDistributedSession(settings, 1023));
        assertThrows(IllegalArgumentException.class, () -> initiator.createDistributedSession(settings, 65536));
        assertThrows(IOException.class, () -> peer.connectToSession("", 9090));
        assertThrows(IOException.class, () -> peer.connectToSession("127.0.0.1", 0));
        assertThrows(IOException.class, () -> peer.connectToSession("127.0.0.1", 1023));
        assertThrows(IOException.class, () -> peer.connectToSession("127.0.0.1", 65536));

        int unusedPort = findFreePort();
        IOException connectEx = assertThrows(IOException.class, () -> peer.connectToSession("127.0.0.1", unusedPort));
        assertTrue(connectEx.getMessage().contains("Не вдалося підключитися"), connectEx.getMessage());
        assertFalse(peer.isConnectedPeer());
        assertFalse(peer.isRunning());
        assertFalse(peerErrors.isEmpty());

        CountDownLatch finished = new CountDownLatch(1);
        initiator.addEventListener(finishedListener(finished));
        initiator.createDistributedSession(settings, findFreePort());
        assertTrue(initiator.getServerPort() >= 1024);
        assertTrue(finished.await(5, TimeUnit.SECONDS));
        assertEquals(SessionStatus.FINISHED, initiator.getActiveSession().getStatus());

        assertThrows(IOException.class, () -> peer.connectToSession("127.0.0.1", unusedPort));
    }

    @Test
    void scenario5And6InitiatorAndPeerJointlyCrawlDeduplicateSaveFilesLocallyAndFinish() throws Exception {
        CountDownLatch rootEntered = new CountDownLatch(1);
        CountDownLatch releaseRoot = new CountDownLatch(1);

        Map<String, StubPageLoader.Response> graph = Map.of(
                "https://p2p.test/", StubPageLoader.blocking(
                        html("Root", "<p>розподілене сканування. " + LONG_TEXT + "</p>"
                                + "<a href='/p1'>1</a><a href='/p2'>2</a><a href='/p3'>3</a>"
                                + "<a href='/p4'>4</a><a href='/p5'>5</a><a href='/p6'>6</a>"),
                        rootEntered, releaseRoot),
                "https://p2p.test/p1", StubPageLoader.delayed(html("P1", "<p>розподілене P1 " + LONG_TEXT + "</p><a href='/'>Back</a>"), 40L),
                "https://p2p.test/p2", StubPageLoader.delayed(html("P2", "<p>розподілене P2 " + LONG_TEXT + "</p>"), 40L),
                "https://p2p.test/p3", StubPageLoader.delayed(html("P3", "<p>інша тема P3 " + "Це змістовний український текст про сканування веб-сайтів у мережі та індексацію сторінок. ".repeat(2) + "</p>"), 40L),
                "https://p2p.test/p4", StubPageLoader.delayed(html("P4", "<p>розподілене P4 " + LONG_TEXT + "</p>"), 40L),
                "https://p2p.test/p5", StubPageLoader.delayed(html("P5", "<p>розподілене P5 " + LONG_TEXT + "</p>"), 40L),
                "https://p2p.test/p6", StubPageLoader.delayed(html("P6", "<p>інша тема P6 " + "Це змістовний український текст про сканування веб-сайтів у мережі та індексацію сторінок. ".repeat(2) + "</p>"), 40L)
        );

        Path initDir = tempDir.resolve("s6-init");
        Path peerDir = tempDir.resolve("s6-peer");
        StubPageLoader initLoader = new StubPageLoader(graph);
        StubPageLoader peerLoader = new StubPageLoader(graph);

        CrawlService initiator = createFastService(initDir, initLoader, 1, 2);
        CrawlService peer = createFastService(peerDir, peerLoader, 1, 2);

        CountDownLatch finished = new CountDownLatch(1);
        initiator.addEventListener(finishedListener(finished));

        CrawlSettings settings = new CrawlSettings("https://p2p.test/", "розподілене", StrategyType.BFS, 2, 20, false, false, FilterType.defaultEnabled());
        initiator.createDistributedSession(settings, findFreePort());
        int port = initiator.getServerPort();
        assertTrue(port > 0);
        assertTrue(rootEntered.await(5, TimeUnit.SECONDS));

        peer.connectToSession("127.0.0.1", port);
        awaitCondition(() -> initiator.getConnectedPeerCount() == 1 && peer.isConnectedPeer(), 3000L);
        assertEquals(2, initiator.getActiveWorkersCount());
        assertEquals(2, peer.getActiveWorkersCount());

        releaseRoot.countDown();
        assertTrue(finished.await(10, TimeUnit.SECONDS), "Joint distributed crawl did not finish in time");
        awaitCondition(() -> !peer.isConnectedPeer() && !initiator.isRunning(), 3000L);

        CrawlSession session = initiator.getActiveSession();
        assertEquals(SessionStatus.FINISHED, session.getStatus());
        assertEquals(7, session.getVisitedCount());
        assertEquals(5, session.getMatchedCount());
        assertEquals(0, session.getErrorCount());

        Set<String> initFetched = initLoader.fetchedUrls();
        Set<String> peerFetched = peerLoader.fetchedUrls();
        assertTrue(initFetched.size() >= 2, "Expected initiator to fetch multiple pages, got " + initFetched);
        assertTrue(peerFetched.size() >= 2, "Expected peer to fetch multiple pages, got " + peerFetched);

        Set<String> overlap = new HashSet<>(initFetched);
        overlap.retainAll(peerFetched);
        assertTrue(overlap.isEmpty(), "Expected zero duplicate fetches across nodes, overlap=" + overlap);

        Set<String> union = new HashSet<>(initFetched);
        union.addAll(peerFetched);
        assertEquals(7, union.size());

        assertTrue(countHtmlFiles(new AppPaths(initDir).getResultsDir()) >= 1);
        assertTrue(countHtmlFiles(new AppPaths(peerDir).getResultsDir()) >= 1);

        List<Page> dbPages = initiator.findPagesBySessionId(session.getId());
        assertEquals(7, dbPages.size());
        assertEquals(-1, initiator.getServerPort());
    }

    @Test
    void scenario7HeartbeatTimeoutRequeuesLostPeerBatchAndGracefulLeavePreservesFiles() throws Exception {
        LocalDateTime t0 = LocalDateTime.of(2026, 11, 19, 12, 0, 0);
        AtomicReference<LocalDateTime> serverClock = new AtomicReference<>(t0);

        CountDownLatch rootEntered = new CountDownLatch(1);
        CountDownLatch releaseRoot = new CountDownLatch(1);
        CountDownLatch initiatorEntered = new CountDownLatch(1);
        CountDownLatch releaseInitiator = new CountDownLatch(1);
        CountDownLatch silentPeerGrabbed = new CountDownLatch(1);
        CountDownLatch releaseSilentPeer = new CountDownLatch(1);

        Map<String, StubPageLoader.Response> initGraph = Map.of(
                "https://lost.test/", StubPageLoader.blocking(
                        html("Root", "<p>слово корінь " + LONG_TEXT + "</p><a href='/p1'>1</a><a href='/p2'>2</a>"),
                        rootEntered, releaseRoot),
                "https://lost.test/p1", StubPageLoader.blocking(
                        html("P1", "<p>слово сторінка1 " + LONG_TEXT + "</p>"),
                        initiatorEntered, releaseInitiator),
                "https://lost.test/p2", StubPageLoader.blocking(
                        html("P2", "<p>слово сторінка2 " + LONG_TEXT + "</p>"),
                        initiatorEntered, releaseInitiator)
        );

        Map<String, StubPageLoader.Response> silentGraph = Map.of(
                "https://lost.test/p1", StubPageLoader.blocking(
                        html("P1-silent", "<p>слово " + LONG_TEXT + "</p>"), silentPeerGrabbed, releaseSilentPeer),
                "https://lost.test/p2", StubPageLoader.blocking(
                        html("P2-silent", "<p>слово " + LONG_TEXT + "</p>"), silentPeerGrabbed, releaseSilentPeer)
        );

        StubPageLoader initLoader = new StubPageLoader(initGraph);
        StubPageLoader silentLoader = new StubPageLoader(silentGraph);

        CrawlService initiator = createService(
                tempDir.resolve("s7-init"), initLoader, 1, 1,
                30L, 500L, 1000L, 25L, 30L, serverClock::get);
        CrawlService silentPeer = createService(
                tempDir.resolve("s7-silent"), silentLoader, 1, 1,
                30L, 500L, 1000L, 25L, 60_000L, LocalDateTime::now);

        CountDownLatch peerLostLatch = new CountDownLatch(1);
        CountDownLatch finishedLatch = new CountDownLatch(1);
        List<PeerNode> lostNodes = new CopyOnWriteArrayList<>();
        initiator.addEventListener(new CrawlEventListener() {
            @Override
            public void onPeerLost(PeerNode peer) {
                lostNodes.add(peer);
                peerLostLatch.countDown();
            }

            @Override
            public void onSessionFinished(CrawlSession session) {
                finishedLatch.countDown();
            }
        });

        CrawlSettings settings = new CrawlSettings("https://lost.test/", "слово", StrategyType.BFS, 2, 10, false, false, FilterType.defaultEnabled());
        try {
            initiator.createDistributedSession(settings, findFreePort());
            assertTrue(rootEntered.await(5, TimeUnit.SECONDS));

            silentPeer.connectToSession("127.0.0.1", initiator.getServerPort());
            awaitCondition(() -> initiator.getConnectedPeerCount() == 1, 3000L);
            releaseRoot.countDown();

            assertTrue(initiatorEntered.await(5, TimeUnit.SECONDS));
            assertTrue(silentPeerGrabbed.await(5, TimeUnit.SECONDS));

            releaseInitiator.countDown();
            awaitCondition(() -> initiator.getActiveSession().getVisitedCount() == 2, 3000L);
            assertTrue(lostNodes.isEmpty());

            serverClock.set(t0.plusSeconds(16));
            assertTrue(peerLostLatch.await(5, TimeUnit.SECONDS));
            assertEquals(1, lostNodes.size());

            assertTrue(finishedLatch.await(5, TimeUnit.SECONDS));
            assertEquals(SessionStatus.FINISHED, initiator.getActiveSession().getStatus());
            assertEquals(3, initiator.getActiveSession().getVisitedCount());
            assertEquals(3, initiator.getActiveSession().getMatchedCount());
            assertEquals(1, initLoader.requestCount("https://lost.test/p1"));
            assertEquals(1, initLoader.requestCount("https://lost.test/p2"));
        } finally {
            releaseRoot.countDown();
            releaseInitiator.countDown();
            releaseSilentPeer.countDown();
        }

        CountDownLatch bRootEntered = new CountDownLatch(1);
        CountDownLatch bReleaseRoot = new CountDownLatch(1);
        Map<String, StubPageLoader.Response> bGraph = Map.of(
                "https://drop.test/", StubPageLoader.blocking(
                        html("Root", "<p>слово корінь " + LONG_TEXT + "</p><a href='/x1'>1</a><a href='/x2'>2</a>"),
                        bRootEntered, bReleaseRoot),
                "https://drop.test/x1", StubPageLoader.ok(html("X1", "<p>слово х1 " + LONG_TEXT + "</p>")),
                "https://drop.test/x2", StubPageLoader.ok(html("X2", "<p>слово х2 " + LONG_TEXT + "</p>"))
        );

        CrawlService init2 = createFastService(tempDir.resolve("s7-init2"), new StubPageLoader(bGraph), 1, 1);
        CrawlService leavingPeer = createFastService(tempDir.resolve("s7-leave"), new StubPageLoader(bGraph), 1, 1);
        CrawlService dropPeer = createFastService(tempDir.resolve("s7-drop"), new StubPageLoader(bGraph), 1, 1);

        List<String> dropPeerStatus = new CopyOnWriteArrayList<>();
        dropPeer.addEventListener(statusCollector(dropPeerStatus));

        init2.createDistributedSession(new CrawlSettings("https://drop.test/", "слово"), findFreePort());
        assertTrue(bRootEntered.await(5, TimeUnit.SECONDS));

        leavingPeer.connectToSession("127.0.0.1", init2.getServerPort());
        dropPeer.connectToSession("127.0.0.1", init2.getServerPort());
        awaitCondition(() -> init2.getConnectedPeerCount() == 2, 3000L);

        leavingPeer.disconnectFromSession();
        awaitCondition(() -> init2.getConnectedPeerCount() == 1 && !leavingPeer.isConnectedPeer(), 3000L);

        bReleaseRoot.countDown();
        init2.shutdown();
        servicesToShutdown.remove(init2);
        awaitCondition(() -> !dropPeer.isConnectedPeer(), 3000L);
        assertTrue(dropPeerStatus.stream().anyMatch(m -> m.contains("Втрачено зв'язок з ініціатором")),
                "Expected Ukrainian initiator-loss message, got " + dropPeerStatus);
    }

    @Test
    void scenario8DistributedPauseResumeStopAndRestartResumeWithPeerReconnect() throws Exception {
        CountDownLatch rootEntered = new CountDownLatch(1);
        CountDownLatch releaseRoot = new CountDownLatch(1);
        CountDownLatch p1Entered = new CountDownLatch(1);
        CountDownLatch allowP1 = new CountDownLatch(1);
        CountDownLatch p3Entered = new CountDownLatch(1);
        CountDownLatch allowP3 = new CountDownLatch(1);

        Map<String, StubPageLoader.Response> graphA = Map.of(
                "https://ctrl.test/", StubPageLoader.blocking(
                        html("Root", "<p>керування старт " + LONG_TEXT + "</p><a href='/p1'>1</a><a href='/p2'>2</a>"),
                        rootEntered, releaseRoot),
                "https://ctrl.test/p1", StubPageLoader.blocking(
                        html("P1", "<p>керування p1 " + LONG_TEXT + "</p><a href='/p3'>3</a><a href='/p4'>4</a>"),
                        p1Entered, allowP1),
                "https://ctrl.test/p2", StubPageLoader.ok(html("P2", "<p>керування p2 " + LONG_TEXT + "</p>")),
                "https://ctrl.test/p3", StubPageLoader.blocking(
                        html("P3", "<p>керування p3 " + LONG_TEXT + "</p>"), p3Entered, allowP3),
                "https://ctrl.test/p4", StubPageLoader.ok(html("P4", "<p>керування p4 " + LONG_TEXT + "</p>"))
        );

        CrawlService initA = createFastService(tempDir.resolve("s8-initA"), new StubPageLoader(graphA), 1, 1);
        CrawlService peerA = createFastService(tempDir.resolve("s8-peerA"), new StubPageLoader(graphA), 1, 1);

        CountDownLatch pausedLatch = new CountDownLatch(1);
        CountDownLatch finishedLatch = new CountDownLatch(1);
        initA.addEventListener(new CrawlEventListener() {
            @Override
            public void onSessionPaused(CrawlSession session) {
                pausedLatch.countDown();
            }

            @Override
            public void onSessionFinished(CrawlSession session) {
                finishedLatch.countDown();
            }
        });

        CrawlSettings settingsA = new CrawlSettings("https://ctrl.test/", "керування", StrategyType.BFS, 3, 10, false, false, FilterType.defaultEnabled());
        initA.createDistributedSession(settingsA, findFreePort());
        assertTrue(rootEntered.await(5, TimeUnit.SECONDS));
        peerA.connectToSession("127.0.0.1", initA.getServerPort());
        awaitCondition(() -> initA.getConnectedPeerCount() == 1, 3000L);
        releaseRoot.countDown();

        assertTrue(p1Entered.await(5, TimeUnit.SECONDS));
        Thread pauseThread = new Thread(initA::pause);
        pauseThread.start();
        awaitCondition(peerA::isPaused, 3000L);
        allowP1.countDown();
        pauseThread.join(5000L);

        assertTrue(pausedLatch.await(5, TimeUnit.SECONDS));
        assertTrue(initA.isPaused());
        assertTrue(peerA.isPaused());
        long sidA = initA.getActiveSession().getId();
        assertEquals(List.of(sidA), initA.getPausedSessions());

        initA.resume();
        assertTrue(initA.isRunning());
        awaitCondition(() -> !peerA.isPaused(), 3000L);
        assertTrue(p3Entered.await(5, TimeUnit.SECONDS));

        Thread stopThread = new Thread(initA::stop);
        stopThread.start();
        Thread.sleep(50L);
        allowP3.countDown();
        stopThread.join(5000L);

        assertTrue(finishedLatch.await(5, TimeUnit.SECONDS));
        assertEquals(SessionStatus.FINISHED, initA.getActiveSession().getStatus());
        assertEquals(CrawlSession.REASON_STOPPED_BY_USER, initA.getActiveSession().getCompletionReason());
        awaitCondition(() -> !peerA.isConnectedPeer(), 3000L);
        assertTrue(initA.getPausedSessions().isEmpty());

        CountDownLatch restartP1Entered = new CountDownLatch(1);
        CountDownLatch restartAllowP1 = new CountDownLatch(1);
        Map<String, StubPageLoader.Response> graphB1 = Map.of(
                "https://restart-p2p.test/", StubPageLoader.ok(
                        html("Root", "<p>відновлення старт " + LONG_TEXT + "</p><a href='/p1'>1</a><a href='/p2'>2</a><a href='/p3'>3</a>")),
                "https://restart-p2p.test/p1", StubPageLoader.blocking(
                        html("P1", "<p>відновлення p1 " + LONG_TEXT + "</p>"), restartP1Entered, restartAllowP1),
                "https://restart-p2p.test/p2", StubPageLoader.ok(html("P2", "<p>відновлення p2 " + LONG_TEXT + "</p>")),
                "https://restart-p2p.test/p3", StubPageLoader.ok(html("P3", "<p>відновлення p3 " + LONG_TEXT + "</p>"))
        );

        Path restartInitDir = tempDir.resolve("s8-restart-init");
        StubPageLoader init1Loader = new StubPageLoader(graphB1);
        CrawlService init1 = createFastService(restartInitDir, init1Loader, 1, 1);

        CountDownLatch init1Paused = new CountDownLatch(1);
        init1.addEventListener(new CrawlEventListener() {
            @Override
            public void onSessionPaused(CrawlSession session) {
                init1Paused.countDown();
            }
        });

        CrawlSettings settingsB = new CrawlSettings("https://restart-p2p.test/", "відновлення", StrategyType.BFS, 2, 10, false, false, FilterType.defaultEnabled());
        init1.createDistributedSession(settingsB, findFreePort());
        long persistedSessionId = init1.getActiveSession().getId();
        assertTrue(restartP1Entered.await(5, TimeUnit.SECONDS));

        Thread p1PauseThread = new Thread(init1::pause);
        p1PauseThread.start();
        Thread.sleep(60L);
        restartAllowP1.countDown();
        p1PauseThread.join(5000L);
        assertTrue(init1Paused.await(5, TimeUnit.SECONDS));

        init1.shutdown();
        servicesToShutdown.remove(init1);

        CountDownLatch p2Entered = new CountDownLatch(1);
        CountDownLatch allowP2 = new CountDownLatch(1);
        Map<String, StubPageLoader.Response> graphB2 = Map.of(
                "https://restart-p2p.test/", StubPageLoader.ok(html("Root", "<p>відновлення " + LONG_TEXT + "</p>")),
                "https://restart-p2p.test/p1", StubPageLoader.ok(html("P1", "<p>відновлення " + LONG_TEXT + "</p>")),
                "https://restart-p2p.test/p2", StubPageLoader.blocking(
                        html("P2", "<p>відновлення p2 " + LONG_TEXT + "</p>"), p2Entered, allowP2),
                "https://restart-p2p.test/p3", StubPageLoader.ok(html("P3", "<p>відновлення p3 " + LONG_TEXT + "</p>"))
        );
        StubPageLoader init2Loader = new StubPageLoader(graphB2);
        StubPageLoader peer2Loader = new StubPageLoader(graphB2);
        CrawlService init2 = createFastService(restartInitDir, init2Loader, 1, 1);
        CrawlService peer2 = createFastService(tempDir.resolve("s8-restart-peer2"), peer2Loader, 1, 1);

        assertEquals(List.of(persistedSessionId), init2.getPausedSessions());
        CountDownLatch init2Finished = new CountDownLatch(1);
        init2.addEventListener(finishedListener(init2Finished));

        init2.resumeDistributedSession(persistedSessionId, findFreePort());
        int resumedPort = init2.getServerPort();
        assertTrue(resumedPort > 0);
        assertTrue(p2Entered.await(5, TimeUnit.SECONDS));

        peer2.connectToSession("127.0.0.1", resumedPort);
        awaitCondition(() -> init2.getConnectedPeerCount() == 1, 3000L);
        allowP2.countDown();

        assertTrue(init2Finished.await(5, TimeUnit.SECONDS));
        awaitCondition(() -> !peer2.isConnectedPeer(), 3000L);

        CrawlSession finalSession = init2.findSessionById(persistedSessionId).orElseThrow();
        assertEquals(SessionStatus.FINISHED, finalSession.getStatus());
        assertEquals(4, finalSession.getVisitedCount());
        assertEquals(4, finalSession.getMatchedCount());
        assertTrue(init2.getPausedSessions().isEmpty());

        assertEquals(0, init2Loader.requestCount("https://restart-p2p.test/"));
        assertEquals(0, init2Loader.requestCount("https://restart-p2p.test/p1"));
        assertEquals(0, peer2Loader.requestCount("https://restart-p2p.test/"));
        assertEquals(0, peer2Loader.requestCount("https://restart-p2p.test/p1"));
        assertEquals(1, peer2Loader.requestCount("https://restart-p2p.test/p3"));
    }

    private static int findFreePort() throws IOException {
        for (int attempt = 0; attempt < 20; attempt++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                int port = socket.getLocalPort();
                if (port >= CrawlService.MIN_P2P_PORT && port <= CrawlService.MAX_P2P_PORT) {
                    return port;
                }
            }
        }
        throw new IOException("Не вдалося підібрати вільний TCP-порт у діапазоні 1024..65535.");
    }

    private static long countHtmlFiles(Path root) throws IOException {
        if (!Files.exists(root)) {
            return 0L;
        }
        try (var stream = Files.walk(root)) {
            return stream.filter(p -> Files.isRegularFile(p) && p.getFileName().toString().endsWith(".html")).count();
        }
    }

    private static CrawlEventListener finishedListener(CountDownLatch latch) {
        return new CrawlEventListener() {
            @Override
            public void onSessionFinished(CrawlSession session) {
                latch.countDown();
            }
        };
    }

    private static CrawlEventListener statusCollector(List<String> messages) {
        return new CrawlEventListener() {
            @Override
            public void onStatusMessage(String message) {
                messages.add(message);
            }
        };
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

    private static String html(String title, String body) {
        return "<html><head><title>" + title + "</title></head><body>" + body + "</body></html>";
    }

    private static final class StubPageLoader implements PageLoader {

        record Response(String html, long delayMs, CountDownLatch entered, CountDownLatch release) {
        }

        static Response ok(String html) {
            return new Response(html, 0L, null, null);
        }

        static Response delayed(String html, long delayMs) {
            return new Response(html, delayMs, null, null);
        }

        static Response blocking(String html, CountDownLatch entered, CountDownLatch release) {
            return new Response(html, 0L, entered, release);
        }

        private final Map<String, Response> responses;
        private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();

        StubPageLoader(Map<String, Response> responses) {
            this.responses = responses;
        }

        int requestCount(String url) {
            AtomicInteger count = counts.get(url);
            return count != null ? count.get() : 0;
        }

        Set<String> fetchedUrls() {
            return Set.copyOf(counts.keySet());
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
            if (response.delayMs() > 0L) {
                try {
                    Thread.sleep(response.delayMs());
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new PageLoadException("Interrupted", 0, false, ex);
                }
            }
            byte[] bytes = response.html().getBytes(StandardCharsets.UTF_8);
            return new LoadResult(response.html(), 200, bytes.length, LocalDateTime.now(), url, false);
        }

        @Override
        public LoadResult loadRobotsTxt(String hostRootUrl) {
            return new LoadResult("", 404, 0L, LocalDateTime.now(), hostRootUrl + "/robots.txt", false);
        }
    }
}
