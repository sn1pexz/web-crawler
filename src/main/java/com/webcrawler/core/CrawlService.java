package com.webcrawler.core;

import com.webcrawler.config.AppPaths;
import com.webcrawler.loader.CachingPageLoader;
import com.webcrawler.loader.DomainRateLimiter;
import com.webcrawler.loader.HttpPageLoader;
import com.webcrawler.loader.PageLoader;
import com.webcrawler.loader.RobotsTxtChecker;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.HostStatistics;
import com.webcrawler.model.Page;
import com.webcrawler.model.QueueEntry;
import com.webcrawler.model.SessionStatus;
import com.webcrawler.model.Site;
import com.webcrawler.model.StateSnapshot;
import com.webcrawler.model.StrategyType;
import com.webcrawler.p2p.PeerClient;
import com.webcrawler.p2p.PeerNode;
import com.webcrawler.p2p.PeerServer;
import com.webcrawler.p2p.SessionCoordinator;
import com.webcrawler.persistence.CacheRepository;
import com.webcrawler.persistence.DatabaseManager;
import com.webcrawler.persistence.PageRepository;
import com.webcrawler.persistence.SessionRepository;
import com.webcrawler.processing.LinkExtractor;
import com.webcrawler.processing.TermSearcher;
import com.webcrawler.util.UrlNormalizer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CrawlService implements AutoCloseable {

    public static final int MIN_P2P_PORT = 1024;
    public static final int MAX_P2P_PORT = 65535;
    private static final Logger LOGGER = Logger.getLogger(CrawlService.class.getName());
    private static final String INITIATOR_LOCAL_PEER_ID = "initiator-local";
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 10L;

    private final AppPaths appPaths;
    private final DatabaseManager databaseManager;
    private final SessionRepository sessionRepository;
    private final PageRepository pageRepository;
    private final CacheRepository cacheRepository;
    private final SnapshotManager snapshotManager;
    private final StatisticsService statisticsService;
    private final CachingPageLoader cachingPageLoader;
    private final RobotsTxtChecker robotsTxtChecker;
    private final DomainRateLimiter rateLimiter;
    private final LinkExtractor linkExtractor;
    private final TermSearcher termSearcher;
    private final Connection dbKeepAliveConnection;

    private final int workerThreadPoolSize;
    private final int coordinatorBatchSize;
    private final long heartbeatCheckIntervalMs;
    private final long commandAckTimeoutMs;
    private final long pauseAckTimeoutMs;
    private final long emptyBatchRetryMs;
    private final long peerHeartbeatIntervalMs;
    private final Supplier<LocalDateTime> serverClock;

    private final CopyOnWriteArrayList<CrawlEventListener> listeners = new CopyOnWriteArrayList<>();
    private final CrawlEventListener eventDispatcher = new SafeEventDispatcher();

    private final ExecutorService orchestrationExecutor = createOrchestrationExecutor();
    private final Object stateLock = new Object();
    private final Object dbSessionSyncLock = new Object();
    private final Object initiatorMonitor = new Object();
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private CrawlSession activeSession;
    private CrawlStrategy activeStrategy;
    private Future<?> localCrawlFuture;

    private SessionCoordinator sessionCoordinator;
    private PeerServer peerServer;
    private Future<?> initiatorWorkerFuture;
    private volatile boolean initiatorPauseRequested;
    private volatile boolean initiatorStopRequested;
    private volatile boolean initiatorInBatch;
    private volatile boolean serviceHandlingStop;
    private AtomicBoolean distributedFinishedNotified;

    private PeerClient peerClient;
    private volatile CrawlStrategy peerStrategy;

    public CrawlService() {
        this(new AppPaths());
    }

    public CrawlService(AppPaths appPaths) {
        this(appPaths, new DatabaseManager(Objects.requireNonNull(appPaths, "appPaths must not be null")));
    }

    public CrawlService(AppPaths appPaths, DatabaseManager databaseManager) {
        this(
                appPaths,
                databaseManager,
                new HttpPageLoader(),
                new DomainRateLimiter(),
                null,
                new LinkExtractor(),
                new TermSearcher(),
                CrawlStrategy.DEFAULT_THREAD_POOL_SIZE,
                SessionCoordinator.DEFAULT_BATCH_SIZE,
                PeerServer.HEARTBEAT_CHECK_INTERVAL_MS,
                PeerServer.COMMAND_ACK_TIMEOUT_MS,
                PeerServer.PAUSE_ACK_TIMEOUT_MS,
                PeerClient.EMPTY_BATCH_RETRY_MS,
                PeerClient.HEARTBEAT_INTERVAL_MS,
                LocalDateTime::now
        );
    }

    public CrawlService(
            AppPaths appPaths,
            DatabaseManager databaseManager,
            PageLoader upstreamLoader,
            DomainRateLimiter rateLimiter,
            RobotsTxtChecker robotsTxtChecker,
            LinkExtractor linkExtractor,
            TermSearcher termSearcher,
            int workerThreadPoolSize,
            int coordinatorBatchSize,
            long heartbeatCheckIntervalMs,
            long commandAckTimeoutMs,
            long pauseAckTimeoutMs,
            long emptyBatchRetryMs,
            long peerHeartbeatIntervalMs) {
        this(
                appPaths,
                databaseManager,
                upstreamLoader,
                rateLimiter,
                robotsTxtChecker,
                linkExtractor,
                termSearcher,
                workerThreadPoolSize,
                coordinatorBatchSize,
                heartbeatCheckIntervalMs,
                commandAckTimeoutMs,
                pauseAckTimeoutMs,
                emptyBatchRetryMs,
                peerHeartbeatIntervalMs,
                LocalDateTime::now
        );
    }

    public CrawlService(
            AppPaths appPaths,
            DatabaseManager databaseManager,
            PageLoader upstreamLoader,
            DomainRateLimiter rateLimiter,
            RobotsTxtChecker robotsTxtChecker,
            LinkExtractor linkExtractor,
            TermSearcher termSearcher,
            int workerThreadPoolSize,
            int coordinatorBatchSize,
            long heartbeatCheckIntervalMs,
            long commandAckTimeoutMs,
            long pauseAckTimeoutMs,
            long emptyBatchRetryMs,
            long peerHeartbeatIntervalMs,
            Supplier<LocalDateTime> serverClock) {
        this.appPaths = Objects.requireNonNull(appPaths, "appPaths must not be null");
        this.databaseManager = Objects.requireNonNull(databaseManager, "databaseManager must not be null");
        Objects.requireNonNull(upstreamLoader, "upstreamLoader must not be null");
        if (workerThreadPoolSize < 1) {
            throw new IllegalArgumentException("workerThreadPoolSize must be >= 1, got " + workerThreadPoolSize);
        }
        if (coordinatorBatchSize < 1) {
            throw new IllegalArgumentException("coordinatorBatchSize must be >= 1, got " + coordinatorBatchSize);
        }
        if (heartbeatCheckIntervalMs <= 0L || commandAckTimeoutMs <= 0L || pauseAckTimeoutMs <= 0L
                || emptyBatchRetryMs <= 0L || peerHeartbeatIntervalMs <= 0L) {
            throw new IllegalArgumentException("P2P timing intervals and timeouts must be > 0");
        }

        this.appPaths.ensureDirectoriesExist();
        this.databaseManager.initializeSchema();
        this.dbKeepAliveConnection = openKeepAliveConnection(databaseManager);

        this.sessionRepository = new SessionRepository(databaseManager);
        this.pageRepository = new PageRepository(databaseManager);
        this.cacheRepository = new CacheRepository(databaseManager, appPaths);
        this.snapshotManager = new SnapshotManager(databaseManager);
        this.statisticsService = new StatisticsService(databaseManager);

        this.cachingPageLoader = new CachingPageLoader(upstreamLoader, this.cacheRepository);
        this.robotsTxtChecker = robotsTxtChecker != null
                ? robotsTxtChecker
                : new RobotsTxtChecker(this.cachingPageLoader);
        this.rateLimiter = rateLimiter != null ? rateLimiter : new DomainRateLimiter();
        this.linkExtractor = linkExtractor != null ? linkExtractor : new LinkExtractor();
        this.termSearcher = termSearcher != null ? termSearcher : new TermSearcher();

        this.workerThreadPoolSize = workerThreadPoolSize;
        this.coordinatorBatchSize = coordinatorBatchSize;
        this.heartbeatCheckIntervalMs = heartbeatCheckIntervalMs;
        this.commandAckTimeoutMs = commandAckTimeoutMs;
        this.pauseAckTimeoutMs = pauseAckTimeoutMs;
        this.emptyBatchRetryMs = emptyBatchRetryMs;
        this.peerHeartbeatIntervalMs = peerHeartbeatIntervalMs;
        this.serverClock = serverClock != null ? serverClock : LocalDateTime::now;
    }

    public void addEventListener(CrawlEventListener listener) {
        if (listener != null && listener != CrawlEventListener.NO_OP) {
            listeners.addIfAbsent(listener);
        }
    }

    public void removeEventListener(CrawlEventListener listener) {
        if (listener != null) {
            listeners.remove(listener);
        }
    }

    public void setEventListener(CrawlEventListener listener) {
        listeners.clear();
        addEventListener(listener);
    }

    public CrawlSession start(CrawlSettings settings) {
        validateSettings(settings);
        CrawlSettings settingsCopy = new CrawlSettings(settings);

        synchronized (stateLock) {
            ensureOperationalAndIdle();
            rateLimiter.setActiveWorkersCount(1);

            CrawlSession session = new CrawlSession(settingsCopy);
            session.setDistributed(false);
            session.setServerPort(null);
            session.start();
            sessionRepository.create(session);

            CrawlStrategy strategy = createStrategy(settingsCopy, createLocalCrawlListener());
            session.setStrategy(null);

            this.activeSession = session;
            this.activeStrategy = strategy;
            this.localCrawlFuture = orchestrationExecutor.submit(() -> runLocalCrawl(session, strategy));

            eventDispatcher.onStatusMessage("Локальний обхід #" + session.getId() + " розпочато.");
            return session;
        }
    }

    public void pause() {
        ensureNotShutdown();
        boolean distributed;
        synchronized (stateLock) {
            if (isConnectedPeer()) {
                throw new IllegalStateException("Керувати розподіленою сесією може лише ініціатор.");
            }
            if (activeSession == null) {
                throw new IllegalStateException("Немає активної сесії для призупинення.");
            }
            if (activeSession.getStatus() == SessionStatus.PAUSED) {
                throw new IllegalStateException("Сесія вже призупинена.");
            }
            if (activeSession.getStatus() != SessionStatus.RUNNING) {
                throw new IllegalStateException("Немає активної сесії для призупинення.");
            }
            distributed = sessionCoordinator != null && activeSession.isDistributed();
        }

        if (distributed) {
            pauseDistributed();
        } else {
            pauseLocal();
        }
    }

    public void resume() {
        ensureNotShutdown();
        CrawlSession current;
        boolean hasOpenCoordinator;
        synchronized (stateLock) {
            ensureOperationalNotPeer();
            if (activeSession == null || activeSession.getStatus() != SessionStatus.PAUSED) {
                throw new IllegalStateException("Немає призупиненої сесії для відновлення.");
            }
            current = activeSession;
            hasOpenCoordinator = sessionCoordinator != null && peerServer != null && peerServer.isRunning();
        }

        if (hasOpenCoordinator) {
            resumeInMemoryDistributed(current);
        } else if (current.isDistributed()) {
            Integer port = current.getServerPort();
            throw new IllegalStateException(
                    "Розподілена сесія #" + current.getId() + " потребує відкриття порту ("
                            + (port != null ? port : 0) + ") через відновлення розподіленої сесії.");
        } else {
            resume(current.getId());
        }
    }

    public void resume(long sessionId) {
        ensureNotShutdown();

        CrawlSession existingInMemory;
        boolean isOpenDistributedMatch;
        synchronized (stateLock) {
            existingInMemory = this.activeSession;
            isOpenDistributedMatch = existingInMemory != null
                    && existingInMemory.getId() == sessionId
                    && existingInMemory.getStatus() == SessionStatus.PAUSED
                    && sessionCoordinator != null
                    && peerServer != null
                    && peerServer.isRunning();
        }
        if (isOpenDistributedMatch) {
            resumeInMemoryDistributed(existingInMemory);
            return;
        }

        synchronized (stateLock) {
            ensureOperationalAndIdle();
        }

        CrawlSession persisted = validateResumableSession(sessionId, false);
        StateSnapshot snapshot = loadSnapshotWithErrorNotification(sessionId);
        Set<String> visitedUrls = pageRepository.findVisitedUrlsBySessionId(sessionId);

        synchronized (stateLock) {
            ensureOperationalAndIdle();
            rateLimiter.setActiveWorkersCount(1);

            CrawlSettings restoredSettings = new CrawlSettings(snapshot.getSettings());
            CrawlSession restoredSession = new CrawlSession(sessionId, restoredSettings);
            restoredSession.setDistributed(false);
            restoredSession.setServerPort(null);

            CrawlStrategy strategy = createStrategy(restoredSettings, createLocalCrawlListener());
            restoredSession.setStrategy(strategy);
            restoredSession.restoreFrom(snapshot);
            strategy.setInitialVisitedUrls(visitedUrls);
            restoredSession.start();
            sessionRepository.update(restoredSession);
            restoredSession.setStrategy(null);

            this.activeSession = restoredSession;
            this.activeStrategy = strategy;
            this.localCrawlFuture = orchestrationExecutor.submit(() -> runLocalCrawl(restoredSession, strategy));

            eventDispatcher.onStatusMessage("Сесію #" + sessionId + " відновлено (попередніх URL: "
                    + visitedUrls.size() + ", у черзі: " + snapshot.getQueue().size() + ").");
        }
    }

    public void stop() {
        ensureNotShutdown();
        boolean distributed;
        synchronized (stateLock) {
            if (isConnectedPeer()) {
                throw new IllegalStateException("Керувати розподіленою сесією може лише ініціатор.");
            }
            if (activeSession == null) {
                throw new IllegalStateException("Немає активної або призупиненої сесії для зупинки.");
            }
            SessionStatus status = activeSession.getStatus();
            if (status == SessionStatus.FINISHED || status == SessionStatus.FAILED) {
                throw new IllegalStateException("Сесія вже остаточно завершена.");
            }
            distributed = sessionCoordinator != null;
        }

        if (distributed) {
            stopDistributed();
        } else {
            stopLocal();
        }
    }

    public void stopSession(long sessionId) {
        ensureNotShutdown();
        synchronized (stateLock) {
            if (activeSession != null && activeSession.getId() == sessionId) {
                stop();
                return;
            }
            CrawlSession stored = sessionRepository.findById(sessionId)
                    .orElseThrow(() -> new IllegalStateException("Сесію #" + sessionId + " не знайдено."));
            if (stored.getStatus() == SessionStatus.FINISHED || stored.getStatus() == SessionStatus.FAILED) {
                throw new IllegalStateException("Сесія #" + sessionId + " вже завершена.");
            }
            stored.stop();
            sessionRepository.update(stored);
            eventDispatcher.onStatusMessage("Сесію #" + sessionId + " зупинено користувачем.");
            eventDispatcher.onSessionFinished(stored);
        }
    }

    public void pauseOnExit() {
        if (isConnectedPeer()) {
            try {
                disconnectFromSession();
            } catch (RuntimeException ex) {
                LOGGER.log(Level.WARNING, "Failed graceful peer disconnect on exit", ex);
            }
            return;
        }

        CrawlSession session;
        boolean distributed;
        PeerServer serverToStop;
        Future<?> initiatorFutureToAwait = null;
        synchronized (stateLock) {
            session = this.activeSession;
            distributed = this.sessionCoordinator != null;
            serverToStop = this.peerServer;
        }

        if (session != null && session.getStatus() == SessionStatus.RUNNING) {
            try {
                if (distributed) {
                    pauseDistributed();
                } else {
                    pauseLocal();
                }
            } catch (RuntimeException ex) {
                LOGGER.log(Level.WARNING, "Failed to auto-pause session #" + session.getId() + " on exit", ex);
            }
        }

        if (distributed) {
            this.serviceHandlingStop = true;
            synchronized (initiatorMonitor) {
                initiatorStopRequested = true;
                initiatorPauseRequested = false;
                initiatorMonitor.notifyAll();
            }
            synchronized (stateLock) {
                initiatorFutureToAwait = this.initiatorWorkerFuture;
            }
            if (serverToStop != null) {
                serverToStop.stop();
            }
            awaitFutureQuietly(initiatorFutureToAwait);
            clearDistributedState();
        }
    }

    public void shutdown() {
        if (!shuttingDown.compareAndSet(false, true)) {
            return;
        }
        try {
            pauseOnExit();
        } catch (RuntimeException ex) {
            LOGGER.log(Level.WARNING, "Error during pauseOnExit in shutdown", ex);
        }

        PeerServer server;
        synchronized (stateLock) {
            server = this.peerServer;
            this.peerServer = null;
        }
        if (server != null) {
            server.stop();
        }

        shutdownExecutor(orchestrationExecutor);
        closeQuietly(dbKeepAliveConnection);
        databaseManager.close();
    }

    @Override
    public void close() {
        shutdown();
    }

    public void createDistributedSession(CrawlSettings settings, int port) throws IOException {
        validateSettings(settings);
        validateServerPort(port);

        CrawlSettings settingsCopy = new CrawlSettings(settings);
        synchronized (stateLock) {
            ensureOperationalAndIdle();
            rateLimiter.setActiveWorkersCount(1);

            CrawlSession session = new CrawlSession(settingsCopy);
            session.setDistributed(true);
            session.start();

            launchDistributedSessionLocked(session, settingsCopy, port, null, null, false);
            eventDispatcher.onStatusMessage("Розподілену сесію #" + session.getId()
                    + " створено на порту " + peerServer.getPort() + ".");
        }
    }

    public void resumeDistributedSession(long sessionId, int port) throws IOException {
        validateServerPort(port);
        ensureNotShutdown();

        synchronized (stateLock) {
            ensureOperationalAndIdle();
        }

        validateResumableSession(sessionId, true);
        StateSnapshot snapshot = loadSnapshotWithErrorNotification(sessionId);
        Set<String> visitedUrls = pageRepository.findVisitedUrlsBySessionId(sessionId);

        synchronized (stateLock) {
            ensureOperationalAndIdle();
            rateLimiter.setActiveWorkersCount(1);

            CrawlSettings restoredSettings = new CrawlSettings(snapshot.getSettings());
            CrawlSession restoredSession = new CrawlSession(sessionId, restoredSettings);
            restoredSession.setDistributed(true);
            restoredSession.restoreFrom(snapshot);
            restoredSession.start();

            launchDistributedSessionLocked(
                    restoredSession, restoredSettings, port, visitedUrls, snapshot.getQueue(), true);
            eventDispatcher.onStatusMessage("Розподілену сесію #" + sessionId
                    + " відновлено на порту " + peerServer.getPort() + ".");
        }
    }

    public void connectToSession(String host, int port) throws IOException {
        ensureNotShutdown();
        if (host == null || host.isBlank()) {
            String msg = "Некоректна адреса ініціатора.";
            eventDispatcher.onStatusMessage(msg);
            throw new IOException(msg);
        }
        if (port < MIN_P2P_PORT || port > MAX_P2P_PORT) {
            String msg = "Порт підключення має бути в діапазоні 1024..65535, отримано: " + port + ".";
            eventDispatcher.onStatusMessage(msg);
            throw new IOException(msg);
        }

        PeerClient client;
        synchronized (stateLock) {
            ensureOperationalAndIdle();
            rateLimiter.setActiveWorkersCount(1);
            this.peerStrategy = null;
            client = new PeerClient(
                    settings -> {
                        CrawlStrategy strategy = createStrategy(settings, eventDispatcher);
                        this.peerStrategy = strategy;
                        return strategy;
                    },
                    rateLimiter,
                    appPaths.getResultsDir(),
                    createPeerClientListener(),
                    emptyBatchRetryMs,
                    peerHeartbeatIntervalMs
            );
        }

        try {
            client.connect(host.trim(), port);
            synchronized (stateLock) {
                if (shuttingDown.get()) {
                    client.disconnectGracefully();
                    throw new IllegalStateException("Сервіс завершує роботу.");
                }
                this.peerClient = client;
            }
            client.startProcessingLoop();
        } catch (IOException ex) {
            synchronized (stateLock) {
                if (this.peerClient == client) {
                    this.peerClient = null;
                }
                this.peerStrategy = null;
            }
            rateLimiter.setActiveWorkersCount(1);
            String msg = ex.getMessage() != null && !ex.getMessage().isBlank()
                    ? ex.getMessage()
                    : "Не вдалося підключитися до ініціатора " + host + ":" + port + ".";
            eventDispatcher.onError(host + ":" + port, msg);
            eventDispatcher.onStatusMessage(msg);
            throw ex;
        }
    }

    public void disconnectFromSession() {
        PeerClient client;
        synchronized (stateLock) {
            client = this.peerClient;
            this.peerClient = null;
            this.peerStrategy = null;
        }
        if (client != null) {
            try {
                client.disconnectGracefully();
            } finally {
                rateLimiter.setActiveWorkersCount(1);
            }
        }
    }

    public List<Long> getPausedSessions() {
        return snapshotManager.getPausedSessionIds();
    }

    public List<CrawlSession> findAllSessions() {
        return sessionRepository.findAllOrderByCreatedDesc();
    }

    public Optional<CrawlSession> findSessionById(long sessionId) {
        return sessionRepository.findById(sessionId);
    }

    public List<Page> findPagesBySessionId(long sessionId) {
        return pageRepository.findBySessionId(sessionId);
    }

    public int getCacheTtlHours() {
        return cachingPageLoader.getTtlHours();
    }

    public void setCacheTtlHours(int ttlHours) {
        if (ttlHours < 0) {
            throw new IllegalArgumentException("Термін дії кешу не може бути від'ємним.");
        }
        cachingPageLoader.setTtlHours(ttlHours);
    }

    public void clearCache() {
        cachingPageLoader.clearCache();
        robotsTxtChecker.clearMemoryCache();
        eventDispatcher.onStatusMessage("Кеш сторінок та robots.txt очищено.");
    }

    public Map<String, Long> getSessionSummary(long sessionId) {
        return statisticsService.getSessionSummary(sessionId);
    }

    public Map<String, Long> getOverallSummary() {
        return statisticsService.getOverallSummary();
    }

    public Map<String, Long> getHostDistribution() {
        return statisticsService.getHostDistribution();
    }

    public Map<String, Long> getHostDistribution(long sessionId) {
        return statisticsService.getHostDistribution(sessionId);
    }

    public List<HostStatistics> getHostMetrics(Long sessionId) {
        return statisticsService.getHostMetrics(sessionId);
    }

    public boolean hasAnySessions() {
        return statisticsService.hasAnySessions();
    }

    public CrawlSession getActiveSession() {
        synchronized (stateLock) {
            return activeSession;
        }
    }

    public Map<String, Site> getSitesSnapshot() {
        CrawlStrategy strategy;
        synchronized (stateLock) {
            strategy = this.activeStrategy != null ? this.activeStrategy : this.peerStrategy;
        }
        return strategy != null ? strategy.getSitesSnapshot() : Map.of();
    }

    public List<Site> getCurrentSites() {
        return List.copyOf(getSitesSnapshot().values());
    }

    public int getServerPort() {
        synchronized (stateLock) {
            if (peerServer != null && peerServer.isRunning()) {
                return peerServer.getPort();
            }
            return -1;
        }
    }

    public List<PeerNode> getConnectedPeers() {
        SessionCoordinator coordinator;
        synchronized (stateLock) {
            coordinator = this.sessionCoordinator;
        }
        return coordinator != null ? coordinator.getActivePeers() : List.of();
    }

    public int getConnectedPeerCount() {
        return getConnectedPeers().size();
    }

    public int getActiveWorkersCount() {
        SessionCoordinator coordinator;
        PeerClient client;
        synchronized (stateLock) {
            coordinator = this.sessionCoordinator;
            client = this.peerClient;
        }
        if (coordinator != null) {
            return coordinator.getActiveWorkersCount();
        }
        if (client != null && client.isConnected()) {
            return rateLimiter.getActiveWorkersCount();
        }
        return 1;
    }

    public boolean isRunning() {
        synchronized (stateLock) {
            if (activeSession != null && activeSession.getStatus() == SessionStatus.RUNNING) {
                return true;
            }
            return peerClient != null && peerClient.isConnected() && !peerClient.isPaused() && !peerClient.isStopped();
        }
    }

    public boolean isPaused() {
        synchronized (stateLock) {
            if (activeSession != null && activeSession.getStatus() == SessionStatus.PAUSED) {
                return true;
            }
            return peerClient != null && peerClient.isConnected() && peerClient.isPaused();
        }
    }

    public boolean isDistributedInitiator() {
        synchronized (stateLock) {
            return activeSession != null
                    && activeSession.isDistributed()
                    && sessionCoordinator != null
                    && (activeSession.getStatus() == SessionStatus.RUNNING
                    || activeSession.getStatus() == SessionStatus.PAUSED
                    || (peerServer != null && peerServer.isRunning()));
        }
    }

    public boolean isConnectedPeer() {
        synchronized (stateLock) {
            return peerClient != null && peerClient.isConnected();
        }
    }

    public AppPaths getAppPaths() {
        return appPaths;
    }

    public DatabaseManager getDatabaseManager() {
        return databaseManager;
    }

    public SessionRepository getSessionRepository() {
        return sessionRepository;
    }

    public PageRepository getPageRepository() {
        return pageRepository;
    }

    public CacheRepository getCacheRepository() {
        return cacheRepository;
    }

    public SnapshotManager getSnapshotManager() {
        return snapshotManager;
    }

    public StatisticsService getStatisticsService() {
        return statisticsService;
    }

    public SessionCoordinator getSessionCoordinator() {
        synchronized (stateLock) {
            return sessionCoordinator;
        }
    }

    private void launchDistributedSessionLocked(
            CrawlSession session,
            CrawlSettings settings,
            int port,
            Set<String> restoredVisitedUrls,
            List<QueueEntry> restoredQueue,
            boolean isResume) throws IOException {
        AtomicBoolean finishedFlag = new AtomicBoolean(false);
        CrawlEventListener coordinatorListener = createDistributedCoordinatorListener(finishedFlag);
        SessionCoordinator coordinator = new SessionCoordinator(
                session, pageRepository, sessionRepository, null, coordinatorListener, coordinatorBatchSize);
        if (isResume) {
            coordinator.restoreFromSession(restoredVisitedUrls, restoredQueue);
        }

        PeerServer server = new PeerServer(
                coordinator, heartbeatCheckIntervalMs, commandAckTimeoutMs, pauseAckTimeoutMs, serverClock);
        try {
            synchronized (coordinator) {
                server.openPort(port);
                session.setServerPort(server.getPort());
                if (isResume) {
                    sessionRepository.update(session);
                } else {
                    sessionRepository.create(session);
                }
            }
        } catch (IOException | RuntimeException ex) {
            server.stop();
            String msg = ex.getMessage() != null && !ex.getMessage().isBlank()
                    ? ex.getMessage()
                    : "Не вдалося відкрити порт " + port + ".";
            eventDispatcher.onError("port:" + port, msg);
            eventDispatcher.onStatusMessage(msg);
            throw ex;
        }

        CrawlStrategy initiatorStrategy = createStrategy(settings, eventDispatcher);
        session.setStrategy(initiatorStrategy);

        this.initiatorPauseRequested = false;
        this.initiatorStopRequested = false;
        this.initiatorInBatch = false;
        this.serviceHandlingStop = false;
        this.distributedFinishedNotified = finishedFlag;
        this.activeSession = session;
        this.activeStrategy = initiatorStrategy;
        this.sessionCoordinator = coordinator;
        this.peerServer = server;
        this.initiatorWorkerFuture = orchestrationExecutor.submit(() -> runInitiatorWorkerLoop(
                session, settings, coordinator, server, initiatorStrategy, finishedFlag));
    }

    private void runLocalCrawl(CrawlSession session, CrawlStrategy strategy) {
        try {
            strategy.crawl(session);
            if (!strategy.isPauseRequested() && !strategy.isStopRequested()) {
                syncSessionToDbQuietly(session);
            }
        } catch (RuntimeException ex) {
            LOGGER.log(Level.SEVERE, "Unexpected error during local crawl session #" + session.getId(), ex);
            if (session.getStatus() != SessionStatus.FINISHED && session.getStatus() != SessionStatus.FAILED) {
                session.fail("Помилка виконання обходу: " + ex.getMessage());
            }
            syncSessionToDbQuietly(session);
            String msg = ex.getMessage() != null ? ex.getMessage() : "Помилка виконання обходу.";
            eventDispatcher.onError("session#" + session.getId(), msg);
            eventDispatcher.onStatusMessage(msg);
            eventDispatcher.onSessionFinished(session);
        }
    }

    private void pauseLocal() {
        CrawlSession session;
        CrawlStrategy strategy;
        Future<?> future;
        synchronized (stateLock) {
            session = this.activeSession;
            strategy = this.activeStrategy;
            future = this.localCrawlFuture;
        }
        if (session == null || strategy == null || session.getStatus() != SessionStatus.RUNNING) {
            throw new IllegalStateException("Немає активного локального обходу для призупинення.");
        }

        awaitStrategyEntered(session, future);
        strategy.requestPause();
        awaitFuture(future);

        session.setStrategy(strategy);
        SessionStatus statusAfterDrain = session.getStatus();
        if (statusAfterDrain == SessionStatus.FINISHED || statusAfterDrain == SessionStatus.FAILED) {
            return;
        }

        StateSnapshot snapshot = session.createSnapshot();
        saveSnapshotOrFailSession(session, snapshot);
        if (session.getStatus() == SessionStatus.RUNNING) {
            session.pause();
        }
        eventDispatcher.onStatusMessage("Сесію #" + session.getId() + " призупинено. Знімок стану збережено.");
        eventDispatcher.onSessionPaused(session);
    }

    private void pauseDistributed() {
        CrawlSession session;
        SessionCoordinator coordinator;
        synchronized (stateLock) {
            session = this.activeSession;
            coordinator = this.sessionCoordinator;
        }
        if (session == null || coordinator == null || session.getStatus() != SessionStatus.RUNNING) {
            throw new IllegalStateException("Немає активної розподіленої сесії для призупинення.");
        }

        synchronized (initiatorMonitor) {
            initiatorPauseRequested = true;
            initiatorMonitor.notifyAll();
        }

        coordinator.broadcastCommand("PAUSE");

        synchronized (initiatorMonitor) {
            while (initiatorInBatch) {
                try {
                    initiatorMonitor.wait();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Очікування паузи ініціатора перервано.", ex);
                }
            }
        }

        SessionStatus currentStatus = coordinator.getSessionStatus();
        if (currentStatus == SessionStatus.FINISHED || currentStatus == SessionStatus.FAILED) {
            return;
        }

        StateSnapshot snapshot = createDistributedSnapshot(session, coordinator);
        saveSnapshotOrFailSession(session, snapshot);
        eventDispatcher.onStatusMessage("Розподілену сесію #" + session.getId()
                + " призупинено. Знімок стану збережено.");
        eventDispatcher.onSessionPaused(session);
    }

    private void resumeInMemoryDistributed(CrawlSession session) {
        SessionCoordinator coordinator;
        synchronized (stateLock) {
            coordinator = this.sessionCoordinator;
        }
        if (coordinator == null) {
            throw new IllegalStateException("Координатор розподіленої сесії недоступний.");
        }
        coordinator.broadcastCommand("RESUME");
        synchronized (initiatorMonitor) {
            initiatorPauseRequested = false;
            initiatorMonitor.notifyAll();
        }
        eventDispatcher.onStatusMessage("Розподілену сесію #" + session.getId() + " відновлено.");
    }

    private void stopLocal() {
        CrawlSession session;
        CrawlStrategy strategy;
        Future<?> future;
        synchronized (stateLock) {
            session = this.activeSession;
            strategy = this.activeStrategy;
            future = this.localCrawlFuture;
        }
        if (session == null) {
            throw new IllegalStateException("Немає активної або призупиненої сесії для зупинки.");
        }

        if (strategy != null) {
            awaitStrategyEntered(session, future);
            strategy.requestStop();
        }
        awaitFuture(future);

        boolean stoppedNow = false;
        synchronized (stateLock) {
            SessionStatus current = session.getStatus();
            if (current != SessionStatus.FINISHED && current != SessionStatus.FAILED) {
                session.stop();
                if (strategy != null) {
                    strategy.restoreQueue(List.of());
                }
                syncSessionToDbQuietly(session);
                stoppedNow = true;
            }
        }
        if (stoppedNow) {
            eventDispatcher.onStatusMessage("Обхід #" + session.getId() + " зупинено користувачем.");
            eventDispatcher.onSessionFinished(session);
        }
    }

    private void stopDistributed() {
        CrawlSession session;
        SessionCoordinator coordinator;
        PeerServer server;
        Future<?> workerFuture;
        AtomicBoolean finishedFlag;
        synchronized (stateLock) {
            session = this.activeSession;
            coordinator = this.sessionCoordinator;
            server = this.peerServer;
            workerFuture = this.initiatorWorkerFuture;
            finishedFlag = this.distributedFinishedNotified;
        }
        if (session == null || coordinator == null) {
            throw new IllegalStateException("Немає активної розподіленої сесії для зупинки.");
        }

        this.serviceHandlingStop = true;
        synchronized (initiatorMonitor) {
            initiatorStopRequested = true;
            initiatorPauseRequested = false;
            initiatorMonitor.notifyAll();
        }

        try {
            SessionStatus status = coordinator.getSessionStatus();
            if (status != SessionStatus.FINISHED && status != SessionStatus.FAILED) {
                coordinator.broadcastCommand("STOP");
            }
        } finally {
            awaitFutureQuietly(workerFuture);
            if (server != null) {
                server.stop();
            }
            clearDistributedState();
        }

        if (finishedFlag == null || finishedFlag.compareAndSet(false, true)) {
            eventDispatcher.onStatusMessage("Розподілений обхід #" + session.getId() + " зупинено користувачем.");
            eventDispatcher.onSessionFinished(session);
        }
    }

    private void runInitiatorWorkerLoop(
            CrawlSession session,
            CrawlSettings settings,
            SessionCoordinator coordinator,
            PeerServer server,
            CrawlStrategy initiatorStrategy,
            AtomicBoolean finishedFlag) {
        PeerNode initiatorNode = new PeerNode(INITIATOR_LOCAL_PEER_ID, "127.0.0.1", server.getPort());
        try {
            //Finish immediately if resumed distributed queue is already empty
            synchronized (coordinator) {
                if (coordinator.getSessionStatus() == SessionStatus.RUNNING
                        && coordinator.getQueueSnapshot().isEmpty()
                        && !coordinator.hasInFlightBatches()) {
                    coordinator.acceptResult(initiatorNode, List.of(), List.of(), List.of());
                }
            }

            while (!initiatorStopRequested && !Thread.currentThread().isInterrupted()) {
                synchronized (initiatorMonitor) {
                    while ((initiatorPauseRequested || coordinator.getSessionStatus() == SessionStatus.PAUSED)
                            && !initiatorStopRequested
                            && coordinator.getSessionStatus() != SessionStatus.FINISHED
                            && coordinator.getSessionStatus() != SessionStatus.FAILED) {
                        initiatorMonitor.wait();
                    }
                    if (initiatorStopRequested
                            || coordinator.getSessionStatus() == SessionStatus.FINISHED
                            || coordinator.getSessionStatus() == SessionStatus.FAILED) {
                        break;
                    }
                    initiatorInBatch = true;
                }

                try {
                    rateLimiter.setActiveWorkersCount(coordinator.getActiveWorkersCount());
                    List<QueueEntry> batch = coordinator.assignBatch(initiatorNode);
                    if (batch.isEmpty()) {
                        if (coordinator.getSessionStatus() == SessionStatus.FINISHED
                                || coordinator.getSessionStatus() == SessionStatus.FAILED) {
                            break;
                        }
                        synchronized (initiatorMonitor) {
                            initiatorInBatch = false;
                            initiatorMonitor.notifyAll();
                            if (!initiatorStopRequested
                                    && !initiatorPauseRequested
                                    && coordinator.getSessionStatus() == SessionStatus.RUNNING) {
                                initiatorMonitor.wait(emptyBatchRetryMs);
                            }
                        }
                        continue;
                    }

                    CrawlStrategy.BatchResult result = initiatorStrategy.processBatch(
                            batch,
                            settings,
                            appPaths.getResultsDir(),
                            () -> initiatorPauseRequested
                                    || initiatorStopRequested
                                    || coordinator.getSessionStatus() == SessionStatus.FINISHED
                                    || coordinator.getSessionStatus() == SessionStatus.FAILED
                    );

                    List<Page> pages = result != null ? result.pages() : List.of();
                    List<QueueEntry> foundLinks = result != null ? result.foundLinks() : List.of();
                    List<QueueEntry> unprocessed = result != null ? result.unprocessedEntries() : List.of();
                    List<String> handledUrls = computeHandledUrls(batch, unprocessed);

                    if (!pages.isEmpty() || !foundLinks.isEmpty() || !handledUrls.isEmpty()) {
                        coordinator.acceptResult(initiatorNode, pages, foundLinks, handledUrls);
                    }
                    if (!unprocessed.isEmpty()) {
                        coordinator.returnUnprocessedEntries(initiatorNode, unprocessed);
                    }
                } finally {
                    synchronized (initiatorMonitor) {
                        initiatorInBatch = false;
                        initiatorMonitor.notifyAll();
                    }
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException ex) {
            LOGGER.log(Level.SEVERE, "Unexpected error in distributed initiator worker loop", ex);
            if (session.getStatus() != SessionStatus.FINISHED && session.getStatus() != SessionStatus.FAILED) {
                session.fail("Помилка розподіленого обходу: " + ex.getMessage());
                syncSessionToDbQuietly(session);
            }
            String errMsg = ex.getMessage() != null ? ex.getMessage() : "Помилка розподіленого обходу.";
            eventDispatcher.onError("session#" + session.getId(), errMsg);
            eventDispatcher.onStatusMessage(errMsg);
        } finally {
            if (!serviceHandlingStop) {
                server.stop();
                clearDistributedState();
                if (finishedFlag.compareAndSet(false, true)) {
                    String reason = session.getCompletionReason() != null
                            ? session.getCompletionReason()
                            : "Завершено";
                    eventDispatcher.onStatusMessage("Розподілений обхід #" + session.getId()
                            + " завершено: " + reason + ".");
                    eventDispatcher.onSessionFinished(session);
                }
            }
        }
    }

    private void clearDistributedState() {
        synchronized (stateLock) {
            this.sessionCoordinator = null;
            this.peerServer = null;
            this.initiatorWorkerFuture = null;
            this.distributedFinishedNotified = null;
        }
        rateLimiter.setActiveWorkersCount(1);
    }

    private CrawlEventListener createLocalCrawlListener() {
        return new CrawlEventListener() {
            @Override
            public void onProgressUpdated(CrawlSession session) {
                syncSessionToDbQuietly(session);
                eventDispatcher.onProgressUpdated(session);
            }

            @Override
            public void onPageProcessed(Page page) {
                eventDispatcher.onPageProcessed(page);
            }

            @Override
            public void onError(String url, String errorMessage) {
                eventDispatcher.onError(url, errorMessage);
            }

            @Override
            public void onSessionPaused(CrawlSession session) {
                eventDispatcher.onSessionPaused(session);
            }

            @Override
            public void onSessionFinished(CrawlSession session) {
                syncSessionToDbQuietly(session);
                String reason = session.getCompletionReason() != null ? session.getCompletionReason() : "Завершено";
                eventDispatcher.onStatusMessage("Обхід #" + session.getId() + " завершено: " + reason + ".");
                eventDispatcher.onSessionFinished(session);
            }

            @Override
            public void onStatusMessage(String message) {
                eventDispatcher.onStatusMessage(message);
            }
        };
    }

    private CrawlEventListener createDistributedCoordinatorListener(AtomicBoolean finishedFlag) {
        return new CrawlEventListener() {
            @Override
            public void onProgressUpdated(CrawlSession session) {
                notifyInitiatorMonitor();
                eventDispatcher.onProgressUpdated(session);
            }

            @Override
            public void onPageProcessed(Page page) {
                eventDispatcher.onPageProcessed(page);
            }

            @Override
            public void onError(String url, String errorMessage) {
                eventDispatcher.onError(url, errorMessage);
            }

            //Defer pause notification until worker settles and snapshot commits
            @Override
            public void onSessionPaused(CrawlSession session) {
            }

            @Override
            public void onSessionFinished(CrawlSession session) {
                synchronized (initiatorMonitor) {
                    initiatorStopRequested = true;
                    initiatorMonitor.notifyAll();
                }
            }

            @Override
            public void onPeerConnected(PeerNode peer) {
                syncCoordinatorWorkerCount();
                eventDispatcher.onPeerConnected(peer);
            }

            @Override
            public void onPeerLost(PeerNode peer) {
                syncCoordinatorWorkerCount();
                notifyInitiatorMonitor();
                eventDispatcher.onPeerLost(peer);
            }

            @Override
            public void onStatusMessage(String message) {
                syncCoordinatorWorkerCount();
                eventDispatcher.onStatusMessage(message);
            }
        };
    }

    private CrawlEventListener createPeerClientListener() {
        return new CrawlEventListener() {
            @Override
            public void onProgressUpdated(CrawlSession session) {
                eventDispatcher.onProgressUpdated(session);
            }

            @Override
            public void onPageProcessed(Page page) {
                eventDispatcher.onPageProcessed(page);
            }

            @Override
            public void onError(String url, String errorMessage) {
                eventDispatcher.onError(url, errorMessage);
            }

            @Override
            public void onStatusMessage(String message) {
                eventDispatcher.onStatusMessage(message);
            }
        };
    }

    private CrawlStrategy createStrategy(CrawlSettings settings, CrawlEventListener listener) {
        return settings.getStrategy() == StrategyType.DFS
                ? new DepthFirstCrawl(
                        cachingPageLoader,
                        linkExtractor,
                        termSearcher,
                        robotsTxtChecker,
                        rateLimiter,
                        pageRepository,
                        appPaths.getResultsDir(),
                        listener,
                        workerThreadPoolSize)
                : new BreadthFirstCrawl(
                        cachingPageLoader,
                        linkExtractor,
                        termSearcher,
                        robotsTxtChecker,
                        rateLimiter,
                        pageRepository,
                        appPaths.getResultsDir(),
                        listener,
                        workerThreadPoolSize);
    }

    private CrawlSession validateResumableSession(long sessionId, boolean expectDistributed) {
        Optional<CrawlSession> storedOpt = sessionRepository.findById(sessionId);
        if (storedOpt.isEmpty()) {
            snapshotManager.load(sessionId);
            throw new IllegalStateException("Сесію #" + sessionId + " не знайдено.");
        }
        CrawlSession stored = storedOpt.get();
        if (stored.getStatus() != SessionStatus.PAUSED) {
            String msg = "Неможливо відновити сесію #" + sessionId
                    + ": сесія має статус " + stored.getStatus() + ".";
            eventDispatcher.onStatusMessage(msg);
            eventDispatcher.onError("session#" + sessionId, msg);
            throw new IllegalStateException(msg);
        }
        if (!expectDistributed && stored.isDistributed()) {
            String msg = "Сесія #" + sessionId
                    + " є розподіленою. Використайте відновлення розподіленої сесії із зазначенням порту.";
            eventDispatcher.onStatusMessage(msg);
            throw new IllegalStateException(msg);
        }
        return stored;
    }

    private StateSnapshot loadSnapshotWithErrorNotification(long sessionId) {
        try {
            return snapshotManager.load(sessionId);
        } catch (RuntimeException ex) {
            String message = ex.getMessage() != null && !ex.getMessage().isBlank()
                    ? ex.getMessage()
                    : "Знімок стану сесії #" + sessionId + " пошкоджено або не знайдено.";
            eventDispatcher.onError("session#" + sessionId, message);
            eventDispatcher.onStatusMessage(message);
            throw ex;
        }
    }

    private void saveSnapshotOrFailSession(CrawlSession session, StateSnapshot snapshot) {
        try {
            snapshotManager.save(snapshot);
        } catch (RuntimeException ex) {
            try {
                if (session.getStatus() == SessionStatus.RUNNING) {
                    session.fail("Помилка збереження знімка стану");
                    syncSessionToDbQuietly(session);
                }
            } catch (RuntimeException secondary) {
                ex.addSuppressed(secondary);
            }
            String errMsg = ex.getMessage() != null && !ex.getMessage().isBlank()
                    ? ex.getMessage()
                    : "Не вдалося зберегти знімок стану сесії #" + session.getId() + ".";
            eventDispatcher.onError("session#" + session.getId(), errMsg);
            eventDispatcher.onStatusMessage(errMsg);
            eventDispatcher.onSessionFinished(session);
            throw ex;
        }
    }

    private static StateSnapshot createDistributedSnapshot(CrawlSession session, SessionCoordinator coordinator) {
        Map<String, Integer> counters = new LinkedHashMap<>();
        counters.put(StateSnapshot.COUNTER_VISITED, session.getVisitedCount());
        counters.put(StateSnapshot.COUNTER_MATCHED, session.getMatchedCount());
        counters.put(StateSnapshot.COUNTER_ERRORS, session.getErrorCount());
        return new StateSnapshot(
                session.getId(),
                new CrawlSettings(session.getSettings()),
                coordinator.getQueueSnapshot(),
                counters
        );
    }

    private void syncCoordinatorWorkerCount() {
        SessionCoordinator coordinator;
        synchronized (stateLock) {
            coordinator = this.sessionCoordinator;
        }
        if (coordinator != null) {
            rateLimiter.setActiveWorkersCount(coordinator.getActiveWorkersCount());
        }
    }

    private void notifyInitiatorMonitor() {
        synchronized (initiatorMonitor) {
            initiatorMonitor.notifyAll();
        }
    }

    private void syncSessionToDbQuietly(CrawlSession session) {
        if (session == null || session.getId() <= 0 || databaseManager.isClosed()) {
            return;
        }
        synchronized (dbSessionSyncLock) {
            if (databaseManager.isClosed()) {
                return;
            }
            sessionRepository.update(session);
        }
    }

    private void validateSettings(CrawlSettings settings) {
        if (settings == null) {
            String msg = "Налаштування обходу не можуть бути порожніми.";
            eventDispatcher.onStatusMessage(msg);
            throw new IllegalArgumentException(msg);
        }
        Optional<String> validationError = settings.validateWithMessage();
        if (validationError.isPresent()) {
            String msg = validationError.get();
            eventDispatcher.onStatusMessage(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    private void validateServerPort(int port) {
        if (port < MIN_P2P_PORT || port > MAX_P2P_PORT) {
            String msg = "Порт має бути в діапазоні 1024..65535, отримано: " + port + ".";
            eventDispatcher.onStatusMessage(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    private void ensureNotShutdown() {
        if (shuttingDown.get()) {
            throw new IllegalStateException("Сервіс обходу вже зупинено.");
        }
    }

    private void ensureOperationalNotPeer() {
        ensureNotShutdown();
        if (peerClient != null && peerClient.isConnected()) {
            throw new IllegalStateException(
                    "Екземпляр підключений до розподіленої сесії як вузол-учасник. Спочатку відключіться.");
        }
    }

    private void ensureOperationalAndIdle() {
        ensureOperationalNotPeer();
        if (activeSession != null && activeSession.getStatus() == SessionStatus.RUNNING) {
            throw new IllegalStateException(
                    "Активний обхід уже виконується. Спочатку призупиніть або зупиніть поточну сесію.");
        }
        if (peerServer != null && peerServer.isRunning()) {
            throw new IllegalStateException(
                    "Розподілена сесія зараз активна або призупинена. Спочатку зупиніть її.");
        }
    }

    private static List<String> computeHandledUrls(List<QueueEntry> batch, List<QueueEntry> unprocessed) {
        if (batch == null || batch.isEmpty()) {
            return List.of();
        }
        Set<String> unprocessedKeys = new HashSet<>();
        if (unprocessed != null) {
            for (QueueEntry entry : unprocessed) {
                if (entry != null && entry.getUrl() != null) {
                    unprocessedKeys.add(canonicalKey(entry.getUrl()));
                }
            }
        }
        List<String> handled = new ArrayList<>();
        for (QueueEntry entry : batch) {
            if (entry != null && entry.getUrl() != null && !unprocessedKeys.contains(canonicalKey(entry.getUrl()))) {
                handled.add(entry.getUrl());
            }
        }
        return handled;
    }

    private static String canonicalKey(String url) {
        return UrlNormalizer.tryNormalize(url).orElse(url.trim());
    }

    private static void awaitStrategyEntered(CrawlSession session, Future<?> future) {
        if (future == null) {
            return;
        }
        while (!future.isDone() && session.getStrategy() == null) {
            Thread.onSpinWait();
            try {
                Thread.sleep(1L);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static void awaitFuture(Future<?> future) {
        if (future == null) {
            return;
        }
        try {
            future.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Очікування завершення фонового завдання перервано.", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException runtimeEx) {
                throw runtimeEx;
            }
            throw new IllegalStateException("Фонове завдання обходу завершилося з помилкою.", cause);
        }
    }

    private static void awaitFutureQuietly(Future<?> future) {
        if (future == null) {
            return;
        }
        try {
            future.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException ex) {
            LOGGER.log(Level.FINE, "Background task finished with exception during cleanup", ex.getCause());
        }
    }

    private static Connection openKeepAliveConnection(DatabaseManager databaseManager) {
        try {
            return databaseManager.getConnection();
        } catch (SQLException ex) {
            LOGGER.log(Level.FINE, "Could not open H2 keep-alive connection", ex);
            return null;
        }
    }

    private static void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ex) {
                LOGGER.log(Level.FINE, "Error closing H2 keep-alive connection", ex);
            }
        }
    }

    private static ExecutorService createOrchestrationExecutor() {
        AtomicInteger threadIndex = new AtomicInteger(1);
        return Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "crawl-service-runner-" + threadIndex.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
    }

    private static void shutdownExecutor(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                executor.awaitTermination(2L, TimeUnit.SECONDS);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private final class SafeEventDispatcher implements CrawlEventListener {

        @Override
        public void onProgressUpdated(CrawlSession session) {
            dispatch(listener -> listener.onProgressUpdated(session));
        }

        @Override
        public void onPageProcessed(Page page) {
            dispatch(listener -> listener.onPageProcessed(page));
        }

        @Override
        public void onError(String url, String errorMessage) {
            dispatch(listener -> listener.onError(url, errorMessage));
        }

        @Override
        public void onSessionPaused(CrawlSession session) {
            dispatch(listener -> listener.onSessionPaused(session));
        }

        @Override
        public void onSessionFinished(CrawlSession session) {
            dispatch(listener -> listener.onSessionFinished(session));
        }

        @Override
        public void onPeerConnected(PeerNode peer) {
            dispatch(listener -> listener.onPeerConnected(peer));
        }

        @Override
        public void onPeerLost(PeerNode peer) {
            dispatch(listener -> listener.onPeerLost(peer));
        }

        @Override
        public void onStatusMessage(String message) {
            dispatch(listener -> listener.onStatusMessage(message));
        }

        private void dispatch(Consumer<CrawlEventListener> action) {
            for (CrawlEventListener listener : listeners) {
                try {
                    action.accept(listener);
                } catch (RuntimeException ex) {
                    LOGGER.log(Level.WARNING, "CrawlEventListener threw an unexpected exception", ex);
                }
            }
        }
    }
}
