package com.webcrawler.p2p;

import com.google.gson.JsonObject;
import com.webcrawler.core.BreadthFirstCrawl;
import com.webcrawler.core.CrawlEventListener;
import com.webcrawler.core.CrawlStrategy;
import com.webcrawler.core.DepthFirstCrawl;
import com.webcrawler.loader.DomainRateLimiter;
import com.webcrawler.loader.PageLoader;
import com.webcrawler.loader.RobotsTxtChecker;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.Page;
import com.webcrawler.model.QueueEntry;
import com.webcrawler.model.StrategyType;
import com.webcrawler.p2p.protocol.MessageType;
import com.webcrawler.p2p.protocol.PeerMessage;
import com.webcrawler.p2p.protocol.PeerMessageCodec;
import com.webcrawler.processing.LinkExtractor;
import com.webcrawler.processing.TermSearcher;
import com.webcrawler.util.UrlNormalizer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

//P2P worker client connecting to initiator over TCP
public class PeerClient {

    public static final int MIN_PORT = 1024;
    public static final int MAX_PORT = 65535;
    public static final long EMPTY_BATCH_RETRY_MS = 1000L;
    public static final long HEARTBEAT_INTERVAL_MS = 5000L;

    private static final Logger LOGGER = Logger.getLogger(PeerClient.class.getName());
    private static final long CONNECT_TIMEOUT_MS = 5000L;
    private static final long RESPONSE_TIMEOUT_MS = 10000L;

    private String initiatorHost;
    private int initiatorPort;

    private final String peerId;
    private final Function<CrawlSettings, CrawlStrategy> strategyFactory;
    private final DomainRateLimiter rateLimiter;
    private final Path resultsRoot;
    private final CrawlEventListener eventListener;
    private final long emptyBatchRetryMs;
    private final long heartbeatIntervalMs;
    private final PeerMessageCodec codec = new PeerMessageCodec();

    private final Object lifecycleLock = new Object();
    private final Object writeLock = new Object();
    private final Object pauseMonitor = new Object();
    private final ConcurrentMap<String, CompletableFuture<PeerMessage>> pendingResponses = new ConcurrentHashMap<>();

    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;
    private Thread socketReaderThread;
    private Thread workerThread;
    private ScheduledExecutorService heartbeatScheduler;

    private volatile CrawlSettings sessionSettings;
    private volatile CrawlStrategy strategy;
    private volatile boolean connected;
    private volatile boolean paused;
    private volatile boolean stopped;
    private volatile boolean pauseRequested;
    private volatile boolean intentionalDisconnect;
    private volatile boolean requestingBatch;
    private List<QueueEntry> currentBatch;
    private String activePauseRequestId;

    public PeerClient(
            PageLoader pageLoader,
            LinkExtractor linkExtractor,
            TermSearcher termSearcher,
            RobotsTxtChecker robotsTxtChecker,
            DomainRateLimiter rateLimiter,
            Path resultsRoot,
            CrawlEventListener eventListener) {
        this(
                createDefaultStrategyFactory(
                        Objects.requireNonNull(pageLoader, "pageLoader must not be null"),
                        linkExtractor,
                        termSearcher,
                        robotsTxtChecker,
                        rateLimiter,
                        resultsRoot,
                        eventListener
                ),
                rateLimiter,
                resultsRoot,
                eventListener
        );
    }

    public PeerClient(
            Function<CrawlSettings, CrawlStrategy> strategyFactory,
            DomainRateLimiter rateLimiter,
            Path resultsRoot,
            CrawlEventListener eventListener) {
        this(strategyFactory, rateLimiter, resultsRoot, eventListener, EMPTY_BATCH_RETRY_MS, HEARTBEAT_INTERVAL_MS);
    }

    public PeerClient(
            Function<CrawlSettings, CrawlStrategy> strategyFactory,
            DomainRateLimiter rateLimiter,
            Path resultsRoot,
            CrawlEventListener eventListener,
            long emptyBatchRetryMs,
            long heartbeatIntervalMs) {
        this(
                "peer-" + UUID.randomUUID().toString().substring(0, 8),
                strategyFactory,
                rateLimiter,
                resultsRoot,
                eventListener,
                emptyBatchRetryMs,
                heartbeatIntervalMs
        );
    }

    public PeerClient(
            String peerId,
            Function<CrawlSettings, CrawlStrategy> strategyFactory,
            DomainRateLimiter rateLimiter,
            Path resultsRoot,
            CrawlEventListener eventListener,
            long emptyBatchRetryMs,
            long heartbeatIntervalMs) {
        if (peerId == null || peerId.isBlank()) {
            throw new IllegalArgumentException("peerId must not be blank");
        }
        this.peerId = peerId.trim();
        this.strategyFactory = Objects.requireNonNull(strategyFactory, "strategyFactory must not be null");
        this.rateLimiter = rateLimiter;
        this.resultsRoot = resultsRoot;
        this.eventListener = eventListener != null ? eventListener : CrawlEventListener.NO_OP;
        if (emptyBatchRetryMs <= 0L) {
            throw new IllegalArgumentException("emptyBatchRetryMs must be > 0");
        }
        if (heartbeatIntervalMs <= 0L) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be > 0");
        }
        this.emptyBatchRetryMs = emptyBatchRetryMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public void connect(String host, int port) throws IOException {
        if (host == null || host.isBlank()) {
            throw new IOException("Некоректна адреса ініціатора.");
        }
        if (port < MIN_PORT || port > MAX_PORT) {
            throw new IOException("Порт підключення має бути в діапазоні 1024..65535, отримано: " + port + ".");
        }

        synchronized (lifecycleLock) {
            if (connected) {
                throw new IllegalStateException("Клієнт уже підключений до ініціатора.");
            }
            this.initiatorHost = host.trim();
            this.initiatorPort = port;
            this.stopped = false;
            this.paused = false;
            this.pauseRequested = false;
            this.intentionalDisconnect = false;
            this.requestingBatch = false;
            this.currentBatch = null;
            this.activePauseRequestId = null;

            Socket tcpSocket = new Socket();
            try {
                tcpSocket.setTcpNoDelay(true);
                tcpSocket.connect(new InetSocketAddress(this.initiatorHost, port), (int) CONNECT_TIMEOUT_MS);
                this.socket = tcpSocket;
                this.reader = new BufferedReader(new InputStreamReader(tcpSocket.getInputStream(), StandardCharsets.UTF_8));
                this.writer = new BufferedWriter(new OutputStreamWriter(tcpSocket.getOutputStream(), StandardCharsets.UTF_8));
            } catch (IOException ex) {
                closeSocketQuietly(tcpSocket);
                throw new IOException("Не вдалося підключитися до ініціатора "
                        + this.initiatorHost + ":" + port + " — " + ex.getMessage(), ex);
            }

            this.socketReaderThread = new Thread(this::socketReadLoop, "p2p-client-reader-" + peerId);
            this.socketReaderThread.setDaemon(true);
            this.socketReaderThread.start();

            try {
                PeerMessage helloReply = sendAndAwaitResponse(
                        new PeerMessage(MessageType.HELLO, peerId, UUID.randomUUID().toString(), null, null),
                        RESPONSE_TIMEOUT_MS
                );
                if (!"OK".equals(helloReply.getStatus())) {
                    String reason = extractReason(helloReply.getPayload());
                    throw new IOException(reason != null ? reason
                            : "Ініціатор відхилив підключення: сесію вже завершено.");
                }

                PeerMessage joinReply = sendAndAwaitResponse(
                        new PeerMessage(MessageType.JOIN, peerId, UUID.randomUUID().toString(), null, null),
                        RESPONSE_TIMEOUT_MS
                );
                if (!"OK".equals(joinReply.getStatus())) {
                    String reason = extractReason(joinReply.getPayload());
                    throw new IOException(reason != null ? reason
                            : "Ініціатор відхилив приєднання до сесії: сесію вже завершено.");
                }

                JsonObject payload = joinReply.getPayload();
                CrawlSettings settings = codec.extractSettings(payload);
                if (settings == null) {
                    throw new IOException("Ініціатор не передав параметри сесії.");
                }
                if (payload != null && payload.has(PeerServer.KEY_WORKER_COUNT) && rateLimiter != null) {
                    rateLimiter.setActiveWorkersCount(payload.get(PeerServer.KEY_WORKER_COUNT).getAsInt());
                }
                if (payload != null && payload.has(PeerServer.KEY_SESSION_STATUS)) {
                    String sessionStatus = payload.get(PeerServer.KEY_SESSION_STATUS).getAsString();
                    if ("PAUSED".equals(sessionStatus)) {
                        this.paused = true;
                        this.pauseRequested = true;
                    }
                }

                this.sessionSettings = settings;
                this.strategy = Objects.requireNonNull(
                        strategyFactory.apply(settings),
                        "strategyFactory must not return null"
                );
                this.connected = true;

                this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "p2p-client-heartbeat-" + peerId);
                    thread.setDaemon(true);
                    return thread;
                });
                this.heartbeatScheduler.scheduleWithFixedDelay(
                        this::sendHeartbeat,
                        heartbeatIntervalMs,
                        heartbeatIntervalMs,
                        TimeUnit.MILLISECONDS
                );
            } catch (IOException ex) {
                cleanupConnectionState(true);
                throw ex;
            }
        }

        eventListener.onStatusMessage("Підключено до ініціатора " + this.initiatorHost + ":" + this.initiatorPort + ".");
    }

    public void startProcessingLoop() {
        synchronized (lifecycleLock) {
            if (!connected || stopped) {
                throw new IllegalStateException("PeerClient is not connected to an active session");
            }
            if (workerThread != null && workerThread.isAlive()) {
                return;
            }
            workerThread = new Thread(this::runProcessingLoop, "p2p-client-worker-" + peerId);
            workerThread.setDaemon(true);
            workerThread.start();
        }
    }

    public List<QueueEntry> requestBatch() throws IOException {
        if (!connected || stopped) {
            return List.of();
        }
        PeerMessage request = new PeerMessage(
                MessageType.TASK_REQUEST,
                peerId,
                UUID.randomUUID().toString(),
                null,
                null
        );
        PeerMessage response = sendAndAwaitResponse(request, RESPONSE_TIMEOUT_MS);
        if (response.getType() == MessageType.STOP) {
            handleRemoteStop(response, false);
            return List.of();
        }
        JsonObject payload = response.getPayload();
        if (payload != null && payload.has(PeerServer.KEY_WORKER_COUNT) && rateLimiter != null) {
            rateLimiter.setActiveWorkersCount(payload.get(PeerServer.KEY_WORKER_COUNT).getAsInt());
        }
        if ("EMPTY".equals(response.getStatus())) {
            return List.of();
        }
        return codec.extractQueueEntries(payload);
    }

    public void sendResult(List<Page> pages, List<QueueEntry> links) throws IOException {
        sendResult(pages, links, List.of());
    }

    public void sendHeartbeat() {
        if (!connected || stopped) {
            return;
        }
        try {
            sendEnvelope(new PeerMessage(MessageType.HEARTBEAT, peerId, null, null, null));
        } catch (IOException ex) {
            if (!intentionalDisconnect && !stopped) {
                LOGGER.log(Level.FINE, "Heartbeat write failed on peer " + peerId, ex);
                handleConnectionLost();
            }
        }
    }

    public void disconnectGracefully() {
        synchronized (lifecycleLock) {
            intentionalDisconnect = true;
            if (!connected && socket == null) {
                stopped = true;
                return;
            }
            if (connected && writer != null) {
                try {
                    sendEnvelope(new PeerMessage(MessageType.LEAVE, peerId, null, null, null));
                } catch (IOException ex) {
                    LOGGER.log(Level.FINE, "Failed sending LEAVE from peer " + peerId, ex);
                }
            }
            cleanupConnectionState(true);
        }
        eventListener.onStatusMessage("Вузол відключився від розподіленої сесії. Збережені файли залишилися на диску.");
    }

    public String getPeerId() {
        return peerId;
    }

    public CrawlSettings getSessionSettings() {
        return sessionSettings != null ? new CrawlSettings(sessionSettings) : null;
    }

    public boolean isConnected() {
        return connected;
    }

    public boolean isPaused() {
        return paused;
    }

    public boolean isStopped() {
        return stopped;
    }

    public String getInitiatorHost() {
        return initiatorHost;
    }

    public int getInitiatorPort() {
        return initiatorPort;
    }

    private void runProcessingLoop() {
        try {
            while (!stopped && connected) {
                synchronized (pauseMonitor) {
                    while (paused && !stopped && connected) {
                        pauseMonitor.wait();
                    }
                    if (stopped || !connected) {
                        break;
                    }
                    requestingBatch = true;
                }

                List<QueueEntry> batch;
                try {
                    batch = requestBatch();
                } finally {
                    synchronized (pauseMonitor) {
                        requestingBatch = false;
                    }
                }

                if (stopped || !connected) {
                    break;
                }

                if (batch.isEmpty()) {
                    String pendingPauseId = null;
                    synchronized (pauseMonitor) {
                        if (pauseRequested && activePauseRequestId != null) {
                            pendingPauseId = activePauseRequestId;
                            activePauseRequestId = null;
                        }
                    }
                    if (pendingPauseId != null) {
                        sendPauseAck(pendingPauseId, List.of());
                    }
                    synchronized (pauseMonitor) {
                        if (paused && !stopped && connected) {
                            while (paused && !stopped && connected) {
                                pauseMonitor.wait();
                            }
                        } else if (!stopped && connected) {
                            pauseMonitor.wait(emptyBatchRetryMs);
                        }
                    }
                    continue;
                }

                List<QueueEntry> batchToReturnImmediately = null;
                String immediatePauseId = null;
                synchronized (pauseMonitor) {
                    if (stopped || !connected) {
                        break;
                    }
                    if (pauseRequested || paused) {
                        batchToReturnImmediately = batch;
                        immediatePauseId = activePauseRequestId;
                        activePauseRequestId = null;
                    } else {
                        currentBatch = batch;
                    }
                }

                if (batchToReturnImmediately != null) {
                    if (immediatePauseId != null) {
                        sendPauseAck(immediatePauseId, batchToReturnImmediately);
                    }
                    synchronized (pauseMonitor) {
                        while (paused && !stopped && connected) {
                            pauseMonitor.wait();
                        }
                    }
                    continue;
                }

                CrawlStrategy.BatchResult result = null;
                try {
                    result = strategy.processBatch(
                            batch,
                            sessionSettings,
                            resultsRoot,
                            () -> pauseRequested || stopped
                    );
                    if (!stopped && connected) {
                        CrawlStrategy.BatchResult safeResult = result != null
                                ? result
                                : new CrawlStrategy.BatchResult(List.of(), List.of(), List.of());
                        List<String> handledUrls = computeHandledUrls(batch, safeResult.unprocessedEntries());
                        if (!safeResult.pages().isEmpty()
                                || !safeResult.foundLinks().isEmpty()
                                || !handledUrls.isEmpty()) {
                            sendResult(safeResult.pages(), safeResult.foundLinks(), handledUrls);
                        }
                    }
                } finally {
                    String pauseIdToAck = null;
                    synchronized (pauseMonitor) {
                        currentBatch = null;
                        if (activePauseRequestId != null) {
                            pauseIdToAck = activePauseRequestId;
                            activePauseRequestId = null;
                        }
                    }
                    if (pauseIdToAck != null && !stopped && connected) {
                        List<QueueEntry> unprocessed = result != null ? result.unprocessedEntries() : List.of();
                        sendPauseAck(pauseIdToAck, unprocessed);
                    }
                }

                synchronized (pauseMonitor) {
                    while (paused && !stopped && connected) {
                        pauseMonitor.wait();
                    }
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (IOException ex) {
            if (!intentionalDisconnect && !stopped) {
                LOGGER.log(Level.WARNING, "Peer worker loop terminated due to I/O error on " + peerId, ex);
                handleConnectionLost();
            }
        }
    }

    private void socketReadLoop() {
        try {
            String line;
            while (!stopped && (line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                PeerMessage msg;
                try {
                    msg = codec.decode(line);
                } catch (RuntimeException ex) {
                    LOGGER.log(Level.WARNING, "Ignoring malformed message from initiator", ex);
                    continue;
                }

                String requestId = msg.getRequestId();
                if (requestId != null) {
                    CompletableFuture<PeerMessage> pending = pendingResponses.remove(requestId);
                    if (pending != null) {
                        pending.complete(msg);
                        continue;
                    }
                }

                switch (msg.getType()) {
                    case PAUSE -> handleRemotePause(msg);
                    case RESUME -> handleRemoteResume(msg);
                    case STOP -> {
                        handleRemoteStop(msg, true);
                        return;
                    }
                    default -> LOGGER.log(Level.FINE, "Ignoring unsolicited message type {0}", msg.getType());
                }
            }
        } catch (IOException ex) {
            if (!intentionalDisconnect && !stopped) {
                LOGGER.log(Level.FINE, "Socket reader ended on peer " + peerId, ex);
            }
        }

        if (!intentionalDisconnect && !stopped) {
            handleConnectionLost();
        }
    }

    private void handleRemotePause(PeerMessage msg) {
        boolean sendImmediateAck = false;
        synchronized (pauseMonitor) {
            paused = true;
            pauseRequested = true;
            if (currentBatch == null && !requestingBatch) {
                sendImmediateAck = true;
            } else {
                activePauseRequestId = msg.getRequestId();
            }
            pauseMonitor.notifyAll();
        }
        if (sendImmediateAck) {
            try {
                sendPauseAck(msg.getRequestId(), List.of());
            } catch (IOException ex) {
                LOGGER.log(Level.FINE, "Failed sending immediate PAUSE ACK on peer " + peerId, ex);
            }
        }
        eventListener.onStatusMessage("Сесію призупинено ініціатором.");
    }

    private void handleRemoteResume(PeerMessage msg) {
        synchronized (pauseMonitor) {
            paused = false;
            pauseRequested = false;
            activePauseRequestId = null;
            pauseMonitor.notifyAll();
        }
        try {
            sendCommandAck(MessageType.RESUME, msg.getRequestId());
        } catch (IOException ex) {
            LOGGER.log(Level.FINE, "Failed sending RESUME ACK on peer " + peerId, ex);
        }
        eventListener.onStatusMessage("Сесію відновлено ініціатором.");
    }

    private void handleRemoteStop(PeerMessage msg, boolean sendAck) {
        synchronized (pauseMonitor) {
            stopped = true;
            paused = false;
            pauseRequested = false;
            activePauseRequestId = null;
            pauseMonitor.notifyAll();
        }
        if (strategy != null) {
            strategy.requestStop();
        }
        if (sendAck) {
            try {
                sendCommandAck(MessageType.STOP, msg.getRequestId());
            } catch (IOException ex) {
                LOGGER.log(Level.FINE, "Failed sending STOP ACK on peer " + peerId, ex);
            }
        }
        String reason = extractReason(msg.getPayload());
        String statusMsg = reason != null
                ? "Сесію завершено ініціатором (" + reason + "). Збережені файли залишилися на диску."
                : "Сесію завершено ініціатором. Збережені файли залишилися на диску.";
        cleanupConnectionState(false);
        eventListener.onStatusMessage(statusMsg);
    }

    private void handleConnectionLost() {
        boolean notify = false;
        synchronized (lifecycleLock) {
            if (!stopped && !intentionalDisconnect) {
                notify = true;
            }
            cleanupConnectionState(false);
        }
        if (notify) {
            eventListener.onStatusMessage("Втрачено зв'язок з ініціатором. Обхід припинено, збережені файли залишилися на диску.");
            eventListener.onError(initiatorHost + ":" + initiatorPort, "Втрачено зв'язок з ініціатором.");
        }
    }

    private PeerMessage sendAndAwaitResponse(PeerMessage request, long timeoutMs) throws IOException {
        CompletableFuture<PeerMessage> future = new CompletableFuture<>();
        pendingResponses.put(request.getRequestId(), future);
        try {
            sendEnvelope(request);
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Операцію обміну даними з ініціатором перервано.", ex);
        } catch (TimeoutException ex) {
            throw new IOException("Час очікування відповіді від ініціатора вичерпано (" + request.getType() + ").", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof IOException ioEx) {
                throw ioEx;
            }
            throw new IOException("Помилка взаємодії з ініціатором: "
                    + (cause != null ? cause.getMessage() : ex.getMessage()), cause);
        } finally {
            pendingResponses.remove(request.getRequestId(), future);
        }
    }

    private void sendResult(List<Page> pages, List<QueueEntry> links, List<String> handledUrls) throws IOException {
        JsonObject payload = codec.createPayload();
        codec.putPages(payload, pages);
        codec.putQueueEntries(payload, links);
        if (handledUrls != null && !handledUrls.isEmpty()) {
            codec.putValue(payload, PeerServer.KEY_HANDLED_URLS, handledUrls);
        }
        sendEnvelope(new PeerMessage(
                MessageType.RESULT,
                peerId,
                UUID.randomUUID().toString(),
                "OK",
                payload
        ));
    }

    private void sendPauseAck(String requestId, List<QueueEntry> unprocessedEntries) throws IOException {
        JsonObject payload = codec.createPayload();
        codec.putQueueEntries(payload, unprocessedEntries == null ? List.of() : unprocessedEntries);
        sendEnvelope(new PeerMessage(
                MessageType.PAUSE,
                peerId,
                requestId,
                "OK",
                payload
        ));
    }

    private void sendCommandAck(MessageType type, String requestId) throws IOException {
        sendEnvelope(new PeerMessage(type, peerId, requestId, "OK", null));
    }

    private void sendEnvelope(PeerMessage message) throws IOException {
        String encoded = codec.encode(message);
        synchronized (writeLock) {
            if (writer == null || socket == null || socket.isClosed()) {
                throw new IOException("Сокет з'єднання з ініціатором закрито.");
            }
            writer.write(encoded);
            writer.write('\n');
            writer.flush();
        }
    }

    private void cleanupConnectionState(boolean interruptWorker) {
        connected = false;
        synchronized (pauseMonitor) {
            stopped = true;
            paused = false;
            pauseRequested = false;
            activePauseRequestId = null;
            pauseMonitor.notifyAll();
        }
        if (strategy != null) {
            strategy.requestStop();
        }
        if (heartbeatScheduler != null) {
            heartbeatScheduler.shutdownNow();
            heartbeatScheduler = null;
        }
        IOException disconnectEx = new IOException("З'єднання з ініціатором закрито.");
        for (String reqId : List.copyOf(pendingResponses.keySet())) {
            CompletableFuture<PeerMessage> pending = pendingResponses.remove(reqId);
            if (pending != null) {
                pending.completeExceptionally(disconnectEx);
            }
        }
        synchronized (writeLock) {
            shutdownOutputQuietly(socket);
            closeSocketQuietly(socket);
            socket = null;
            writer = null;
            reader = null;
        }
        if (interruptWorker && workerThread != null && workerThread != Thread.currentThread()) {
            workerThread.interrupt();
        }
    }

    private static Function<CrawlSettings, CrawlStrategy> createDefaultStrategyFactory(
            PageLoader pageLoader,
            LinkExtractor linkExtractor,
            TermSearcher termSearcher,
            RobotsTxtChecker robotsTxtChecker,
            DomainRateLimiter rateLimiter,
            Path resultsRoot,
            CrawlEventListener eventListener) {
        return settings -> settings != null && settings.getStrategy() == StrategyType.DFS
                ? new DepthFirstCrawl(
                        pageLoader, linkExtractor, termSearcher, robotsTxtChecker,
                        rateLimiter, null, resultsRoot, eventListener)
                : new BreadthFirstCrawl(
                        pageLoader, linkExtractor, termSearcher, robotsTxtChecker,
                        rateLimiter, null, resultsRoot, eventListener);
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

    private static String extractReason(JsonObject payload) {
        if (payload == null || !payload.has(PeerServer.KEY_REASON) || payload.get(PeerServer.KEY_REASON).isJsonNull()) {
            return null;
        }
        String reason = payload.get(PeerServer.KEY_REASON).getAsString();
        return reason.isBlank() ? null : reason;
    }

    private static void shutdownOutputQuietly(Socket socket) {
        if (socket != null && !socket.isClosed() && !socket.isOutputShutdown()) {
            try {
                socket.shutdownOutput();
            } catch (IOException ignored) {
            }
        }
    }

    private static void closeSocketQuietly(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }
}
