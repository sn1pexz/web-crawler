package com.webcrawler.p2p;

import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.webcrawler.model.Page;
import com.webcrawler.model.QueueEntry;
import com.webcrawler.p2p.protocol.MessageType;
import com.webcrawler.p2p.protocol.PeerMessage;
import com.webcrawler.p2p.protocol.PeerMessageCodec;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PeerServer implements PeerCommandSender {

    public static final int MIN_PORT = 1024;
    public static final int MAX_PORT = 65535;
    public static final long COMMAND_ACK_TIMEOUT_MS = 5000L;
    public static final long PAUSE_ACK_TIMEOUT_MS = 20000L;
    public static final long HEARTBEAT_CHECK_INTERVAL_MS = 2000L;

    static final String KEY_HANDLED_URLS = "handledUrls";
    static final String KEY_WORKER_COUNT = "workerCount";
    static final String KEY_SESSION_STATUS = "sessionStatus";
    static final String KEY_ASSIGNED_PEER_ID = "assignedPeerId";
    static final String KEY_REASON = "reason";

    private static final Logger LOGGER = Logger.getLogger(PeerServer.class.getName());
    private static final String INITIATOR_PEER_ID = "initiator";
    private static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() { }.getType();

    private final SessionCoordinator coordinator;
    private final PeerMessageCodec codec = new PeerMessageCodec();
    private final long heartbeatCheckIntervalMs;
    private final long commandAckTimeoutMs;
    private final long pauseAckTimeoutMs;
    private final Supplier<LocalDateTime> clock;

    private final ConcurrentMap<String, PeerConnection> connections = new ConcurrentHashMap<>();
    private final Set<PeerConnection> unregisteredConnections = ConcurrentHashMap.newKeySet();
    private final Set<String> stopDeliveredPeers = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, PendingAck> pendingAcksByRequestId = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CopyOnWriteArrayList<PendingAck>> pendingAcksByPeer = new ConcurrentHashMap<>();

    private volatile int port;
    private volatile ServerSocket serverSocket;
    private volatile boolean running;
    private Thread acceptThread;
    private ScheduledExecutorService heartbeatMonitor;

    public PeerServer(SessionCoordinator coordinator) {
        this(coordinator, HEARTBEAT_CHECK_INTERVAL_MS, COMMAND_ACK_TIMEOUT_MS, PAUSE_ACK_TIMEOUT_MS, LocalDateTime::now);
    }

    public PeerServer(
            SessionCoordinator coordinator,
            long heartbeatCheckIntervalMs,
            long commandAckTimeoutMs,
            long pauseAckTimeoutMs) {
        this(coordinator, heartbeatCheckIntervalMs, commandAckTimeoutMs, pauseAckTimeoutMs, LocalDateTime::now);
    }

    public PeerServer(
            SessionCoordinator coordinator,
            long heartbeatCheckIntervalMs,
            long commandAckTimeoutMs,
            long pauseAckTimeoutMs,
            Supplier<LocalDateTime> clock) {
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator must not be null");
        if (heartbeatCheckIntervalMs <= 0L) {
            throw new IllegalArgumentException("heartbeatCheckIntervalMs must be > 0");
        }
        if (commandAckTimeoutMs <= 0L) {
            throw new IllegalArgumentException("commandAckTimeoutMs must be > 0");
        }
        if (pauseAckTimeoutMs <= 0L) {
            throw new IllegalArgumentException("pauseAckTimeoutMs must be > 0");
        }
        this.heartbeatCheckIntervalMs = heartbeatCheckIntervalMs;
        this.commandAckTimeoutMs = commandAckTimeoutMs;
        this.pauseAckTimeoutMs = pauseAckTimeoutMs;
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.coordinator.setCommandSenderIfAbsent(this);
    }

    public synchronized void openPort(int port) throws IOException {
        if (port < MIN_PORT || port > MAX_PORT) {
            throw new IllegalArgumentException("Порт має бути в діапазоні 1024..65535, отримано: " + port);
        }
        if (running) {
            throw new IllegalStateException("P2P-сервер уже запущено на порту " + this.port);
        }

        ServerSocket socket;
        try {
            socket = new ServerSocket(port);
        } catch (IOException ex) {
            throw new IOException("Не вдалося відкрити порт " + port + ": порт зайнятий або недоступний.", ex);
        }

        this.serverSocket = socket;
        this.port = socket.getLocalPort();
        this.running = true;

        this.acceptThread = new Thread(() -> acceptLoop(socket), "p2p-server-accept-" + this.port);
        this.acceptThread.setDaemon(true);
        this.acceptThread.start();

        this.heartbeatMonitor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "p2p-heartbeat-monitor-" + this.port);
            thread.setDaemon(true);
            return thread;
        });
        this.heartbeatMonitor.scheduleWithFixedDelay(
                this::runLostPeerCheck,
                heartbeatCheckIntervalMs,
                heartbeatCheckIntervalMs,
                TimeUnit.MILLISECONDS
        );

        LOGGER.log(Level.INFO, "P2P server listening on port {0}", this.port);
    }

    public void handleMessage(PeerNode node, String message) {
        if (message == null || message.isBlank()) {
            throw new IllegalArgumentException("message must not be blank");
        }
        PeerMessage decoded = codec.decode(message);
        PeerMessage reply = processDecodedMessage(node, decoded, null);
        if (reply != null && node != null) {
            sendToPeer(node.getId(), reply);
        }
    }

    public synchronized void stop() {
        if (!running && serverSocket == null) {
            return;
        }
        running = false;

        if (heartbeatMonitor != null) {
            heartbeatMonitor.shutdownNow();
            heartbeatMonitor = null;
        }

        ServerSocket socketToClose = this.serverSocket;
        this.serverSocket = null;
        if (socketToClose != null) {
            try {
                socketToClose.close();
            } catch (IOException ex) {
                LOGGER.log(Level.FINE, "Error closing ServerSocket", ex);
            }
        }

        for (String peerId : List.copyOf(connections.keySet())) {
            disconnectPeer(peerId);
        }
        for (PeerConnection unregistered : List.copyOf(unregisteredConnections)) {
            unregisteredConnections.remove(unregistered);
            unregistered.close();
        }
        stopDeliveredPeers.clear();
        failAllPendingAcks();

        Thread threadToJoin = this.acceptThread;
        this.acceptThread = null;
        if (threadToJoin != null && threadToJoin != Thread.currentThread()) {
            threadToJoin.interrupt();
            try {
                threadToJoin.join(1000L);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        LOGGER.log(Level.INFO, "P2P server stopped (port {0})", this.port);
    }

    @Override
    public void sendToPeer(String peerId, PeerMessage message) {
        if (peerId == null || peerId.isBlank() || message == null) {
            return;
        }
        PeerConnection connection = connections.get(peerId);
        if (connection == null || connection.isClosed()) {
            return;
        }
        try {
            connection.send(codec.encode(message));
        } catch (IOException ex) {
            LOGGER.log(Level.WARNING, "Failed sending message " + message.getType() + " to peer " + peerId, ex);
            disconnectPeer(peerId);
        }
    }

    @Override
    public void broadcastCommand(PeerMessage command) {
        Objects.requireNonNull(command, "command must not be null");
        MessageType type = Objects.requireNonNull(command.getType(), "command.type must not be null");
        List<PeerNode> targets = coordinator.getActivePeers();
        if (targets.isEmpty()) {
            return;
        }

        boolean requiresAck = type == MessageType.PAUSE || type == MessageType.RESUME || type == MessageType.STOP;
        if (!requiresAck) {
            for (PeerNode peer : targets) {
                sendToPeer(peer.getId(), command);
            }
            return;
        }

        long timeoutMs = type == MessageType.PAUSE ? pauseAckTimeoutMs : commandAckTimeoutMs;
        Map<PeerNode, PendingAck> sent = new LinkedHashMap<>();

        for (PeerNode peer : targets) {
            if (type == MessageType.STOP && stopDeliveredPeers.remove(peer.getId())) {
                disconnectPeer(peer.getId());
                continue;
            }
            String requestId = UUID.randomUUID().toString();
            CompletableFuture<PeerMessage> future = new CompletableFuture<>();
            PendingAck ack = new PendingAck(peer.getId(), type, requestId, future);
            pendingAcksByRequestId.put(requestId, ack);
            pendingAcksByPeer.computeIfAbsent(peer.getId(), ignored -> new CopyOnWriteArrayList<>()).add(ack);

            PeerMessage outbound = new PeerMessage(
                    type,
                    INITIATOR_PEER_ID,
                    requestId,
                    command.getStatus(),
                    command.getPayload()
            );

            PeerConnection connection = connections.get(peer.getId());
            if (connection == null || connection.isClosed()) {
                future.completeExceptionally(new IOException("Peer is disconnected: " + peer.getId()));
            } else {
                try {
                    connection.send(codec.encode(outbound));
                } catch (IOException ex) {
                    future.completeExceptionally(ex);
                }
            }
            sent.put(peer, ack);
        }

        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        for (Map.Entry<PeerNode, PendingAck> entry : sent.entrySet()) {
            PeerNode peer = entry.getKey();
            PendingAck ack = entry.getValue();
            boolean acknowledged = awaitPendingAck(ack, deadlineNanos);
            cleanupPendingAck(ack);
            if (!acknowledged) {
                if (type == MessageType.STOP
                        && ("COMPLETED".equals(command.getStatus()) || stopDeliveredPeers.remove(peer.getId()))) {
                    disconnectPeer(peer.getId());
                    continue;
                }
                LOGGER.log(Level.WARNING, "Peer {0} did not acknowledge {1} within {2} ms; treating as lost",
                        new Object[]{peer.getId(), type, timeoutMs});
                coordinator.markPeerLost(peer);
                disconnectPeer(peer.getId());
            }
        }
    }

    @Override
    public void disconnectPeer(String peerId) {
        if (peerId == null || peerId.isBlank()) {
            return;
        }
        PeerConnection connection = connections.remove(peerId);
        if (connection != null) {
            unregisteredConnections.remove(connection);
            connection.close();
        }
        failPendingAcksForPeer(peerId);
    }

    public int getPort() {
        return port;
    }

    public boolean isRunning() {
        return running;
    }

    private void acceptLoop(ServerSocket socket) {
        while (running && !socket.isClosed()) {
            try {
                Socket clientSocket = socket.accept();
                clientSocket.setTcpNoDelay(true);
                PeerConnection connection = new PeerConnection(clientSocket);
                unregisteredConnections.add(connection);
                Thread readerThread = new Thread(
                        () -> readConnectionLoop(connection),
                        "p2p-server-peer-reader-" + clientSocket.getPort()
                );
                readerThread.setDaemon(true);
                readerThread.start();
            } catch (IOException ex) {
                if (running && !socket.isClosed()) {
                    LOGGER.log(Level.WARNING, "Accept loop I/O error on port " + port, ex);
                }
            }
        }
    }

    private void readConnectionLoop(PeerConnection connection) {
        try {
            String line;
            while (running && !connection.isClosed() && (line = connection.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                PeerMessage decoded;
                try {
                    decoded = codec.decode(line);
                } catch (RuntimeException ex) {
                    LOGGER.log(Level.WARNING, "Ignoring malformed P2P message from "
                            + connection.getRemoteHost() + ":" + connection.getRemotePort(), ex);
                    continue;
                }

                PeerNode currentNode = connection.getNode();
                if (decoded.getType() == MessageType.HEARTBEAT && currentNode != null) {
                    coordinator.onHeartbeat(currentNode.getId(), clock.get());
                    continue;
                }
                if ((decoded.getType() == MessageType.STOP || decoded.getType() == MessageType.RESUME)
                        && completePendingAck(currentNode, decoded)) {
                    continue;
                }

                connection.submitTask(() -> handleConnectionMessage(connection, decoded));
            }
        } catch (IOException ex) {
            if (running && !connection.isClosed()) {
                LOGGER.log(Level.FINE, "Peer socket read ended for "
                        + connection.getRemoteHost() + ":" + connection.getRemotePort(), ex);
            }
        } finally {
            unregisteredConnections.remove(connection);
            PeerNode node = connection.getNode();
            if (node != null) {
                connections.remove(node.getId(), connection);
                failPendingAcksForPeer(node.getId());
            }
            connection.close();
        }
    }

    private void handleConnectionMessage(PeerConnection connection, PeerMessage decoded) {
        PeerNode node = resolveOrBindNode(connection, decoded);
        PeerMessage reply = processDecodedMessage(node, decoded, connection);
        if (reply != null) {
            try {
                connection.send(codec.encode(reply));
            } catch (IOException ex) {
                LOGGER.log(Level.FINE, "Failed sending reply to peer " + node.getId(), ex);
                disconnectPeer(node.getId());
                return;
            }
        }

        if (decoded.getType() == MessageType.LEAVE
                || (decoded.getType() == MessageType.JOIN && "REJECTED".equals(reply != null ? reply.getStatus() : null))) {
            connections.remove(node.getId(), connection);
            unregisteredConnections.remove(connection);
            connection.close();
        }
    }

    private PeerNode resolveOrBindNode(PeerConnection connection, PeerMessage decoded) {
        PeerNode existing = connection.getNode();
        if (existing != null) {
            return existing;
        }
        String peerId = decoded.getPeerId();
        if (peerId == null || peerId.isBlank()) {
            peerId = "peer-" + UUID.randomUUID().toString().substring(0, 8);
        }
        PeerNode node = new PeerNode(peerId, connection.getRemoteHost(), connection.getRemotePort());
        if (decoded.getType() == MessageType.JOIN) {
            connection.setNode(node);
            unregisteredConnections.remove(connection);
            PeerConnection previous = connections.put(peerId, connection);
            if (previous != null && previous != connection) {
                previous.close();
            }
        }
        return node;
    }

    private PeerMessage processDecodedMessage(PeerNode node, PeerMessage decoded, PeerConnection connection) {
        MessageType type = decoded.getType();
        return switch (type) {
            case HELLO -> handleHello(decoded);
            case JOIN -> handleJoin(node, decoded);
            case TASK_REQUEST -> handleTaskRequest(node, decoded);
            case RESULT -> {
                handleResult(node, decoded);
                yield null;
            }
            case HEARTBEAT -> {
                if (node != null) {
                    coordinator.onHeartbeat(node.getId(), clock.get());
                }
                yield null;
            }
            case LEAVE -> {
                if (node != null) {
                    coordinator.unregisterPeer(node);
                }
                yield null;
            }
            case PAUSE -> {
                handlePauseAck(node, decoded);
                yield null;
            }
            case RESUME, STOP -> {
                completePendingAck(node, decoded);
                yield null;
            }
            case TASK_BATCH -> {
                LOGGER.log(Level.WARNING, "Unexpected TASK_BATCH received by PeerServer from {0}",
                        node != null ? node.getId() : "unknown");
                yield null;
            }
        };
    }

    private PeerMessage handleHello(PeerMessage request) {
        if (coordinator.isAcceptingPeers()) {
            return new PeerMessage(MessageType.HELLO, INITIATOR_PEER_ID, request.getRequestId(), "OK", null);
        }
        JsonObject payload = codec.createPayload();
        payload.addProperty(KEY_REASON, "Сесія вже завершена і не приймає підключень.");
        return new PeerMessage(MessageType.HELLO, INITIATOR_PEER_ID, request.getRequestId(), "REJECTED", payload);
    }

    private PeerMessage handleJoin(PeerNode node, PeerMessage request) {
        if (node == null || !coordinator.registerPeer(node, clock.get())) {
            JsonObject payload = codec.createPayload();
            payload.addProperty(KEY_REASON, "Сесія вже завершена і не приймає підключень.");
            return new PeerMessage(MessageType.JOIN, INITIATOR_PEER_ID, request.getRequestId(), "REJECTED", payload);
        }
        stopDeliveredPeers.remove(node.getId());
        JsonObject payload = codec.createPayload();
        codec.putSettings(payload, coordinator.getSessionSettings());
        payload.addProperty(KEY_WORKER_COUNT, coordinator.getActiveWorkersCount());
        payload.addProperty(KEY_SESSION_STATUS, coordinator.getSessionStatus().name());
        payload.addProperty(KEY_ASSIGNED_PEER_ID, node.getId());
        return new PeerMessage(MessageType.JOIN, INITIATOR_PEER_ID, request.getRequestId(), "OK", payload);
    }

    private PeerMessage handleTaskRequest(PeerNode node, PeerMessage request) {
        if (!coordinator.isAcceptingPeers()) {
            if (node != null) {
                stopDeliveredPeers.add(node.getId());
            }
            return createStopReply(request.getRequestId(), coordinator.getCompletionReason());
        }
        if (node == null || !coordinator.isPeerActive(node.getId())) {
            return createStopReply(request.getRequestId(), "Вузол більше не активний у сесії.");
        }

        List<QueueEntry> batch = coordinator.assignBatch(node);
        if (!coordinator.isAcceptingPeers()) {
            stopDeliveredPeers.add(node.getId());
            return createStopReply(request.getRequestId(), coordinator.getCompletionReason());
        }

        JsonObject payload = codec.createPayload();
        payload.addProperty(KEY_WORKER_COUNT, coordinator.getActiveWorkersCount());
        codec.putQueueEntries(payload, batch);
        String status = batch.isEmpty() ? "EMPTY" : "OK";
        return new PeerMessage(MessageType.TASK_BATCH, INITIATOR_PEER_ID, request.getRequestId(), status, payload);
    }

    private void handleResult(PeerNode node, PeerMessage message) {
        if (node == null) {
            return;
        }
        JsonObject payload = message.getPayload();
        List<Page> pages = codec.extractPages(payload);
        List<QueueEntry> foundLinks = codec.extractQueueEntries(payload);
        List<String> handledUrls = extractHandledUrls(payload);
        coordinator.acceptResult(node, pages, foundLinks, handledUrls);
    }

    private void handlePauseAck(PeerNode node, PeerMessage message) {
        if (node != null && message.getPayload() != null) {
            List<QueueEntry> unprocessed = codec.extractQueueEntries(message.getPayload());
            if (!unprocessed.isEmpty()) {
                coordinator.returnUnprocessedEntries(node, unprocessed);
            }
        }
        completePendingAck(node, message);
    }

    private List<String> extractHandledUrls(JsonObject payload) {
        if (payload == null) {
            return List.of();
        }
        List<String> list = codec.extractValue(payload, KEY_HANDLED_URLS, STRING_LIST_TYPE);
        return list == null ? List.of() : list;
    }

    private PeerMessage createStopReply(String requestId, String reason) {
        JsonObject payload = codec.createPayload();
        if (reason != null && !reason.isBlank()) {
            payload.addProperty(KEY_REASON, reason);
        }
        return new PeerMessage(MessageType.STOP, INITIATOR_PEER_ID, requestId, "OK", payload);
    }

    private void runLostPeerCheck() {
        if (!running) {
            return;
        }
        try {
            List<PeerNode> lost = coordinator.checkLostPeers(clock.get());
            for (PeerNode peer : lost) {
                disconnectPeer(peer.getId());
            }
        } catch (RuntimeException ex) {
            LOGGER.log(Level.WARNING, "Error during lost-peer heartbeat check", ex);
        }
    }

    private boolean completePendingAck(PeerNode node, PeerMessage message) {
        if (message == null) {
            return false;
        }
        String requestId = message.getRequestId();
        if (requestId != null && !requestId.isBlank()) {
            PendingAck ack = pendingAcksByRequestId.remove(requestId);
            if (ack != null) {
                removeFromPeerPendingList(ack);
                ack.future.complete(message);
                return true;
            }
        }

        String peerId = node != null ? node.getId() : message.getPeerId();
        if (peerId == null || peerId.isBlank()) {
            return false;
        }
        CopyOnWriteArrayList<PendingAck> peerAcks = pendingAcksByPeer.get(peerId);
        if (peerAcks == null) {
            return false;
        }
        for (PendingAck ack : peerAcks) {
            if (ack.type == message.getType() && !ack.future.isDone()) {
                pendingAcksByRequestId.remove(ack.requestId);
                peerAcks.remove(ack);
                ack.future.complete(message);
                return true;
            }
        }
        return false;
    }

    private boolean awaitPendingAck(PendingAck ack, long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0L) {
            return ack.future.isDone() && !ack.future.isCompletedExceptionally();
        }
        try {
            ack.future.get(remainingNanos, TimeUnit.NANOSECONDS);
            return true;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        } catch (TimeoutException | ExecutionException ex) {
            return false;
        }
    }

    private void cleanupPendingAck(PendingAck ack) {
        pendingAcksByRequestId.remove(ack.requestId, ack);
        removeFromPeerPendingList(ack);
    }

    private void removeFromPeerPendingList(PendingAck ack) {
        CopyOnWriteArrayList<PendingAck> list = pendingAcksByPeer.get(ack.peerId);
        if (list != null) {
            list.remove(ack);
        }
    }

    private void failPendingAcksForPeer(String peerId) {
        CopyOnWriteArrayList<PendingAck> acks = pendingAcksByPeer.remove(peerId);
        if (acks == null) {
            return;
        }
        IOException disconnected = new IOException("Peer disconnected: " + peerId);
        for (PendingAck ack : acks) {
            pendingAcksByRequestId.remove(ack.requestId, ack);
            ack.future.completeExceptionally(disconnected);
        }
    }

    private void failAllPendingAcks() {
        for (String peerId : List.copyOf(pendingAcksByPeer.keySet())) {
            failPendingAcksForPeer(peerId);
        }
        IOException stopped = new IOException("PeerServer stopped");
        for (PendingAck ack : List.copyOf(pendingAcksByRequestId.values())) {
            pendingAcksByRequestId.remove(ack.requestId, ack);
            ack.future.completeExceptionally(stopped);
        }
    }

    private record PendingAck(
            String peerId,
            MessageType type,
            String requestId,
            CompletableFuture<PeerMessage> future) {
    }

    private static final class PeerConnection {
        private final Socket socket;
        private final BufferedReader reader;
        private final BufferedWriter writer;
        private final Object writeLock = new Object();
        private final ExecutorService sequentialExecutor;
        private final String remoteHost;
        private final int remotePort;
        private volatile PeerNode node;
        private volatile boolean closed;

        PeerConnection(Socket socket) throws IOException {
            this.socket = socket;
            this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            this.remoteHost = socket.getInetAddress() != null ? socket.getInetAddress().getHostAddress() : "127.0.0.1";
            this.remotePort = socket.getPort();
            this.sequentialExecutor = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "p2p-server-peer-worker-" + remotePort);
                thread.setDaemon(true);
                return thread;
            });
        }

        String readLine() throws IOException {
            return reader.readLine();
        }

        void send(String jsonLine) throws IOException {
            synchronized (writeLock) {
                if (closed) {
                    throw new IOException("Connection closed");
                }
                writer.write(jsonLine);
                writer.write('\n');
                writer.flush();
            }
        }

        void submitTask(Runnable task) {
            if (!closed && !sequentialExecutor.isShutdown()) {
                sequentialExecutor.execute(task);
            }
        }

        PeerNode getNode() {
            return node;
        }

        void setNode(PeerNode node) {
            this.node = node;
        }

        String getRemoteHost() {
            return remoteHost;
        }

        int getRemotePort() {
            return remotePort;
        }

        boolean isClosed() {
            return closed || socket.isClosed();
        }

        void close() {
            if (closed) {
                return;
            }
            closed = true;
            sequentialExecutor.shutdownNow();
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }
}
