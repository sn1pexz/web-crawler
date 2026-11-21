package com.webcrawler.p2p;

import com.google.gson.JsonObject;
import com.webcrawler.core.CrawlEventListener;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.Page;
import com.webcrawler.model.QueueEntry;
import com.webcrawler.model.SessionStatus;
import com.webcrawler.model.StrategyType;
import com.webcrawler.p2p.protocol.MessageType;
import com.webcrawler.p2p.protocol.PeerMessage;
import com.webcrawler.persistence.PageRepository;
import com.webcrawler.persistence.SessionRepository;
import com.webcrawler.util.UrlNormalizer;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

//P2P initiator coordinator managing shared queue and peers
public class SessionCoordinator {

    public static final int DEFAULT_BATCH_SIZE = 10;

    public static final String REASON_PAGE_LIMIT_REACHED = "Досягнуто ліміту сторінок";
    public static final String REASON_QUEUE_EXHAUSTED = "Чергу адрес вичерпано";

    private static final Logger LOGGER = Logger.getLogger(SessionCoordinator.class.getName());
    private static final String INITIATOR_PEER_ID = "initiator";

    private final CrawlSession session;
    private final PageRepository pageRepository;
    private final SessionRepository sessionRepository;
    private volatile PeerCommandSender commandSender;
    private final CrawlEventListener eventListener;
    private final int batchSize;
    private final String startHost;

    private final Set<String> visitedUrls = new LinkedHashSet<>();
    private final Set<String> recordedPageUrls = new LinkedHashSet<>();
    private final LinkedList<QueueEntry> queue = new LinkedList<>();
    private final Map<PeerNode, List<QueueEntry>> assignedBatches = new LinkedHashMap<>();
    private final Set<PeerNode> activePeers = new LinkedHashSet<>();
    private final Map<String, PeerNode> activePeersById = new LinkedHashMap<>();

    private volatile boolean pauseInProgress;

    public SessionCoordinator(
            CrawlSession session,
            PageRepository pageRepository,
            SessionRepository sessionRepository,
            PeerCommandSender commandSender,
            CrawlEventListener eventListener) {
        this(session, pageRepository, sessionRepository, commandSender, eventListener, DEFAULT_BATCH_SIZE);
    }

    public SessionCoordinator(
            CrawlSession session,
            PageRepository pageRepository,
            SessionRepository sessionRepository,
            PeerCommandSender commandSender,
            CrawlEventListener eventListener,
            int batchSize) {
        this.session = Objects.requireNonNull(session, "session must not be null");
        CrawlSettings settings = Objects.requireNonNull(session.getSettings(), "session.settings must not be null");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0, got " + batchSize);
        }
        this.pageRepository = pageRepository;
        this.sessionRepository = sessionRepository;
        this.commandSender = commandSender;
        this.eventListener = eventListener != null ? eventListener : CrawlEventListener.NO_OP;
        this.batchSize = batchSize;
        this.startHost = extractHostOrNull(settings.getStartUrl());

        String normalizedStart = normalizeOrNull(settings.getStartUrl());
        if (normalizedStart != null && session.getVisitedCount() == 0) {
            this.visitedUrls.add(normalizedStart);
            this.queue.add(new QueueEntry(normalizedStart, 0));
        }
    }

    public synchronized void setCommandSenderIfAbsent(PeerCommandSender commandSender) {
        if (this.commandSender == null) {
            this.commandSender = commandSender;
        }
    }

    public synchronized void setCommandSender(PeerCommandSender commandSender) {
        this.commandSender = commandSender;
    }

    public synchronized List<QueueEntry> assignBatch(PeerNode node) {
        Objects.requireNonNull(node, "node must not be null");
        SessionStatus status = session.getStatus();
        if (pauseInProgress
                || status == SessionStatus.PAUSED
                || status == SessionStatus.FINISHED
                || status == SessionStatus.FAILED) {
            return List.of();
        }

        int pageLimit = session.getSettings().getPageLimit();
        int inFlightReserve = countAssignedEntriesLocked();
        int remainingBudget = pageLimit - (session.getVisitedCount() + inFlightReserve);
        if (remainingBudget <= 0 || queue.isEmpty()) {
            return List.of();
        }

        int toAssign = Math.min(batchSize, Math.min(remainingBudget, queue.size()));
        List<QueueEntry> batch = new ArrayList<>(toAssign);
        for (int i = 0; i < toAssign; i++) {
            batch.add(queue.removeFirst());
        }

        assignedBatches.computeIfAbsent(node, ignored -> new ArrayList<>()).addAll(batch);
        return List.copyOf(batch);
    }

    public void acceptResult(PeerNode node, List<Page> pages, List<QueueEntry> links) {
        acceptResult(node, pages, links, List.of());
    }

    public void acceptResult(
            PeerNode node,
            List<Page> pages,
            List<QueueEntry> links,
            Collection<String> handledBatchUrls) {
        Objects.requireNonNull(node, "node must not be null");

        List<Page> safePages = pages == null ? List.of() : pages;
        List<QueueEntry> safeLinks = links == null ? List.of() : links;
        Collection<String> safeHandledUrls = handledBatchUrls == null ? List.of() : handledBatchUrls;

        List<Page> acceptedPages = new ArrayList<>();
        boolean completedNow = false;
        CrawlSession completedSessionSnapshot = null;

        synchronized (this) {
            Set<String> processedKeys = new HashSet<>();
            for (Page page : safePages) {
                if (page != null && page.getUrl() != null && !page.getUrl().isBlank()) {
                    processedKeys.add(canonicalKey(page.getUrl()));
                }
            }
            for (String handledUrl : safeHandledUrls) {
                if (handledUrl != null && !handledUrl.isBlank()) {
                    processedKeys.add(canonicalKey(handledUrl));
                }
            }

            removeAssignedUrlsLocked(node, processedKeys);

            CrawlSettings settings = session.getSettings();
            int pageLimit = settings.getPageLimit();

            for (Page page : safePages) {
                if (page == null || page.getUrl() == null || page.getUrl().isBlank()) {
                    continue;
                }
                SessionStatus status = session.getStatus();
                if (status == SessionStatus.FINISHED || status == SessionStatus.FAILED) {
                    break;
                }
                if (session.getVisitedCount() >= pageLimit) {
                    break;
                }
                String canonicalUrl = canonicalKey(page.getUrl());
                if (!recordedPageUrls.add(canonicalUrl)) {
                    LOGGER.log(Level.FINE, "Discarding duplicate result for URL {0} from peer {1}",
                            new Object[]{canonicalUrl, node.getId()});
                    continue;
                }

                visitedUrls.add(canonicalUrl);
                removeUrlFromQueueLocked(canonicalUrl);

                page.setSessionId(session.getId());
                if (pageRepository != null && session.getId() > 0) {
                    pageRepository.save(page, extractHostOrFallback(page.getUrl()));
                }
                session.addPage(page);
                session.incrementVisited();
                if (page.isHasTerm()) {
                    session.incrementMatched();
                }
                if (page.getError() != null && !page.getError().isBlank()) {
                    session.incrementError();
                }
                acceptedPages.add(page);
            }

            if (session.getStatus() != SessionStatus.FINISHED && session.getStatus() != SessionStatus.FAILED) {
                enqueueDiscoveredLinksLocked(safeLinks, settings);
            }

            if (!acceptedPages.isEmpty() && sessionRepository != null && session.getId() > 0) {
                sessionRepository.update(session);
            }

            SessionStatus currentStatus = session.getStatus();
            if (currentStatus == SessionStatus.RUNNING || currentStatus == SessionStatus.IDLE) {
                String finishReason = determineCompletionReasonLocked(settings);
                if (finishReason != null) {
                    session.finish(finishReason);
                    if (sessionRepository != null && session.getId() > 0) {
                        sessionRepository.update(session);
                    }
                    completedNow = true;
                    completedSessionSnapshot = session;
                }
            }
        }

        for (Page accepted : acceptedPages) {
            eventListener.onPageProcessed(accepted);
            if (accepted.getError() != null && !accepted.getError().isBlank()) {
                eventListener.onError(accepted.getUrl(), accepted.getError());
            }
        }
        if (!acceptedPages.isEmpty()) {
            eventListener.onProgressUpdated(session);
        }

        if (completedNow) {
            PeerCommandSender sender = this.commandSender;
            if (sender != null) {
                sender.broadcastCommand(createStopCommand(
                        "COMPLETED", completedSessionSnapshot.getCompletionReason()));
            }
            eventListener.onStatusMessage("Розподілений обхід завершено: " + completedSessionSnapshot.getCompletionReason() + ".");
            eventListener.onSessionFinished(completedSessionSnapshot);
        }
    }

    public synchronized void returnBatch(PeerNode node) {
        if (node == null) {
            return;
        }
        List<QueueEntry> batch = assignedBatches.remove(node);
        if (batch == null || batch.isEmpty()) {
            return;
        }
        prependEntriesLocked(batch);
    }

    public synchronized void returnUnprocessedEntries(PeerNode node, List<QueueEntry> unprocessed) {
        if (node == null || unprocessed == null || unprocessed.isEmpty()) {
            return;
        }
        Set<String> keys = new HashSet<>();
        for (QueueEntry entry : unprocessed) {
            if (entry != null && entry.getUrl() != null && !entry.getUrl().isBlank()) {
                keys.add(canonicalKey(entry.getUrl()));
            }
        }
        removeAssignedUrlsLocked(node, keys);
        prependEntriesLocked(unprocessed);
    }

    public void broadcastCommand(String command) {
        if (command == null || command.isBlank()) {
            throw new IllegalArgumentException("command must not be blank");
        }
        String normalized = command.trim().toUpperCase(Locale.ROOT);
        MessageType messageType;
        try {
            messageType = MessageType.valueOf(normalized);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Unsupported control command: " + command, ex);
        }

        switch (messageType) {
            case PAUSE -> executePauseCommand();
            case RESUME -> executeResumeCommand();
            case STOP -> executeStopCommand();
            default -> throw new IllegalArgumentException("Unsupported control command: " + command);
        }
    }

    public synchronized boolean registerPeer(PeerNode node) {
        return registerPeer(node, LocalDateTime.now());
    }

    public synchronized boolean registerPeer(PeerNode node, LocalDateTime timestamp) {
        Objects.requireNonNull(node, "node must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        if (!isAcceptingPeers()) {
            return false;
        }
        node.updateHeartbeat(timestamp);
        PeerNode existing = activePeersById.get(node.getId());
        if (existing != null) {
            activePeers.remove(existing);
        }
        activePeersById.put(node.getId(), node);
        activePeers.add(node);
        eventListener.onPeerConnected(node);
        eventListener.onStatusMessage("Підключено вузол " + node.getId()
                + " (" + node.getHost() + ":" + node.getPort() + ").");
        return true;
    }

    public synchronized void unregisterPeer(PeerNode node) {
        if (node == null) {
            return;
        }
        PeerNode removed = activePeersById.remove(node.getId());
        activePeers.remove(removed != null ? removed : node);
        returnBatch(removed != null ? removed : node);
        if (removed != null) {
            eventListener.onStatusMessage("Вузол " + removed.getId()
                    + " відключився. Необроблений пакет повернуто в чергу.");
        }
    }

    public synchronized void onHeartbeat(String peerId) {
        onHeartbeat(peerId, LocalDateTime.now());
    }

    public synchronized void onHeartbeat(String peerId, LocalDateTime timestamp) {
        if (peerId == null || peerId.isBlank() || timestamp == null) {
            return;
        }
        PeerNode peer = activePeersById.get(peerId);
        if (peer != null) {
            peer.updateHeartbeat(timestamp);
        }
    }

    public synchronized List<PeerNode> checkLostPeers(LocalDateTime now) {
        Objects.requireNonNull(now, "now must not be null");
        List<PeerNode> lost = new ArrayList<>();
        for (PeerNode node : List.copyOf(activePeersById.values())) {
            if (node.isLost(now)) {
                markPeerLostLocked(node);
                lost.add(node);
            }
        }
        return List.copyOf(lost);
    }

    public synchronized boolean markPeerLost(PeerNode node) {
        if (node == null) {
            return false;
        }
        PeerNode existing = activePeersById.get(node.getId());
        if (existing == null) {
            return false;
        }
        markPeerLostLocked(existing);
        return true;
    }

    public synchronized int getActiveWorkersCount() {
        return 1 + activePeersById.size();
    }

    public synchronized boolean isAcceptingPeers() {
        SessionStatus status = session.getStatus();
        return status != SessionStatus.FINISHED && status != SessionStatus.FAILED;
    }

    public synchronized boolean isPeerActive(String peerId) {
        return peerId != null && activePeersById.containsKey(peerId);
    }

    public synchronized boolean hasInFlightBatches() {
        for (List<QueueEntry> batch : assignedBatches.values()) {
            if (batch != null && !batch.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public synchronized List<QueueEntry> getQueueSnapshot() {
        return List.copyOf(queue);
    }

    public synchronized Set<String> getVisitedUrlsSnapshot() {
        return Set.copyOf(visitedUrls);
    }

    public synchronized List<PeerNode> getActivePeers() {
        return List.copyOf(activePeers);
    }

    public synchronized void restoreFromSession(
            Collection<String> initialVisited,
            Collection<QueueEntry> initialQueue) {
        visitedUrls.clear();
        recordedPageUrls.clear();
        queue.clear();
        assignedBatches.clear();

        if (initialVisited != null) {
            for (String url : initialVisited) {
                if (url != null && !url.isBlank()) {
                    String canonical = canonicalKey(url);
                    visitedUrls.add(canonical);
                    recordedPageUrls.add(canonical);
                }
            }
        }

        if (initialQueue != null) {
            Set<String> queuedKeys = new HashSet<>();
            for (QueueEntry entry : initialQueue) {
                if (entry == null || entry.getUrl() == null || entry.getUrl().isBlank()) {
                    continue;
                }
                String canonical = canonicalKey(entry.getUrl());
                visitedUrls.add(canonical);
                if (!recordedPageUrls.contains(canonical) && queuedKeys.add(canonical)) {
                    queue.add(new QueueEntry(canonical, entry.getDepth()));
                }
            }
        }
    }

    public CrawlSession getSession() {
        return session;
    }

    public CrawlSettings getSessionSettings() {
        return new CrawlSettings(session.getSettings());
    }

    public SessionStatus getSessionStatus() {
        return session.getStatus();
    }

    public String getCompletionReason() {
        return session.getCompletionReason();
    }

    public int getBatchSize() {
        return batchSize;
    }

    private void executePauseCommand() {
        synchronized (this) {
            SessionStatus status = session.getStatus();
            if (status == SessionStatus.FINISHED || status == SessionStatus.FAILED || status == SessionStatus.PAUSED) {
                return;
            }
            pauseInProgress = true;
        }

        PeerCommandSender sender = this.commandSender;
        try {
            if (sender != null) {
                sender.broadcastCommand(new PeerMessage(MessageType.PAUSE, INITIATOR_PEER_ID));
            }
        } finally {
            boolean paused = false;
            synchronized (this) {
                pauseInProgress = false;
                if (session.getStatus() == SessionStatus.RUNNING) {
                    session.pause();
                    if (sessionRepository != null && session.getId() > 0) {
                        sessionRepository.update(session);
                    }
                    paused = true;
                }
            }
            if (paused) {
                eventListener.onStatusMessage("Розподілену сесію призупинено.");
                eventListener.onSessionPaused(session);
            }
        }
    }

    private void executeResumeCommand() {
        synchronized (this) {
            pauseInProgress = false;
            SessionStatus status = session.getStatus();
            if (status == SessionStatus.PAUSED || status == SessionStatus.IDLE) {
                session.start();
                if (sessionRepository != null && session.getId() > 0) {
                    sessionRepository.update(session);
                }
            } else if (status == SessionStatus.FINISHED || status == SessionStatus.FAILED) {
                throw new IllegalStateException("Cannot resume session in terminal status " + status);
            }
        }

        PeerCommandSender sender = this.commandSender;
        if (sender != null) {
            sender.broadcastCommand(new PeerMessage(MessageType.RESUME, INITIATOR_PEER_ID));
        }
        eventListener.onStatusMessage("Розподілену сесію відновлено.");
    }

    private void executeStopCommand() {
        boolean transitioned = false;
        synchronized (this) {
            pauseInProgress = false;
            SessionStatus status = session.getStatus();
            if (status != SessionStatus.FINISHED && status != SessionStatus.FAILED) {
                session.stop();
                assignedBatches.clear();
                if (sessionRepository != null && session.getId() > 0) {
                    sessionRepository.update(session);
                }
                transitioned = true;
            }
        }

        PeerCommandSender sender = this.commandSender;
        if (sender != null) {
            sender.broadcastCommand(createStopCommand("OK", CrawlSession.REASON_STOPPED_BY_USER));
        }
        if (transitioned) {
            eventListener.onStatusMessage("Розподілений обхід зупинено користувачем.");
            eventListener.onSessionFinished(session);
        }
    }

    private void markPeerLostLocked(PeerNode node) {
        activePeersById.remove(node.getId());
        activePeers.remove(node);
        returnBatch(node);
        eventListener.onPeerLost(node);
        eventListener.onStatusMessage("Вузол " + node.getId()
                + " втрачено (немає сигналу 15 с). Пакет повернуто в чергу.");
    }

    private int countAssignedEntriesLocked() {
        int total = 0;
        for (List<QueueEntry> entries : assignedBatches.values()) {
            if (entries != null) {
                total += entries.size();
            }
        }
        return total;
    }

    private void removeAssignedUrlsLocked(PeerNode node, Set<String> processedKeys) {
        if (processedKeys.isEmpty()) {
            return;
        }
        List<QueueEntry> assigned = assignedBatches.get(node);
        if (assigned == null) {
            return;
        }
        assigned.removeIf(entry -> entry != null && processedKeys.contains(canonicalKey(entry.getUrl())));
        if (assigned.isEmpty()) {
            assignedBatches.remove(node);
        }
    }

    private void enqueueDiscoveredLinksLocked(List<QueueEntry> links, CrawlSettings settings) {
        if (links.isEmpty()) {
            return;
        }
        int maxDepth = settings.getMaxDepth();
        boolean includeSubdomains = settings.isIncludeSubdomains();
        List<QueueEntry> accepted = new ArrayList<>();

        for (QueueEntry link : links) {
            if (link == null || link.getUrl() == null || link.getDepth() > maxDepth) {
                continue;
            }
            String normalized = normalizeOrNull(link.getUrl());
            if (normalized == null) {
                continue;
            }
            if (!UrlNormalizer.isHostAllowed(normalized, startHost, includeSubdomains)) {
                continue;
            }
            if (visitedUrls.add(normalized)) {
                accepted.add(new QueueEntry(normalized, link.getDepth()));
            }
        }

        if (accepted.isEmpty()) {
            return;
        }

        if (settings.getStrategy() == StrategyType.DFS) {
            for (int i = accepted.size() - 1; i >= 0; i--) {
                queue.addFirst(accepted.get(i));
            }
        } else {
            for (QueueEntry entry : accepted) {
                queue.addLast(entry);
            }
        }
    }

    private void prependEntriesLocked(List<QueueEntry> entries) {
        Set<String> existingInQueue = new HashSet<>();
        for (QueueEntry queued : queue) {
            if (queued != null && queued.getUrl() != null) {
                existingInQueue.add(canonicalKey(queued.getUrl()));
            }
        }
        for (int i = entries.size() - 1; i >= 0; i--) {
            QueueEntry entry = entries.get(i);
            if (entry == null || entry.getUrl() == null || entry.getUrl().isBlank()) {
                continue;
            }
            String canonical = canonicalKey(entry.getUrl());
            if (!recordedPageUrls.contains(canonical) && existingInQueue.add(canonical)) {
                queue.addFirst(new QueueEntry(canonical, entry.getDepth()));
            }
        }
    }

    private void removeUrlFromQueueLocked(String canonicalUrl) {
        queue.removeIf(entry -> entry != null && canonicalKey(entry.getUrl()).equals(canonicalUrl));
    }

    private String determineCompletionReasonLocked(CrawlSettings settings) {
        if (session.getVisitedCount() >= settings.getPageLimit()) {
            return REASON_PAGE_LIMIT_REACHED;
        }
        if (queue.isEmpty() && !hasInFlightBatches()) {
            return REASON_QUEUE_EXHAUSTED;
        }
        return null;
    }

    private static PeerMessage createStopCommand(String status, String reason) {
        JsonObject payload = new JsonObject();
        if (reason != null && !reason.isBlank()) {
            payload.addProperty(PeerServer.KEY_REASON, reason);
        }
        return new PeerMessage(MessageType.STOP, INITIATOR_PEER_ID, null, status, payload);
    }

    private static String normalizeOrNull(String url) {
        return UrlNormalizer.tryNormalize(url).orElse(null);
    }

    private static String canonicalKey(String url) {
        if (url == null) {
            return "";
        }
        return UrlNormalizer.tryNormalize(url).orElse(url.trim());
    }

    private static String extractHostOrNull(String url) {
        if (!UrlNormalizer.isValidHttpUrl(url)) {
            return null;
        }
        return UrlNormalizer.extractHost(url);
    }

    private String extractHostOrFallback(String url) {
        if (UrlNormalizer.isValidHttpUrl(url)) {
            return UrlNormalizer.extractHost(url);
        }
        return startHost != null ? startHost : "unknown";
    }
}
