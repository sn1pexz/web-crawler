package com.webcrawler.model;

import com.webcrawler.core.CrawlStrategy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

//Memento originator holding crawl session state
public class CrawlSession {

    public static final String REASON_STOPPED_BY_USER = "Зупинено користувачем";

    private long id;
    private SessionStatus status = SessionStatus.IDLE;
    private String completionReason;
    private int visitedCount;
    private int matchedCount;
    private int errorCount;
    private CrawlSettings settings;
    private CrawlStrategy strategy;
    private final List<Page> pages = new ArrayList<>();
    private boolean distributed;
    private Integer serverPort;

    public CrawlSession(CrawlSettings settings) {
        this(0L, settings);
    }

    public CrawlSession(long id, CrawlSettings settings) {
        this.id = id;
        this.settings = new CrawlSettings(Objects.requireNonNull(settings, "settings must not be null"));
    }

    public synchronized void start() {
        if (status != SessionStatus.IDLE && status != SessionStatus.PAUSED) {
            throw new IllegalStateException("Cannot start session from status " + status);
        }
        this.status = SessionStatus.RUNNING;
        this.completionReason = null;
    }

    public synchronized void pause() {
        if (status != SessionStatus.RUNNING) {
            throw new IllegalStateException("Cannot pause session from status " + status);
        }
        this.status = SessionStatus.PAUSED;
    }

    public synchronized void stop() {
        if (status == SessionStatus.FINISHED || status == SessionStatus.FAILED) {
            throw new IllegalStateException("Cannot stop session already in terminal status " + status);
        }
        this.status = SessionStatus.FINISHED;
        this.completionReason = REASON_STOPPED_BY_USER;
    }

    public synchronized void finish(String reason) {
        if (status == SessionStatus.FINISHED || status == SessionStatus.FAILED) {
            throw new IllegalStateException("Cannot finish session already in terminal status " + status);
        }
        this.status = SessionStatus.FINISHED;
        this.completionReason = reason;
    }

    public synchronized void fail(String reason) {
        if (status == SessionStatus.FINISHED || status == SessionStatus.FAILED) {
            throw new IllegalStateException("Cannot fail session already in terminal status " + status);
        }
        this.status = SessionStatus.FAILED;
        this.completionReason = reason;
    }

    public synchronized StateSnapshot createSnapshot() {
        List<QueueEntry> queueSnapshot = strategy != null ? strategy.getQueueSnapshot() : List.of();
        Map<String, Integer> counters = new LinkedHashMap<>();
        counters.put(StateSnapshot.COUNTER_VISITED, visitedCount);
        counters.put(StateSnapshot.COUNTER_MATCHED, matchedCount);
        counters.put(StateSnapshot.COUNTER_ERRORS, errorCount);
        return new StateSnapshot(id, new CrawlSettings(settings), queueSnapshot, counters);
    }

    public synchronized void restoreFrom(StateSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        this.id = snapshot.getSessionId();
        if (snapshot.getSettings() != null) {
            this.settings = new CrawlSettings(snapshot.getSettings());
        }
        Map<String, Integer> counters = snapshot.getCounters();
        this.visitedCount = counters.getOrDefault(StateSnapshot.COUNTER_VISITED, 0);
        this.matchedCount = counters.getOrDefault(StateSnapshot.COUNTER_MATCHED, 0);
        this.errorCount = counters.getOrDefault(StateSnapshot.COUNTER_ERRORS, 0);
        if (this.strategy != null) {
            this.strategy.restoreQueue(snapshot.getQueue());
        }
        this.status = SessionStatus.PAUSED;
        this.completionReason = null;
    }

    public synchronized int incrementVisited() {
        return ++visitedCount;
    }

    public synchronized int incrementMatched() {
        return ++matchedCount;
    }

    public synchronized int incrementError() {
        return ++errorCount;
    }

    public synchronized void addPage(Page page) {
        if (page != null) {
            pages.add(page);
        }
    }

    public synchronized long getId() {
        return id;
    }

    public synchronized void setId(long id) {
        this.id = id;
    }

    public synchronized SessionStatus getStatus() {
        return status;
    }

    public synchronized void setStatus(SessionStatus status) {
        this.status = Objects.requireNonNull(status, "status must not be null");
    }

    public synchronized String getCompletionReason() {
        return completionReason;
    }

    public synchronized void setCompletionReason(String completionReason) {
        this.completionReason = completionReason;
    }

    public synchronized int getVisitedCount() {
        return visitedCount;
    }

    public synchronized void setVisitedCount(int visitedCount) {
        this.visitedCount = visitedCount;
    }

    public synchronized int getMatchedCount() {
        return matchedCount;
    }

    public synchronized void setMatchedCount(int matchedCount) {
        this.matchedCount = matchedCount;
    }

    public synchronized int getErrorCount() {
        return errorCount;
    }

    public synchronized void setErrorCount(int errorCount) {
        this.errorCount = errorCount;
    }

    public synchronized CrawlSettings getSettings() {
        return settings;
    }

    public synchronized void setSettings(CrawlSettings settings) {
        this.settings = new CrawlSettings(Objects.requireNonNull(settings, "settings must not be null"));
    }

    public synchronized CrawlStrategy getStrategy() {
        return strategy;
    }

    public synchronized void setStrategy(CrawlStrategy strategy) {
        this.strategy = strategy;
    }

    public synchronized List<Page> getPages() {
        return List.copyOf(pages);
    }

    public synchronized boolean isDistributed() {
        return distributed;
    }

    public synchronized void setDistributed(boolean distributed) {
        this.distributed = distributed;
    }

    public synchronized Integer getServerPort() {
        return serverPort;
    }

    public synchronized void setServerPort(Integer serverPort) {
        this.serverPort = serverPort;
    }
}
