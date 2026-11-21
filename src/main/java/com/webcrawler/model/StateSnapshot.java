package com.webcrawler.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

//Memento holding immutable snapshot of paused session
public class StateSnapshot {

    public static final String COUNTER_VISITED = "visited";
    public static final String COUNTER_MATCHED = "matched";
    public static final String COUNTER_ERRORS = "errors";

    private long sessionId;
    private CrawlSettings settings;
    private List<QueueEntry> queue = new ArrayList<>();
    private Map<String, Integer> counters = new LinkedHashMap<>();

    public StateSnapshot() {
    }

    public StateSnapshot(
            long sessionId,
            CrawlSettings settings,
            List<QueueEntry> queue,
            Map<String, Integer> counters) {
        this.sessionId = sessionId;
        this.settings = settings != null ? new CrawlSettings(settings) : null;
        setQueue(queue);
        setCounters(counters);
    }

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public CrawlSettings getSettings() {
        return settings;
    }

    public void setSettings(CrawlSettings settings) {
        this.settings = settings != null ? new CrawlSettings(settings) : null;
    }

    public List<QueueEntry> getQueue() {
        return queue == null ? List.of() : Collections.unmodifiableList(queue);
    }

    public void setQueue(List<QueueEntry> queue) {
        this.queue = queue == null ? new ArrayList<>() : new ArrayList<>(queue);
    }

    public Map<String, Integer> getCounters() {
        return counters == null ? Map.of() : Collections.unmodifiableMap(counters);
    }

    public void setCounters(Map<String, Integer> counters) {
        this.counters = counters == null ? new LinkedHashMap<>() : new LinkedHashMap<>(counters);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StateSnapshot other)) {
            return false;
        }
        return sessionId == other.sessionId
                && Objects.equals(settings, other.settings)
                && Objects.equals(queue, other.queue)
                && Objects.equals(counters, other.counters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, settings, queue, counters);
    }

    @Override
    public String toString() {
        return "StateSnapshot{"
                + "sessionId=" + sessionId
                + ", queueSize=" + (queue != null ? queue.size() : 0)
                + ", counters=" + counters
                + '}';
    }
}
