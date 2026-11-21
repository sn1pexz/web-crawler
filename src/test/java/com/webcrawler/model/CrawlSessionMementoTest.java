package com.webcrawler.model;

import com.webcrawler.core.CrawlStrategy;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CrawlSessionMementoTest {

    private static final class FifoStubStrategy extends CrawlStrategy {
        private final ArrayDeque<QueueEntry> deque = new ArrayDeque<>();

        private FifoStubStrategy() {
            this.queue = deque;
        }

        @Override
        protected synchronized QueueEntry nextEntry() {
            return deque.pollFirst();
        }

        @Override
        protected synchronized void addEntry(QueueEntry entry) {
            deque.addLast(entry);
        }
    }

    @Test
    void createsAndRestoresMementoSnapshot() {
        CrawlSettings settings = new CrawlSettings("https://example.com", "критерій");
        settings.setMaxDepth(3);
        settings.setPageLimit(50);

        CrawlSession original = new CrawlSession(42L, settings);
        FifoStubStrategy strategy = new FifoStubStrategy();
        strategy.restoreQueue(List.of(
                new QueueEntry("https://example.com/a", 1),
                new QueueEntry("https://example.com/b", 2)));
        original.setStrategy(strategy);
        original.start();
        original.incrementVisited();
        original.incrementVisited();
        original.incrementMatched();
        original.incrementError();
        original.pause();

        StateSnapshot snapshot = original.createSnapshot();
        assertEquals(42L, snapshot.getSessionId());
        assertEquals(2, snapshot.getQueue().size());
        assertEquals(2, snapshot.getCounters().get(StateSnapshot.COUNTER_VISITED));
        assertEquals(1, snapshot.getCounters().get(StateSnapshot.COUNTER_MATCHED));
        assertEquals(1, snapshot.getCounters().get(StateSnapshot.COUNTER_ERRORS));

        CrawlSession restored = new CrawlSession(new CrawlSettings("https://other.example", "other"));
        FifoStubStrategy restoredStrategy = new FifoStubStrategy();
        restored.setStrategy(restoredStrategy);
        restored.restoreFrom(snapshot);

        assertEquals(42L, restored.getId());
        assertEquals(SessionStatus.PAUSED, restored.getStatus());
        assertNull(restored.getCompletionReason());
        assertEquals("https://example.com", restored.getSettings().getStartUrl());
        assertEquals(3, restored.getSettings().getMaxDepth());
        assertEquals(2, restored.getVisitedCount());
        assertEquals(1, restored.getMatchedCount());
        assertEquals(1, restored.getErrorCount());
        assertEquals(List.of(
                new QueueEntry("https://example.com/a", 1),
                new QueueEntry("https://example.com/b", 2)),
                restoredStrategy.getQueueSnapshot());
    }
}
