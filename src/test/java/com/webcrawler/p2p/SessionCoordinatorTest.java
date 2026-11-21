package com.webcrawler.p2p;

import com.webcrawler.core.CrawlEventListener;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.Page;
import com.webcrawler.model.QueueEntry;
import com.webcrawler.model.SessionStatus;
import com.webcrawler.p2p.protocol.MessageType;
import com.webcrawler.p2p.protocol.PeerMessage;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionCoordinatorTest {

    @Test
    void enforcesGlobalVisitedDeduplicationHostLimitsMaxDepthAndPageLimit() {
        CrawlSettings settings = new CrawlSettings("https://example.com/start", "java");
        settings.setMaxDepth(1);
        settings.setPageLimit(3);
        settings.setIncludeSubdomains(false);

        CrawlSession session = new CrawlSession(settings);
        session.start();

        RecordingCommandSender sender = new RecordingCommandSender();
        AtomicInteger finishedCount = new AtomicInteger();
        CrawlEventListener listener = new CrawlEventListener() {
            @Override
            public void onSessionFinished(CrawlSession finished) {
                finishedCount.incrementAndGet();
            }
        };

        SessionCoordinator coordinator = new SessionCoordinator(session, null, null, sender, listener);
        PeerNode peer1 = new PeerNode("peer-1", "127.0.0.1", 9101);
        PeerNode peer2 = new PeerNode("peer-2", "127.0.0.1", 9102);
        coordinator.registerPeer(peer1);
        coordinator.registerPeer(peer2);

        List<QueueEntry> first = coordinator.assignBatch(peer1);
        assertEquals(1, first.size());

        coordinator.acceptResult(peer1,
                List.of(successPage("https://example.com/start", 0, true)),
                List.of(
                        new QueueEntry("https://example.com/start#section", 1),
                        new QueueEntry("https://example.com/p1?b=2&a=1", 1),
                        new QueueEntry("https://example.com/p1?a=1&b=2", 1),
                        new QueueEntry("https://sub.example.com/ignored", 1),
                        new QueueEntry("https://other.org/ignored", 1),
                        new QueueEntry("https://example.com/too-deep", 2),
                        new QueueEntry("https://example.com/p2", 1),
                        new QueueEntry("https://example.com/p3", 1)
                ));

        assertEquals(3, coordinator.getQueueSnapshot().size());

        List<QueueEntry> batchForPeer1 = coordinator.assignBatch(peer1);
        List<QueueEntry> batchForPeer2 = coordinator.assignBatch(peer2);
        assertEquals(2, batchForPeer1.size());
        assertTrue(batchForPeer2.isEmpty());

        coordinator.acceptResult(peer1, List.of(
                successPage("https://example.com/p1?a=1&b=2", 1, false),
                successPage("https://example.com/p2", 1, true),
                successPage("https://example.com/p3", 1, true)
        ), List.of());

        assertEquals(3, session.getVisitedCount());
        assertEquals(2, session.getMatchedCount());
        assertEquals(SessionStatus.FINISHED, session.getStatus());
        assertEquals(SessionCoordinator.REASON_PAGE_LIMIT_REACHED, session.getCompletionReason());
        assertEquals(1, finishedCount.get());
        assertTrue(sender.broadcastedMessages.stream().anyMatch(m -> m.getType() == MessageType.STOP));
    }

    @Test
    void discardsDuplicateResultFromLostPeerScenario7Exception1() {
        CrawlSettings settings = new CrawlSettings("https://example.com", "java");
        settings.setPageLimit(10);
        CrawlSession session = new CrawlSession(settings);
        session.start();

        SessionCoordinator coordinator = new SessionCoordinator(session, null, null, null, CrawlEventListener.NO_OP);
        LocalDateTime t0 = LocalDateTime.of(2026, 11, 19, 12, 0, 0);
        PeerNode peer1 = new PeerNode("peer-1", "127.0.0.1", 9101, t0);
        PeerNode peer2 = new PeerNode("peer-2", "127.0.0.1", 9102, t0);
        coordinator.registerPeer(peer1, t0);
        coordinator.registerPeer(peer2, t0);

        List<QueueEntry> batchForPeer1 = coordinator.assignBatch(peer1);
        assertEquals(1, batchForPeer1.size());

        coordinator.checkLostPeers(t0.plusSeconds(16));
        List<QueueEntry> batchForPeer2 = coordinator.assignBatch(peer2);
        assertEquals(batchForPeer1, batchForPeer2);

        coordinator.acceptResult(peer2,
                List.of(successPage("https://example.com/", 0, true)),
                List.of(new QueueEntry("https://example.com/next", 1)));

        assertEquals(1, session.getVisitedCount());
        assertEquals(1, session.getMatchedCount());

        coordinator.acceptResult(peer1,
                List.of(successPage("https://example.com/", 0, true)),
                List.of());

        assertEquals(1, session.getVisitedCount());
        assertEquals(1, session.getMatchedCount());
    }

    private static Page successPage(String url, int depth, boolean hasTerm) {
        Page page = new Page(url, depth);
        page.setTitle("Title " + url);
        page.setDownloadedAt(LocalDateTime.now());
        page.setSize(1024L);
        page.setHttpStatus(200);
        page.setHasTerm(hasTerm);
        return page;
    }

    private static final class RecordingCommandSender implements PeerCommandSender {
        private final List<PeerMessage> broadcastedMessages = new CopyOnWriteArrayList<>();

        @Override
        public void sendToPeer(String peerId, PeerMessage message) {
        }

        @Override
        public void broadcastCommand(PeerMessage command) {
            broadcastedMessages.add(command);
        }

        @Override
        public void disconnectPeer(String peerId) {
        }
    }
}
