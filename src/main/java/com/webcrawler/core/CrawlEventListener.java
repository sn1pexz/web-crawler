package com.webcrawler.core;

import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.Page;
import com.webcrawler.p2p.PeerNode;

public interface CrawlEventListener {

    CrawlEventListener NO_OP = new CrawlEventListener() { };

    default void onProgressUpdated(CrawlSession session) {
    }

    default void onPageProcessed(Page page) {
    }

    default void onError(String url, String errorMessage) {
    }

    default void onSessionPaused(CrawlSession session) {
    }

    default void onSessionFinished(CrawlSession session) {
    }

    default void onPeerConnected(PeerNode peer) {
    }

    default void onPeerLost(PeerNode peer) {
    }

    default void onStatusMessage(String message) {
    }
}
