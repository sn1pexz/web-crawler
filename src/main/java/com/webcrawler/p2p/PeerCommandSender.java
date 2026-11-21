package com.webcrawler.p2p;

import com.webcrawler.p2p.protocol.PeerMessage;

public interface PeerCommandSender {

    void sendToPeer(String peerId, PeerMessage message);

    void broadcastCommand(PeerMessage command);

    void disconnectPeer(String peerId);
}
