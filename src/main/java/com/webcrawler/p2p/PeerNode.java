package com.webcrawler.p2p;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;

public class PeerNode {

    public static final long LOST_TIMEOUT_SECONDS = 15L;

    private final String id;
    private final String host;
    private final int port;
    private volatile LocalDateTime lastHeartbeat;

    public PeerNode(String id, String host, int port) {
        this(id, host, port, LocalDateTime.now());
    }

    public PeerNode(String id, String host, int port, LocalDateTime lastHeartbeat) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Peer id must not be blank");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Peer host must not be blank");
        }
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Peer port must be in 0..65535, got " + port);
        }
        this.id = id;
        this.host = host;
        this.port = port;
        this.lastHeartbeat = Objects.requireNonNull(lastHeartbeat, "lastHeartbeat must not be null");
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public LocalDateTime getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void updateHeartbeat() {
        this.lastHeartbeat = LocalDateTime.now();
    }

    public void updateHeartbeat(LocalDateTime timestamp) {
        this.lastHeartbeat = Objects.requireNonNull(timestamp, "timestamp must not be null");
    }

    public boolean isLost() {
        return isLost(LocalDateTime.now());
    }

    public boolean isLost(LocalDateTime now) {
        Objects.requireNonNull(now, "now must not be null");
        LocalDateTime heartbeat = this.lastHeartbeat;
        if (heartbeat == null) {
            return true;
        }
        return Duration.between(heartbeat, now).compareTo(Duration.ofSeconds(LOST_TIMEOUT_SECONDS)) > 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PeerNode other)) {
            return false;
        }
        return Objects.equals(id, other.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        return "PeerNode{id='" + id + "', host='" + host + "', port=" + port + '}';
    }
}
