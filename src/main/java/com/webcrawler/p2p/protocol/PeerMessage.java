package com.webcrawler.p2p.protocol;

import com.google.gson.JsonObject;

import java.util.Objects;

public class PeerMessage {

    private MessageType type;
    private String peerId;
    private String requestId;
    private String status;
    private JsonObject payload;

    public PeerMessage() {
    }

    public PeerMessage(MessageType type, String peerId) {
        this(type, peerId, null, null, null);
    }

    public PeerMessage(
            MessageType type,
            String peerId,
            String requestId,
            String status,
            JsonObject payload) {
        this.type = type;
        this.peerId = peerId;
        this.requestId = requestId;
        this.status = status;
        this.payload = payload;
    }

    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public String getPeerId() {
        return peerId;
    }

    public void setPeerId(String peerId) {
        this.peerId = peerId;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public JsonObject getPayload() {
        return payload;
    }

    public void setPayload(JsonObject payload) {
        this.payload = payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PeerMessage other)) {
            return false;
        }
        return type == other.type
                && Objects.equals(peerId, other.peerId)
                && Objects.equals(requestId, other.requestId)
                && Objects.equals(status, other.status)
                && Objects.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, peerId, requestId, status, payload);
    }

    @Override
    public String toString() {
        return "PeerMessage{"
                + "type=" + type
                + ", peerId='" + peerId + '\''
                + ", requestId='" + requestId + '\''
                + ", status='" + status + '\''
                + '}';
    }
}
