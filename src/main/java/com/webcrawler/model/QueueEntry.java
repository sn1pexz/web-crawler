package com.webcrawler.model;

import java.util.Objects;

public class QueueEntry {

    private String url;
    private int depth;

    public QueueEntry() {
    }

    public QueueEntry(String url, int depth) {
        this.url = Objects.requireNonNull(url, "url must not be null");
        if (depth < 0) {
            throw new IllegalArgumentException("depth must be >= 0, got " + depth);
        }
        this.depth = depth;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        if (depth < 0) {
            throw new IllegalArgumentException("depth must be >= 0, got " + depth);
        }
        this.depth = depth;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueueEntry other)) {
            return false;
        }
        return depth == other.depth && Objects.equals(url, other.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, depth);
    }

    @Override
    public String toString() {
        return "QueueEntry{url='" + url + "', depth=" + depth + '}';
    }
}
