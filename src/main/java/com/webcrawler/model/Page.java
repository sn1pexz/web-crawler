package com.webcrawler.model;

import java.time.LocalDateTime;
import java.util.Objects;

public class Page {

    private long id;
    private long sessionId;
    private long siteId;
    private String url;
    private String title;
    private LocalDateTime downloadedAt;
    private long size;
    private int depth;
    private int httpStatus;
    private String error;
    private boolean hasTerm;
    private String filePath;

    public Page() {
    }

    public Page(String url, int depth) {
        this.url = url;
        this.depth = depth;
    }

    public Page(
            long id,
            long sessionId,
            long siteId,
            String url,
            String title,
            LocalDateTime downloadedAt,
            long size,
            int depth,
            int httpStatus,
            String error,
            boolean hasTerm,
            String filePath) {
        this.id = id;
        this.sessionId = sessionId;
        this.siteId = siteId;
        this.url = url;
        this.title = title;
        this.downloadedAt = downloadedAt;
        this.size = size;
        this.depth = depth;
        this.httpStatus = httpStatus;
        this.error = error;
        this.hasTerm = hasTerm;
        this.filePath = filePath;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public long getSiteId() {
        return siteId;
    }

    public void setSiteId(long siteId) {
        this.siteId = siteId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public LocalDateTime getDownloadedAt() {
        return downloadedAt;
    }

    public void setDownloadedAt(LocalDateTime downloadedAt) {
        this.downloadedAt = downloadedAt;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public boolean isHasTerm() {
        return hasTerm;
    }

    public void setHasTerm(boolean hasTerm) {
        this.hasTerm = hasTerm;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Page other)) {
            return false;
        }
        return id == other.id
                && sessionId == other.sessionId
                && siteId == other.siteId
                && size == other.size
                && depth == other.depth
                && httpStatus == other.httpStatus
                && hasTerm == other.hasTerm
                && Objects.equals(url, other.url)
                && Objects.equals(title, other.title)
                && Objects.equals(downloadedAt, other.downloadedAt)
                && Objects.equals(error, other.error)
                && Objects.equals(filePath, other.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, sessionId, siteId, url, title, downloadedAt, size, depth,
                httpStatus, error, hasTerm, filePath);
    }

    @Override
    public String toString() {
        return "Page{"
                + "id=" + id
                + ", sessionId=" + sessionId
                + ", url='" + url + '\''
                + ", httpStatus=" + httpStatus
                + ", hasTerm=" + hasTerm
                + '}';
    }
}
