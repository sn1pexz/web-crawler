package com.webcrawler.model;

public record HostStatistics(
        String host,
        long visitedCount,
        long matchedCount,
        long errorCount,
        long totalBytes
) {
}
