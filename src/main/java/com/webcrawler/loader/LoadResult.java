package com.webcrawler.loader;

import java.time.LocalDateTime;

public record LoadResult(
        String html,
        int httpStatus,
        long contentLength,
        LocalDateTime downloadedAt,
        String finalUrl,
        boolean fromCache
) {
}
