package com.webcrawler.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public final class UrlNormalizer {

    private static final Comparator<String> QUERY_PARAM_COMPARATOR = (a, b) -> {
        int eqA = a.indexOf('=');
        int eqB = b.indexOf('=');
        String keyA = eqA >= 0 ? a.substring(0, eqA) : a;
        String keyB = eqB >= 0 ? b.substring(0, eqB) : b;
        int keyCmp = keyA.compareTo(keyB);
        if (keyCmp != 0) {
            return keyCmp;
        }
        String valA = eqA >= 0 ? a.substring(eqA + 1) : null;
        String valB = eqB >= 0 ? b.substring(eqB + 1) : null;
        if (valA == null && valB == null) {
            return 0;
        }
        if (valA == null) {
            return -1;
        }
        if (valB == null) {
            return 1;
        }
        return valA.compareTo(valB);
    };

    private UrlNormalizer() {
    }

    public static boolean isValidHttpUrl(String url) {
        return tryNormalize(url).isPresent();
    }

    public static String normalize(String url) {
        return tryNormalize(url)
                .orElseThrow(() -> new IllegalArgumentException("Invalid HTTP/HTTPS URL: " + url));
    }

    public static Optional<String> tryNormalize(String url) {
        URI uri = parseValidHttpUri(url);
        if (uri == null) {
            return Optional.empty();
        }
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        int port = normalizePort(scheme, uri.getPort());
        String path = normalizePath(uri.getRawPath());
        String query = canonicalizeQuery(uri.getRawQuery());

        StringBuilder sb = new StringBuilder();
        sb.append(scheme).append("://").append(host);
        if (port != -1) {
            sb.append(':').append(port);
        }
        sb.append(path);
        if (query != null) {
            sb.append('?').append(query);
        }
        return Optional.of(sb.toString());
    }

    public static String extractHost(String url) {
        URI uri = parseValidHttpUri(url);
        if (uri == null) {
            throw new IllegalArgumentException("Invalid HTTP/HTTPS URL: " + url);
        }
        return uri.getHost().toLowerCase(Locale.ROOT);
    }

    public static String extractOrigin(String url) {
        URI uri = parseValidHttpUri(url);
        if (uri == null) {
            throw new IllegalArgumentException("Invalid HTTP/HTTPS URL: " + url);
        }
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        int port = normalizePort(scheme, uri.getPort());
        return port != -1
                ? scheme + "://" + host + ":" + port
                : scheme + "://" + host;
    }

    public static boolean isHostAllowed(String candidateUrl, String startHost, boolean includeSubdomains) {
        if (startHost == null || startHost.isBlank()) {
            return false;
        }
        String normalizedStart = startHost.trim().toLowerCase(Locale.ROOT);
        if (normalizedStart.startsWith(".") || normalizedStart.contains("/") || normalizedStart.contains(" ")) {
            return false;
        }
        URI uri = parseValidHttpUri(candidateUrl);
        if (uri == null) {
            return false;
        }
        String candidateHost = uri.getHost().toLowerCase(Locale.ROOT);
        if (candidateHost.equals(normalizedStart)) {
            return true;
        }
        return includeSubdomains && candidateHost.endsWith("." + normalizedStart);
    }

    public static String canonicalizeQuery(String rawQuery) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            return null;
        }
        String[] parts = rawQuery.split("&");
        List<String> params = new ArrayList<>(parts.length);
        for (String part : parts) {
            if (!part.isEmpty()) {
                params.add(part);
            }
        }
        if (params.isEmpty()) {
            return null;
        }
        params.sort(QUERY_PARAM_COMPARATOR);
        return String.join("&", params);
    }

    public static String normalizePathSegments(String rawPath, boolean preserveDirectoryTrailingSlash) {
        if (rawPath == null || rawPath.isEmpty() || "/".equals(rawPath)) {
            return "/";
        }
        boolean endsWithSlash = rawPath.endsWith("/");
        Deque<String> stack = new ArrayDeque<>();
        int start = 0;
        int len = rawPath.length();
        while (start <= len) {
            int slash = rawPath.indexOf('/', start);
            String segment = slash >= 0 ? rawPath.substring(start, slash) : rawPath.substring(start);
            if ("..".equals(segment)) {
                if (!stack.isEmpty()) {
                    stack.removeLast();
                }
            } else if (!segment.isEmpty() && !".".equals(segment)) {
                stack.addLast(segment);
            }
            if (slash < 0) {
                break;
            }
            start = slash + 1;
        }
        if (stack.isEmpty()) {
            return "/";
        }
        String joined = "/" + String.join("/", stack);
        if (preserveDirectoryTrailingSlash && (endsWithSlash || rawPath.endsWith("/.") || rawPath.endsWith("/.."))) {
            return joined + "/";
        }
        return joined;
    }

    private static String normalizePath(String rawPath) {
        return normalizePathSegments(rawPath, false);
    }

    private static int normalizePort(String scheme, int port) {
        if (("http".equals(scheme) && port == 80) || ("https".equals(scheme) && port == 443)) {
            return -1;
        }
        return port;
    }

    private static URI parseValidHttpUri(String url) {
        if (url == null) {
            return null;
        }
        String trimmed = url.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        URI uri;
        try {
            uri = new URI(trimmed);
        } catch (URISyntaxException ex) {
            return null;
        }
        if (uri.isOpaque()) {
            return null;
        }
        String scheme = uri.getScheme();
        if (scheme == null) {
            return null;
        }
        String lowerScheme = scheme.toLowerCase(Locale.ROOT);
        if (!"http".equals(lowerScheme) && !"https".equals(lowerScheme)) {
            return null;
        }
        if (uri.getRawUserInfo() != null) {
            return null;
        }
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            return null;
        }
        int port = uri.getPort();
        if (port != -1 && (port < 1 || port > 65535)) {
            return null;
        }
        return uri;
    }
}
