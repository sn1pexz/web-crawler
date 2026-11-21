package com.webcrawler.storage;

import com.webcrawler.util.UrlNormalizer;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public final class PathSanitizer {

    //Clamp path to 230 chars and escape Windows reserved device names
    public static final int MAX_RELATIVE_PATH_LENGTH = 230;
    private static final int MAX_SEGMENT_LENGTH = 80;

    private static final Set<String> WINDOWS_RESERVED_NAMES = Set.of(
            "CON", "PRN", "AUX", "NUL",
            "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
            "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
    );

    public record SanitizedPath(
            String hostSegment,
            List<String> dirSegments,
            String fileName,
            String relativePath
    ) {
        public SanitizedPath {
            if (hostSegment == null || hostSegment.isBlank()) {
                throw new IllegalArgumentException("hostSegment must not be blank");
            }
            dirSegments = dirSegments == null ? List.of() : List.copyOf(dirSegments);
            if (fileName == null || fileName.isBlank()) {
                throw new IllegalArgumentException("fileName must not be blank");
            }
            if (relativePath == null || relativePath.isBlank()) {
                throw new IllegalArgumentException("relativePath must not be blank");
            }
        }
    }

    private PathSanitizer() {
    }

    public static String sanitizeHost(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        return sanitizeSegment(host.trim().toLowerCase(Locale.ROOT), "host", MAX_SEGMENT_LENGTH);
    }

    public static SanitizedPath sanitize(String normalizedUrl) {
        String canonicalUrl = UrlNormalizer.normalize(normalizedUrl);
        URI originalUri = URI.create(normalizedUrl.trim());
        URI canonicalUri = URI.create(canonicalUrl);

        String hostSegment = sanitizeHost(canonicalUri.getHost());

        String preservedPath = UrlNormalizer.normalizePathSegments(originalUri.getRawPath(), true);
        boolean directoryUrl = preservedPath.endsWith("/");

        List<String> rawSegments = splitNonEmptySegments(preservedPath);
        List<String> dirSegments = new ArrayList<>();
        String rawStem;
        if (directoryUrl) {
            for (String seg : rawSegments) {
                dirSegments.add(sanitizeSegment(decodePercent(seg), "dir", MAX_SEGMENT_LENGTH));
            }
            rawStem = "index";
        } else {
            for (int i = 0; i < rawSegments.size() - 1; i++) {
                dirSegments.add(sanitizeSegment(decodePercent(rawSegments.get(i)), "dir", MAX_SEGMENT_LENGTH));
            }
            String lastSegment = decodePercent(rawSegments.get(rawSegments.size() - 1));
            rawStem = stripHtmlExtension(lastSegment);
        }

        String stem = sanitizeSegment(rawStem, "page", MAX_SEGMENT_LENGTH);
        String canonicalQuery = canonicalUri.getRawQuery();
        String fileName = buildFileName(stem, canonicalQuery);
        String relativePath = buildRelativePath(hostSegment, dirSegments, fileName);

        if (relativePath.length() <= MAX_RELATIVE_PATH_LENGTH) {
            return new SanitizedPath(hostSegment, dirSegments, fileName, relativePath);
        }

        return enforceMaxLength(canonicalUrl, hostSegment, dirSegments, stem);
    }

    private static SanitizedPath enforceMaxLength(
            String canonicalUrl,
            String hostSegment,
            List<String> originalDirs,
            String stem) {
        String urlHash = sha256Hex8(canonicalUrl);
        String suffix = "_" + urlHash + ".html";

        int maxHostLen = MAX_RELATIVE_PATH_LENGTH - suffix.length() - 2;
        String safeHost = hostSegment.length() > maxHostLen
                ? truncateAndEscape(hostSegment, maxHostLen, "host")
                : hostSegment;

        List<String> keptDirs = new ArrayList<>();
        int usedLen = safeHost.length();
        int minFileBudget = 1 + Math.min(stem.length(), 16) + suffix.length();

        for (String dir : originalDirs) {
            int candidateLen = usedLen + 1 + dir.length();
            if (candidateLen + minFileBudget <= MAX_RELATIVE_PATH_LENGTH) {
                keptDirs.add(dir);
                usedLen = candidateLen;
            } else {
                break;
            }
        }

        int maxStemLen = MAX_RELATIVE_PATH_LENGTH - usedLen - 1 - suffix.length();
        String safeStem = truncateAndEscape(stem, Math.max(1, maxStemLen), "p");
        String fileName = safeStem + suffix;
        String relativePath = buildRelativePath(safeHost, keptDirs, fileName);

        if (relativePath.length() > MAX_RELATIVE_PATH_LENGTH && !keptDirs.isEmpty()) {
            keptDirs.clear();
            int fallbackStemLen = Math.max(1, MAX_RELATIVE_PATH_LENGTH - safeHost.length() - 1 - suffix.length());
            safeStem = truncateAndEscape(stem, fallbackStemLen, "p");
            fileName = safeStem + suffix;
            relativePath = buildRelativePath(safeHost, keptDirs, fileName);
        }

        return new SanitizedPath(safeHost, keptDirs, fileName, relativePath);
    }

    private static String buildFileName(String stem, String canonicalQuery) {
        if (canonicalQuery != null && !canonicalQuery.isEmpty()) {
            return stem + "_" + sha256Hex8(canonicalQuery) + ".html";
        }
        return stem + ".html";
    }

    private static String buildRelativePath(String hostSegment, List<String> dirSegments, String fileName) {
        StringBuilder sb = new StringBuilder(hostSegment);
        for (String dir : dirSegments) {
            sb.append('/').append(dir);
        }
        sb.append('/').append(fileName);
        return sb.toString();
    }

    private static String stripHtmlExtension(String segment) {
        String lower = segment.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".html") && segment.length() > 5) {
            return segment.substring(0, segment.length() - 5);
        }
        if (lower.endsWith(".htm") && segment.length() > 4) {
            return segment.substring(0, segment.length() - 4);
        }
        return segment;
    }

    private static List<String> splitNonEmptySegments(String path) {
        List<String> result = new ArrayList<>();
        String[] parts = path.split("/");
        for (String part : parts) {
            if (!part.isEmpty()) {
                result.add(part);
            }
        }
        return result;
    }

    private static String sanitizeSegment(String raw, String fallback, int maxLen) {
        String cleaned = replaceInvalidWindowsChars(raw);
        cleaned = stripLeadingAndTrailingInvalid(cleaned);
        if (cleaned.isEmpty()) {
            cleaned = fallback;
        }
        cleaned = escapeWindowsReservedName(cleaned);
        if (cleaned.length() > maxLen) {
            String hash = sha256Hex8(raw);
            int prefixLen = Math.max(1, maxLen - 1 - hash.length());
            String prefix = stripLeadingAndTrailingInvalid(cleaned.substring(0, prefixLen));
            if (prefix.isEmpty()) {
                prefix = fallback.substring(0, Math.min(fallback.length(), prefixLen));
            }
            cleaned = escapeWindowsReservedName(prefix) + "_" + hash;
            if (cleaned.length() > maxLen) {
                cleaned = cleaned.substring(0, maxLen);
                cleaned = stripLeadingAndTrailingInvalid(cleaned);
            }
        }
        return cleaned.isEmpty() ? fallback : cleaned;
    }

    private static String truncateAndEscape(String value, int maxLen, String fallback) {
        String truncated = value.length() > maxLen ? value.substring(0, maxLen) : value;
        truncated = stripLeadingAndTrailingInvalid(truncated);
        if (truncated.isEmpty()) {
            truncated = fallback;
        }
        truncated = escapeWindowsReservedName(truncated);
        if (truncated.length() > maxLen) {
            truncated = truncated.substring(0, maxLen);
            truncated = stripLeadingAndTrailingInvalid(truncated);
        }
        return truncated.isEmpty() ? fallback : truncated;
    }

    private static String replaceInvalidWindowsChars(String input) {
        StringBuilder sb = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            char ch = input.charAt(i);
            if (ch < 0x20 || ch == '<' || ch == '>' || ch == ':' || ch == '"'
                    || ch == '/' || ch == '\\' || ch == '|' || ch == '?' || ch == '*') {
                sb.append('_');
            } else {
                sb.append(ch);
            }
        }
        return sb.toString();
    }

    private static String stripLeadingAndTrailingInvalid(String input) {
        int start = 0;
        int end = input.length();
        while (start < end && Character.isWhitespace(input.charAt(start))) {
            start++;
        }
        while (end > start) {
            char ch = input.charAt(end - 1);
            if (ch == '.' || ch == ' ' || Character.isWhitespace(ch)) {
                end--;
            } else {
                break;
            }
        }
        return input.substring(start, end);
    }

    private static String escapeWindowsReservedName(String segment) {
        int dotIndex = segment.indexOf('.');
        String stem = dotIndex >= 0 ? segment.substring(0, dotIndex) : segment;
        if (WINDOWS_RESERVED_NAMES.contains(stem.toUpperCase(Locale.ROOT))) {
            return "_" + segment;
        }
        return segment;
    }

    private static String decodePercent(String input) {
        if (input.indexOf('%') < 0) {
            return input;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(input.length());
        for (int i = 0; i < input.length(); i++) {
            char ch = input.charAt(i);
            if (ch == '%' && i + 2 < input.length()) {
                int hi = Character.digit(input.charAt(i + 1), 16);
                int lo = Character.digit(input.charAt(i + 2), 16);
                if (hi >= 0 && lo >= 0) {
                    out.write((hi << 4) | lo);
                    i += 2;
                    continue;
                }
            }
            byte[] utf8 = String.valueOf(ch).getBytes(StandardCharsets.UTF_8);
            out.writeBytes(utf8);
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    private static String sha256Hex8(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(8);
            for (int i = 0; i < 4; i++) {
                int b = hash[i] & 0xFF;
                if (b < 0x10) {
                    hex.append('0');
                }
                hex.append(Integer.toHexString(b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 algorithm unavailable", ex);
        }
    }
}
