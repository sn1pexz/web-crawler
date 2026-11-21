package com.webcrawler.persistence;

import com.webcrawler.config.AppPaths;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class CacheRepository {

    public static final String TYPE_PAGE = "PAGE";
    public static final String TYPE_ROBOTS = "ROBOTS";

    private static final Logger LOGGER = Logger.getLogger(CacheRepository.class.getName());

    private final DatabaseManager databaseManager;
    private final Path cacheDir;
    private final PageRepository pageRepository;

    public record CacheRecord(
            String cacheKey,
            String entryType,
            String filePath,
            int httpStatus,
            long contentLength,
            String finalUrl,
            LocalDateTime cachedAt,
            String content
    ) {
    }

    public CacheRepository(DatabaseManager databaseManager, AppPaths appPaths) {
        this(databaseManager, Objects.requireNonNull(appPaths, "appPaths must not be null").getCacheDir());
    }

    public CacheRepository(DatabaseManager databaseManager, Path cacheDir) {
        this.databaseManager = Objects.requireNonNull(databaseManager, "databaseManager must not be null");
        this.cacheDir = Objects.requireNonNull(cacheDir, "cacheDir must not be null").toAbsolutePath().normalize();
        this.pageRepository = new PageRepository(databaseManager);
    }

    public Optional<CacheRecord> findValidEntry(String cacheKey, String entryType, int ttlHours) {
        validateKey(cacheKey);
        String normalizedType = normalizeEntryType(entryType);
        if (ttlHours <= 0) {
            return Optional.empty();
        }

        LocalDateTime cutoff = LocalDateTime.now().minusHours(ttlHours);
        String sql = """
                SELECT cache_key, entry_type, file_path, http_status, content_length, final_url, cached_at
                FROM cache_entries
                WHERE cache_key = ? AND entry_type = ? AND cached_at >= ?
                """;

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, cacheKey);
            stmt.setString(2, normalizedType);
            stmt.setTimestamp(3, Timestamp.valueOf(cutoff));

            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }

                String storedPath = rs.getString("file_path");
                Path payloadPath = resolveCacheFilePath(storedPath);
                if (!Files.isRegularFile(payloadPath)) {
                    LOGGER.log(Level.FINE, "Cache index hit but file missing on disk: {0}", payloadPath);
                    return Optional.empty();
                }

                String content;
                try {
                    content = Files.readString(payloadPath, StandardCharsets.UTF_8);
                } catch (IOException ex) {
                    LOGGER.log(Level.WARNING, "Failed to read cached payload file: " + payloadPath, ex);
                    return Optional.empty();
                }

                Timestamp cachedTs = rs.getTimestamp("cached_at");
                LocalDateTime cachedAt = cachedTs != null ? cachedTs.toLocalDateTime() : null;
                return Optional.of(new CacheRecord(
                        rs.getString("cache_key"),
                        rs.getString("entry_type"),
                        storedPath,
                        rs.getInt("http_status"),
                        rs.getLong("content_length"),
                        rs.getString("final_url"),
                        cachedAt,
                        content
                ));
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to query cache entry for key=" + cacheKey, ex);
        }
    }

    public void saveEntry(
            String cacheKey,
            String entryType,
            String host,
            String html,
            int httpStatus,
            long contentLength,
            String finalUrl) {
        validateKey(cacheKey);
        String normalizedType = normalizeEntryType(entryType);
        String payload = html != null ? html : "";

        Path typeDir = cacheDir.resolve(normalizedType);
        Path targetPath = typeDir.resolve(sha256Hex(cacheKey) + ".dat").toAbsolutePath().normalize();
        writePayloadAtomically(typeDir, targetPath, payload);

        Long siteId = (host != null && !host.isBlank()) ? pageRepository.getOrCreateSiteId(host) : null;
        LocalDateTime now = LocalDateTime.now();

        String sql = """
                MERGE INTO cache_entries (
                    cache_key, entry_type, site_id, file_path,
                    http_status, content_length, final_url, cached_at
                ) KEY (cache_key, entry_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """;

        databaseManager.executeInTransaction(conn -> {
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, cacheKey);
                stmt.setString(2, normalizedType);
                if (siteId == null) {
                    stmt.setNull(3, Types.BIGINT);
                } else {
                    stmt.setLong(3, siteId);
                }
                stmt.setString(4, targetPath.toString());
                stmt.setInt(5, httpStatus);
                stmt.setLong(6, contentLength);
                stmt.setString(7, finalUrl);
                stmt.setTimestamp(8, Timestamp.valueOf(now));
                stmt.executeUpdate();
                return null;
            }
        });
    }

    public void clearAll() {
        databaseManager.executeInTransaction(conn -> {
            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM cache_entries")) {
                stmt.executeUpdate();
                return null;
            }
        });

        if (!Files.exists(cacheDir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(cacheDir)) {
            walk.sorted(Comparator.reverseOrder())
                    .filter(path -> !path.equals(cacheDir))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ex) {
                            LOGGER.log(Level.WARNING, "Failed to delete cache artifact: " + path, ex);
                        }
                    });
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to clear cache directory: " + cacheDir, ex);
        }
    }

    public Path getCacheDir() {
        return cacheDir;
    }

    private Path resolveCacheFilePath(String storedPath) {
        Path path = Path.of(storedPath);
        if (path.isAbsolute()) {
            return path.normalize();
        }
        return cacheDir.resolve(path).normalize();
    }

    private static void writePayloadAtomically(Path typeDir, Path targetPath, String payload) {
        Path tempFile = null;
        try {
            Files.createDirectories(typeDir);
            tempFile = Files.createTempFile(typeDir, "cache-", ".tmp");
            Files.writeString(tempFile, payload, StandardCharsets.UTF_8);
            try {
                Files.move(tempFile, targetPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ex) {
                Files.move(tempFile, targetPath, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException ex) {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ignored) {
                }
            }
            throw new IllegalStateException("Failed to write cache payload to " + targetPath, ex);
        }
    }

    private static void validateKey(String cacheKey) {
        if (cacheKey == null || cacheKey.isBlank()) {
            throw new IllegalArgumentException("cacheKey must not be blank");
        }
    }

    private static String normalizeEntryType(String entryType) {
        if (entryType == null || entryType.isBlank()) {
            throw new IllegalArgumentException("entryType must not be blank");
        }
        String normalized = entryType.trim().toUpperCase(Locale.ROOT);
        if (!TYPE_PAGE.equals(normalized) && !TYPE_ROBOTS.equals(normalized)) {
            throw new IllegalArgumentException("Unsupported cache entryType: " + entryType);
        }
        return normalized;
    }

    private static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 algorithm unavailable", ex);
        }
    }
}
