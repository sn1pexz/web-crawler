package com.webcrawler.config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public final class AppPaths {

    public static final String DATA_DIR_PROPERTY = "webcrawler.dataDir";
    private static final String DATA_DIR_ARG = "--data-dir";
    private static final String DATA_DIR_ARG_PREFIX = "--data-dir=";

    private final Path baseDir;

    public AppPaths() {
        this(resolveDefaultBaseDir());
    }

    public AppPaths(Path baseDir) {
        Objects.requireNonNull(baseDir, "baseDir must not be null");
        this.baseDir = baseDir.toAbsolutePath().normalize();
    }

    public static AppPaths fromArgs(String[] args) {
        String cliPath = extractCliDataDir(args);
        if (cliPath != null) {
            return new AppPaths(Path.of(cliPath));
        }
        return new AppPaths();
    }

    private static Path resolveDefaultBaseDir() {
        String prop = System.getProperty(DATA_DIR_PROPERTY);
        if (prop != null && !prop.isBlank()) {
            return Path.of(prop.trim());
        }
        return Path.of("").toAbsolutePath().normalize();
    }

    private static String extractCliDataDir(String[] args) {
        if (args == null) {
            return null;
        }
        String resolved = null;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg == null) {
                continue;
            }
            if (arg.startsWith(DATA_DIR_ARG_PREFIX)) {
                String value = arg.substring(DATA_DIR_ARG_PREFIX.length()).trim();
                if (value.isEmpty()) {
                    throw new IllegalArgumentException("Missing directory path after --data-dir=");
                }
                resolved = value;
            } else if (DATA_DIR_ARG.equals(arg)) {
                if (i + 1 >= args.length || args[i + 1] == null || args[i + 1].isBlank()) {
                    throw new IllegalArgumentException("Missing directory path after --data-dir");
                }
                resolved = args[++i].trim();
            }
        }
        return resolved;
    }

    public Path getBaseDir() {
        return baseDir;
    }

    public Path getDatabasePrefix() {
        return baseDir.resolve("webcrawler").toAbsolutePath().normalize();
    }

    public String getJdbcUrl() {
        return "jdbc:h2:file:" + getDatabasePrefix() + ";AUTO_SERVER=FALSE";
    }

    public Path getResultsDir() {
        return baseDir.resolve("results");
    }

    public Path getCacheDir() {
        return baseDir.resolve("cache");
    }

    public void ensureDirectoriesExist() {
        try {
            Files.createDirectories(baseDir);
            Files.createDirectories(getResultsDir());
            Files.createDirectories(getCacheDir());
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to create application directories under " + baseDir, ex);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AppPaths other)) {
            return false;
        }
        return baseDir.equals(other.baseDir);
    }

    @Override
    public int hashCode() {
        return baseDir.hashCode();
    }

    @Override
    public String toString() {
        return "AppPaths{baseDir=" + baseDir + '}';
    }
}
