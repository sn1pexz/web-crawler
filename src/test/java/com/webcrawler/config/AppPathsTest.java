package com.webcrawler.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AppPathsTest {

    private String previousDataDirProp;

    @BeforeEach
    void captureSystemProperty() {
        previousDataDirProp = System.getProperty(AppPaths.DATA_DIR_PROPERTY);
    }

    @AfterEach
    void restoreSystemProperty() {
        if (previousDataDirProp == null) {
            System.clearProperty(AppPaths.DATA_DIR_PROPERTY);
        } else {
            System.setProperty(AppPaths.DATA_DIR_PROPERTY, previousDataDirProp);
        }
    }

    @Test
    void resolvesDataDirFromCliArgOrDefaultsToWorkingDirectory(@TempDir Path tempDir) {
        System.clearProperty(AppPaths.DATA_DIR_PROPERTY);

        AppPaths defaultPaths = AppPaths.fromArgs(new String[0]);
        Path expectedWorkingDir = Path.of("").toAbsolutePath().normalize();
        assertEquals(expectedWorkingDir, defaultPaths.getBaseDir());
        assertEquals(expectedWorkingDir.resolve("webcrawler"), defaultPaths.getDatabasePrefix());
        assertEquals(expectedWorkingDir.resolve("results"), defaultPaths.getResultsDir());
        assertEquals(expectedWorkingDir.resolve("cache"), defaultPaths.getCacheDir());
        assertEquals("jdbc:h2:file:" + expectedWorkingDir.resolve("webcrawler") + ";AUTO_SERVER=FALSE",
                defaultPaths.getJdbcUrl());

        Path propDir = tempDir.resolve("prop-dir");
        System.setProperty(AppPaths.DATA_DIR_PROPERTY, propDir.toString());
        assertEquals(propDir.toAbsolutePath().normalize(), AppPaths.fromArgs(null).getBaseDir());

        Path cliEqualsDir = tempDir.resolve("cli-equals-dir");
        AppPaths cliEqualsPaths = AppPaths.fromArgs(new String[]{"--other=1", "--data-dir=" + cliEqualsDir});
        assertEquals(cliEqualsDir.toAbsolutePath().normalize(), cliEqualsPaths.getBaseDir());

        Path cliSpaceDir = tempDir.resolve("cli-space-dir");
        AppPaths cliSpacePaths = AppPaths.fromArgs(new String[]{"--data-dir", cliSpaceDir.toString()});
        assertEquals(cliSpaceDir.toAbsolutePath().normalize(), cliSpacePaths.getBaseDir());
    }
}
