package com.webcrawler.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PathSanitizerTest {

    @Test
    void appendsDeterministicEightCharSha256QueryHashBeforeHtmlExtension() {
        PathSanitizer.SanitizedPath first = PathSanitizer.sanitize("https://example.com/search?b=2&a=1");
        PathSanitizer.SanitizedPath reordered = PathSanitizer.sanitize("https://example.com/search?a=1&b=2");
        PathSanitizer.SanitizedPath different = PathSanitizer.sanitize("https://example.com/search?a=1&b=3");

        assertEquals(first, reordered);
        assertTrue(first.fileName().matches("^search_[0-9a-f]{8}\\.html$"));
        assertNotEquals(first.fileName(), different.fileName());
    }

    @Test
    void escapesWindowsReservedDeviceNamesAndInvalidChars(@TempDir Path tempDir) throws IOException {
        PathSanitizer.SanitizedPath reserved = PathSanitizer.sanitize("https://example.com/CON/nul/COM1");
        assertEquals(List.of("_CON", "_nul"), reserved.dirSegments());
        assertEquals("_COM1.html", reserved.fileName());

        PathSanitizer.SanitizedPath reservedWithExt = PathSanitizer.sanitize("https://example.com/lpt9.txt");
        assertEquals("_lpt9.txt.html", reservedWithExt.fileName());

        PathSanitizer.SanitizedPath invalidChars =
                PathSanitizer.sanitize("https://example.com/a%3Cb%3Ec%3Ad/file%2A%3F%7C...%20");
        for (String seg : invalidChars.dirSegments()) {
            assertFalseContainsInvalidWindowsChars(seg);
        }
        assertFalseContainsInvalidWindowsChars(invalidChars.fileName());
        assertTrue(invalidChars.fileName().endsWith(".html"));

        Path target = tempDir.resolve(reserved.relativePath());
        Files.createDirectories(target.getParent());
        Files.writeString(target, "<html>ok</html>", StandardCharsets.UTF_8);
        assertTrue(Files.exists(target));
    }

    private static void assertFalseContainsInvalidWindowsChars(String segment) {
        String forbidden = "<>:\"/\\|?*";
        for (int i = 0; i < forbidden.length(); i++) {
            assertTrue(segment.indexOf(forbidden.charAt(i)) < 0,
                    "Segment contains forbidden char " + forbidden.charAt(i) + ": " + segment);
        }
    }
}
