package com.webcrawler.storage;

import com.webcrawler.model.Page;
import com.webcrawler.model.Site;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SiteCompositeTest {

    @Test
    void buildsCompositeTreeAndWritesUtf8HtmlToDisk(@TempDir Path tempDir) throws IOException {
        Site site = new Site(1L, "example.com");

        Page rootPage = new Page("https://example.com/", 0);
        Page nestedPage = new Page("https://example.com/blog/2026/article", 2);

        String rootHtml = "<html><body><h1>Головна сторінка</h1></body></html>";
        String nestedHtml = "<html><body><p>Стаття українською мовою</p></body></html>";

        PathSanitizer.SanitizedPath rootPath = PathSanitizer.sanitize(rootPage.getUrl());
        PathSanitizer.SanitizedPath nestedPath = PathSanitizer.sanitize(nestedPage.getUrl());

        PageElement rootElement = site.registerMatchedPage(
                rootPage, rootHtml, rootPath.dirSegments(), rootPath.fileName());
        PageElement nestedElement = site.registerMatchedPage(
                nestedPage, nestedHtml, nestedPath.dirSegments(), nestedPath.fileName());

        assertNotNull(site.getRootDirectory());
        assertEquals("example.com", site.getRootDirectory().getName());
        assertEquals(2, site.getPages().size());
        assertNotNull(rootElement);
        assertNotNull(nestedElement);

        site.getRootDirectory().write(tempDir.toString());

        Path expectedRootFile = tempDir.resolve("example.com/index.html");
        Path expectedNestedFile = tempDir.resolve("example.com/blog/2026/article.html");

        assertTrue(Files.exists(expectedRootFile));
        assertTrue(Files.exists(expectedNestedFile));
        assertEquals(rootHtml, Files.readString(expectedRootFile, StandardCharsets.UTF_8));
        assertEquals(nestedHtml, Files.readString(expectedNestedFile, StandardCharsets.UTF_8));
    }

    @Test
    void resolvesParentPathConsistentlyWithIncrementalPageElementWrite(@TempDir Path tempDir) throws IOException {
        Site site = new Site("example.com");
        Page page = new Page("https://example.com/docs/guide", 1);
        PathSanitizer.SanitizedPath sanitized = PathSanitizer.sanitize(page.getUrl());

        PageElement element = site.registerMatchedPage(
                page, "<html>guide</html>", sanitized.dirSegments(), sanitized.fileName());
        Path parentDir = site.resolveParentPath(tempDir, sanitized.dirSegments());
        element.write(parentDir.toString());

        Path expected = tempDir.resolve(sanitized.relativePath());
        assertTrue(Files.exists(expected));
        assertEquals("<html>guide</html>", Files.readString(expected, StandardCharsets.UTF_8));
    }
}
