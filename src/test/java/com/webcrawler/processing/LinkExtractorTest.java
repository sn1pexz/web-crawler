package com.webcrawler.processing;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LinkExtractorTest {

    private final LinkExtractor extractor = new LinkExtractor();

    @Test
    void preservesNavigationLinksFromRawHtmlBeforeFiltering() {
        String rawHtml = """
                <html>
                  <body>
                    <header><a href="/home">Home</a></header>
                    <nav><a href="/catalog">Catalog</a><a href="/blog">Blog</a></nav>
                    <main><a href="/article-1">Article</a></main>
                    <aside><a href="/sidebar-item">Sidebar</a></aside>
                    <footer><a href="/privacy">Privacy</a></footer>
                  </body>
                </html>
                """;

        List<String> links = extractor.extract(rawHtml, "https://example.com/");

        assertEquals(List.of(
                "https://example.com/home",
                "https://example.com/catalog",
                "https://example.com/blog",
                "https://example.com/article-1",
                "https://example.com/sidebar-item",
                "https://example.com/privacy"
        ), links);
    }
}
