package com.webcrawler.processing;

import com.webcrawler.filter.AdFilter;
import com.webcrawler.filter.ContentFilter;
import com.webcrawler.filter.ScriptFilter;
import com.webcrawler.filter.StyleFilter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TermSearcherTest {

    private final TermSearcher searcher = new TermSearcher();

    @Test
    void supportsCaseInsensitiveCyrillicAndWordBoundaries() {
        String text = "Пошук інформації: Київ, Їжак, Єнот, Ґанок, Істина — «Програма» працює.";

        assertTrue(searcher.search(text, "київ", false));
        assertTrue(searcher.search(text, "КИЇВ", true));
        assertTrue(searcher.search(text, "їжак", true));
        assertTrue(searcher.search(text, "ЄНОТ", true));
        assertTrue(searcher.search(text, "ґанок", true));
        assertTrue(searcher.search(text, "істина", true));
        assertTrue(searcher.search(text, "програма", true));

        String derivativeOnly = "Сучасне програмування та кодування даних.";
        assertTrue(searcher.search(derivativeOnly, "програм", false));
        assertFalse(searcher.search(derivativeOnly, "програм", true));
        assertTrue(searcher.search(derivativeOnly, "код", false));
        assertFalse(searcher.search(derivativeOnly, "код", true));
    }

    @Test
    void ignoresHtmlTagsAndAttributesWhenSearchingVisibleText() {
        String html = """
                <html>
                  <head>
                    <title>InvisibleHeadTerm</title>
                    <style>.secretCssTerm { color: red; }</style>
                    <script>var secretJsTerm = 'hidden';</script>
                  </head>
                  <body>
                    <div class="ad-box">secretAdTerm</div>
                    <article id="secretAttrId" class="secretAttrClass" data-term="secretDataTerm">
                      <p>Visible content about web crawling and indexing in Ukrainian: Пошукова система.
                         Additional semantic text to exceed one hundred characters cleanly for filtering.</p>
                    </article>
                  </body>
                </html>
                """;

        Document doc = Jsoup.parse(html);
        ContentFilter scriptFilter = new ScriptFilter();
        ContentFilter styleFilter = new StyleFilter();
        ContentFilter adFilter = new AdFilter();
        scriptFilter.setNext(styleFilter);
        styleFilter.setNext(adFilter);

        assertTrue(scriptFilter.process(doc));
        String visibleBodyText = doc.body().text();

        assertTrue(searcher.search(visibleBodyText, "пошукова", true));
        assertTrue(searcher.search(visibleBodyText, "crawling", true));

        assertFalse(searcher.search(visibleBodyText, "article", false));
        assertFalse(searcher.search(visibleBodyText, "secretAttrId", false));
        assertFalse(searcher.search(visibleBodyText, "secretAttrClass", false));
        assertFalse(searcher.search(visibleBodyText, "secretDataTerm", false));
        assertFalse(searcher.search(visibleBodyText, "secretJsTerm", false));
        assertFalse(searcher.search(visibleBodyText, "secretCssTerm", false));
        assertFalse(searcher.search(visibleBodyText, "secretAdTerm", false));
        assertFalse(searcher.search(visibleBodyText, "InvisibleHeadTerm", false));
    }
}
