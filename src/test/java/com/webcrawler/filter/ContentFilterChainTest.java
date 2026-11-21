package com.webcrawler.filter;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContentFilterChainTest {

    private static final String LONG_FILLER = "Meaningful semantic article text that comfortably exceeds the minimum threshold of one hundred visible characters for content validation.";

    @Test
    void adFilterMatchesWholeTokensAndPreservesNonAdWordsAndRoots() {
        String html = """
                <html class="ad">
                  <body id="ads" class="ad-page">
                    <div class="ad-box">Remove ad-box</div>
                    <div id="google_ads">Remove google_ads</div>
                    <div class="adUnit">Remove adUnit</div>
                    <div class="реклама">Remove Cyrillic reklama</div>
                    <div class="блокРеклама">Remove camelCase Cyrillic reklama</div>
                    <div class="ad-wrapper">
                      <div class="ad-box">Nested ad</div>
                    </div>
                    <div class="sponsorBlock">Remove sponsor camelCase</div>
                    <div id="taboola_feed">Remove taboola</div>
                    <div class="outbrain-widget">Remove outbrain</div>
                    <div class="adsense">Remove adsense</div>
                    <div class="doubleclick">Remove doubleclick</div>
                    <div class="reklama-top">Remove reklama</div>
                    <div class="header myHeader">Keep header</div>
                    <div class="loading">Keep loading</div>
                    <div class="shadow">Keep shadow</div>
                    <div class="badge">Keep badge</div>
                    <div class="sidebar">Keep sidebar</div>
                    <div class="download">Keep download</div>
                    <div class="banner banners">Keep banner</div>
                    <div class="promo promotion">Keep promo</div>
                    <div class="commercial">Keep commercial</div>
                    <p>%s</p>
                  </body>
                </html>
                """.formatted(LONG_FILLER);

        Document doc = Jsoup.parse(html);
        AdFilter filter = new AdFilter();

        assertTrue(filter.process(doc));
        assertNotNull(doc.selectFirst("html"));
        assertNotNull(doc.body());

        assertTrue(doc.select(".ad-box, #google_ads, .adUnit, .реклама, .блокРеклама, .ad-wrapper, "
                + ".sponsorBlock, #taboola_feed, .outbrain-widget, .adsense, .doubleclick, .reklama-top").isEmpty());

        assertNotNull(doc.selectFirst(".header.myHeader"));
        assertNotNull(doc.selectFirst(".loading"));
        assertNotNull(doc.selectFirst(".shadow"));
        assertNotNull(doc.selectFirst(".badge"));
        assertNotNull(doc.selectFirst(".sidebar"));
        assertNotNull(doc.selectFirst(".download"));
        assertNotNull(doc.selectFirst(".banner.banners"));
        assertNotNull(doc.selectFirst(".promo.promotion"));
        assertNotNull(doc.selectFirst(".commercial"));
    }

    @Test
    void chainOfResponsibilitySupportsFullAndSubsetPipelines() {
        String html = """
                <html>
                  <head><style>.x{}</style><script>bad()</script></head>
                  <body>
                    <nav>Nav links</nav>
                    <iframe src="https://example.com/frame"></iframe>
                    <div class="ad-box">Buy now</div>
                    <article>
                      <header><h1>Article Title</h1></header>
                      <p onclick="x()">%s</p>
                    </article>
                  </body>
                </html>
                """.formatted(LONG_FILLER);

        Document fullDoc = Jsoup.parse(html);
        ContentFilter fullChain = buildChain(
                new ScriptFilter(),
                new StyleFilter(),
                new EmbeddedObjectFilter(),
                new AdFilter(),
                new NavigationFilter()
        );
        assertTrue(fullChain.process(fullDoc));
        assertTrue(fullDoc.select("script, style, nav, iframe, .ad-box").isEmpty());
        assertNotNull(fullDoc.selectFirst("article header"));
        assertFalse(fullDoc.selectFirst("article p").hasAttr("onclick"));

        Document defaultDoc = Jsoup.parse(html);
        ContentFilter defaultChain = buildChain(
                new ScriptFilter(),
                new StyleFilter(),
                new EmbeddedObjectFilter(),
                new AdFilter()
        );
        assertTrue(defaultChain.process(defaultDoc));
        assertTrue(defaultDoc.select("script, style, iframe, .ad-box").isEmpty());
        assertNotNull(defaultDoc.selectFirst("nav"));

        Document subsetDoc = Jsoup.parse(html);
        ContentFilter subsetChain = buildChain(new ScriptFilter(), new AdFilter());
        assertTrue(subsetChain.process(subsetDoc));
        assertTrue(subsetDoc.select("script, .ad-box").isEmpty());
        assertNotNull(subsetDoc.selectFirst("style"));
        assertNotNull(subsetDoc.selectFirst("iframe"));
        assertNotNull(subsetDoc.selectFirst("nav"));
    }

    @Test
    void chainEnforcesMinTextLengthThresholdAfterRemoval() {
        assertEquals(100, ContentFilter.MIN_TEXT_LENGTH);

        ContentFilter chain = buildChain(
                new ScriptFilter(),
                new StyleFilter(),
                new EmbeddedObjectFilter(),
                new AdFilter(),
                new NavigationFilter()
        );

        String insufficientHtml = """
                <html>
                  <head><script>%s</script></head>
                  <body>
                    <nav>%s</nav>
                    <div class="ad-box">%s</div>
                    <p>%s</p>
                  </body>
                </html>
                """.formatted("a".repeat(200), "b".repeat(200), "c".repeat(200), "x".repeat(99));

        assertFalse(chain.process(Jsoup.parse(insufficientHtml)));
        assertFalse(chain.process(null));

        String exactHtml = "<html><body><p>" + "x".repeat(100) + "</p></body></html>";
        assertTrue(chain.process(Jsoup.parse(exactHtml)));
    }

    private static ContentFilter buildChain(ContentFilter... filters) {
        for (int i = 0; i < filters.length - 1; i++) {
            filters[i].setNext(filters[i + 1]);
        }
        return filters[0];
    }
}
