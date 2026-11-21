package com.webcrawler.filter;

import org.jsoup.nodes.Document;

public class EmbeddedObjectFilter extends ContentFilter {

    private static final String EMBEDDED_SELECTOR = "iframe, embed, object, video, audio, canvas, svg";

    @Override
    public boolean process(Document document) {
        if (document == null) {
            return false;
        }

        document.select(EMBEDDED_SELECTOR).remove();
        return passToNext(document);
    }
}
