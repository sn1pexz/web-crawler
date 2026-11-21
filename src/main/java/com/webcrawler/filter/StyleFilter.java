package com.webcrawler.filter;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class StyleFilter extends ContentFilter {

    @Override
    public boolean process(Document document) {
        if (document == null) {
            return false;
        }

        document.select("style").remove();

        List<Element> stylesheetLinks = new ArrayList<>();
        for (Element link : document.select("link")) {
            if (isStylesheetLink(link)) {
                stylesheetLinks.add(link);
            }
        }
        for (Element link : stylesheetLinks) {
            if (link.parentNode() != null) {
                link.remove();
            }
        }

        for (Element element : document.getAllElements()) {
            if (element.hasAttr("style")) {
                element.removeAttr("style");
            }
        }

        return passToNext(document);
    }

    private static boolean isStylesheetLink(Element link) {
        String rel = link.attr("rel");
        if (!rel.isEmpty()) {
            for (String token : rel.trim().split("\\s+")) {
                if ("stylesheet".equalsIgnoreCase(token)) {
                    return true;
                }
            }
        }
        String type = link.attr("type").trim().toLowerCase(Locale.ROOT);
        return "text/css".equals(type) || type.startsWith("text/css;");
    }
}
