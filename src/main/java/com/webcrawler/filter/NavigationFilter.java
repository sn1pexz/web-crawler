package com.webcrawler.filter;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.ArrayList;
import java.util.List;

public class NavigationFilter extends ContentFilter {

    @Override
    public boolean process(Document document) {
        if (document == null) {
            return false;
        }

        List<Element> toRemove = new ArrayList<>(document.select("nav, footer, aside"));
        for (Element header : document.select("header")) {
            if (!hasArticleOrMainAncestor(header)) {
                toRemove.add(header);
            }
        }

        for (Element element : toRemove) {
            if (element.parentNode() != null) {
                element.remove();
            }
        }

        return passToNext(document);
    }

    private static boolean hasArticleOrMainAncestor(Element element) {
        Element parent = element.parent();
        while (parent != null) {
            String tag = parent.tagName();
            if ("article".equalsIgnoreCase(tag) || "main".equalsIgnoreCase(tag)) {
                return true;
            }
            parent = parent.parent();
        }
        return false;
    }
}
