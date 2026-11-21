package com.webcrawler.filter;

import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class ScriptFilter extends ContentFilter {

    @Override
    public boolean process(Document document) {
        if (document == null) {
            return false;
        }

        document.select("script, noscript").remove();

        List<Element> javascriptLinks = new ArrayList<>();
        for (Element element : document.select("a, area")) {
            if (element.hasAttr("href") && isJavascriptUri(element.attr("href"))) {
                javascriptLinks.add(element);
            }
        }
        for (Element link : javascriptLinks) {
            if (link.parentNode() != null) {
                link.remove();
            }
        }

        for (Element element : document.getAllElements()) {
            List<String> attributesToRemove = null;
            for (Attribute attribute : element.attributes()) {
                String key = attribute.getKey();
                String lowerKey = key.trim().toLowerCase(Locale.ROOT);
                boolean isEventHandler = lowerKey.length() > 2 && lowerKey.startsWith("on");
                boolean isJavascriptAttr = ("href".equals(lowerKey)
                        || "src".equals(lowerKey)
                        || "action".equals(lowerKey))
                        && isJavascriptUri(attribute.getValue());
                if (isEventHandler || isJavascriptAttr) {
                    if (attributesToRemove == null) {
                        attributesToRemove = new ArrayList<>();
                    }
                    attributesToRemove.add(key);
                }
            }
            if (attributesToRemove != null) {
                for (String key : attributesToRemove) {
                    element.removeAttr(key);
                }
            }
        }

        return passToNext(document);
    }

    private static boolean isJavascriptUri(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        String normalized = stripControlAndWhitespace(value).toLowerCase(Locale.ROOT);
        return normalized.startsWith("javascript:");
    }

    private static String stripControlAndWhitespace(String value) {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch > ' ') {
                sb.append(ch);
            }
        }
        return sb.toString();
    }
}
