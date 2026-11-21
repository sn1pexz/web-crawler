package com.webcrawler.filter;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

public class AdFilter extends ContentFilter {

    private static final Set<String> AD_TOKENS = Set.of(
            "ad",
            "ads",
            "advert",
            "advertisement",
            "advertisements",
            "advertising",
            "sponsor",
            "sponsored",
            "adsense",
            "doubleclick",
            "outbrain",
            "taboola",
            "adbox",
            "adunit",
            "reklama",
            "реклама"
    );

    private static final Pattern NON_ALPHANUMERIC = Pattern.compile("[^\\p{L}\\p{N}]+");
    private static final Pattern CAMEL_CASE_BOUNDARY = Pattern.compile(
            "(?<=[\\p{Ll}\\p{N}])(?=\\p{Lu})|(?<=\\p{Lu})(?=\\p{Lu}\\p{Ll})"
    );

    @Override
    public boolean process(Document document) {
        if (document == null) {
            return false;
        }

        List<Element> toRemove = new ArrayList<>();
        for (Element element : document.getAllElements()) {
            String tagName = element.tagName();
            if (element == document
                    || "html".equalsIgnoreCase(tagName)
                    || "body".equalsIgnoreCase(tagName)) {
                continue;
            }
            if (hasAdToken(element.id()) || hasAdToken(element.className())) {
                toRemove.add(element);
            }
        }

        for (Element element : toRemove) {
            if (element.parentNode() != null) {
                element.remove();
            }
        }

        return passToNext(document);
    }

    private static boolean hasAdToken(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return false;
        }
        for (String chunk : NON_ALPHANUMERIC.split(rawValue)) {
            if (chunk.isEmpty()) {
                continue;
            }
            for (String token : CAMEL_CASE_BOUNDARY.split(chunk)) {
                if (!token.isEmpty() && AD_TOKENS.contains(token.toLowerCase(Locale.ROOT))) {
                    return true;
                }
            }
        }
        return false;
    }
}
