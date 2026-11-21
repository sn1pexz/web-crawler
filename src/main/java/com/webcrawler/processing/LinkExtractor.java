package com.webcrawler.processing;

import com.webcrawler.util.UrlNormalizer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class LinkExtractor {

    public List<String> extract(String html, String baseUrl) {
        if (html == null || html.isBlank()) {
            return List.of();
        }

        String effectiveBaseUrl = baseUrl != null ? baseUrl : "";
        Document document = Jsoup.parse(html, effectiveBaseUrl);
        Set<String> uniqueUrls = new LinkedHashSet<>();

        for (Element link : document.select("a[href]")) {
            String rawHref = link.attr("href").trim();
            if (rawHref.isEmpty() || rawHref.startsWith("#")) {
                continue;
            }
            String resolved = link.attr("abs:href");
            if (resolved.isEmpty()) {
                continue;
            }
            Optional<String> normalized = UrlNormalizer.tryNormalize(resolved);
            normalized.ifPresent(uniqueUrls::add);
        }

        return List.copyOf(uniqueUrls);
    }
}
