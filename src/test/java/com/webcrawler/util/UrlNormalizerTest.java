package com.webcrawler.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UrlNormalizerTest {

    @Test
    void normalizesSchemeHostDefaultPortsFragmentPathAndQuery() {
        assertEquals(
                "http://example.com/",
                UrlNormalizer.normalize("HTTP://Example.COM:80#top"));
        assertEquals(
                "https://example.com/a/c?a=1&b=2",
                UrlNormalizer.normalize("HTTPS://Example.COM:443/a/b/../c/./?b=2&a=1#frag"));
        assertEquals(
                "http://example.com:8080/news",
                UrlNormalizer.normalize("http://example.com:8080/news/"));
        assertEquals(
                "https://example.com:8443/api",
                UrlNormalizer.normalize("https://example.com:8443/api?"));
        assertEquals(
                "http://example.com/search",
                UrlNormalizer.normalize("http://example.com/search?&&"));
    }

    @Test
    void enforcesHostAndSubdomainPolicy() {
        assertTrue(UrlNormalizer.isHostAllowed("https://example.com/news", "example.com", false));
        assertFalse(UrlNormalizer.isHostAllowed("https://blog.example.com/news", "example.com", false));
        assertTrue(UrlNormalizer.isHostAllowed("https://blog.example.com/news", "example.com", true));
        assertTrue(UrlNormalizer.isHostAllowed("https://a.b.example.com/news", "Example.COM", true));
        assertFalse(UrlNormalizer.isHostAllowed("https://badexample.com/news", "example.com", true));
        assertFalse(UrlNormalizer.isHostAllowed("javascript:alert(1)", "example.com", true));
    }
}
