package com.webcrawler.loader;

import com.webcrawler.util.UrlNormalizer;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class RobotsTxtChecker {

    private static final Logger LOGGER = Logger.getLogger(RobotsTxtChecker.class.getName());
    private static final String BOT_TOKEN = "webcrawlercoursework";
    private static final String BOT_TOKEN_FULL = HttpPageLoader.USER_AGENT.toLowerCase(Locale.ROOT);
    private static final RobotsRules ALLOW_ALL_RULES = new RobotsRules(List.of(), 0L);

    private final PageLoader pageLoader;
    private final ConcurrentMap<String, RobotsRules> rulesCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Object> originLocks = new ConcurrentHashMap<>();

    public RobotsTxtChecker(PageLoader pageLoader) {
        this.pageLoader = Objects.requireNonNull(pageLoader, "pageLoader must not be null");
    }

    public boolean isAllowed(String url) {
        if (!UrlNormalizer.isValidHttpUrl(url)) {
            return true;
        }
        String origin = UrlNormalizer.extractOrigin(url);
        RobotsRules rules = getOrLoadRules(origin);
        String pathAndQuery = extractPathAndQuery(url);
        return rules.isPathAllowed(pathAndQuery);
    }

    public long getCrawlDelayMs(String url) {
        if (!UrlNormalizer.isValidHttpUrl(url)) {
            return 0L;
        }
        String origin = UrlNormalizer.extractOrigin(url);
        return getOrLoadRules(origin).crawlDelayMs();
    }

    public void clearMemoryCache() {
        rulesCache.clear();
    }

    private RobotsRules getOrLoadRules(String origin) {
        RobotsRules cached = rulesCache.get(origin);
        if (cached != null) {
            return cached;
        }
        Object lock = originLocks.computeIfAbsent(origin, ignored -> new Object());
        synchronized (lock) {
            cached = rulesCache.get(origin);
            if (cached != null) {
                return cached;
            }
            RobotsRules loaded = fetchAndParse(origin);
            if (!Thread.currentThread().isInterrupted()) {
                rulesCache.put(origin, loaded);
            }
            return loaded;
        }
    }

    private RobotsRules fetchAndParse(String origin) {
        try {
            LoadResult result = pageLoader.loadRobotsTxt(origin);
            if (result == null || result.html() == null || result.html().isBlank()
                    || result.httpStatus() == 404 || result.httpStatus() == 410) {
                return ALLOW_ALL_RULES;
            }
            return parse(result.html());
        } catch (PageLoadException ex) {
            LOGGER.log(Level.FINE, "robots.txt unavailable for {0}: {1}",
                    new Object[]{origin, ex.getMessage()});
            return ALLOW_ALL_RULES;
        } catch (RuntimeException ex) {
            LOGGER.log(Level.WARNING, "Failed to parse robots.txt for " + origin, ex);
            return ALLOW_ALL_RULES;
        }
    }

    static RobotsRules parse(String content) {
        if (content == null || content.isBlank()) {
            return ALLOW_ALL_RULES;
        }
        String normalizedContent = content.startsWith("\uFEFF") ? content.substring(1) : content;
        String[] lines = normalizedContent.split("\\R");

        List<AgentGroup> groups = new ArrayList<>();
        AgentGroup currentGroup = null;

        for (String rawLine : lines) {
            int commentIdx = rawLine.indexOf('#');
            String line = (commentIdx >= 0 ? rawLine.substring(0, commentIdx) : rawLine).trim();
            if (line.isEmpty()) {
                continue;
            }
            int colonIdx = line.indexOf(':');
            if (colonIdx <= 0) {
                continue;
            }
            String directive = line.substring(0, colonIdx).trim().toLowerCase(Locale.ROOT);
            String value = line.substring(colonIdx + 1).trim();

            switch (directive) {
                case "user-agent" -> {
                    if (value.isEmpty()) {
                        continue;
                    }
                    if (currentGroup == null || currentGroup.hasRules) {
                        currentGroup = new AgentGroup();
                        groups.add(currentGroup);
                    }
                    currentGroup.userAgents.add(value.toLowerCase(Locale.ROOT));
                }
                case "allow" -> {
                    if (currentGroup != null && isValidPathPattern(value)) {
                        currentGroup.hasRules = true;
                        currentGroup.rules.add(PathRule.of(true, value));
                    }
                }
                case "disallow" -> {
                    if (currentGroup != null) {
                        currentGroup.hasRules = true;
                        if (isValidPathPattern(value)) {
                            currentGroup.rules.add(PathRule.of(false, value));
                        }
                    }
                }
                case "crawl-delay" -> {
                    if (currentGroup != null) {
                        currentGroup.hasRules = true;
                        long parsedDelayMs = parseCrawlDelayMs(value);
                        if (parsedDelayMs > currentGroup.crawlDelayMs) {
                            currentGroup.crawlDelayMs = parsedDelayMs;
                        }
                    }
                }
                default -> {
                }
            }
        }

        List<AgentGroup> specificGroups = new ArrayList<>();
        List<AgentGroup> wildcardGroups = new ArrayList<>();

        for (AgentGroup group : groups) {
            if (group.matchesSpecificBot()) {
                specificGroups.add(group);
            } else if (group.matchesWildcard()) {
                wildcardGroups.add(group);
            }
        }

        List<AgentGroup> effectiveGroups = !specificGroups.isEmpty() ? specificGroups : wildcardGroups;
        if (effectiveGroups.isEmpty()) {
            return ALLOW_ALL_RULES;
        }

        List<PathRule> mergedRules = new ArrayList<>();
        long maxCrawlDelayMs = 0L;
        for (AgentGroup group : effectiveGroups) {
            mergedRules.addAll(group.rules);
            if (group.crawlDelayMs > maxCrawlDelayMs) {
                maxCrawlDelayMs = group.crawlDelayMs;
            }
        }
        return new RobotsRules(List.copyOf(mergedRules), maxCrawlDelayMs);
    }

    private static boolean isValidPathPattern(String pattern) {
        return !pattern.isEmpty() && (pattern.startsWith("/") || pattern.startsWith("*"));
    }

    private static long parseCrawlDelayMs(String value) {
        if (value.isEmpty()) {
            return 0L;
        }
        try {
            double seconds = Double.parseDouble(value);
            if (Double.isNaN(seconds) || Double.isInfinite(seconds) || seconds <= 0.0) {
                return 0L;
            }
            return Math.round(seconds * 1000.0);
        } catch (NumberFormatException ignored) {
            return 0L;
        }
    }

    private static String extractPathAndQuery(String url) {
        try {
            URI uri = new URI(url.trim());
            String path = uri.getRawPath();
            if (path == null || path.isEmpty()) {
                path = "/";
            }
            String query = uri.getRawQuery();
            return query != null ? path + "?" + query : path;
        } catch (URISyntaxException ex) {
            return "/";
        }
    }

    private static final class AgentGroup {
        private final List<String> userAgents = new ArrayList<>();
        private final List<PathRule> rules = new ArrayList<>();
        private boolean hasRules;
        private long crawlDelayMs;

        boolean matchesSpecificBot() {
            for (String agent : userAgents) {
                if (agent.equals(BOT_TOKEN) || agent.equals(BOT_TOKEN_FULL)) {
                    return true;
                }
            }
            return false;
        }

        boolean matchesWildcard() {
            for (String agent : userAgents) {
                if ("*".equals(agent)) {
                    return true;
                }
            }
            return false;
        }
    }

    record RobotsRules(List<PathRule> rules, long crawlDelayMs) {

        boolean isPathAllowed(String pathAndQuery) {
            PathRule bestMatch = null;
            int bestLength = -1;

            for (PathRule rule : rules) {
                if (!rule.matches(pathAndQuery)) {
                    continue;
                }
                int length = rule.patternLength();
                if (length > bestLength || (length == bestLength && rule.allow() && bestMatch != null && !bestMatch.allow())) {
                    bestLength = length;
                    bestMatch = rule;
                }
            }
            return bestMatch == null || bestMatch.allow();
        }
    }

    record PathRule(boolean allow, String pattern, Pattern compiledWildcard) {

        static PathRule of(boolean allow, String pattern) {
            if (pattern.indexOf('*') >= 0 || pattern.endsWith("$")) {
                return new PathRule(allow, pattern, compileWildcardPattern(pattern));
            }
            return new PathRule(allow, pattern, null);
        }

        int patternLength() {
            return pattern.length();
        }

        boolean matches(String pathAndQuery) {
            if (compiledWildcard == null) {
                return pathAndQuery.startsWith(pattern);
            }
            return compiledWildcard.matcher(pathAndQuery).find();
        }

        private static Pattern compileWildcardPattern(String pattern) {
            StringBuilder regex = new StringBuilder("^");
            boolean anchorEnd = pattern.endsWith("$");
            int limit = anchorEnd ? pattern.length() - 1 : pattern.length();

            StringBuilder literal = new StringBuilder();
            for (int i = 0; i < limit; i++) {
                char ch = pattern.charAt(i);
                if (ch == '*') {
                    if (!literal.isEmpty()) {
                        regex.append(Pattern.quote(literal.toString()));
                        literal.setLength(0);
                    }
                    regex.append(".*");
                } else {
                    literal.append(ch);
                }
            }
            if (!literal.isEmpty()) {
                regex.append(Pattern.quote(literal.toString()));
            }
            if (anchorEnd) {
                regex.append('$');
            }
            return Pattern.compile(regex.toString());
        }
    }
}
