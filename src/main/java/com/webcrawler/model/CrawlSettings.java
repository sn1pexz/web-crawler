package com.webcrawler.model;

import com.webcrawler.filter.FilterType;
import com.webcrawler.util.UrlNormalizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class CrawlSettings {

    public static final int DEFAULT_MAX_DEPTH = 2;
    public static final int DEFAULT_PAGE_LIMIT = 100;

    private String startUrl;
    private String searchTerm;
    private StrategyType strategy = StrategyType.BFS;
    private int maxDepth = DEFAULT_MAX_DEPTH;
    private int pageLimit = DEFAULT_PAGE_LIMIT;
    private boolean wholeWord = false;
    private boolean includeSubdomains = false;
    private Set<FilterType> enabledFilters = EnumSet.copyOf(FilterType.defaultEnabled());

    public CrawlSettings() {
    }

    public CrawlSettings(String startUrl, String searchTerm) {
        this.startUrl = startUrl;
        this.searchTerm = searchTerm;
    }

    public CrawlSettings(
            String startUrl,
            String searchTerm,
            StrategyType strategy,
            int maxDepth,
            int pageLimit,
            boolean wholeWord,
            boolean includeSubdomains,
            Set<FilterType> enabledFilters) {
        this.startUrl = startUrl;
        this.searchTerm = searchTerm;
        this.strategy = strategy;
        this.maxDepth = maxDepth;
        this.pageLimit = pageLimit;
        this.wholeWord = wholeWord;
        this.includeSubdomains = includeSubdomains;
        setEnabledFilters(enabledFilters);
    }

    public CrawlSettings(CrawlSettings other) {
        Objects.requireNonNull(other, "other settings must not be null");
        this.startUrl = other.startUrl;
        this.searchTerm = other.searchTerm;
        this.strategy = other.strategy;
        this.maxDepth = other.maxDepth;
        this.pageLimit = other.pageLimit;
        this.wholeWord = other.wholeWord;
        this.includeSubdomains = other.includeSubdomains;
        setEnabledFilters(other.enabledFilters);
    }

    public boolean validate() {
        return validateWithMessage().isEmpty();
    }

    public Optional<String> validateWithMessage() {
        List<String> errors = new ArrayList<>();
        if (!UrlNormalizer.isValidHttpUrl(startUrl)) {
            errors.add("Некоректна стартова адреса. Вкажіть URL у форматі http:// або https://.");
        }
        if (searchTerm == null || searchTerm.isBlank()) {
            errors.add("Поле терміну пошуку не може бути порожнім.");
        }
        if (strategy == null) {
            errors.add("Не обрано стратегію обходу (BFS або DFS).");
        }
        if (maxDepth < 0) {
            errors.add("Глибина обходу не може бути від'ємною (мінімум 0).");
        }
        if (pageLimit < 1) {
            errors.add("Ліміт сторінок має бути не менше 1.");
        }
        if (errors.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(String.join("\n", errors));
    }

    public String getStartUrl() {
        return startUrl;
    }

    public void setStartUrl(String startUrl) {
        this.startUrl = startUrl;
    }

    public String getSearchTerm() {
        return searchTerm;
    }

    public void setSearchTerm(String searchTerm) {
        this.searchTerm = searchTerm;
    }

    public StrategyType getStrategy() {
        return strategy;
    }

    public void setStrategy(StrategyType strategy) {
        this.strategy = strategy;
    }

    public int getMaxDepth() {
        return maxDepth;
    }

    public void setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
    }

    public int getPageLimit() {
        return pageLimit;
    }

    public void setPageLimit(int pageLimit) {
        this.pageLimit = pageLimit;
    }

    public boolean isWholeWord() {
        return wholeWord;
    }

    public void setWholeWord(boolean wholeWord) {
        this.wholeWord = wholeWord;
    }

    public boolean isIncludeSubdomains() {
        return includeSubdomains;
    }

    public void setIncludeSubdomains(boolean includeSubdomains) {
        this.includeSubdomains = includeSubdomains;
    }

    public Set<FilterType> getEnabledFilters() {
        if (enabledFilters == null || enabledFilters.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(EnumSet.copyOf(enabledFilters));
    }

    public void setEnabledFilters(Set<FilterType> enabledFilters) {
        if (enabledFilters == null || enabledFilters.isEmpty()) {
            this.enabledFilters = EnumSet.noneOf(FilterType.class);
        } else {
            this.enabledFilters = EnumSet.copyOf(enabledFilters);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CrawlSettings other)) {
            return false;
        }
        return maxDepth == other.maxDepth
                && pageLimit == other.pageLimit
                && wholeWord == other.wholeWord
                && includeSubdomains == other.includeSubdomains
                && Objects.equals(startUrl, other.startUrl)
                && Objects.equals(searchTerm, other.searchTerm)
                && strategy == other.strategy
                && Objects.equals(getEnabledFilters(), other.getEnabledFilters());
    }

    @Override
    public int hashCode() {
        return Objects.hash(startUrl, searchTerm, strategy, maxDepth, pageLimit,
                wholeWord, includeSubdomains, getEnabledFilters());
    }

    @Override
    public String toString() {
        return "CrawlSettings{"
                + "startUrl='" + startUrl + '\''
                + ", searchTerm='" + searchTerm + '\''
                + ", strategy=" + strategy
                + ", maxDepth=" + maxDepth
                + ", pageLimit=" + pageLimit
                + ", wholeWord=" + wholeWord
                + ", includeSubdomains=" + includeSubdomains
                + ", enabledFilters=" + enabledFilters
                + '}';
    }
}
