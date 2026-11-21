package com.webcrawler.core;

import com.webcrawler.loader.DomainRateLimiter;
import com.webcrawler.loader.PageLoader;
import com.webcrawler.loader.RobotsTxtChecker;
import com.webcrawler.model.QueueEntry;
import com.webcrawler.persistence.PageRepository;
import com.webcrawler.processing.LinkExtractor;
import com.webcrawler.processing.TermSearcher;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DepthFirstCrawl extends CrawlStrategy {

    private final ArrayDeque<QueueEntry> deque;

    public DepthFirstCrawl() {
        super();
        this.deque = new ArrayDeque<>();
        this.queue = this.deque;
    }

    public DepthFirstCrawl(Collection<QueueEntry> queue) {
        this();
        if (queue != null && !queue.isEmpty()) {
            List<QueueEntry> initial = new ArrayList<>(queue.size());
            for (QueueEntry entry : queue) {
                if (entry != null) {
                    initial.add(entry);
                }
            }
            restoreQueue(initial);
        }
    }

    public DepthFirstCrawl(
            PageLoader pageLoader,
            LinkExtractor linkExtractor,
            TermSearcher termSearcher,
            RobotsTxtChecker robotsTxtChecker,
            DomainRateLimiter rateLimiter,
            PageRepository pageRepository,
            Path resultsRoot,
            CrawlEventListener eventListener) {
        this(
                pageLoader,
                linkExtractor,
                termSearcher,
                robotsTxtChecker,
                rateLimiter,
                pageRepository,
                resultsRoot,
                eventListener,
                DEFAULT_THREAD_POOL_SIZE
        );
    }

    public DepthFirstCrawl(
            PageLoader pageLoader,
            LinkExtractor linkExtractor,
            TermSearcher termSearcher,
            RobotsTxtChecker robotsTxtChecker,
            DomainRateLimiter rateLimiter,
            PageRepository pageRepository,
            Path resultsRoot,
            CrawlEventListener eventListener,
            int threadPoolSize) {
        super(
                pageLoader,
                linkExtractor,
                termSearcher,
                robotsTxtChecker,
                rateLimiter,
                pageRepository,
                resultsRoot,
                eventListener,
                threadPoolSize
        );
        this.deque = new ArrayDeque<>();
        this.queue = this.deque;
    }

    @Override
    protected synchronized QueueEntry nextEntry() {
        return deque.pollFirst();
    }

    //LIFO stack order
    @Override
    protected synchronized void addEntry(QueueEntry entry) {
        if (entry != null) {
            deque.offerFirst(entry);
        }
    }

    @Override
    protected synchronized void addDiscoveredEntries(List<QueueEntry> entries) {
        pushInReverseOrder(entries);
    }

    @Override
    public synchronized void restoreQueue(List<QueueEntry> entries) {
        deque.clear();
        pushInReverseOrder(entries);
    }

    //Push in reverse so first link or snapshot top pops first
    private void pushInReverseOrder(List<QueueEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }
        for (int i = entries.size() - 1; i >= 0; i--) {
            QueueEntry entry = entries.get(i);
            if (entry != null) {
                addEntry(entry);
            }
        }
    }
}
