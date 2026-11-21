package com.webcrawler.core;

import com.webcrawler.filter.AdFilter;
import com.webcrawler.filter.ContentFilter;
import com.webcrawler.filter.EmbeddedObjectFilter;
import com.webcrawler.filter.FilterType;
import com.webcrawler.filter.NavigationFilter;
import com.webcrawler.filter.ScriptFilter;
import com.webcrawler.filter.StyleFilter;
import com.webcrawler.loader.DomainRateLimiter;
import com.webcrawler.loader.LoadResult;
import com.webcrawler.loader.PageLoadException;
import com.webcrawler.loader.PageLoader;
import com.webcrawler.loader.RobotsTxtChecker;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.Page;
import com.webcrawler.model.QueueEntry;
import com.webcrawler.model.SessionStatus;
import com.webcrawler.model.Site;
import com.webcrawler.persistence.PageRepository;
import com.webcrawler.processing.LinkExtractor;
import com.webcrawler.processing.TermSearcher;
import com.webcrawler.storage.PageElement;
import com.webcrawler.storage.PathSanitizer;
import com.webcrawler.util.UrlNormalizer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

//Template Method base class defining shared crawl loop
public abstract class CrawlStrategy {

    public static final int DEFAULT_THREAD_POOL_SIZE = 4;

    private static final Logger LOGGER = Logger.getLogger(CrawlStrategy.class.getName());
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 30L;

    protected Collection<QueueEntry> queue;
    protected final AtomicBoolean pauseRequested = new AtomicBoolean(false);
    protected final AtomicBoolean stopRequested = new AtomicBoolean(false);

    private final PageLoader pageLoader;
    private final LinkExtractor linkExtractor;
    private final TermSearcher termSearcher;
    private final RobotsTxtChecker robotsTxtChecker;
    private final DomainRateLimiter rateLimiter;
    private final PageRepository pageRepository;
    private final Path resultsRoot;
    private final CrawlEventListener eventListener;
    private final int threadPoolSize;

    private final Set<String> initialVisitedUrls = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, Site> sites = new ConcurrentHashMap<>();
    private final ReentrantLock coordinationLock = new ReentrantLock();
    private final Condition coordinationCondition = coordinationLock.newCondition();

    public record BatchResult(
            List<Page> pages,
            List<QueueEntry> foundLinks,
            List<QueueEntry> unprocessedEntries
    ) {
        public BatchResult {
            pages = pages == null ? List.of() : List.copyOf(pages);
            foundLinks = foundLinks == null ? List.of() : List.copyOf(foundLinks);
            unprocessedEntries = unprocessedEntries == null ? List.of() : List.copyOf(unprocessedEntries);
        }
    }

    protected CrawlStrategy() {
        this.pageLoader = null;
        this.linkExtractor = new LinkExtractor();
        this.termSearcher = new TermSearcher();
        this.robotsTxtChecker = null;
        this.rateLimiter = new DomainRateLimiter();
        this.pageRepository = null;
        this.resultsRoot = null;
        this.eventListener = CrawlEventListener.NO_OP;
        this.threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    }

    protected CrawlStrategy(Collection<QueueEntry> queue) {
        this();
        this.queue = queue;
    }

    protected CrawlStrategy(
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

    protected CrawlStrategy(
            PageLoader pageLoader,
            LinkExtractor linkExtractor,
            TermSearcher termSearcher,
            RobotsTxtChecker robotsTxtChecker,
            DomainRateLimiter rateLimiter,
            PageRepository pageRepository,
            Path resultsRoot,
            CrawlEventListener eventListener,
            int threadPoolSize) {
        if (threadPoolSize < 1) {
            throw new IllegalArgumentException("threadPoolSize must be >= 1, got " + threadPoolSize);
        }
        this.pageLoader = Objects.requireNonNull(pageLoader, "pageLoader must not be null");
        this.linkExtractor = linkExtractor != null ? linkExtractor : new LinkExtractor();
        this.termSearcher = termSearcher != null ? termSearcher : new TermSearcher();
        this.robotsTxtChecker = robotsTxtChecker != null ? robotsTxtChecker : new RobotsTxtChecker(pageLoader);
        this.rateLimiter = rateLimiter != null ? rateLimiter : new DomainRateLimiter();
        this.pageRepository = pageRepository;
        this.resultsRoot = resultsRoot != null ? resultsRoot.toAbsolutePath().normalize() : null;
        this.eventListener = eventListener != null ? eventListener : CrawlEventListener.NO_OP;
        this.threadPoolSize = threadPoolSize;
    }

    public void setInitialVisitedUrls(Collection<String> visitedUrls) {
        initialVisitedUrls.clear();
        if (visitedUrls != null) {
            for (String url : visitedUrls) {
                if (url != null && !url.isBlank()) {
                    initialVisitedUrls.add(normalizeUrlOrFallback(url));
                }
            }
        }
    }

    public synchronized Map<String, Site> getSitesSnapshot() {
        return Map.copyOf(sites);
    }

    public void crawl(CrawlSession session) {
        Objects.requireNonNull(session, "session must not be null");
        ensurePageLoaderConfigured();

        CrawlSettings settings = Objects.requireNonNull(session.getSettings(), "session.settings must not be null");
        Optional<String> validationError = settings.validateWithMessage();
        if (validationError.isPresent()) {
            throw new IllegalArgumentException(validationError.get());
        }

        pauseRequested.set(false);
        stopRequested.set(false);
        sites.clear();
        session.setStrategy(this);

        SessionStatus currentStatus = session.getStatus();
        if (currentStatus == SessionStatus.IDLE || currentStatus == SessionStatus.PAUSED) {
            session.start();
        } else if (currentStatus != SessionStatus.RUNNING) {
            throw new IllegalStateException("Cannot crawl session in status " + currentStatus);
        }

        Set<String> seenUrls = initializeSeenUrlsAndSeedQueue(settings);
        String startHost = UrlNormalizer.extractHost(settings.getStartUrl());
        int pageLimit = settings.getPageLimit();
        CrawlCoordinator coordinator = new CrawlCoordinator(Math.max(0, session.getVisitedCount()));

        ExecutorService executor = createWorkerPool(threadPoolSize, "crawler-worker-");
        boolean interrupted = false;
        try {
            while (!pauseRequested.get() && !stopRequested.get()) {
                QueueEntry entryToDispatch = null;
                coordinationLock.lock();
                try {
                    if (coordinator.inFlight < threadPoolSize && coordinator.reservedCount < pageLimit) {
                        entryToDispatch = pollNextEntryIfAvailable();
                        if (entryToDispatch != null) {
                            coordinator.reservedCount++;
                            coordinator.inFlight++;
                        }
                    }
                    if (entryToDispatch == null) {
                        if (coordinator.inFlight > 0) {
                            try {
                                coordinationCondition.await();
                            } catch (InterruptedException ex) {
                                Thread.currentThread().interrupt();
                                interrupted = true;
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                } finally {
                    coordinationLock.unlock();
                }

                if (entryToDispatch != null) {
                    final QueueEntry entry = entryToDispatch;
                    executor.execute(() -> executeLocalWorker(
                            entry, session, settings, startHost, seenUrls, coordinator));
                }
            }
        } finally {
            shutdownExecutorGracefully(executor, interrupted);
        }

        if (stopRequested.get() || interrupted) {
            clearQueue();
            return;
        }

        if (!pauseRequested.get()) {
            String reason = session.getVisitedCount() >= pageLimit
                    ? "Досягнуто ліміту сторінок"
                    : "Обхід завершено (чергу вичерпано)";
            if (session.getStatus() == SessionStatus.RUNNING) {
                session.finish(reason);
            }
            eventListener.onSessionFinished(session);
        }
    }

    public BatchResult processBatch(
            List<QueueEntry> batch,
            CrawlSettings settings,
            Path resultsRoot,
            BooleanSupplier stopOrPauseCheck) {
        ensurePageLoaderConfigured();
        Objects.requireNonNull(settings, "settings must not be null");
        Optional<String> validationError = settings.validateWithMessage();
        if (validationError.isPresent()) {
            throw new IllegalArgumentException(validationError.get());
        }
        if (batch == null || batch.isEmpty()) {
            return new BatchResult(List.of(), List.of(), List.of());
        }

        BooleanSupplier abortCheck = stopOrPauseCheck != null ? stopOrPauseCheck : () -> false;
        Path effectiveResultsRoot = resultsRoot != null ? resultsRoot.toAbsolutePath().normalize() : this.resultsRoot;
        String startHost = UrlNormalizer.extractHost(settings.getStartUrl());

        List<Page> pages = new ArrayList<>();
        Set<String> deduplicatedLinks = new LinkedHashSet<>();
        List<QueueEntry> foundLinks = new ArrayList<>();
        List<QueueEntry> unprocessedEntries = new ArrayList<>();
        Object collectorLock = new Object();

        AtomicInteger nextIndex = new AtomicInteger(0);
        int workerCount = Math.min(threadPoolSize, batch.size());
        ExecutorService executor = createWorkerPool(workerCount, "crawler-batch-worker-");
        List<Future<?>> futures = new ArrayList<>(workerCount);
        boolean interrupted = false;

        try {
            for (int w = 0; w < workerCount; w++) {
                futures.add(executor.submit(() -> {
                    while (true) {
                        if (abortCheck.getAsBoolean()) {
                            return;
                        }
                        int index = nextIndex.getAndIncrement();
                        if (index >= batch.size()) {
                            return;
                        }
                        QueueEntry entry = batch.get(index);
                        if (entry == null) {
                            continue;
                        }
                        if (abortCheck.getAsBoolean()) {
                            synchronized (collectorLock) {
                                unprocessedEntries.add(entry);
                            }
                            return;
                        }

                        EntryOutcome outcome = processSingleEntry(
                                entry,
                                settings,
                                effectiveResultsRoot,
                                startHost,
                                0L,
                                false,
                                abortCheck
                        );

                        switch (outcome.status()) {
                            case ABORTED_BEFORE_FETCH -> {
                                synchronized (collectorLock) {
                                    unprocessedEntries.add(entry);
                                }
                                return;
                            }
                            case ROBOTS_DISALLOWED -> {
                            }
                            case ERROR -> {
                                synchronized (collectorLock) {
                                    pages.add(outcome.page());
                                }
                                eventListener.onError(entry.getUrl(), outcome.page().getError());
                                eventListener.onPageProcessed(outcome.page());
                            }
                            case PROCESSED -> {
                                synchronized (collectorLock) {
                                    pages.add(outcome.page());
                                    for (QueueEntry discovered : outcome.discoveredEntries()) {
                                        if (deduplicatedLinks.add(discovered.getUrl())) {
                                            foundLinks.add(discovered);
                                        }
                                    }
                                }
                                eventListener.onPageProcessed(outcome.page());
                            }
                        }
                    }
                }));
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    interrupted = true;
                    break;
                } catch (ExecutionException ex) {
                    throw new IllegalStateException("Batch worker failed unexpectedly", ex.getCause());
                }
            }
        } finally {
            shutdownExecutorGracefully(executor, interrupted);
        }

        int unstartedFrom = nextIndex.get();
        if (unstartedFrom < batch.size()) {
            synchronized (collectorLock) {
                for (int i = Math.max(0, unstartedFrom); i < batch.size(); i++) {
                    QueueEntry remaining = batch.get(i);
                    if (remaining != null) {
                        unprocessedEntries.add(remaining);
                    }
                }
            }
        }

        return new BatchResult(pages, foundLinks, unprocessedEntries);
    }

    protected abstract QueueEntry nextEntry();

    protected abstract void addEntry(QueueEntry entry);

    protected synchronized void addDiscoveredEntries(List<QueueEntry> entries) {
        if (entries != null) {
            for (QueueEntry entry : entries) {
                if (entry != null) {
                    addEntry(entry);
                }
            }
        }
    }

    public synchronized List<QueueEntry> getQueueSnapshot() {
        if (queue == null || queue.isEmpty()) {
            return List.of();
        }
        return List.copyOf(queue);
    }

    public synchronized void restoreQueue(List<QueueEntry> entries) {
        if (queue != null) {
            queue.clear();
        }
        if (entries != null) {
            for (QueueEntry entry : entries) {
                if (entry != null) {
                    addEntry(entry);
                }
            }
        }
    }

    public void requestPause() {
        pauseRequested.set(true);
        signalCoordinator();
    }

    public void requestStop() {
        stopRequested.set(true);
        signalCoordinator();
    }

    public boolean isPauseRequested() {
        return pauseRequested.get();
    }

    public boolean isStopRequested() {
        return stopRequested.get();
    }

    private void executeLocalWorker(
            QueueEntry entry,
            CrawlSession session,
            CrawlSettings settings,
            String startHost,
            Set<String> seenUrls,
            CrawlCoordinator coordinator) {
        boolean releaseReservation = false;
        try {
            if (pauseRequested.get() || stopRequested.get()) {
                returnPendingOnPause(entry);
                releaseReservation = true;
                return;
            }

            EntryOutcome outcome = processSingleEntry(
                    entry,
                    settings,
                    resultsRoot,
                    startHost,
                    session.getId(),
                    true,
                    () -> pauseRequested.get() || stopRequested.get()
            );

            switch (outcome.status()) {
                case ABORTED_BEFORE_FETCH -> {
                    returnPendingOnPause(entry);
                    releaseReservation = true;
                }
                case ROBOTS_DISALLOWED -> releaseReservation = true;
                case ERROR -> {
                    Page errorPage = outcome.page();
                    session.incrementVisited();
                    session.incrementError();
                    session.addPage(errorPage);
                    eventListener.onError(entry.getUrl(), errorPage.getError());
                    eventListener.onPageProcessed(errorPage);
                    eventListener.onProgressUpdated(session);
                }
                case PROCESSED -> {
                    List<QueueEntry> toEnqueue = new ArrayList<>();
                    for (QueueEntry candidate : outcome.discoveredEntries()) {
                        if (seenUrls.add(candidate.getUrl())) {
                            toEnqueue.add(candidate);
                        }
                    }
                    if (!toEnqueue.isEmpty()) {
                        addDiscoveredEntries(toEnqueue);
                    }

                    Page processedPage = outcome.page();
                    if (processedPage.isHasTerm()) {
                        session.incrementMatched();
                    }
                    session.incrementVisited();
                    session.addPage(processedPage);
                    eventListener.onPageProcessed(processedPage);
                    eventListener.onProgressUpdated(session);
                }
            }
        } catch (RuntimeException ex) {
            LOGGER.log(Level.WARNING, "Unexpected worker error for URL " + entry.getUrl(), ex);
            releaseReservation = true;
            eventListener.onError(entry.getUrl(), ex.getMessage());
        } finally {
            coordinationLock.lock();
            try {
                if (releaseReservation && coordinator.reservedCount > 0) {
                    coordinator.reservedCount--;
                }
                coordinator.inFlight--;
                coordinationCondition.signalAll();
            } finally {
                coordinationLock.unlock();
            }
        }
    }

    private EntryOutcome processSingleEntry(
            QueueEntry entry,
            CrawlSettings settings,
            Path targetResultsRoot,
            String startHost,
            long sessionId,
            boolean saveToRepo,
            BooleanSupplier preFetchAbortCheck) {
        String url = entry.getUrl();

        if (robotsTxtChecker != null && !robotsTxtChecker.isAllowed(url)) {
            eventListener.onStatusMessage("Пропущено згідно з robots.txt: " + url);
            return EntryOutcome.robotsDisallowed();
        }

        long crawlDelayMs = robotsTxtChecker != null ? robotsTxtChecker.getCrawlDelayMs(url) : 0L;
        if (rateLimiter != null) {
            rateLimiter.acquire(url, crawlDelayMs);
        }

        if (preFetchAbortCheck != null && preFetchAbortCheck.getAsBoolean()) {
            return EntryOutcome.abortedBeforeFetch();
        }

        LoadResult loadResult;
        try {
            loadResult = pageLoader.loadWithMetadata(url);
        } catch (PageLoadException ex) {
            Page errorPage = createErrorPage(entry, sessionId, ex);
            persistPageIfNeeded(errorPage, saveToRepo);
            return EntryOutcome.error(errorPage);
        }

        String rawHtml = loadResult.html() != null ? loadResult.html() : "";
        String baseUrl = (loadResult.finalUrl() != null && !loadResult.finalUrl().isBlank())
                ? loadResult.finalUrl()
                : url;

        List<QueueEntry> discoveredEntries = extractAllowedLinks(
                rawHtml, baseUrl, entry.getDepth(), settings, startHost);

        Document doc = Jsoup.parse(rawHtml, baseUrl);
        doc.outputSettings().charset(StandardCharsets.UTF_8);

        ContentFilter filterChain = buildFilterChain(settings.getEnabledFilters());
        boolean sufficientText = filterChain != null
                ? filterChain.process(doc)
                : ContentFilter.hasSufficientText(doc);

        Page page = new Page(url, entry.getDepth());
        if (sessionId > 0) {
            page.setSessionId(sessionId);
        }
        page.setTitle(doc.title());
        page.setSize(loadResult.contentLength() > 0
                ? loadResult.contentLength()
                : rawHtml.getBytes(StandardCharsets.UTF_8).length);
        page.setHttpStatus(loadResult.httpStatus());
        page.setDownloadedAt(loadResult.downloadedAt() != null
                ? loadResult.downloadedAt()
                : LocalDateTime.now());

        if (!sufficientText) {
            page.setHasTerm(false);
            page.setFilePath(null);
            persistPageIfNeeded(page, saveToRepo);
            return EntryOutcome.processed(page, discoveredEntries);
        }

        String visibleText = doc.body() != null ? doc.body().text() : doc.text();
        boolean hasTerm = termSearcher.search(visibleText, settings.getSearchTerm(), settings.isWholeWord());
        page.setHasTerm(hasTerm);

        if (hasTerm) {
            absolutizeDocumentUrls(doc);
            String cleanedHtml = doc.outerHtml();
            PathSanitizer.SanitizedPath sanitized = PathSanitizer.sanitize(url);
            Site site = sites.computeIfAbsent(sanitized.hostSegment(), Site::new);
            PageElement pageElement = site.registerMatchedPage(
                    page, cleanedHtml, sanitized.dirSegments(), sanitized.fileName());

            if (targetResultsRoot != null) {
                Path parentDir = site.resolveParentPath(targetResultsRoot, sanitized.dirSegments());
                try {
                    pageElement.write(parentDir.toString());
                    Path savedFile = parentDir.resolve(sanitized.fileName()).toAbsolutePath().normalize();
                    page.setFilePath(savedFile.toString());
                } catch (IOException ex) {
                    LOGGER.log(Level.WARNING, "Failed to write HTML file for " + url, ex);
                    page.setError("Помилка запису файлу: " + ex.getMessage());
                    eventListener.onError(url, page.getError());
                }
            } else {
                page.setFilePath(sanitized.relativePath());
            }
        }

        persistPageIfNeeded(page, saveToRepo);
        return EntryOutcome.processed(page, discoveredEntries);
    }

    private List<QueueEntry> extractAllowedLinks(
            String rawHtml,
            String baseUrl,
            int currentDepth,
            CrawlSettings settings,
            String startHost) {
        if (currentDepth >= settings.getMaxDepth()) {
            return List.of();
        }
        int nextDepth = currentDepth + 1;
        List<String> extractedLinks = linkExtractor.extract(rawHtml, baseUrl);
        if (extractedLinks.isEmpty()) {
            return List.of();
        }
        List<QueueEntry> result = new ArrayList<>();
        for (String link : extractedLinks) {
            Optional<String> normalizedOpt = UrlNormalizer.tryNormalize(link);
            if (normalizedOpt.isEmpty()) {
                continue;
            }
            String normalizedLink = normalizedOpt.get();
            if (UrlNormalizer.isHostAllowed(normalizedLink, startHost, settings.isIncludeSubdomains())) {
                result.add(new QueueEntry(normalizedLink, nextDepth));
            }
        }
        return result;
    }

    private void persistPageIfNeeded(Page page, boolean saveToRepo) {
        if (!saveToRepo || pageRepository == null) {
            return;
        }
        String host = UrlNormalizer.isValidHttpUrl(page.getUrl())
                ? UrlNormalizer.extractHost(page.getUrl())
                : "unknown";
        pageRepository.save(page, host);
    }

    private static Page createErrorPage(QueueEntry entry, long sessionId, PageLoadException ex) {
        Page page = new Page(entry.getUrl(), entry.getDepth());
        if (sessionId > 0) {
            page.setSessionId(sessionId);
        }
        page.setDownloadedAt(LocalDateTime.now());
        page.setHttpStatus(ex.getHttpStatus());
        page.setError(ex.getMessage());
        page.setHasTerm(false);
        page.setSize(0L);
        return page;
    }

    private Set<String> initializeSeenUrlsAndSeedQueue(CrawlSettings settings) {
        Set<String> seenUrls = ConcurrentHashMap.newKeySet();
        seenUrls.addAll(initialVisitedUrls);

        List<QueueEntry> existingQueue = getQueueSnapshot();
        for (QueueEntry entry : existingQueue) {
            if (entry != null && entry.getUrl() != null && !entry.getUrl().isBlank()) {
                seenUrls.add(normalizeUrlOrFallback(entry.getUrl()));
            }
        }

        if (existingQueue.isEmpty() && initialVisitedUrls.isEmpty()) {
            String normalizedStart = UrlNormalizer.normalize(settings.getStartUrl());
            seenUrls.add(normalizedStart);
            addEntry(new QueueEntry(normalizedStart, 0));
        }
        return seenUrls;
    }

    private synchronized QueueEntry pollNextEntryIfAvailable() {
        if (queue == null || queue.isEmpty()) {
            return null;
        }
        return nextEntry();
    }

    private synchronized void clearQueue() {
        if (queue != null) {
            queue.clear();
        }
    }

    private void returnPendingOnPause(QueueEntry entry) {
        if (pauseRequested.get() && !stopRequested.get() && entry != null) {
            addEntry(entry);
        }
    }

    private void signalCoordinator() {
        coordinationLock.lock();
        try {
            coordinationCondition.signalAll();
        } finally {
            coordinationLock.unlock();
        }
    }

    private void ensurePageLoaderConfigured() {
        if (pageLoader == null) {
            throw new IllegalStateException("PageLoader must be configured before executing crawl");
        }
    }

    private static ContentFilter buildFilterChain(Set<FilterType> enabledFilters) {
        if (enabledFilters == null || enabledFilters.isEmpty()) {
            return null;
        }
        ContentFilter head = null;
        ContentFilter tail = null;
        for (FilterType type : FilterType.values()) {
            if (!enabledFilters.contains(type)) {
                continue;
            }
            ContentFilter filter = createFilter(type);
            if (head == null) {
                head = filter;
                tail = filter;
            } else {
                tail.setNext(filter);
                tail = filter;
            }
        }
        return head;
    }

    private static ContentFilter createFilter(FilterType type) {
        return switch (type) {
            case SCRIPT -> new ScriptFilter();
            case STYLE -> new StyleFilter();
            case EMBEDDED -> new EmbeddedObjectFilter();
            case AD -> new AdFilter();
            case NAVIGATION -> new NavigationFilter();
        };
    }

    private static void absolutizeDocumentUrls(Document document) {
        for (Element element : document.select("[href]")) {
            if (!element.attr("href").isBlank()) {
                String absHref = element.attr("abs:href");
                if (!absHref.isEmpty()) {
                    element.attr("href", absHref);
                }
            }
        }
        for (Element element : document.select("[src]")) {
            if (!element.attr("src").isBlank()) {
                String absSrc = element.attr("abs:src");
                if (!absSrc.isEmpty()) {
                    element.attr("src", absSrc);
                }
            }
        }
    }

    private static String normalizeUrlOrFallback(String url) {
        return UrlNormalizer.tryNormalize(url).orElse(url);
    }

    private static ExecutorService createWorkerPool(int poolSize, String namePrefix) {
        AtomicInteger threadNumber = new AtomicInteger(1);
        return Executors.newFixedThreadPool(poolSize, runnable -> {
            Thread thread = new Thread(runnable, namePrefix + threadNumber.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
    }

    private static void shutdownExecutorGracefully(ExecutorService executor, boolean forceNow) {
        if (forceNow) {
            executor.shutdownNow();
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                executor.awaitTermination(5L, TimeUnit.SECONDS);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private static final class CrawlCoordinator {
        private int reservedCount;
        private int inFlight;

        private CrawlCoordinator(int initialReservedCount) {
            this.reservedCount = initialReservedCount;
        }
    }

    private enum OutcomeStatus {
        PROCESSED,
        ERROR,
        ROBOTS_DISALLOWED,
        ABORTED_BEFORE_FETCH
    }

    private record EntryOutcome(
            OutcomeStatus status,
            Page page,
            List<QueueEntry> discoveredEntries
    ) {
        static EntryOutcome processed(Page page, List<QueueEntry> discoveredEntries) {
            return new EntryOutcome(OutcomeStatus.PROCESSED, page, List.copyOf(discoveredEntries));
        }

        static EntryOutcome error(Page page) {
            return new EntryOutcome(OutcomeStatus.ERROR, page, List.of());
        }

        static EntryOutcome robotsDisallowed() {
            return new EntryOutcome(OutcomeStatus.ROBOTS_DISALLOWED, null, List.of());
        }

        static EntryOutcome abortedBeforeFetch() {
            return new EntryOutcome(OutcomeStatus.ABORTED_BEFORE_FETCH, null, List.of());
        }
    }
}
