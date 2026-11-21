package com.webcrawler.model;

import com.webcrawler.storage.DirectoryElement;
import com.webcrawler.storage.PageElement;
import com.webcrawler.storage.PathSanitizer;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Site {

    private long id;
    private final String host;
    private final List<Page> pages = new ArrayList<>();
    private DirectoryElement rootDirectory;

    public Site(String host) {
        this(0L, host);
    }

    public Site(long id, String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        this.id = id;
        this.host = host;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public synchronized List<Page> getPages() {
        return List.copyOf(pages);
    }

    public synchronized void addPage(Page page) {
        if (page != null && !pages.contains(page)) {
            pages.add(page);
        }
    }

    public synchronized DirectoryElement getRootDirectory() {
        return rootDirectory;
    }

    public synchronized PageElement registerMatchedPage(
            Page page,
            String cleanedHtml,
            List<String> dirSegments,
            String fileName) {
        Objects.requireNonNull(page, "page must not be null");
        Objects.requireNonNull(cleanedHtml, "cleanedHtml must not be null");
        if (fileName == null || fileName.isBlank()) {
            throw new IllegalArgumentException("fileName must not be blank");
        }
        addPage(page);
        if (rootDirectory == null) {
            rootDirectory = new DirectoryElement(PathSanitizer.sanitizeHost(host));
        }
        DirectoryElement current = rootDirectory;
        if (dirSegments != null) {
            for (String segment : dirSegments) {
                current = current.getOrCreateSubdirectory(segment);
            }
        }
        PageElement leaf = new PageElement(fileName, page, cleanedHtml);
        current.add(leaf);
        return leaf;
    }

    public Path resolveParentPath(Path resultsRoot, List<String> dirSegments) {
        Objects.requireNonNull(resultsRoot, "resultsRoot must not be null");
        Path current = resultsRoot.resolve(PathSanitizer.sanitizeHost(host));
        if (dirSegments != null) {
            for (String segment : dirSegments) {
                current = current.resolve(segment);
            }
        }
        return current;
    }

    @Override
    public String toString() {
        return "Site{id=" + id + ", host='" + host + "'}";
    }
}
