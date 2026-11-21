package com.webcrawler.storage;

import com.webcrawler.model.Page;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class PageElement extends SiteElement {

    private final Page page;
    private final String cleanedHtml;

    public PageElement(String name, Page page, String cleanedHtml) {
        super(name);
        this.page = Objects.requireNonNull(page, "page must not be null");
        this.cleanedHtml = Objects.requireNonNull(cleanedHtml, "cleanedHtml must not be null");
    }

    public Page getPage() {
        return page;
    }

    public String getCleanedHtml() {
        return cleanedHtml;
    }

    @Override
    public void write(String rootPath) throws IOException {
        Objects.requireNonNull(rootPath, "rootPath must not be null");
        Path parentDir = Path.of(rootPath);
        Files.createDirectories(parentDir);
        Path targetFile = parentDir.resolve(getName());
        Files.writeString(targetFile, cleanedHtml, StandardCharsets.UTF_8);
    }
}
