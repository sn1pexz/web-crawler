package com.webcrawler.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DirectoryElement extends SiteElement {

    private final List<SiteElement> children = new ArrayList<>();

    public DirectoryElement(String name) {
        super(name);
    }

    public synchronized void add(SiteElement element) {
        Objects.requireNonNull(element, "element must not be null");
        if (element == this) {
            throw new IllegalArgumentException("Directory cannot contain itself");
        }
        children.add(element);
    }

    //Return immutable copy to avoid concurrent modification
    public synchronized List<SiteElement> getChildren() {
        return List.copyOf(children);
    }

    public synchronized DirectoryElement getOrCreateSubdirectory(String dirName) {
        if (dirName == null || dirName.isBlank()) {
            throw new IllegalArgumentException("dirName must not be blank");
        }
        for (SiteElement child : children) {
            if (child instanceof DirectoryElement dir && dir.getName().equals(dirName)) {
                return dir;
            }
        }
        DirectoryElement created = new DirectoryElement(dirName);
        children.add(created);
        return created;
    }

    @Override
    public void write(String rootPath) throws IOException {
        Objects.requireNonNull(rootPath, "rootPath must not be null");
        Path dirPath = Path.of(rootPath).resolve(getName());
        Files.createDirectories(dirPath);
        List<SiteElement> snapshot;
        synchronized (this) {
            snapshot = List.copyOf(children);
        }
        for (SiteElement child : snapshot) {
            child.write(dirPath.toString());
        }
    }
}
