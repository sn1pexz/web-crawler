package com.webcrawler.storage;

import java.io.IOException;

//Composite base component for site storage tree
public abstract class SiteElement {

    private final String name;

    protected SiteElement(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Element name must not be blank");
        }
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public abstract void write(String rootPath) throws IOException;
}
