package com.webcrawler.loader;

public interface PageLoader {

    LoadResult loadWithMetadata(String url) throws PageLoadException;

    default String load(String url) throws PageLoadException {
        return loadWithMetadata(url).html();
    }

    LoadResult loadRobotsTxt(String hostRootUrl) throws PageLoadException;
}
