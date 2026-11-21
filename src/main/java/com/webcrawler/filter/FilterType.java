package com.webcrawler.filter;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public enum FilterType {
    SCRIPT,
    STYLE,
    EMBEDDED,
    AD,
    NAVIGATION;

    private static final Set<FilterType> DEFAULT_ENABLED =
            Collections.unmodifiableSet(EnumSet.of(SCRIPT, STYLE, EMBEDDED, AD));

    public static Set<FilterType> defaultEnabled() {
        return DEFAULT_ENABLED;
    }
}
