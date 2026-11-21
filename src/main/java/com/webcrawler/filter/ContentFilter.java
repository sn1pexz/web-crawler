package com.webcrawler.filter;

import org.jsoup.nodes.Document;

//Chain of Responsibility base handler for HTML filtering
public abstract class ContentFilter {

    public static final int MIN_TEXT_LENGTH = 100;

    private ContentFilter next;

    public void setNext(ContentFilter next) {
        this.next = next;
    }

    public ContentFilter getNext() {
        return next;
    }

    public abstract boolean process(Document document);

    protected boolean passToNext(Document document) {
        if (document == null) {
            return false;
        }
        if (next != null) {
            return next.process(document);
        }
        return hasSufficientText(document);
    }

    public static boolean hasSufficientText(Document document) {
        if (document == null) {
            return false;
        }
        String visibleText = document.body() != null ? document.body().text() : document.text();
        return visibleText.trim().length() >= MIN_TEXT_LENGTH;
    }
}
