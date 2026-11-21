package com.webcrawler.loader;

public class PageLoadException extends Exception {

    private final int httpStatus;
    private final boolean retryable;

    public PageLoadException(String message) {
        this(message, 0, false);
    }

    public PageLoadException(String message, Throwable cause) {
        this(message, 0, false, cause);
    }

    public PageLoadException(String message, int httpStatus, boolean retryable) {
        super(message);
        this.httpStatus = httpStatus;
        this.retryable = retryable;
    }

    public PageLoadException(String message, int httpStatus, boolean retryable, Throwable cause) {
        super(message, cause);
        this.httpStatus = httpStatus;
        this.retryable = retryable;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
