package com.dnanexus.exceptions;

/**
 * Exception used to indicate that the request yielded 503 Service Unavailable and suggested
 * that we retry at some point in the future.
 */
@SuppressWarnings("serial")
public class ServiceUnavailableException extends DXAPIException {

    /**
     * Creates a {@code ServiceUnavailableException} with the specified
     * message and HTTP status code.
     */
    public ServiceUnavailableException(String message, int statusCode) {
        this(message, statusCode, 60);
    }

    public ServiceUnavailableException(String message, int statusCode, int retryAfterSeconds) {
        super(message, statusCode);
        secondsToWaitForRetry = retryAfterSeconds;
    }

    private final int secondsToWaitForRetry;

    public int getSecondsToWaitForRetry() {
        return secondsToWaitForRetry;
    }
}
