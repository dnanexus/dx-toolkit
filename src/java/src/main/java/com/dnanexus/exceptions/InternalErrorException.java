package com.dnanexus.exceptions;

/**
 * Exception which indicates a 500 status code Internal Server Error.
 */
@SuppressWarnings("serial")
public class InternalErrorException extends DXAPIException {
    /**
     * Creates an {@code InternalErrorException} with the specified
     * message and HTTP status code.
     */
    public InternalErrorException(String message, int statusCode) {
        super(message, statusCode);
    }
}
