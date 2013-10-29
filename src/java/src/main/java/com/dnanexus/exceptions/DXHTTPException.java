package com.dnanexus.exceptions;

/**
 * Represents an error encountered while making an HTTP request or parsing its
 * results.
 */
public class DXHTTPException extends RuntimeException {

    /**
     * Initializes a new {@code DXHTTPException} with the specified cause.
     *
     * @param cause
     *            Immediate cause of this exception.
     */
    public DXHTTPException(Throwable cause) {
        super(cause);
    }

    private static final long serialVersionUID = -6944363469302926283L;

}
